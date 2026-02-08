"""
Name Extraction v2 - Classifier C6
Extracts person names from email discussions using Claude Sonnet 3.5
"""

import sqlite3
import json
import asyncio
import aiohttp
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment
load_dotenv()
API_KEY = os.getenv("ANTHROPIC_KEY")
DB_PATH = "epstein_analysis.db"
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"

# Classifier config
RUN_ID = "C6"
RUN_NAME = "name_extraction_v2"
# Optimal settings
BATCH_SIZE = 50
MAX_CONCURRENT = 5
RETRY_DELAY = 15  # seconds to wait on rate limit
INPUT_TABLE = "SAMPLE_1_discussion_messages"
OUTPUT_TABLE = "SAMPLE_2_CLASSIFIER_name_extraction"

# Pricing (per 1M tokens) - Claude Sonnet
PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """Extract person names from emails. Return JSON only: {{"thread_id": ["Name1"], ...}}
No emails/orgs, only person names. Empty array if none.

{emails}"""


def get_connection():
    return sqlite3.connect(DB_PATH)


def setup_tables():
    """Create output table and register classifier"""
    conn = get_connection()
    cursor = conn.cursor()

    # Create output table
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{OUTPUT_TABLE}" (
            thread_id TEXT PRIMARY KEY,
            sender TEXT,
            receiver TEXT,
            cc TEXT,
            body TEXT,
            "names_mentioned [C6]" TEXT,
            "mention_count [C6]" INTEGER
        )
    """
    )

    # Register classifier (if not exists)
    cursor.execute(
        """
        INSERT OR REPLACE INTO ai_classification_runs
        (run_id, run_name, run_type, model_used, prompt_used, input_columns, output_columns, total_cost, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            RUN_ID,
            RUN_NAME,
            "extraction",
            MODEL,
            PROMPT_TEMPLATE,
            json.dumps(
                [
                    f"{OUTPUT_TABLE}.thread_id",
                    f"{OUTPUT_TABLE}.sender",
                    f"{OUTPUT_TABLE}.body",
                ]
            ),
            json.dumps(
                [
                    f"{OUTPUT_TABLE}.names_mentioned [C6]",
                    f"{OUTPUT_TABLE}.mention_count [C6]",
                ]
            ),
            0.0,
            "Extracts person names from email body using Claude Sonnet 3.5",
        ),
    )

    conn.commit()
    conn.close()


def load_data():
    """Load sample discussion messages"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        f'SELECT id, thread_id, sender, receiver, cc, body FROM "{INPUT_TABLE}"'
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


async def call_sonnet(session, batch, semaphore):
    """Call Claude Sonnet API with retry on rate limit"""
    async with semaphore:
        emails_text = "\n---\n".join(
            [f"ID:{row[1]}\n{(row[5] or '')[:1500]}" for row in batch]
        )
        prompt = PROMPT_TEMPLATE.format(emails=emails_text)

        headers = {
            "x-api-key": API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": MODEL,
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}],
        }

        for attempt in range(5):  # Max 5 retries
            try:
                async with session.post(API_URL, json=payload, headers=headers) as response:
                    result = await response.json()

                    if "error" in result:
                        error_msg = result.get("error", {}).get("message", "")
                        if "rate_limit" in error_msg.lower():
                            await asyncio.sleep(RETRY_DELAY)
                            continue
                        tqdm.write(f"API Error: {error_msg[:60]}...")
                        return batch, None, 0, 0

                    if "content" not in result:
                        return batch, None, 0, 0

                    text = result["content"][0]["text"]
                    usage = result.get("usage", {})
                    input_tokens = usage.get("input_tokens", 0)
                    output_tokens = usage.get("output_tokens", 0)

                    text = text.strip()
                    if text.startswith("```"):
                        text = text.split("\n", 1)[1].rsplit("```", 1)[0]

                    parsed = json.loads(text)
                    return batch, parsed, input_tokens, output_tokens

            except Exception as e:
                if attempt < 4:
                    await asyncio.sleep(5)
                    continue
                return batch, None, 0, 0

        return batch, None, 0, 0


async def run_classification():
    """Main classification loop with batching and concurrency"""
    setup_tables()
    rows = load_data()

    print(f"Processing {len(rows)} emails in batches of {BATCH_SIZE} with {MAX_CONCURRENT} concurrent requests")
    print(f"Model: {MODEL}")

    # Create batches
    batches = [rows[i : i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]

    total_input_tokens = 0
    total_output_tokens = 0
    results = {}

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    # High-performance connection pool
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT,
        limit_per_host=MAX_CONCURRENT,
        keepalive_timeout=60,
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [call_sonnet(session, batch, semaphore) for batch in batches]

        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="API calls"):
            try:
                res = await coro
                _, parsed, input_tokens, output_tokens = res
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                if parsed:
                    results.update(parsed)
            except Exception as e:
                tqdm.write(f"Batch error: {e}")

    # Save results to database
    conn = get_connection()
    cursor = conn.cursor()

    for row in tqdm(rows, desc="Saving to DB"):
        thread_id = row[1]
        names = results.get(thread_id, [])
        if isinstance(names, list):
            names_json = json.dumps(names)
            count = len(names)
        else:
            names_json = "[]"
            count = 0

        cursor.execute(
            f"""
            INSERT OR REPLACE INTO "{OUTPUT_TABLE}"
            (thread_id, sender, receiver, cc, body, "names_mentioned [C6]", "mention_count [C6]")
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (row[1], row[2], row[3], row[4], row[5], names_json, count),
        )

    # Calculate cost
    total_cost = (total_input_tokens * PRICE_INPUT) + (
        total_output_tokens * PRICE_OUTPUT
    )

    # Update cost in registry
    cursor.execute(
        """
        UPDATE ai_classification_runs SET total_cost = ? WHERE run_id = ?
    """,
        (total_cost, RUN_ID),
    )

    conn.commit()
    conn.close()

    # Export to file
    export_data = {
        "run_id": RUN_ID,
        "run_name": RUN_NAME,
        "model": MODEL,
        "timestamp": datetime.now().isoformat(),
        "total_rows": len(rows),
        "total_cost": total_cost,
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "results": results,
    }

    export_path = Path("data/classification") / f"{RUN_ID}_{RUN_NAME}.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with open(export_path, "w") as f:
        json.dump(export_data, f, indent=2)

    print(f"\nDone!")
    print(f"Total tokens: {total_input_tokens:,} input, {total_output_tokens:,} output")
    print(f"Total cost: ${total_cost:.4f}")
    print(f"Results saved to: {export_path}")

    return results, total_cost


if __name__ == "__main__":
    results, cost = asyncio.run(run_classification())

    # Show sample results
    print("\n--- Sample Results ---")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT thread_id, sender, "names_mentioned [C6]", "mention_count [C6]"
        FROM "{OUTPUT_TABLE}"
        WHERE "mention_count [C6]" > 0
        ORDER BY RANDOM()
        LIMIT 5
    """
    )
    for row in cursor.fetchall():
        print(f"\nThread: {row[0][:50]}...")
        print(f"Sender: {row[1]}")
        print(f"Names: {row[2]}")
        print(f"Count: {row[3]}")
    conn.close()
