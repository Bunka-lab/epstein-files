"""
Name Extraction - Gemini 2.5 Flash Speed Test
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

# Config - aggressive for speed
RUN_ID = "C7"
RUN_NAME = "name_extraction_all_fields"
BATCH_SIZE = 50
MAX_CONCURRENT = 10
RETRY_DELAY = 5
INPUT_TABLE = "SAMPLE_1_discussion_messages"
OUTPUT_TABLE = "SAMPLE_2_CLASSIFIER_name_extraction"

# Pricing (per 1M tokens) - Claude Sonnet
PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """Extract person names from emails (from sender, receiver, CC, and body). Return JSON only: {{"thread_id": ["Name1"], ...}}
No email addresses or organizations, only person names. Empty array if none found.

{emails}"""


def get_connection():
    return sqlite3.connect(DB_PATH)


def load_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        f'SELECT id, thread_id, sender, receiver, cc, body FROM "{INPUT_TABLE}"'
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


async def call_api(session, batch, semaphore):
    """Call Claude Sonnet API with retry"""
    async with semaphore:
        # Format: ID, Sender, Receiver, CC, Body
        emails_text = "\n---\n".join([
            f"ID:{row[1]}\nFrom:{row[2] or ''}\nTo:{row[3] or ''}\nCC:{row[4] or ''}\nBody:{(row[5] or '')[:1200]}"
            for row in batch
        ])
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

        for attempt in range(5):
            try:
                async with session.post(API_URL, json=payload, headers=headers) as response:
                    result = await response.json()

                    if "error" in result:
                        error_msg = result.get("error", {}).get("message", "")
                        if "rate" in error_msg.lower():
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
                    await asyncio.sleep(2)
                    continue
                return batch, None, 0, 0

        return batch, None, 0, 0


async def run_classification():
    rows = load_data()

    print(f"Processing {len(rows)} emails in batches of {BATCH_SIZE} with {MAX_CONCURRENT} concurrent requests")
    print(f"Model: {MODEL}")

    batches = [rows[i : i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]

    total_input_tokens = 0
    total_output_tokens = 0
    results = {}

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT,
        limit_per_host=MAX_CONCURRENT,
        keepalive_timeout=60,
    )
    timeout = aiohttp.ClientTimeout(total=120)

    start_time = datetime.now()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [call_api(session, batch, semaphore) for batch in batches]

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

    elapsed = (datetime.now() - start_time).total_seconds()

    # Calculate cost
    total_cost = (total_input_tokens * PRICE_INPUT) + (total_output_tokens * PRICE_OUTPUT)

    # Save to database
    conn = get_connection()
    cursor = conn.cursor()

    # Create/replace table
    cursor.execute(f'DROP TABLE IF EXISTS "{OUTPUT_TABLE}"')
    cursor.execute(f"""
        CREATE TABLE "{OUTPUT_TABLE}" (
            thread_id TEXT PRIMARY KEY,
            sender TEXT,
            receiver TEXT,
            cc TEXT,
            body TEXT,
            "names_mentioned [{RUN_ID}]" TEXT,
            "mention_count [{RUN_ID}]" INTEGER
        )
    """)

    for row in tqdm(rows, desc="Saving to DB"):
        thread_id = row[1]
        names = results.get(thread_id, [])
        if isinstance(names, list):
            names_json = json.dumps(names)
            count = len(names)
        else:
            names_json = "[]"
            count = 0

        cursor.execute(f"""
            INSERT OR REPLACE INTO "{OUTPUT_TABLE}"
            (thread_id, sender, receiver, cc, body, "names_mentioned [{RUN_ID}]", "mention_count [{RUN_ID}]")
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (row[1], row[2], row[3], row[4], row[5], names_json, count))

    conn.commit()
    conn.close()

    print(f"\nDone!")
    print(f"Time: {elapsed:.1f} seconds")
    print(f"Speed: {len(rows) / elapsed:.2f} emails/second")
    print(f"Total tokens: {total_input_tokens:,} input, {total_output_tokens:,} output")
    print(f"Total cost: ${total_cost:.4f}")
    print(f"Results saved to: {OUTPUT_TABLE}")

    return results, total_cost, elapsed


if __name__ == "__main__":
    results, cost, elapsed = asyncio.run(run_classification())

    print(f"\n--- Sample Results ({len(results)} extracted) ---")
    for i, (thread_id, names) in enumerate(list(results.items())[:5]):
        print(f"{thread_id[:50]}... → {names}")
