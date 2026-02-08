"""
FINAL Name Consolidation V2 - Second pass
Cleans up remaining duplicates from F2 (reversed names, partial matches, etc.)
Input: FINAL_3_unique_names with canonical_name [F2]
Output: Updates canonical_name [F2] with merged results
"""

import sqlite3
import json
import asyncio
import aiohttp
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from collections import defaultdict
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
API_KEY = os.getenv("ANTHROPIC_KEY")
DB_PATH = "../../epstein_analysis.db"
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"

RUN_ID = "F2b"
RUN_NAME = "name_consolidation_v2_final"
INPUT_TABLE = "FINAL_3_unique_names"

PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """You are doing a SECOND PASS of name consolidation. The first pass already grouped obvious matches, but some duplicates remain.

Here are the CURRENT canonical names (after first pass). Find duplicates that should be merged:

CANONICAL NAMES TO REVIEW:
{names}

LOOK FOR:
1. Reversed names: "Landon Thomas" vs "Thomas Jr. Landon" → merge to "Landon Thomas Jr."
2. With/without middle initials: "John A. Smith" vs "John Smith" → merge to "John A. Smith"
3. With/without titles: "Dr. John Smith" vs "John Smith" → merge to "John Smith"
4. Nicknames that slipped through: "Bill Gates" vs "William Gates" → merge to "Bill Gates"
5. Epstein variants that remain: "E. Jeffrey", "Jeffrey E." → should be "None"
6. Partial matches: "Maxwell" vs "Ghislaine Maxwell" → merge to "Ghislaine Maxwell"

IMPORTANT:
- Only merge when you're CONFIDENT they're the same person
- When merging, pick the most complete/formal name as canonical
- Return ONLY names that need changes (not all names)

Return JSON mapping OLD canonical name -> NEW canonical name:
{{"Old Name": "New Canonical Name", "E. Jeffrey": "None", ...}}

Return empty {{}} if no changes needed."""


def get_connection():
    return sqlite3.connect(DB_PATH)


def get_canonical_names():
    """Get all unique canonical names from F2"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT DISTINCT "canonical_name [F2]"
        FROM "{INPUT_TABLE}"
        WHERE "canonical_name [F2]" IS NOT NULL
          AND "canonical_name [F2]" != 'None'
    """
    )
    names = [row[0] for row in cursor.fetchall()]
    conn.close()
    return names


async def call_sonnet(session, names_batch):
    """Call API with a batch of names"""
    prompt = PROMPT_TEMPLATE.format(names=json.dumps(names_batch, indent=2))

    headers = {
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": MODEL,
        "max_tokens": 8000,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        async with session.post(API_URL, json=payload, headers=headers) as response:
            response_text = await response.text()
            print(f"  Response status: {response.status}, length: {len(response_text)}")
            if not response_text:
                print(f"Empty response, status: {response.status}")
                return {}, 0, 0
            if response.status != 200:
                print(f"  Response: {response_text[:300]}")
                return {}, 0, 0
            result = json.loads(response_text)

            if "error" in result:
                error_msg = result.get("error", {}).get("message", "")
                print(f"API Error: {error_msg[:100]}...")
                if "rate" in error_msg.lower() or "overloaded" in error_msg.lower():
                    print("  Waiting 30s for rate limit...")
                    await asyncio.sleep(30)
                return {}, 0, 0

            if "content" not in result:
                print(f"No content in response: {str(result)[:200]}")
                return {}, 0, 0

            text = result["content"][0]["text"]
            usage = result.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)

            text = text.strip()
            print(f"  Model response (first 200 chars): {text[:200]}")

            # Try to extract JSON from response
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]

            # Find JSON object in text
            start_idx = text.find("{")
            end_idx = text.rfind("}") + 1
            if start_idx >= 0 and end_idx > start_idx:
                text = text[start_idx:end_idx]

            parsed = json.loads(text)
            return parsed, input_tokens, output_tokens

    except Exception as e:
        print(f"Error: {e}")
        return {}, 0, 0


async def run_consolidation():
    if not API_KEY:
        print("ERROR: ANTHROPIC_KEY not found in environment")
        return {}, 0
    print(f"API Key loaded: {API_KEY[:10]}...")
    print(f"Loading canonical names from {INPUT_TABLE}...")
    canonical_names = get_canonical_names()
    print(f"Found {len(canonical_names)} unique canonical names")

    print(f"\nModel: {MODEL}")
    print(f"Run ID: {RUN_ID}")

    # Process in batches of 300 names
    BATCH_SIZE = 300
    batches = [
        canonical_names[i : i + BATCH_SIZE]
        for i in range(0, len(canonical_names), BATCH_SIZE)
    ]
    print(f"Processing in {len(batches)} batches of {BATCH_SIZE} names each")

    all_changes = {}
    total_input_tokens = 0
    total_output_tokens = 0

    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i, batch in enumerate(batches):
            print(f"\nBatch {i+1}/{len(batches)} ({len(batch)} names)...")
            changes, input_tokens, output_tokens = await call_sonnet(session, batch)

            if changes:
                all_changes.update(changes)
                print(f"  Found {len(changes)} names to update")
            else:
                print(f"  No changes needed")

            total_input_tokens += input_tokens
            total_output_tokens += output_tokens

    print(f"\n\nTotal changes to apply: {len(all_changes)}")

    if all_changes:
        print("\nChanges preview:")
        for old, new in list(all_changes.items())[:20]:
            print(f"  {old} -> {new}")
        if len(all_changes) > 20:
            print(f"  ... and {len(all_changes) - 20} more")

    # Apply changes to database
    conn = get_connection()
    cursor = conn.cursor()

    updated_count = 0
    for old_name, new_name in tqdm(all_changes.items(), desc="Applying changes"):
        cursor.execute(
            f"""
            UPDATE "{INPUT_TABLE}"
            SET "canonical_name [F2]" = ?
            WHERE "canonical_name [F2]" = ?
        """,
            (new_name, old_name),
        )
        updated_count += cursor.rowcount

    # Calculate cost
    total_cost = (total_input_tokens * PRICE_INPUT) + (
        total_output_tokens * PRICE_OUTPUT
    )

    # Register this run
    cursor.execute(
        """
        INSERT OR REPLACE INTO ai_classification_runs
        (run_id, run_name, run_type, model_used, prompt_used, input_columns, output_columns, total_cost, script_path, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            RUN_ID,
            RUN_NAME,
            "consolidation",
            MODEL,
            PROMPT_TEMPLATE,
            json.dumps([f"{INPUT_TABLE}.canonical_name [F2]"]),
            json.dumps([f"{INPUT_TABLE}.canonical_name [F2]"]),
            total_cost,
            "scripts/final/run_name_consolidation_v2.py",
            "FINAL: Second pass consolidation - catches reversed names, remaining duplicates",
        ),
    )

    conn.commit()
    conn.close()

    # Export
    export_path = Path("../../data/classification") / f"{RUN_ID}_{RUN_NAME}.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with open(export_path, "w") as f:
        json.dump(
            {
                "run_id": RUN_ID,
                "model": MODEL,
                "timestamp": datetime.now().isoformat(),
                "changes_applied": len(all_changes),
                "rows_updated": updated_count,
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "cost": total_cost,
                "changes": all_changes,
            },
            f,
            indent=2,
        )

    print(f"\nDone!")
    print(f"Changes applied: {len(all_changes)}")
    print(f"Rows updated: {updated_count}")
    print(f"Total tokens: {total_input_tokens:,} input, {total_output_tokens:,} output")
    print(f"Total cost: ${total_cost:.4f}")

    return all_changes, total_cost


if __name__ == "__main__":
    changes, cost = asyncio.run(run_consolidation())
