"""
FINAL Name Consolidation V4 - Fourth pass (suffix matching)
Catches names differing only by suffix: "Landon Thomas" vs "Landon Thomas Jr."
Input: FINAL_3_unique_names with canonical_name [F2c]
Output: Adds canonical_name [F2d] column
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
import re

# Load environment
load_dotenv()
API_KEY = os.getenv("ANTHROPIC_KEY")
DB_PATH = "../../epstein_analysis.db"
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"

RUN_ID = "F2d"
RUN_NAME = "name_consolidation_v4_final"
INPUT_TABLE = "FINAL_3_unique_names"

PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """You are doing a FOURTH PASS of name consolidation, specifically looking for names that differ only by SUFFIX (Jr., Sr., III, etc.) or slight variations.

These names may be the same person with/without suffix:

POTENTIAL DUPLICATES:
{groups}

For each group:
1. If they ARE the same person, merge to the MORE COMPLETE name (with suffix if applicable)
2. If they are DIFFERENT people (e.g., father and son), keep them separate
3. Use context clues: in a corporate/professional email context, "Landon Thomas" and "Landon Thomas Jr." are likely the same person

Return JSON mapping OLD name -> NEW canonical name for names that should change:
{{"Landon Thomas": "Landon Thomas Jr."}}

Return empty {{}} if no changes needed."""


def get_connection():
    return sqlite3.connect(DB_PATH)


def get_canonical_names():
    """Get all unique canonical names from F2c"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT DISTINCT "canonical_name [F2c]"
        FROM "{INPUT_TABLE}"
        WHERE "canonical_name [F2c]" IS NOT NULL
          AND "canonical_name [F2c]" != 'None'
    ''')
    names = [row[0] for row in cursor.fetchall()]
    conn.close()
    return names


def normalize_name(name):
    """Remove suffixes and normalize for comparison"""
    name = name.lower().strip()
    # Remove common suffixes
    suffixes = [' jr.', ' jr', ' sr.', ' sr', ' iii', ' ii', ' iv', ' esq.', ' esq', ' phd', ' md']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    return name.strip()


def find_suffix_duplicates(names):
    """Find names that differ only by suffix"""
    # Group by normalized name
    normalized_groups = defaultdict(list)
    for name in names:
        norm = normalize_name(name)
        normalized_groups[norm].append(name)

    # Return groups with 2+ names (potential duplicates)
    return [group for group in normalized_groups.values() if len(group) >= 2]


async def call_sonnet(session, groups_batch):
    """Call API with a batch of potential duplicate groups"""
    groups_text = ""
    for i, group in enumerate(groups_batch, 1):
        groups_text += f"\nGroup {i}:\n"
        for name in sorted(group):
            groups_text += f"  - {name}\n"

    prompt = PROMPT_TEMPLATE.format(groups=groups_text)

    headers = {
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": MODEL,
        "max_tokens": 4000,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        async with session.post(API_URL, json=payload, headers=headers) as response:
            response_text = await response.text()
            if not response_text:
                return {}, 0, 0

            result = json.loads(response_text)

            if "error" in result:
                error_msg = result.get("error", {}).get("message", "")
                print(f"API Error: {error_msg[:100]}...")
                return {}, 0, 0

            if "content" not in result:
                return {}, 0, 0

            text = result["content"][0]["text"]
            usage = result.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)

            text = text.strip()

            # Find JSON in response
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

    print(f"Loading canonical names from {INPUT_TABLE}...")
    canonical_names = get_canonical_names()
    print(f"Found {len(canonical_names)} unique canonical names")

    print(f"\nFinding names differing by suffix...")
    groups = find_suffix_duplicates(canonical_names)
    print(f"Found {len(groups)} groups of potential suffix duplicates")

    if not groups:
        print("No potential duplicates found!")
        return {}, 0

    # Show all groups (should be small)
    print("\nGroups found:")
    for i, group in enumerate(groups):
        print(f"  Group {i+1}: {group}")

    print(f"\nModel: {MODEL}")
    print(f"Run ID: {RUN_ID}")

    # Process all groups in one batch (should be small)
    all_changes = {}
    total_input_tokens = 0
    total_output_tokens = 0

    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print("\nCalling API...")
        changes, input_tokens, output_tokens = await call_sonnet(session, groups)

        if changes:
            all_changes.update(changes)

        total_input_tokens += input_tokens
        total_output_tokens += output_tokens

    print(f"\n\nTotal changes to apply: {len(all_changes)}")

    if all_changes:
        print("\nChanges:")
        for old, new in all_changes.items():
            print(f"  {old} -> {new}")

    # Apply changes to database - create new column F2d
    conn = get_connection()
    cursor = conn.cursor()

    # Add new column for F2d results
    try:
        cursor.execute(f'ALTER TABLE "{INPUT_TABLE}" ADD COLUMN "canonical_name [F2d]" TEXT')
        print("\nAdded column 'canonical_name [F2d]'")
    except:
        print("\nColumn 'canonical_name [F2d]' already exists")

    # First, copy F2c values to F2d
    cursor.execute(f'''
        UPDATE "{INPUT_TABLE}"
        SET "canonical_name [F2d]" = "canonical_name [F2c]"
        WHERE "canonical_name [F2d]" IS NULL
    ''')
    print(f"Copied F2c values to F2d column")

    # Then apply the new changes to F2d
    updated_count = 0
    for old_name, new_name in all_changes.items():
        cursor.execute(f'''
            UPDATE "{INPUT_TABLE}"
            SET "canonical_name [F2d]" = ?
            WHERE "canonical_name [F2d]" = ?
        ''', (new_name, old_name))
        updated_count += cursor.rowcount

    # Calculate cost
    total_cost = (total_input_tokens * PRICE_INPUT) + (total_output_tokens * PRICE_OUTPUT)

    # Register this run
    cursor.execute("""
        INSERT OR REPLACE INTO ai_classification_runs
        (run_id, run_name, run_type, model_used, prompt_used, input_columns, output_columns, total_cost, script_path, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        RUN_ID,
        RUN_NAME,
        "consolidation",
        MODEL,
        PROMPT_TEMPLATE,
        json.dumps([f"{INPUT_TABLE}.canonical_name [F2c]"]),
        json.dumps([f"{INPUT_TABLE}.canonical_name [F2d]"]),
        total_cost,
        "scripts/final/run_name_consolidation_v4.py",
        "FINAL: Fourth pass - suffix variations (Jr., Sr., etc.)"
    ))

    conn.commit()
    conn.close()

    # Export
    export_path = Path("../../data/classification") / f"{RUN_ID}_{RUN_NAME}.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with open(export_path, "w") as f:
        json.dump({
            "run_id": RUN_ID,
            "model": MODEL,
            "timestamp": datetime.now().isoformat(),
            "potential_duplicate_groups": len(groups),
            "changes_applied": len(all_changes),
            "rows_updated": updated_count,
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "cost": total_cost,
            "changes": all_changes
        }, f, indent=2)

    print(f"\nDone!")
    print(f"Potential duplicate groups analyzed: {len(groups)}")
    print(f"Changes applied: {len(all_changes)}")
    print(f"Rows updated: {updated_count}")
    print(f"Total tokens: {total_input_tokens:,} input, {total_output_tokens:,} output")
    print(f"Total cost: ${total_cost:.4f}")

    return all_changes, total_cost


if __name__ == "__main__":
    changes, cost = asyncio.run(run_consolidation())
