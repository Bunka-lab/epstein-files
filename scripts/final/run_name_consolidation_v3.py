"""
FINAL Name Consolidation V3 - Third pass (token-based matching)
Catches reversed names like "Thomas Jr. Landon" vs "Landon Thomas"
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
import re

# Load environment
load_dotenv()
API_KEY = os.getenv("ANTHROPIC_KEY")
DB_PATH = "../../epstein_analysis.db"
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"

RUN_ID = "F2c"
RUN_NAME = "name_consolidation_v3_final"
INPUT_TABLE = "FINAL_3_unique_names"

PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """You are doing a THIRD PASS of name consolidation, specifically looking for REVERSED or REORDERED names.

These names share common tokens and may be the same person with different name ordering:

POTENTIAL DUPLICATES (share 2+ name tokens):
{groups}

For each group, determine if they are the same person. If yes, pick the BEST canonical form:
- Prefer "First Last" over "Last First"
- Prefer full names over partial
- Include Jr./Sr./III if present
- If NOT the same person, don't merge them

Return JSON mapping OLD name -> NEW canonical name for names that should change:
{{"Old Name": "New Canonical Name"}}

Return empty {{}} if no changes needed in this group.
Only include names that need to CHANGE (don't include names staying the same)."""


def get_connection():
    return sqlite3.connect(DB_PATH)


def get_canonical_names():
    """Get all unique canonical names from F2"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT DISTINCT "canonical_name [F2]"
        FROM "{INPUT_TABLE}"
        WHERE "canonical_name [F2]" IS NOT NULL
          AND "canonical_name [F2]" != 'None'
    ''')
    names = [row[0] for row in cursor.fetchall()]
    conn.close()
    return names


def tokenize_name(name):
    """Extract meaningful tokens from a name"""
    # Remove common suffixes/titles for matching
    name_lower = name.lower()
    # Remove punctuation and split
    tokens = re.findall(r'[a-z]+', name_lower)
    # Filter out very short tokens and common suffixes
    stopwords = {'jr', 'sr', 'ii', 'iii', 'iv', 'dr', 'mr', 'ms', 'mrs', 'prof'}
    meaningful = [t for t in tokens if len(t) > 1 and t not in stopwords]
    return set(meaningful)


def find_potential_duplicates(names):
    """Find groups of names that share 2+ tokens"""
    # Build token -> names mapping
    token_to_names = defaultdict(set)
    name_tokens = {}

    for name in names:
        tokens = tokenize_name(name)
        name_tokens[name] = tokens
        for token in tokens:
            token_to_names[token].add(name)

    # Find pairs with 2+ shared tokens
    potential_groups = defaultdict(set)
    processed_pairs = set()

    for name1 in names:
        tokens1 = name_tokens[name1]
        if len(tokens1) < 2:
            continue

        for token in tokens1:
            for name2 in token_to_names[token]:
                if name1 >= name2:  # Avoid duplicate pairs
                    continue
                pair = (name1, name2)
                if pair in processed_pairs:
                    continue
                processed_pairs.add(pair)

                tokens2 = name_tokens[name2]
                shared = tokens1 & tokens2

                # Need 2+ shared tokens to be potential match
                if len(shared) >= 2:
                    # Create a group key from shared tokens
                    group_key = frozenset(shared)
                    potential_groups[group_key].add(name1)
                    potential_groups[group_key].add(name2)

    # Convert to list of groups
    groups = []
    seen_names = set()

    for group_key, group_names in sorted(potential_groups.items(), key=lambda x: -len(x[1])):
        # Only include names not already in a group
        new_names = [n for n in group_names if n not in seen_names]
        if len(new_names) >= 2:
            groups.append(list(new_names))
            seen_names.update(new_names)

    return groups


async def call_sonnet(session, groups_batch):
    """Call API with a batch of potential duplicate groups"""
    groups_text = ""
    for i, group in enumerate(groups_batch, 1):
        groups_text += f"\nGroup {i}:\n"
        for name in group:
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
                print(f"Empty response, status: {response.status}")
                return {}, 0, 0

            result = json.loads(response_text)

            if "error" in result:
                error_msg = result.get("error", {}).get("message", "")
                print(f"API Error: {error_msg[:100]}...")
                if "rate" in error_msg.lower() or "overloaded" in error_msg.lower():
                    await asyncio.sleep(30)
                return {}, 0, 0

            if "content" not in result:
                print(f"No content in response")
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

    print(f"\nFinding potential duplicates (token-based matching)...")
    groups = find_potential_duplicates(canonical_names)
    print(f"Found {len(groups)} groups of potential duplicates")

    if not groups:
        print("No potential duplicates found!")
        return {}, 0

    # Show some examples
    print("\nExample groups found:")
    for i, group in enumerate(groups[:5]):
        print(f"  Group {i+1}: {group}")
    if len(groups) > 5:
        print(f"  ... and {len(groups) - 5} more groups")

    print(f"\nModel: {MODEL}")
    print(f"Run ID: {RUN_ID}")

    # Process groups in batches of 10
    BATCH_SIZE = 10
    batches = [groups[i:i + BATCH_SIZE] for i in range(0, len(groups), BATCH_SIZE)]
    print(f"Processing {len(groups)} groups in {len(batches)} batches")

    all_changes = {}
    total_input_tokens = 0
    total_output_tokens = 0

    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i, batch in enumerate(tqdm(batches, desc="Processing batches")):
            changes, input_tokens, output_tokens = await call_sonnet(session, batch)

            if changes:
                all_changes.update(changes)

            total_input_tokens += input_tokens
            total_output_tokens += output_tokens

    print(f"\n\nTotal changes to apply: {len(all_changes)}")

    if all_changes:
        print("\nChanges preview:")
        for old, new in list(all_changes.items())[:20]:
            print(f"  {old} -> {new}")
        if len(all_changes) > 20:
            print(f"  ... and {len(all_changes) - 20} more")

    # Apply changes to database - create new column F2c
    conn = get_connection()
    cursor = conn.cursor()

    # Add new column for F2c results
    try:
        cursor.execute(f'ALTER TABLE "{INPUT_TABLE}" ADD COLUMN "canonical_name [F2c]" TEXT')
        print("Added column 'canonical_name [F2c]'")
    except:
        print("Column 'canonical_name [F2c]' already exists")

    # First, copy F2 values to F2c
    cursor.execute(f'''
        UPDATE "{INPUT_TABLE}"
        SET "canonical_name [F2c]" = "canonical_name [F2]"
        WHERE "canonical_name [F2c]" IS NULL
    ''')
    print(f"Copied F2 values to F2c column")

    # Then apply the new changes to F2c
    updated_count = 0
    for old_name, new_name in tqdm(all_changes.items(), desc="Applying changes"):
        cursor.execute(f'''
            UPDATE "{INPUT_TABLE}"
            SET "canonical_name [F2c]" = ?
            WHERE "canonical_name [F2c]" = ?
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
        json.dumps([f"{INPUT_TABLE}.canonical_name [F2]"]),
        json.dumps([f"{INPUT_TABLE}.canonical_name [F2]"]),
        total_cost,
        "scripts/final/run_name_consolidation_v3.py",
        "FINAL: Third pass - token-based matching for reversed names"
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
