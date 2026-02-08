"""
FINAL Name Consolidation - Based on C11 (last name matching only)
Consolidates name variants into canonical names
Input: FINAL_3_unique_names
Output: Adds canonical_name [F2] column to FINAL_3_unique_names
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

RUN_ID = "F2"
RUN_NAME = "name_consolidation_final"
INPUT_TABLE = "FINAL_3_unique_names"

PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """You are consolidating person names extracted from emails.

I have pre-analyzed the names and found LAST NAME matches only (first name matches are excluded):

LAST NAME MATCHES (partial name -> possible full names):
{matches_text}

STRICT RULES:
1. LAST NAMES: Use the matches above. If a partial name matches exactly ONE full name's last name, use that.
2. FIRST NAMES ONLY (Jeff, Boris, Woody, Bill, etc.): Map to "None" - do NOT guess who they are.
3. If a partial name has MULTIPLE potential last name matches, keep it separate or map to "None" if ambiguous.
4. Group obvious variants of the same full name (e.g., "Bill Clinton", "President Clinton" -> "Bill Clinton")
5. Map garbage, email fragments, org names to "None"

ALL NAMES TO CONSOLIDATE:
{names}

Return ONLY valid JSON mapping canonical names to their variants:
{{"Full Name": ["variant1", "variant2"], "None": ["garbage1", "firstname1"]}}"""


def get_connection():
    return sqlite3.connect(DB_PATH)


def load_names():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f'SELECT id, name_extracted, occurrences FROM "{INPUT_TABLE}"')
    rows = cursor.fetchall()
    conn.close()
    return rows


def find_lastname_matches(names):
    """For each single-word name, find full names where it matches the LAST NAME only."""
    single_words = []
    multi_words = []

    for name in names:
        clean = name.strip()
        words = clean.split()
        if len(words) == 1:
            single_words.append(clean)
        elif len(words) >= 2:
            multi_words.append(clean)

    matches = defaultdict(list)

    for single in single_words:
        single_lower = single.lower()
        for multi in multi_words:
            parts = multi.split()
            last_name = parts[-1].lower()
            if single_lower == last_name:
                matches[single].append(multi)

    return dict(matches)


async def call_sonnet(session, names_list, matches):
    matches_text = "\n".join([f"  {partial} -> {full_names}" for partial, full_names in sorted(matches.items()) if full_names])

    prompt = PROMPT_TEMPLATE.format(
        matches_text=matches_text if matches_text else "  (none found)",
        names=json.dumps(names_list, indent=2)
    )

    headers = {
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": MODEL,
        "max_tokens": 16000,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        async with session.post(API_URL, json=payload, headers=headers) as response:
            result = await response.json()

            if "error" in result:
                print(f"API Error: {result.get('error', {}).get('message', '')[:80]}...")
                return None, 0, 0

            if "content" not in result:
                return None, 0, 0

            text = result["content"][0]["text"]
            usage = result.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)

            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]

            parsed = json.loads(text)
            return parsed, input_tokens, output_tokens

    except Exception as e:
        print(f"Error: {e}")
        return None, 0, 0


async def run_consolidation():
    print(f"Loading names from {INPUT_TABLE}...")
    name_rows = load_names()
    names = [row[1] for row in name_rows]

    print(f"Found {len(names)} unique names")

    # Find last name matches
    print("Finding LAST NAME matches only...")
    matches = find_lastname_matches(names)
    print(f"Found {len(matches)} partial names with last name matches")

    print(f"\nModel: {MODEL}")
    print(f"Run ID: {RUN_ID}")

    # Batch names to avoid token limits (500 names per batch)
    BATCH_SIZE = 500
    batches = [names[i:i + BATCH_SIZE] for i in range(0, len(names), BATCH_SIZE)]
    print(f"Processing in {len(batches)} batches of {BATCH_SIZE} names each")

    consolidation_map = {}
    total_input_tokens = 0
    total_output_tokens = 0

    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i, batch in enumerate(batches):
            print(f"\nBatch {i+1}/{len(batches)} ({len(batch)} names)...")
            # Get matches relevant to this batch
            batch_matches = {k: v for k, v in matches.items() if k in batch}
            result, input_tokens, output_tokens = await call_sonnet(session, batch, batch_matches)

            if result:
                # Merge results
                for canonical, variants in result.items():
                    if canonical in consolidation_map:
                        consolidation_map[canonical].extend(variants)
                    else:
                        consolidation_map[canonical] = variants
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                print(f"  OK: {len([k for k in result.keys() if k != 'None'])} canonical names")
            else:
                print(f"  FAILED - batch {i+1}")

    input_tokens = total_input_tokens
    output_tokens = total_output_tokens

    if not consolidation_map:
        print("Failed to get consolidation map")
        return None, 0

    # Create reverse mapping
    variant_to_canonical = {}
    for canonical, variants in consolidation_map.items():
        for variant in variants:
            variant_to_canonical[variant] = canonical

    print(f"\nConsolidation results:")
    print(f"  Canonical names: {len([k for k in consolidation_map.keys() if k != 'None'])}")
    print(f"  Mapped to None: {len(consolidation_map.get('None', []))}")

    # Add column to table
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f'ALTER TABLE "{INPUT_TABLE}" ADD COLUMN "canonical_name [{RUN_ID}]" TEXT')
    except:
        pass  # Column exists

    # Update
    for row_id, name, _ in tqdm(name_rows, desc="Updating table"):
        canonical = variant_to_canonical.get(name, name)
        cursor.execute(f'''
            UPDATE "{INPUT_TABLE}"
            SET "canonical_name [{RUN_ID}]" = ?
            WHERE id = ?
        ''', (canonical, row_id))

    # Calculate cost
    total_cost = (input_tokens * PRICE_INPUT) + (output_tokens * PRICE_OUTPUT)

    # Register classifier
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
        json.dumps([f"{INPUT_TABLE}.name_extracted"]),
        json.dumps([f"{INPUT_TABLE}.canonical_name [{RUN_ID}]"]),
        total_cost,
        "scripts/final/run_name_consolidation.py",
        "FINAL: Consolidates names, last names only can match"
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
            "unique_names": len(names),
            "canonical_names": len([k for k in consolidation_map.keys() if k != "None"]),
            "mapped_to_none": len(consolidation_map.get("None", [])),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": total_cost,
            "consolidation_map": consolidation_map
        }, f, indent=2)

    print(f"\nDone!")
    print(f"Total tokens: {input_tokens:,} input, {output_tokens:,} output")
    print(f"Total cost: ${total_cost:.4f}")

    return consolidation_map, total_cost


if __name__ == "__main__":
    result, cost = asyncio.run(run_consolidation())
