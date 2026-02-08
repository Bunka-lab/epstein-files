"""
Name Consolidation - Classifier C8
Consolidates name variants into canonical names using Claude Sonnet
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
from collections import Counter

# Load environment
load_dotenv()
API_KEY = os.getenv("ANTHROPIC_KEY")
DB_PATH = "../epstein_analysis.db"
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"

# Config
RUN_ID = "C8"
RUN_NAME = "name_consolidation"
BATCH_SIZE = 1  # Process all names in one call
MAX_CONCURRENT = 1
INPUT_TABLE = "SAMPLE_2_CLASSIFIER_name_extraction"
INPUT_COLUMN = "names_mentioned [C7]"
OUTPUT_TABLE = "SAMPLE_3_CLASSIFIER_name_consolidation"

# Pricing (per 1M tokens) - Claude Sonnet
PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """You are consolidating person names extracted from emails.

Given this list of names, create a mapping where:
1. Group names that clearly refer to the SAME person (e.g., "Bill Clinton", "Clinton", "President Clinton" -> "Bill Clinton")
2. Use the most complete/formal version as the canonical name
3. If a name is NOT a real person name (garbage, email fragments, org names), map it to "None"
4. Keep names separate if you're unsure they're the same person

Names to consolidate:
{names}

Return ONLY valid JSON like:
{{"Bill Clinton": ["Bill Clinton", "Clinton", "President Clinton"], "Donald Trump": ["Trump", "DJT"], "None": ["jeffrey E.", "LHS", "unknown"]}}"""


def get_connection():
    return sqlite3.connect(DB_PATH)


def load_names():
    """Load all unique names from the input table"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f'SELECT "{INPUT_COLUMN}" FROM "{INPUT_TABLE}"')
    rows = cursor.fetchall()
    conn.close()

    # Collect all names with their counts
    all_names = []
    for row in rows:
        if row[0]:
            try:
                names = json.loads(row[0])
                all_names.extend(names)
            except:
                pass

    # Count occurrences
    name_counts = Counter(all_names)
    return name_counts


async def call_sonnet(session, names_list):
    """Call Claude Sonnet API"""
    prompt = PROMPT_TEMPLATE.format(names=json.dumps(names_list, indent=2))

    headers = {
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": MODEL,
        "max_tokens": 8192,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        async with session.post(API_URL, json=payload, headers=headers) as response:
            result = await response.json()

            if "error" in result:
                error_msg = result.get("error", {}).get("message", "")
                print(f"API Error: {error_msg[:80]}...")
                return None, 0, 0

            if "content" not in result:
                return None, 0, 0

            text = result["content"][0]["text"]
            usage = result.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)

            # Parse JSON
            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]

            parsed = json.loads(text)
            return parsed, input_tokens, output_tokens

    except Exception as e:
        print(f"Error: {e}")
        return None, 0, 0


async def run_consolidation():
    """Main consolidation"""
    print(f"Loading names from {INPUT_TABLE}...")
    name_counts = load_names()
    unique_names = list(name_counts.keys())

    print(f"Found {len(unique_names)} unique names")
    print(f"Model: {MODEL}")

    # Call API
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print("\nCalling API for consolidation...")
        consolidation_map, input_tokens, output_tokens = await call_sonnet(session, unique_names)

    if not consolidation_map:
        print("Failed to get consolidation map")
        return None, 0

    # Create reverse mapping: variant -> canonical
    variant_to_canonical = {}
    for canonical, variants in consolidation_map.items():
        for variant in variants:
            variant_to_canonical[variant] = canonical

    print(f"\nConsolidation results:")
    print(f"  Canonical names: {len([k for k in consolidation_map.keys() if k != 'None'])}")
    print(f"  Mapped to None: {len(consolidation_map.get('None', []))}")

    # Create output table and save results
    conn = get_connection()
    cursor = conn.cursor()

    # Drop and recreate output table
    cursor.execute(f'DROP TABLE IF EXISTS "{OUTPUT_TABLE}"')
    cursor.execute(f"""
        CREATE TABLE "{OUTPUT_TABLE}" (
            thread_id TEXT PRIMARY KEY,
            sender TEXT,
            receiver TEXT,
            cc TEXT,
            body TEXT,
            "names_original [{RUN_ID}]" TEXT,
            "names_consolidated [{RUN_ID}]" TEXT,
            "name_count [{RUN_ID}]" INTEGER
        )
    """)

    # Load original data and apply consolidation
    cursor.execute(f"""
        SELECT thread_id, sender, receiver, cc, body, "{INPUT_COLUMN}"
        FROM "{INPUT_TABLE}"
    """)
    rows = cursor.fetchall()

    for row in tqdm(rows, desc="Applying consolidation"):
        thread_id, sender, receiver, cc, body, names_json = row
        original_names = []
        consolidated_names = []

        if names_json:
            try:
                original_names = json.loads(names_json)
                # Apply consolidation, filter out "None"
                for name in original_names:
                    canonical = variant_to_canonical.get(name, name)
                    if canonical != "None" and canonical not in consolidated_names:
                        consolidated_names.append(canonical)
            except:
                pass

        cursor.execute(f"""
            INSERT INTO "{OUTPUT_TABLE}"
            (thread_id, sender, receiver, cc, body, "names_original [{RUN_ID}]", "names_consolidated [{RUN_ID}]", "name_count [{RUN_ID}]")
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            thread_id, sender, receiver, cc, body,
            json.dumps(original_names),
            json.dumps(consolidated_names),
            len(consolidated_names)
        ))

    # Register classifier
    total_cost = (input_tokens * PRICE_INPUT) + (output_tokens * PRICE_OUTPUT)

    cursor.execute("""
        INSERT OR REPLACE INTO ai_classification_runs
        (run_id, run_name, run_type, model_used, prompt_used, input_columns, output_columns, total_cost, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        RUN_ID,
        RUN_NAME,
        "consolidation",
        MODEL,
        PROMPT_TEMPLATE,
        json.dumps([f"{INPUT_TABLE}.{INPUT_COLUMN}"]),
        json.dumps([
            f"{OUTPUT_TABLE}.names_consolidated [{RUN_ID}]",
            f"{OUTPUT_TABLE}.name_count [{RUN_ID}]"
        ]),
        total_cost,
        "Consolidates name variants into canonical names, maps garbage to None"
    ))

    conn.commit()
    conn.close()

    # Save consolidation map
    export_path = Path("../data/classification") / f"{RUN_ID}_{RUN_NAME}.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with open(export_path, "w") as f:
        json.dump({
            "run_id": RUN_ID,
            "model": MODEL,
            "timestamp": datetime.now().isoformat(),
            "unique_names": len(unique_names),
            "canonical_names": len([k for k in consolidation_map.keys() if k != "None"]),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": total_cost,
            "consolidation_map": consolidation_map,
            "variant_to_canonical": variant_to_canonical
        }, f, indent=2)

    print(f"\nDone!")
    print(f"Total tokens: {input_tokens:,} input, {output_tokens:,} output")
    print(f"Total cost: ${total_cost:.4f}")
    print(f"Results saved to: {OUTPUT_TABLE}")
    print(f"Map saved to: {export_path}")

    return consolidation_map, total_cost


if __name__ == "__main__":
    result, cost = asyncio.run(run_consolidation())

    if result:
        print("\n--- Sample Consolidations ---")
        for canonical, variants in list(result.items())[:5]:
            if canonical != "None":
                print(f"  {canonical}: {variants}")

        if "None" in result:
            print(f"\n--- Mapped to None ({len(result['None'])}) ---")
            print(f"  {result['None'][:10]}...")
