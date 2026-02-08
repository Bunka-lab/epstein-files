"""
FINAL Relationship Description - Based on C12
Describes the relationship between each person and Jeffrey Epstein
Input: FINAL_3_unique_names, FINAL_2_CLASSIFIER_name_extraction
Output: FINAL_4_CLASSIFIER_relationship_description
"""

import sqlite3
import json
import asyncio
import aiohttp
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
API_KEY = os.getenv("ANTHROPIC_KEY")
DB_PATH = "../../epstein_analysis.db"
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"

RUN_ID = "F3"
RUN_NAME = "relationship_description_final"
OUTPUT_TABLE = "FINAL_4_CLASSIFIER_relationship_description"

PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """Based on these email excerpts involving {person_name}, write a brief description (2-3 sentences) of their apparent relationship with Jeffrey Epstein.

Focus on:
- The nature of the relationship (professional, social, legal, etc.)
- Any specific context mentioned (meetings, business, legal matters, etc.)
- The tone/nature of interactions if apparent

If the emails don't provide enough context, say "Insufficient context to determine relationship."

EMAIL EXCERPTS:
{emails}

Write a concise relationship description:"""


def get_connection():
    return sqlite3.connect(DB_PATH)


def get_canonical_names_with_occurrences():
    """Get canonical names with 3+ occurrences, excluding Jeffrey Epstein and variants"""
    conn = get_connection()
    cursor = conn.cursor()

    # Exclude Epstein variants
    excluded = ("None", "Jeffrey Epstein", "Jeffrey E.", "Jeffrey E", "Jeffrey",
                "E. Jeffrey", "Epstein", "Jeff Epstein", "J. Epstein")

    placeholders = ",".join(["?" for _ in excluded])
    cursor.execute(f'''
        SELECT "canonical_name [F2d]", SUM(occurrences) as total_occurrences
        FROM "FINAL_3_unique_names"
        WHERE "canonical_name [F2d]" NOT IN ({placeholders})
          AND "canonical_name [F2d]" IS NOT NULL
        GROUP BY "canonical_name [F2d]"
        HAVING SUM(occurrences) >= 4
        ORDER BY total_occurrences DESC
    ''', excluded)
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_emails_for_person(canonical_name):
    """Get all emails mentioning a person"""
    conn = get_connection()
    cursor = conn.cursor()

    # Get variants
    cursor.execute('''
        SELECT name_extracted FROM "FINAL_3_unique_names"
        WHERE "canonical_name [F2d]" = ?
    ''', (canonical_name,))
    variants = [row[0] for row in cursor.fetchall()]

    emails = []
    cursor.execute('''
        SELECT thread_id, sender, receiver, body, "names_mentioned [F1]"
        FROM "FINAL_2_CLASSIFIER_name_extraction"
    ''')

    for row in cursor.fetchall():
        thread_id, sender, receiver, body, names_json = row
        if names_json:
            try:
                names = json.loads(names_json)
                if any(v in names for v in variants):
                    emails.append({
                        'thread_id': thread_id,
                        'sender': sender or '',
                        'receiver': receiver or '',
                        'body': (body or '')[:500]
                    })
            except:
                pass

    conn.close()
    return emails


async def call_sonnet(session, person_name, emails, semaphore):
    async with semaphore:
        email_text = "\n---\n".join([
            f"From: {e['sender']}\nTo: {e['receiver']}\n{e['body']}"
            for e in emails[:10]
        ])

        prompt = PROMPT_TEMPLATE.format(person_name=person_name, emails=email_text)

        headers = {
            "x-api-key": API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": MODEL,
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}],
        }

        try:
            async with session.post(API_URL, json=payload, headers=headers) as response:
                result = await response.json()

                if "error" in result:
                    error_msg = result.get("error", {}).get("message", "")
                    if "rate" in error_msg.lower():
                        await asyncio.sleep(10)
                        async with session.post(API_URL, json=payload, headers=headers) as retry:
                            result = await retry.json()
                    else:
                        return person_name, "Error: " + error_msg[:50], 0, 0

                if "content" not in result:
                    return person_name, "Error: No content", 0, 0

                text = result["content"][0]["text"].strip()
                usage = result.get("usage", {})
                return person_name, text, usage.get("input_tokens", 0), usage.get("output_tokens", 0)

        except Exception as e:
            return person_name, f"Error: {str(e)[:50]}", 0, 0


async def run_classification():
    print("Getting canonical names with 2+ occurrences...")
    names_data = get_canonical_names_with_occurrences()
    print(f"Found {len(names_data)} people to analyze")

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(f'DROP TABLE IF EXISTS "{OUTPUT_TABLE}"')
    cursor.execute(f'''
        CREATE TABLE "{OUTPUT_TABLE}" (
            name_id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT UNIQUE,
            occurrences INTEGER,
            "relationship_description [{RUN_ID}]" TEXT
        )
    ''')
    conn.commit()
    conn.close()

    print("\nPreparing email data...")
    person_emails = {}
    for canonical_name, occurrences in tqdm(names_data, desc="Loading emails"):
        emails = get_emails_for_person(canonical_name)
        person_emails[canonical_name] = (occurrences, emails)

    print(f"\nModel: {MODEL}")
    print(f"Run ID: {RUN_ID}")
    print(f"Analyzing {len(names_data)} people...")

    total_input_tokens = 0
    total_output_tokens = 0
    results = {}

    semaphore = asyncio.Semaphore(5)
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for canonical_name, (occurrences, emails) in person_emails.items():
            if emails:
                tasks.append(call_sonnet(session, canonical_name, emails, semaphore))

        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="API calls"):
            person_name, description, input_tokens, output_tokens = await coro
            results[person_name] = description
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens

    print("\nSaving results...")
    conn = get_connection()
    cursor = conn.cursor()

    for canonical_name, occurrences in names_data:
        description = results.get(canonical_name, "No emails found")
        cursor.execute(f'''
            INSERT INTO "{OUTPUT_TABLE}" (canonical_name, occurrences, "relationship_description [{RUN_ID}]")
            VALUES (?, ?, ?)
        ''', (canonical_name, occurrences, description))

    total_cost = (total_input_tokens * PRICE_INPUT) + (total_output_tokens * PRICE_OUTPUT)

    cursor.execute("""
        INSERT OR REPLACE INTO ai_classification_runs
        (run_id, run_name, run_type, model_used, prompt_used, input_columns, output_columns, total_cost, script_path, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        RUN_ID,
        RUN_NAME,
        "relationship",
        MODEL,
        PROMPT_TEMPLATE,
        json.dumps(["FINAL_2_CLASSIFIER_name_extraction.body", "FINAL_3_unique_names.canonical_name [F2d]"]),
        json.dumps([f"{OUTPUT_TABLE}.relationship_description [{RUN_ID}]"]),
        total_cost,
        "scripts/final/run_relationship_description.py",
        "FINAL: Describes relationship between each person and Jeffrey Epstein"
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
            "total_people": len(names_data),
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "cost": total_cost,
            "results": {name: {"occurrences": occ, "description": results.get(name, "")}
                       for name, occ in names_data}
        }, f, indent=2)

    print(f"\nDone!")
    print(f"Total tokens: {total_input_tokens:,} input, {total_output_tokens:,} output")
    print(f"Total cost: ${total_cost:.4f}")

    return results, total_cost


if __name__ == "__main__":
    results, cost = asyncio.run(run_classification())
