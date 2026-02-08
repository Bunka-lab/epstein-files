"""
FINAL Create Unique Names Table
Extracts all unique names from FINAL_2_CLASSIFIER_name_extraction
Creates FINAL_3_unique_names with occurrences
"""

import sqlite3
import json
from collections import Counter
from tqdm import tqdm

DB_PATH = "../../epstein_analysis.db"
INPUT_TABLE = "FINAL_2_CLASSIFIER_name_extraction"
OUTPUT_TABLE = "FINAL_3_unique_names"


def get_connection():
    return sqlite3.connect(DB_PATH)


def main():
    print(f"Loading names from {INPUT_TABLE}...")
    conn = get_connection()
    cursor = conn.cursor()

    # Get thread_id and names mentioned
    cursor.execute(f'SELECT thread_id, "names_mentioned [F1]" FROM "{INPUT_TABLE}"')
    rows = cursor.fetchall()

    # Count occurrences = number of unique discussions (thread_ids) where name appears
    # If name appears 10 times in same discussion, count as 1
    name_threads = {}  # name -> set of thread_ids
    for row in tqdm(rows, desc="Counting unique discussions"):
        thread_id, names_json = row
        if names_json:
            try:
                names = json.loads(names_json)
                for name in names:
                    if name and name.strip():
                        name = name.strip()
                        if name not in name_threads:
                            name_threads[name] = set()
                        name_threads[name].add(thread_id)
            except:
                pass

    # Convert to counts
    name_counts = Counter({name: len(threads) for name, threads in name_threads.items()})

    print(f"Found {len(name_counts)} unique names")

    # Check if table exists and has canonical_name column (from consolidation)
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{OUTPUT_TABLE}'")
    table_exists = cursor.fetchone() is not None

    if table_exists:
        # Update occurrences in place (preserve F2 consolidation)
        print("Table exists - updating occurrences in place...")
        for name, count in tqdm(name_counts.items(), desc="Updating occurrences"):
            cursor.execute(f'''
                UPDATE "{OUTPUT_TABLE}"
                SET occurrences = ?
                WHERE name_extracted = ?
            ''', (count, name))
    else:
        # Create new table
        cursor.execute(f'''
            CREATE TABLE "{OUTPUT_TABLE}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name_extracted TEXT UNIQUE,
                occurrences INTEGER
            )
        ''')
        # Insert unique names
        for name in tqdm(sorted(name_counts.keys()), desc="Saving names"):
            cursor.execute(f'''
                INSERT INTO "{OUTPUT_TABLE}" (name_extracted, occurrences)
                VALUES (?, ?)
            ''', (name, name_counts[name]))

    conn.commit()
    conn.close()

    print(f"Updated {OUTPUT_TABLE} with {len(name_counts)} names")

    # Show top names
    print("\nTop 20 names by occurrence:")
    for name, count in name_counts.most_common(20):
        print(f"  {count:4d} - {name}")


if __name__ == "__main__":
    main()
