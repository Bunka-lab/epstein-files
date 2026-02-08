"""
FINAL Cluster Analysis - Based on C13
Analyzes network communities using Claude Sonnet
Input: FINAL_5_network_nodes, FINAL_4_CLASSIFIER_relationship_description
Output: FINAL_6_CLASSIFIER_cluster_analysis
"""

import sqlite3
import json
import asyncio
import aiohttp
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
API_KEY = os.getenv("ANTHROPIC_KEY")
DB_PATH = "../../epstein_analysis.db"
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"

RUN_ID = "F4"
RUN_NAME = "cluster_analysis_final"
OUTPUT_TABLE = "FINAL_6_CLASSIFIER_cluster_analysis"

PRICE_INPUT = 3.00 / 1_000_000
PRICE_OUTPUT = 15.00 / 1_000_000

PROMPT_TEMPLATE = """Analyze this community/cluster from Jeffrey Epstein's email network.

Members ({member_count} people):
{members_text}

Based on the relationships and people involved, write a concise analysis (3-5 sentences) that:
1. Identifies the main theme or characteristic of this cluster
2. Explains what connects these people
3. Notes any notable patterns or key figures

Be factual and analytical."""


def get_connection():
    return sqlite3.connect(DB_PATH)


def load_network_data():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT "canonical_name [F2d]", occurrences, degree, community
        FROM "FINAL_5_network_nodes"
    ''')
    nodes = cursor.fetchall()

    cursor.execute('''
        SELECT canonical_name, "relationship_description [F3]"
        FROM "FINAL_4_CLASSIFIER_relationship_description"
    ''')
    relationships = {row[0]: row[1] for row in cursor.fetchall()}

    conn.close()

    communities = defaultdict(list)
    for name, occurrences, degree, community in nodes:
        communities[community].append({
            "name": name,
            "occurrences": occurrences,
            "degree": degree,
            "relationship": relationships.get(name, "No relationship description available")
        })

    for comm_id in communities:
        communities[comm_id] = sorted(communities[comm_id], key=lambda x: -x["degree"])

    return dict(communities)


async def call_sonnet(session, cluster_id, members, semaphore):
    async with semaphore:
        members_info = []
        for m in members[:10]:
            rel_short = m["relationship"][:150] + "..." if len(m["relationship"]) > 150 else m["relationship"]
            members_info.append(f"- {m['name']} ({m['degree']} connections, {m['occurrences']} mentions): {rel_short}")

        members_text = "\n".join(members_info)
        prompt = PROMPT_TEMPLATE.format(member_count=len(members), members_text=members_text)

        headers = {
            "x-api-key": API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": MODEL,
            "max_tokens": 400,
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
                        return cluster_id, members, "Error: " + error_msg[:50], 0, 0

                if "content" not in result:
                    return cluster_id, members, "Error: No content", 0, 0

                text = result["content"][0]["text"].strip()
                usage = result.get("usage", {})
                return cluster_id, members, text, usage.get("input_tokens", 0), usage.get("output_tokens", 0)

        except Exception as e:
            return cluster_id, members, f"Error: {str(e)[:50]}", 0, 0


async def run_analysis():
    print("Loading network data...")
    communities = load_network_data()
    sorted_communities = sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)

    print(f"Found {len(sorted_communities)} communities")

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(f'DROP TABLE IF EXISTS "{OUTPUT_TABLE}"')
    cursor.execute(f'''
        CREATE TABLE "{OUTPUT_TABLE}" (
            cluster_id INTEGER PRIMARY KEY,
            size INTEGER,
            top_members TEXT,
            all_members TEXT,
            "cluster_description [{RUN_ID}]" TEXT
        )
    ''')
    conn.commit()
    conn.close()

    print(f"\nModel: {MODEL}")
    print(f"Run ID: {RUN_ID}")
    print(f"Analyzing {len(sorted_communities)} clusters...")

    total_input_tokens = 0
    total_output_tokens = 0
    results = []

    semaphore = asyncio.Semaphore(3)
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [call_sonnet(session, cid, members, semaphore) for cid, members in sorted_communities]

        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="API calls"):
            cluster_id, members, analysis, input_tokens, output_tokens = await coro
            results.append((cluster_id, members, analysis))
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens

    print("\nSaving results...")
    conn = get_connection()
    cursor = conn.cursor()

    for cluster_id, members, analysis in results:
        top_members = json.dumps([m["name"] for m in members[:5]])
        all_members = json.dumps([m["name"] for m in members])
        cursor.execute(f'''
            INSERT INTO "{OUTPUT_TABLE}" (cluster_id, size, top_members, all_members, "cluster_description [{RUN_ID}]")
            VALUES (?, ?, ?, ?, ?)
        ''', (cluster_id, len(members), top_members, all_members, analysis))

    total_cost = (total_input_tokens * PRICE_INPUT) + (total_output_tokens * PRICE_OUTPUT)

    cursor.execute("""
        INSERT OR REPLACE INTO ai_classification_runs
        (run_id, run_name, run_type, model_used, prompt_used, input_columns, output_columns, total_cost, script_path, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        RUN_ID,
        RUN_NAME,
        "clustering",
        MODEL,
        PROMPT_TEMPLATE,
        json.dumps(["FINAL_5_network_nodes.community", "FINAL_4_CLASSIFIER_relationship_description.relationship_description [F3]"]),
        json.dumps([f"{OUTPUT_TABLE}.cluster_description [{RUN_ID}]"]),
        total_cost,
        "scripts/final/analyze_clusters.py",
        "FINAL: Analyzes network communities"
    ))

    conn.commit()
    conn.close()

    # Export
    export_path = Path("../../data/classification") / f"{RUN_ID}_{RUN_NAME}.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)

    export_data = [{"cluster_id": cid, "size": len(m), "top_members": [x["name"] for x in m[:10]], "analysis": a}
                   for cid, m, a in results]

    with open(export_path, "w") as f:
        json.dump({
            "run_id": RUN_ID,
            "model": MODEL,
            "timestamp": datetime.now().isoformat(),
            "total_clusters": len(results),
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "cost": total_cost,
            "clusters": export_data
        }, f, indent=2)

    print(f"\nDone!")
    print(f"Total tokens: {total_input_tokens:,} input, {total_output_tokens:,} output")
    print(f"Total cost: ${total_cost:.4f}")

    return results, total_cost


if __name__ == "__main__":
    results, cost = asyncio.run(run_analysis())

    print("\n" + "=" * 70)
    print("CLUSTER ANALYSIS SUMMARY")
    print("=" * 70)

    for cluster_id, members, analysis in sorted(results, key=lambda x: x[0]):
        top_names = [m["name"] for m in members[:5]]
        print(f"\n### Cluster {cluster_id} ({len(members)} members)")
        print(f"Top: {', '.join(top_names)}")
        print(f"\n{analysis}")
        print("-" * 70)
