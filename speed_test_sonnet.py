"""
Speed Testing for Claude Sonnet 3.5
Tests concurrency levels with batch size 50
"""

import sqlite3
import json
import asyncio
import aiohttp
import time
from tqdm import tqdm
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
API_KEY = os.getenv("ANTHROPIC_KEY")
DB_PATH = "epstein_analysis.db"
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"

# Fixed batch size (optimal from previous test)
BATCH_SIZE = 50
TEST_SAMPLE_SIZE = 100

# Test different concurrency levels
CONCURRENCY_LEVELS = [5, 10, 15]

PROMPT_TEMPLATE = """Extract person names from emails. Return JSON only: {{"thread_id": ["Name1"], ...}}
No emails/orgs, only person names. Empty array if none.

{emails}"""


def load_test_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        f'SELECT id, thread_id, sender, receiver, cc, body FROM "SAMPLE_1_discussion_messages" LIMIT {TEST_SAMPLE_SIZE}'
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


async def call_sonnet(session, batch, semaphore):
    """Call Claude Sonnet API for a batch of emails"""
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

        try:
            async with session.post(API_URL, json=payload, headers=headers) as response:
                result = await response.json()

                if "content" not in result:
                    error = result.get("error", {}).get("message", str(result))
                    return batch, None, 0, 0, error

                text = result["content"][0]["text"]
                input_tokens = result.get("usage", {}).get("input_tokens", 0)
                output_tokens = result.get("usage", {}).get("output_tokens", 0)

                text = text.strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[1].rsplit("```", 1)[0]

                parsed = json.loads(text)
                return batch, parsed, input_tokens, output_tokens, None

        except Exception as e:
            return batch, None, 0, 0, str(e)


async def run_test(concurrency, rows):
    """Run test with given concurrency"""
    batches = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
    num_batches = len(batches)

    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        keepalive_timeout=60,
    )
    timeout = aiohttp.ClientTimeout(total=120)

    total_input = 0
    total_output = 0
    results = {}
    errors = 0
    rate_limit_hits = 0

    start_time = time.time()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [call_sonnet(session, batch, semaphore) for batch in batches]

        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"C={concurrency}", leave=False):
            try:
                _, parsed, input_tokens, output_tokens, error = await coro

                if error:
                    errors += 1
                    if "rate_limit" in str(error).lower():
                        rate_limit_hits += 1
                else:
                    total_input += input_tokens
                    total_output += output_tokens
                    if parsed:
                        results.update(parsed)
            except Exception as e:
                errors += 1

    elapsed = time.time() - start_time

    return {
        "concurrency": concurrency,
        "batch_size": BATCH_SIZE,
        "elapsed_seconds": round(elapsed, 2),
        "emails_per_second": round(len(rows) / elapsed, 4),
        "api_calls": num_batches,
        "input_tokens": total_input,
        "output_tokens": total_output,
        "success_rate": round(len(results) / len(rows) * 100, 1),
        "errors": errors,
        "rate_limit_hits": rate_limit_hits,
    }


async def main():
    print("=" * 60)
    print("SPEED TEST: Claude Sonnet 3.5 - Concurrency Test")
    print(f"Batch size: {BATCH_SIZE} (fixed)")
    print(f"Testing with {TEST_SAMPLE_SIZE} emails")
    print("=" * 60)

    rows = load_test_data()
    print(f"Loaded {len(rows)} emails\n")

    results = []

    for concurrency in CONCURRENCY_LEVELS:
        print(f"\nTesting concurrency={concurrency}...")
        result = await run_test(concurrency, rows)
        results.append(result)
        print(f"  → {result['elapsed_seconds']}s | {result['emails_per_second']:.4f} emails/s | {result['success_rate']}% success | {result['rate_limit_hits']} rate limits")

        # Wait between tests to reset rate limit
        if concurrency != CONCURRENCY_LEVELS[-1]:
            print("  Waiting 20s for rate limit reset...")
            await asyncio.sleep(20)

    # Summary
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    print(f"{'Concurrency':<12} {'Time(s)':<10} {'Emails/s':<12} {'Success%':<10} {'Rate Limits':<12}")
    print("-" * 60)

    results.sort(key=lambda x: (-x["success_rate"], -x["emails_per_second"]))

    for r in results:
        print(f"{r['concurrency']:<12} {r['elapsed_seconds']:<10} {r['emails_per_second']:<12.4f} {r['success_rate']:<10} {r['rate_limit_hits']:<12}")

    # Find best (highest success rate, then fastest)
    successful = [r for r in results if r["success_rate"] > 50]
    if successful:
        best = max(successful, key=lambda x: x["emails_per_second"])
        print("\n" + "=" * 60)
        print(f"OPTIMAL: concurrency={best['concurrency']} with batch_size={BATCH_SIZE}")
        print(f"Speed: {best['emails_per_second']:.4f} emails/second")
        print(f"Time for 500 emails: ~{500 / best['emails_per_second'] / 60:.1f} minutes")
        print("=" * 60)

    # Save results
    save_results(results)

    return results


def save_results(results):
    """Save results to JSON file"""
    output = {
        "timestamp": datetime.now().isoformat(),
        "model": MODEL,
        "batch_size": BATCH_SIZE,
        "test_sample_size": TEST_SAMPLE_SIZE,
        "results": results,
    }

    output_path = "data/speed_test_sonnet_results.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
