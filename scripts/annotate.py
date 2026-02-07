"""
Annotate Epstein email discussions using Gemini API.
Tracks token consumption in data/token_usage.csv
"""

import json
import asyncio
import aiohttp
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime
import os

API_KEY = "AIzaSyBDV9HJ_RJnZ3g7UQZhXssiF_vGrUbDFOk"
URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}"

# Token tracking
token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

prompt_template = """Analyze this email thread and extract:
1. senders: list of sender names (clean names only, no emails)
2. receivers: list of receiver names (clean names only)
3. people_mentioned: other people mentioned in the emails
4. relationship: nature of relationship between Epstein and the person, in 2 words
5. topic: what this thread is about, in 2 words
6. example: one short quote (max 15 words) that illustrates the interaction

Return ONLY valid JSON:
{"senders": [...], "receivers": [...], "people_mentioned": [...], "relationship": "two words", "topic": "two words", "example": "short quote"}

Email thread:
"""

async def process(session, disc):
    global token_usage

    thread_text = f"Subject: {disc['subject']}\n\n"
    for msg in disc["messages"]:
        thread_text += f"From: {msg['sender']}\nTo: {', '.join(msg['receivers']) if msg['receivers'] else 'Unknown'}\n{msg['body']}\n---\n"

    payload = {"contents": [{"parts": [{"text": prompt_template + thread_text[:6000]}]}]}

    try:
        async with session.post(URL, json=payload) as resp:
            data = await resp.json()

            # Track tokens
            if "usageMetadata" in data:
                usage = data["usageMetadata"]
                token_usage["prompt_tokens"] += usage.get("promptTokenCount", 0)
                token_usage["completion_tokens"] += usage.get("candidatesTokenCount", 0)
                token_usage["total_tokens"] += usage.get("totalTokenCount", 0)

            text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            if text.startswith("```"):
                text = text.split("```")[1].replace("json", "").strip()
            result = json.loads(text)
            result["thread_id"] = disc["thread_id"]
            return result
    except Exception as e:
        return {
            "thread_id": disc["thread_id"],
            "senders": [],
            "receivers": [],
            "people_mentioned": [],
            "relationship": "error",
            "topic": "error",
            "example": "",
            "error": str(e)[:100],
        }

def save_token_usage(num_discussions):
    """Append token usage to CSV file"""
    csv_path = "data/token_usage.csv"

    # Create header if file doesn't exist
    if not os.path.exists(csv_path):
        with open(csv_path, "w") as f:
            f.write("timestamp,discussions,prompt_tokens,completion_tokens,total_tokens\n")

    # Append usage
    with open(csv_path, "a") as f:
        f.write(f"{datetime.now().isoformat()},{num_discussions},{token_usage['prompt_tokens']},{token_usage['completion_tokens']},{token_usage['total_tokens']}\n")

    print(f"\nToken usage: {token_usage['total_tokens']:,} total ({token_usage['prompt_tokens']:,} prompt, {token_usage['completion_tokens']:,} completion)")

async def main(discussions):
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [process(session, d) for d in discussions]
        results = await tqdm_asyncio.gather(*tasks, desc="Annotating")
    return results

if __name__ == "__main__":
    import random

    with open("data/epstein_discussions.json") as f:
        all_data = json.load(f)

    # Sample 500
    random.seed(42)
    discussions = random.sample(all_data, 500)

    results = asyncio.run(main(discussions))

    with open("data/epstein_annotated.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    save_token_usage(len(discussions))
    print(f"Done: {len(results)} annotated")
