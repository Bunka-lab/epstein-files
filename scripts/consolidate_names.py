import json
import asyncio
import aiohttp
from tqdm.asyncio import tqdm_asyncio

API_KEY = "AIzaSyBDV9HJ_RJnZ3g7UQZhXssiF_vGrUbDFOk"
URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}"

with open("data/all_names.json") as f:
    all_names = json.load(f)

# Batch names (50 per batch)
BATCH_SIZE = 50
batches = [all_names[i : i + BATCH_SIZE] for i in range(0, len(all_names), BATCH_SIZE)]

prompt_template = """Given this list of names from emails, identify which names refer to the SAME person.
Return a JSON object where keys are the canonical name (full proper name) and values are lists of all variants.
Only group names that are CLEARLY the same person. Keep separate if unsure.

Names:
{}

Return ONLY valid JSON like:
{{"Bill Clinton": ["Bill Clinton", "Clinton", "President Clinton", "clinton"], "Donald Trump": ["Trump", "Donald Trump", "DJT"]}}
"""


async def process_batch(session, batch, batch_idx):
    names_str = "\n".join(batch)
    payload = {"contents": [{"parts": [{"text": prompt_template.format(names_str)}]}]}

    try:
        async with session.post(URL, json=payload) as resp:
            data = await resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            if text.startswith("```"):
                text = text.split("```")[1].replace("json", "").strip()
            return json.loads(text)
    except Exception as e:
        return {"error": str(e)[:100], "batch": batch_idx}


async def main():
    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [process_batch(session, b, i) for i, b in enumerate(batches)]
        results = await tqdm_asyncio.gather(*tasks, desc="Processing batches")

    # Merge all mappings
    name_mapping = {}
    for r in results:
        if "error" not in r:
            for canonical, variants in r.items():
                if canonical in name_mapping:
                    name_mapping[canonical].extend(variants)
                else:
                    name_mapping[canonical] = variants

    # Create reverse mapping: variant -> canonical
    reverse_map = {}
    for canonical, variants in name_mapping.items():
        for v in variants:
            reverse_map[v] = canonical

    with open("data/name_mapping.json", "w") as f:
        json.dump(reverse_map, f, indent=2, ensure_ascii=False)

    print(
        f"Done: {len(reverse_map)} names mapped to {len(name_mapping)} canonical names"
    )


asyncio.run(main())
