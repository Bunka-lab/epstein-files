"""
Filter out journalist interview requests from discussions.
Uses parallel processing for speed.
"""

import json
import google.generativeai as genai
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure Gemini
genai.configure(api_key='AIzaSyBDV9HJ_RJnZ3g7UQZhXssiF_vGrUbDFOk')
model = genai.GenerativeModel('gemini-2.0-flash')

# Load data
with open("data/epstein_discussions_names.json") as f:
    data = json.load(f)

print(f"Loaded {len(data)} discussions")

journalist_request_ids = []
lock = threading.Lock()

def process_batch(batch):
    """Process a batch of discussions."""
    batch_info = []
    for disc in batch:
        thread_id = disc.get("thread_id", "")
        subject = disc.get("subject", "")[:100]
        senders = ", ".join(disc.get("senders", []))[:100]
        
        messages = disc.get("messages", [])
        content = messages[0].get("body", "")[:300] if messages else ""
        
        batch_info.append({
            "id": thread_id,
            "subject": subject,
            "from": senders,
            "content": content
        })
    
    prompt = f"""Analyze these emails. Return ONLY thread IDs that are JOURNALIST REQUESTS:
- Journalist asking for interview
- Reporter asking questions for a story
- Media requesting comment/statement

Emails:
{json.dumps(batch_info, indent=2)}

Return JSON array of matching IDs only. Example: ["id1"] or []"""

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        if "```" in text:
            text = text.split("```")[1].replace("json", "").strip()
        ids = json.loads(text)
        return ids if isinstance(ids, list) else []
    except:
        return []

# Create batches
batch_size = 15
batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]

print(f"Processing {len(batches)} batches with 20 parallel workers...")

# Process in parallel
results = []
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}
    
    for future in tqdm(as_completed(futures), total=len(futures), desc="Filtering"):
        try:
            ids = future.result()
            results.extend(ids)
        except Exception as e:
            pass

journalist_request_ids = list(set(results))

# Save journalist request IDs
with open("data/journalist_request_ids.json", "w") as f:
    json.dump(journalist_request_ids, f, indent=2)

print(f"\nFound {len(journalist_request_ids)} journalist request emails")

# Filter out these discussions
filtered_data = [d for d in data if d.get("thread_id") not in set(journalist_request_ids)]
print(f"Remaining discussions: {len(filtered_data)}")

with open("data/epstein_discussions_filtered.json", "w") as f:
    json.dump(filtered_data, f, indent=2, ensure_ascii=False)

print(f"Saved to data/epstein_discussions_filtered.json")
