"""
Extract detailed relationship descriptions between each person and Epstein using Gemini.
"""

import json
import google.generativeai as genai
from collections import Counter
from tqdm import tqdm
import time

# Configure Gemini
genai.configure(api_key='AIzaSyBDV9HJ_RJnZ3g7UQZhXssiF_vGrUbDFOk')
model = genai.GenerativeModel('gemini-2.0-flash')

# Load name mapping
with open("data/name_matching_table.json") as f:
    name_mapping = json.load(f)


def consolidate_name(name):
    if not name:
        return None
    cleaned = name
    if "<" in cleaned:
        cleaned = cleaned.split("<")[0].strip()
    if "[" in cleaned:
        cleaned = cleaned.split("[")[0].strip()
    cleaned = cleaned.strip('"').strip()
    for variant in [name, cleaned, cleaned.strip("'"), name.strip("'")]:
        if variant in name_mapping:
            m = name_mapping[variant]
            return None if m == "GARBAGE" else m
    return cleaned if cleaned else None


# Load data
with open("data/epstein_discussions_filtered.json") as f:
    data = json.load(f)

with open("data/names_to_remove.json") as f:
    exclude = set(json.load(f))

print(f"Loaded {len(data)} discussions")

# Get person counts and discussions
person_counts = Counter()
person_discussions = {}

for disc in data:
    all_persons = set()
    for p in disc.get("senders", []) + disc.get("receivers", []) + disc.get("people_mentioned", []):
        consolidated = consolidate_name(p)
        if consolidated:
            all_persons.add(consolidated)
    
    for p in all_persons:
        person_counts[p] += 1
        if p not in person_discussions:
            person_discussions[p] = []
        person_discussions[p].append(disc)

# Filter valid persons
valid_persons = [
    p for p, count in person_counts.items()
    if count >= 3 and p not in exclude and len(p) > 2
]

print(f"Analyzing {len(valid_persons)} persons")

results = []

for person in tqdm(valid_persons, desc="Extracting relationships"):
    discussions = person_discussions.get(person, [])
    if not discussions:
        continue
    
    # Use up to 10 discussions for better context
    sample = discussions[:10]
    
    # Build detailed context
    context_parts = []
    thread_ids = []
    
    for disc in sample:
        thread_id = disc.get("thread_id", "")
        thread_ids.append(thread_id)
        
        subject = disc.get("subject", "No subject")
        senders = ", ".join(disc.get("senders", []))
        receivers = ", ".join(disc.get("receivers", []))
        mentioned = ", ".join(disc.get("people_mentioned", []))
        
        # Get message content
        messages = disc.get("messages", [])
        content = ""
        for msg in messages[:2]:  # First 2 messages
            body = msg.get("body", "")[:400]
            if body:
                content += body + "\n"
        
        context_parts.append(f"""
Email (ID: {thread_id}):
Subject: {subject}
From: {senders}
To: {receivers}
Mentioned: {mentioned}
Content: {content}
""")
    
    context = "\n---\n".join(context_parts)
    
    prompt = f"""Based on these emails involving "{person}" from Jeffrey Epstein's correspondence, describe their relationship with Epstein.

{context}

Write a specific description (2-3 sentences) explaining:
- Who is {person} (their profession/role if identifiable)
- The nature of their connection to Epstein based on the evidence in these emails
- What kind of interactions they had

Be specific and factual based on the email content. If the relationship is unclear, explain what IS visible in the emails.
Do not use generic terms like "social acquaintance" - describe the actual relationship you see.

Reply with ONLY the description, no preamble."""

    try:
        response = model.generate_content(prompt)
        description = response.text.strip()
        
        results.append({
            "name": person,
            "relationship": description,
            "count": person_counts[person],
            "thread_ids": thread_ids
        })
    except Exception as e:
        results.append({
            "name": person,
            "relationship": f"Error: {str(e)}",
            "count": person_counts[person],
            "thread_ids": thread_ids
        })
    
    time.sleep(0.05)

# Save
with open("data/person_relationships.json", "w") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"\nSaved {len(results)} relationships to data/person_relationships.json")
