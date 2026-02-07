"""
Extract names from Epstein discussions dataset using Gemini 2.5 Flash.
Uses batching and multiprocessing for efficiency.
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import google.generativeai as genai

# Configuration
INPUT_FILE = "data/epstein_discussions.json"
OUTPUT_FILE = "data/epstein_discussions_enriched.json"
CHECKPOINT_FILE = "data/extraction_checkpoint.json"
BATCH_SIZE = 25  # threads per API call
MAX_WORKERS = 15  # parallel API calls
MODEL_NAME = "gemini-2.0-flash"

# Initialize Gemini - load from .env file
API_KEY = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
if not API_KEY:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                if "GEMINI_API_KEY" in line:
                    API_KEY = line.split(":", 1)[1].strip()
                    break
if not API_KEY:
    raise ValueError("Please set GEMINI_API_KEY")

genai.configure(api_key=API_KEY)
model = genai.GenerativeModel(MODEL_NAME)

EXTRACTION_PROMPT = """Extract all person names mentioned in the following email thread(s).
Return ONLY a JSON object with thread_id as keys and arrays of names as values.
Do NOT include email addresses, only proper names of people.
Do NOT include organization names, only individual people.
If no names are found, return an empty array.

Example output format:
{"thread_123": ["John Smith", "Jane Doe"], "thread_456": []}

Email threads to analyze:
"""


def extract_senders_receivers(thread):
    """Extract senders and receivers directly from message data."""
    senders = set()
    receivers = set()

    for msg in thread.get("messages", []):
        sender = msg.get("sender", "")
        if sender:
            # Clean sender name (remove email in brackets)
            clean_sender = sender.split("[")[0].strip()
            if clean_sender:
                senders.add(clean_sender)

        for receiver in msg.get("receivers", []):
            if receiver:
                clean_receiver = receiver.split("[")[0].strip()
                if clean_receiver:
                    receivers.add(clean_receiver)

    return list(senders), list(receivers)


def prepare_batch_text(batch):
    """Prepare text for a batch of threads."""
    texts = []
    for thread in batch:
        thread_text = f"\n--- THREAD ID: {thread['thread_id']} ---\n"
        thread_text += f"Subject: {thread.get('subject', 'No subject')}\n"
        for msg in thread.get("messages", []):
            body = msg.get("body", "")
            if body:
                # Truncate very long messages
                thread_text += body[:2000] + "\n"
        texts.append(thread_text)
    return "\n".join(texts)


def extract_names_batch(batch, retry_count=3):
    """Extract names from a batch of threads using Gemini."""
    batch_text = prepare_batch_text(batch)
    prompt = EXTRACTION_PROMPT + batch_text

    for attempt in range(retry_count):
        try:
            response = model.generate_content(prompt)
            response_text = response.text.strip()

            # Parse JSON from response
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]

            result = json.loads(response_text)
            return result
        except json.JSONDecodeError:
            # Try to extract JSON from response
            try:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                if start >= 0 and end > start:
                    result = json.loads(response_text[start:end])
                    return result
            except:
                pass
        except Exception as e:
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Error processing batch: {e}")
                return {}

    return {}


def load_checkpoint():
    """Load checkpoint if exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"processed_ids": [], "results": {}}


def save_checkpoint(checkpoint):
    """Save checkpoint."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f)


def main():
    # Load data
    print("Loading data...")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    print(f"Total threads: {len(data)}")

    # Load checkpoint
    checkpoint = load_checkpoint()
    processed_ids = set(checkpoint["processed_ids"])
    results = checkpoint["results"]

    # Filter unprocessed threads
    threads_to_process = [t for t in data if t["thread_id"] not in processed_ids]
    print(f"Already processed: {len(processed_ids)}")
    print(f"Remaining: {len(threads_to_process)}")

    if not threads_to_process:
        print("All threads already processed!")
    else:
        # Create batches
        batches = [threads_to_process[i:i+BATCH_SIZE]
                   for i in range(0, len(threads_to_process), BATCH_SIZE)]

        print(f"Processing {len(batches)} batches with {MAX_WORKERS} workers...")

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(extract_names_batch, batch): batch
                      for batch in batches}

            with tqdm(total=len(batches), desc="Extracting names") as pbar:
                for future in as_completed(futures):
                    batch = futures[future]
                    try:
                        batch_results = future.result()

                        # Store results
                        for thread in batch:
                            tid = thread["thread_id"]
                            results[tid] = batch_results.get(tid, [])
                            processed_ids.add(tid)

                        # Save checkpoint every batch
                        checkpoint["processed_ids"] = list(processed_ids)
                        checkpoint["results"] = results
                        save_checkpoint(checkpoint)

                    except Exception as e:
                        print(f"Batch failed: {e}")

                    pbar.update(1)

    # Build final enriched dataset
    print("\nBuilding enriched dataset...")
    enriched_data = []

    for thread in tqdm(data, desc="Enriching threads"):
        tid = thread["thread_id"]
        senders, receivers = extract_senders_receivers(thread)

        enriched_thread = {
            **thread,
            "senders": senders,
            "receivers": receivers,
            "people_mentioned": results.get(tid, [])
        }
        enriched_data.append(enriched_thread)

    # Save enriched data
    print(f"\nSaving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(enriched_data, f, indent=2)

    print("Done!")
    print(f"Enriched {len(enriched_data)} threads")


if __name__ == "__main__":
    main()
