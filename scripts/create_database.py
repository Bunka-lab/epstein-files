"""
Create SQLite database with all Epstein analysis data.
Organized with AI classifier tables and metadata.
"""

import sqlite3
import json
from datetime import datetime

DB_PATH = "data/epstein_analysis.db"

# Connect and create database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("Creating database schema...")

# ============================================
# META TABLES
# ============================================

# Classification versions/runs metadata
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classification_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_name TEXT NOT NULL,
    run_type TEXT NOT NULL,
    model_used TEXT,
    prompt_used TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
)
""")

# Prompts library
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_prompts (
    prompt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    prompt_name TEXT NOT NULL,
    prompt_text TEXT NOT NULL,
    prompt_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# ============================================
# RAW DATA TABLES
# ============================================

# All discussions (raw)
cursor.execute("""
CREATE TABLE IF NOT EXISTS discussions (
    thread_id TEXT PRIMARY KEY,
    subject TEXT,
    message_count INTEGER,
    messages_json TEXT,
    is_filtered_out BOOLEAN DEFAULT 0,
    filter_reason TEXT
)
""")

# ============================================
# AI CLASSIFIER TABLES
# ============================================

# Extracted names from emails
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classifier_extracted_names (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT,
    name_raw TEXT,
    name_type TEXT,  -- sender, receiver, mentioned
    run_id INTEGER,
    FOREIGN KEY (thread_id) REFERENCES discussions(thread_id),
    FOREIGN KEY (run_id) REFERENCES ai_classification_runs(run_id)
)
""")

# Names consolidation mapping
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classifier_names_consolidation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name_variant TEXT NOT NULL,
    name_canonical TEXT NOT NULL,
    run_id INTEGER,
    FOREIGN KEY (run_id) REFERENCES ai_classification_runs(run_id)
)
""")

# Names to remove
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classifier_names_to_remove (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    removal_reason TEXT,
    run_id INTEGER,
    FOREIGN KEY (run_id) REFERENCES ai_classification_runs(run_id)
)
""")

# Relationships with Epstein
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classifier_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_name TEXT NOT NULL,
    relationship_description TEXT,
    appearance_count INTEGER,
    run_id INTEGER,
    FOREIGN KEY (run_id) REFERENCES ai_classification_runs(run_id)
)
""")

# Relationship source threads
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classifier_relationship_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_name TEXT,
    thread_id TEXT,
    FOREIGN KEY (thread_id) REFERENCES discussions(thread_id)
)
""")

# Journalist requests (filtered out)
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classifier_journalist_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT,
    run_id INTEGER,
    FOREIGN KEY (thread_id) REFERENCES discussions(thread_id),
    FOREIGN KEY (run_id) REFERENCES ai_classification_runs(run_id)
)
""")

# Cluster analysis
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classifier_clusters (
    cluster_id INTEGER PRIMARY KEY,
    cluster_name TEXT,
    cluster_size INTEGER,
    analysis_text TEXT,
    run_id INTEGER,
    FOREIGN KEY (run_id) REFERENCES ai_classification_runs(run_id)
)
""")

# Cluster membership
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_classifier_cluster_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_id INTEGER,
    person_name TEXT,
    FOREIGN KEY (cluster_id) REFERENCES ai_classifier_clusters(cluster_id)
)
""")

# Network edges
cursor.execute("""
CREATE TABLE IF NOT EXISTS network_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT,
    target_name TEXT,
    weight INTEGER,
    examples TEXT
)
""")

conn.commit()
print("Schema created.")

# ============================================
# INSERT DATA
# ============================================

print("\nInserting classification runs metadata...")

# Insert classification run records
runs = [
    ("name_extraction_v1", "name_extraction", "gemini-2.0-flash", 
     "Extract senders, receivers, and people mentioned from email threads", None),
    ("name_consolidation_v1", "name_consolidation", "gemini-2.0-flash",
     "Map name variants to canonical forms", None),
    ("journalist_filter_v1", "filtering", "gemini-2.0-flash",
     "Identify journalist interview requests to filter out", None),
    ("relationship_extraction_v1", "relationship", "gemini-2.0-flash",
     "Analyze relationship between each person and Epstein", None),
    ("cluster_analysis_v1", "clustering", "gemini-2.0-flash",
     "Analyze and name network clusters based on member relationships", None),
]

for run in runs:
    cursor.execute("""
        INSERT INTO ai_classification_runs (run_name, run_type, model_used, prompt_used, notes)
        VALUES (?, ?, ?, ?, ?)
    """, run)

conn.commit()

# Get run IDs
cursor.execute("SELECT run_id, run_name FROM ai_classification_runs")
run_ids = {name: rid for rid, name in cursor.fetchall()}

print("Inserting discussions...")

# Load and insert discussions
with open("data/epstein_discussions_names.json") as f:
    all_discussions = json.load(f)

with open("data/journalist_request_ids.json") as f:
    journalist_ids = set(json.load(f))

for disc in all_discussions:
    thread_id = disc.get("thread_id", "")
    is_filtered = thread_id in journalist_ids
    cursor.execute("""
        INSERT OR REPLACE INTO discussions (thread_id, subject, message_count, messages_json, is_filtered_out, filter_reason)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        thread_id,
        disc.get("subject", ""),
        disc.get("message_count", 0),
        json.dumps(disc.get("messages", [])),
        is_filtered,
        "journalist_request" if is_filtered else None
    ))

    # Insert extracted names
    run_id = run_ids["name_extraction_v1"]
    for name in disc.get("senders", []):
        cursor.execute("""
            INSERT INTO ai_classifier_extracted_names (thread_id, name_raw, name_type, run_id)
            VALUES (?, ?, ?, ?)
        """, (thread_id, name, "sender", run_id))
    
    for name in disc.get("receivers", []):
        cursor.execute("""
            INSERT INTO ai_classifier_extracted_names (thread_id, name_raw, name_type, run_id)
            VALUES (?, ?, ?, ?)
        """, (thread_id, name, "receiver", run_id))
    
    for name in disc.get("people_mentioned", []):
        cursor.execute("""
            INSERT INTO ai_classifier_extracted_names (thread_id, name_raw, name_type, run_id)
            VALUES (?, ?, ?, ?)
        """, (thread_id, name, "mentioned", run_id))

conn.commit()
print(f"  Inserted {len(all_discussions)} discussions")

print("Inserting name consolidation mappings...")

with open("data/name_matching_table.json") as f:
    name_mapping = json.load(f)

run_id = run_ids["name_consolidation_v1"]
for variant, canonical in name_mapping.items():
    cursor.execute("""
        INSERT INTO ai_classifier_names_consolidation (name_variant, name_canonical, run_id)
        VALUES (?, ?, ?)
    """, (variant, canonical, run_id))

conn.commit()
print(f"  Inserted {len(name_mapping)} mappings")

print("Inserting names to remove...")

with open("data/names_to_remove.json") as f:
    names_to_remove = json.load(f)

run_id = run_ids["name_consolidation_v1"]
for name in names_to_remove:
    cursor.execute("""
        INSERT INTO ai_classifier_names_to_remove (name, removal_reason, run_id)
        VALUES (?, ?, ?)
    """, (name, "generic_or_incomplete", run_id))

conn.commit()
print(f"  Inserted {len(names_to_remove)} names")

print("Inserting journalist request IDs...")

run_id = run_ids["journalist_filter_v1"]
for thread_id in journalist_ids:
    cursor.execute("""
        INSERT INTO ai_classifier_journalist_requests (thread_id, run_id)
        VALUES (?, ?)
    """, (thread_id, run_id))

conn.commit()
print(f"  Inserted {len(journalist_ids)} journalist requests")

print("Inserting relationships...")

with open("data/person_relationships.json") as f:
    relationships = json.load(f)

run_id = run_ids["relationship_extraction_v1"]
for rel in relationships:
    cursor.execute("""
        INSERT INTO ai_classifier_relationships (person_name, relationship_description, appearance_count, run_id)
        VALUES (?, ?, ?, ?)
    """, (rel["name"], rel["relationship"], rel["count"], run_id))
    
    # Insert source threads
    for thread_id in rel.get("thread_ids", []):
        cursor.execute("""
            INSERT INTO ai_classifier_relationship_sources (person_name, thread_id)
            VALUES (?, ?)
        """, (rel["name"], thread_id))

conn.commit()
print(f"  Inserted {len(relationships)} relationships")

print("Inserting cluster analysis...")

with open("data/cluster_analysis.json") as f:
    clusters = json.load(f)

run_id = run_ids["cluster_analysis_v1"]
for cluster in clusters:
    cursor.execute("""
        INSERT INTO ai_classifier_clusters (cluster_id, cluster_name, cluster_size, analysis_text, run_id)
        VALUES (?, ?, ?, ?, ?)
    """, (cluster["cluster_id"], cluster.get("name", ""), cluster["size"], cluster["analysis"], run_id))
    
    for member in cluster["members"]:
        cursor.execute("""
            INSERT INTO ai_classifier_cluster_members (cluster_id, person_name)
            VALUES (?, ?)
        """, (cluster["cluster_id"], member))

conn.commit()
print(f"  Inserted {len(clusters)} clusters")

print("Inserting network edges...")

import csv
with open("data/network_edges.csv") as f:
    reader = csv.DictReader(f)
    edges = list(reader)

for edge in edges:
    cursor.execute("""
        INSERT INTO network_edges (source_name, target_name, weight, examples)
        VALUES (?, ?, ?, ?)
    """, (edge["source"], edge["target"], int(edge["weight"]), edge.get("examples", "")))

conn.commit()
print(f"  Inserted {len(edges)} edges")

# ============================================
# SUMMARY
# ============================================

print("\n" + "="*50)
print("DATABASE CREATED: data/epstein_analysis.db")
print("="*50)

cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cursor.fetchall()

print("\nTables:")
for (table,) in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  {table}: {count} rows")

conn.close()
print("\nDone!")
