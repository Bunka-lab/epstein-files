"""
Analyze network clusters using Gemini.
Combines community membership with relationship data.
"""

import json
import google.generativeai as genai
from collections import defaultdict, Counter
import networkx as nx
import leidenalg as la
from igraph import Graph
from tqdm import tqdm

# Configure Gemini
genai.configure(api_key='AIzaSyBDV9HJ_RJnZ3g7UQZhXssiF_vGrUbDFOk')
model = genai.GenerativeModel('gemini-2.0-flash')

# Load name mapping
with open("data/name_matching_table.json") as f:
    name_mapping = json.load(f)

# Load relationships
with open("data/person_relationships.json") as f:
    rel_data = json.load(f)
    person_relationships = {p["name"]: p["relationship"] for p in rel_data}

# Load exclusions
with open("data/names_to_remove.json") as f:
    exclude = set(json.load(f))

# Load filtered data
with open("data/epstein_discussions_filtered.json") as f:
    data = json.load(f)

print(f"Loaded {len(data)} discussions")

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

# Build network (same as build_network.py)
person_counts = Counter()
for disc in data:
    all_persons = set()
    for p in disc.get("senders", []) + disc.get("receivers", []) + disc.get("people_mentioned", []):
        consolidated = consolidate_name(p)
        if consolidated:
            all_persons.add(consolidated)
    for p in all_persons:
        person_counts[p] += 1

valid_persons = {
    p for p, count in person_counts.items()
    if count >= 3 and p not in exclude and len(p) > 2
}

edges = Counter()
for disc in data:
    all_persons = set()
    for p in disc.get("senders", []) + disc.get("receivers", []) + disc.get("people_mentioned", []):
        consolidated = consolidate_name(p)
        if consolidated and consolidated in valid_persons:
            all_persons.add(consolidated)
    from itertools import combinations
    for p1, p2 in combinations(sorted(all_persons), 2):
        edges[(p1, p2)] += 1

G = nx.Graph()
for (p1, p2), weight in edges.items():
    if weight >= 2:
        G.add_edge(p1, p2, weight=weight)

G.remove_nodes_from(list(nx.isolates(G)))

for node in G.nodes():
    G.nodes[node]["size"] = person_counts[node]

# Leiden clustering
ig_graph = Graph.from_networkx(G)
part = la.find_partition(ig_graph, la.ModularityVertexPartition, n_iterations=100, seed=42)

node_names = [v["_nx_name"] for v in ig_graph.vs]
communities = defaultdict(list)
for cluster_id, members in enumerate(part):
    for idx in members:
        name = node_names[idx]
        communities[cluster_id].append({
            "name": name,
            "connections": G.degree(name),
            "appearances": person_counts[name],
            "relationship": person_relationships.get(name, "Unknown")
        })

# Sort communities by size
sorted_communities = sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)

print(f"\nFound {len(sorted_communities)} communities")
for cid, members in sorted_communities[:5]:
    print(f"  Community {cid}: {len(members)} members")

# Analyze each community with Gemini
cluster_analyses = []

for cluster_id, members in tqdm(sorted_communities, desc="Analyzing clusters"):
    # Sort members by connections
    members_sorted = sorted(members, key=lambda x: x["connections"], reverse=True)
    
    # Build context
    members_info = []
    for m in members_sorted[:15]:  # Top 15 members
        members_info.append(f"- {m['name']} ({m['connections']} connections): {m['relationship'][:200]}")
    
    members_text = "\n".join(members_info)
    all_names = [m["name"] for m in members_sorted]
    
    prompt = f"""Analyze this community/cluster from Jeffrey Epstein's email network.

Members ({len(members)} people, showing top 15):
{members_text}

Based on the relationships and people involved, write a concise analysis (3-5 sentences) that:
1. Identifies the main theme or characteristic of this cluster
2. Explains what connects these people
3. Notes any notable patterns or key figures

Write in English. Be factual and analytical."""

    try:
        response = model.generate_content(prompt)
        analysis = response.text.strip()
    except Exception as e:
        analysis = f"Error: {str(e)}"
    
    cluster_analyses.append({
        "cluster_id": cluster_id,
        "size": len(members),
        "members": all_names,
        "top_members": [m["name"] for m in members_sorted[:10]],
        "analysis": analysis
    })

# Save results
with open("data/cluster_analysis.json", "w") as f:
    json.dump(cluster_analyses, f, indent=2, ensure_ascii=False)

print(f"\nSaved {len(cluster_analyses)} cluster analyses to data/cluster_analysis.json")

# Print summary
print("\n" + "="*70)
print("CLUSTER ANALYSIS SUMMARY")
print("="*70)

for c in cluster_analyses:
    print(f"\n### Cluster {c['cluster_id']} ({c['size']} members)")
    print(f"Top members: {', '.join(c['top_members'][:5])}")
    print(f"\n{c['analysis']}")
    print("-"*70)
