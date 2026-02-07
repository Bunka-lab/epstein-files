"""
Build co-occurrence network from Epstein emails with Leiden communities.
Uses consolidated names from name_matching_table.json.
"""

import json
from collections import Counter, defaultdict
from itertools import combinations
import networkx as nx
import pandas as pd
import leidenalg as la
from igraph import Graph
from ipysigma import Sigma

# Load name mapping
with open("data/name_matching_table.json") as f:
    name_mapping = json.load(f)


def consolidate_name(name):
    """Consolidate name using mapping table."""
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


# Load enriched data
with open("data/epstein_discussions_filtered.json") as f:
    data = json.load(f)

print(f"Loaded {len(data)} discussions")

# Get person counts (consolidated)
person_counts = Counter()
for disc in data:
    all_persons = set()
    for p in (
        disc.get("senders", [])
        + disc.get("receivers", [])
        + disc.get("people_mentioned", [])
    ):
        consolidated = consolidate_name(p)
        if consolidated:
            all_persons.add(consolidated)
    for p in all_persons:
        person_counts[p] += 1

print(f"Total unique persons (consolidated): {len(person_counts)}")

# Load names to exclude
with open("data/names_to_remove.json") as f:
    exclude = set(json.load(f))

# Filter: only people appearing 3+ times, excluding the exclude set
valid_persons = {
    p
    for p, count in person_counts.items()
    if count >= 3 and p not in exclude and len(p) > 2
}

print(f"Valid persons (>=3 appearances): {len(valid_persons)}")

# Build edges
edges = Counter()
edge_examples = defaultdict(list)

for disc in data:
    all_persons = set()
    for p in (
        disc.get("senders", [])
        + disc.get("receivers", [])
        + disc.get("people_mentioned", [])
    ):
        consolidated = consolidate_name(p)
        if consolidated and consolidated in valid_persons:
            all_persons.add(consolidated)

    persons_list = sorted(all_persons)
    subject = disc.get("subject", "")[:80]

    for p1, p2 in combinations(persons_list, 2):
        edges[(p1, p2)] += 1
        if subject and len(edge_examples[(p1, p2)]) < 3:
            edge_examples[(p1, p2)].append(subject)

# Create NetworkX graph
G = nx.Graph()
for (p1, p2), weight in edges.items():
    if weight >= 2:  # Only edges with 2+ co-occurrences
        G.add_edge(p1, p2, weight=weight)

# Remove isolated nodes
G.remove_nodes_from(list(nx.isolates(G)))

# Set node attributes
for node in G.nodes():
    G.nodes[node]["size"] = person_counts[node]
    G.nodes[node]["label"] = node

# Leiden community detection
ig_graph = Graph.from_networkx(G)
part = la.find_partition(
    ig_graph, la.ModularityVertexPartition, n_iterations=100, seed=42
)

node_names = [v["_nx_name"] for v in ig_graph.vs]
for cluster_id, members in enumerate(part):
    for idx in members:
        G.nodes[node_names[idx]]["community"] = cluster_id

print(f"Nodes: {G.number_of_nodes()}")
print(f"Edges: {G.number_of_edges()}")
print(f"Communities: {len(part)}")

# Save with Sigma
Sigma.write_html(
    G,
    "network.html",
    fullscreen=True,
    node_size="size",
    node_size_range=(3, 20),
    node_color="community",
    node_label="label",
    node_label_size="size",
    node_label_size_range=(10, 22),
    edge_size="weight",
    edge_size_range=(0.5, 4),
    default_edge_type="curve",
)
print("Saved: network.html")

# Save edges CSV
edge_data = []
for (p1, p2), weight in edges.items():
    if weight >= 2 and p1 in G.nodes() and p2 in G.nodes():
        examples = " | ".join(edge_examples[(p1, p2)])
        edge_data.append(
            {"source": p1, "target": p2, "weight": weight, "examples": examples}
        )

pd.DataFrame(edge_data).to_csv("data/network_edges.csv", index=False)
print("Saved: data/network_edges.csv")

# Print top connected people
print("\nTop 20 most connected:")
degrees = sorted(G.degree(), key=lambda x: x[1], reverse=True)[:20]
for name, degree in degrees:
    print(f"  {degree:3d} connections - {name}")
