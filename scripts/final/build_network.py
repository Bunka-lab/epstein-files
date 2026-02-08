"""
FINAL Build Network
Creates co-occurrence network with Leiden communities
Input: FINAL_2_CLASSIFIER_name_extraction, FINAL_3_unique_names
Output: FINAL_5_network_nodes, FINAL_5_network_edges, visualization/FINAL_network.html
"""

import sqlite3
import json
from collections import Counter, defaultdict
from itertools import combinations
import networkx as nx
import leidenalg as la
from igraph import Graph
from ipysigma import Sigma
from tqdm import tqdm

DB_PATH = "../../epstein_analysis.db"
INPUT_TABLE_EMAILS = "FINAL_2_CLASSIFIER_name_extraction"
INPUT_TABLE_NAMES = "FINAL_3_unique_names"


def get_connection():
    return sqlite3.connect(DB_PATH)


def main():
    # Load name mapping
    print("Loading name consolidation mapping...")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f'SELECT name_extracted, "canonical_name [F2d]" FROM "{INPUT_TABLE_NAMES}"')
    name_mapping = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()

    print(f"Loaded {len(name_mapping)} name mappings")

    # Names to exclude (Jeffrey Epstein and variants)
    EXCLUDED_NAMES = {
        "Jeffrey Epstein", "Jeffrey E.", "Jeffrey E", "Jeffrey", "E. Jeffrey",
        "Epstein", "Jeff Epstein", "J. Epstein", "JE", "J.E.", "jeffrey E.",
        "jeffrey", "JEFFREY", "Jeff", "jeff"
    }

    def consolidate_name(name):
        if not name:
            return None
        canonical = name_mapping.get(name)
        if canonical == "None" or canonical is None:
            return None
        # Exclude Jeffrey Epstein variants
        if canonical in EXCLUDED_NAMES or name in EXCLUDED_NAMES:
            return None
        return canonical

    # Load email data
    print(f"Loading email data from {INPUT_TABLE_EMAILS}...")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT thread_id, sender, receiver, cc, body, "names_mentioned [F1]"
        FROM "{INPUT_TABLE_EMAILS}"
    ''')
    emails = cursor.fetchall()
    conn.close()

    print(f"Loaded {len(emails)} emails")

    # Get person counts
    person_counts = Counter()
    email_persons = []

    for email in tqdm(emails, desc="Processing emails"):
        thread_id, sender, receiver, cc, body, names_json = email
        all_persons = set()

        if names_json:
            try:
                names = json.loads(names_json)
                for name in names:
                    consolidated = consolidate_name(name)
                    if consolidated and consolidated != "Jeffrey Epstein":
                        all_persons.add(consolidated)
            except:
                pass

        email_persons.append((thread_id, body, all_persons))
        for p in all_persons:
            person_counts[p] += 1

    print(f"Total unique persons (consolidated): {len(person_counts)}")

    # Filter: people appearing 4+ times (must appear in at least 4 discussions)
    valid_persons = {p for p, count in person_counts.items() if count >= 4}
    print(f"Valid persons (>=4 appearances): {len(valid_persons)}")

    # Build edges
    edges = Counter()
    edge_examples = defaultdict(list)

    for thread_id, body, all_persons in tqdm(email_persons, desc="Building edges"):
        persons_in_email = {p for p in all_persons if p in valid_persons}
        persons_list = sorted(persons_in_email)
        excerpt = (body or "")[:80].replace("\n", " ")

        for p1, p2 in combinations(persons_list, 2):
            edges[(p1, p2)] += 1
            if excerpt and len(edge_examples[(p1, p2)]) < 3:
                edge_examples[(p1, p2)].append(excerpt)

    print(f"Total edges: {len(edges)}")

    # Create NetworkX graph
    G = nx.Graph()
    for (p1, p2), weight in edges.items():
        if weight >= 1:
            G.add_edge(p1, p2, weight=weight)

    G.remove_nodes_from(list(nx.isolates(G)))

    for node in G.nodes():
        G.nodes[node]["size"] = person_counts[node]
        G.nodes[node]["label"] = node

    print(f"Nodes in graph: {G.number_of_nodes()}")
    print(f"Edges in graph: {G.number_of_edges()}")

    if G.number_of_nodes() > 0:
        # Leiden community detection
        ig_graph = Graph.from_networkx(G)
        part = la.find_partition(ig_graph, la.ModularityVertexPartition, n_iterations=100, seed=42)

        node_names = [v["_nx_name"] for v in ig_graph.vs]
        for cluster_id, members in enumerate(part):
            for idx in members:
                G.nodes[node_names[idx]]["community"] = cluster_id

        print(f"Communities: {len(part)}")

        # Save visualization
        output_html = "../../visualization/FINAL_network.html"
        Sigma.write_html(
            G,
            output_html,
            fullscreen=True,
            start_layout=True,
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
        print(f"Saved: {output_html}")

        # Save to database
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute('DROP TABLE IF EXISTS "FINAL_5_network_edges"')
        cursor.execute('''
            CREATE TABLE "FINAL_5_network_edges" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                "source [F2d]" TEXT,
                "target [F2d]" TEXT,
                weight INTEGER,
                examples TEXT
            )
        ''')

        for (p1, p2), weight in edges.items():
            if p1 in G.nodes() and p2 in G.nodes():
                examples = " | ".join(edge_examples[(p1, p2)])
                cursor.execute('''
                    INSERT INTO "FINAL_5_network_edges" ("source [F2d]", "target [F2d]", weight, examples)
                    VALUES (?, ?, ?, ?)
                ''', (p1, p2, weight, examples))

        cursor.execute('DROP TABLE IF EXISTS "FINAL_5_network_nodes"')
        cursor.execute('''
            CREATE TABLE "FINAL_5_network_nodes" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                "canonical_name [F2d]" TEXT UNIQUE,
                occurrences INTEGER,
                degree INTEGER,
                community INTEGER
            )
        ''')

        for node in G.nodes():
            cursor.execute('''
                INSERT INTO "FINAL_5_network_nodes" ("canonical_name [F2d]", occurrences, degree, community)
                VALUES (?, ?, ?, ?)
            ''', (node, person_counts[node], G.degree(node), G.nodes[node].get("community", 0)))

        conn.commit()
        conn.close()

        print("Saved: FINAL_5_network_edges")
        print("Saved: FINAL_5_network_nodes")

        # Print top connected
        print("\nTop 20 most connected:")
        degrees = sorted(G.degree(), key=lambda x: x[1], reverse=True)[:20]
        for name, degree in degrees:
            comm = G.nodes[name].get("community", "?")
            print(f"  {degree:3d} connections - {name} (community {comm})")


if __name__ == "__main__":
    main()
