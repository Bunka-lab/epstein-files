"""
FINAL Enrich Network Tables
Creates FINAL_5_networks_community and enriches FINAL_5_network_nodes
Input: FINAL_6_CLASSIFIER_cluster_analysis, FINAL_4_CLASSIFIER_relationship_description, FINAL_2_CLASSIFIER_name_extraction, FINAL_3_unique_names
Output: FINAL_5_networks_community, Updated FINAL_5_network_nodes
"""

import sqlite3
import json
from collections import defaultdict
from tqdm import tqdm

DB_PATH = "../../epstein_analysis.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_community_table():
    """Create FINAL_5_networks_community from cluster analysis results"""
    print("Creating FINAL_5_networks_community table...")

    conn = get_connection()
    cursor = conn.cursor()

    # Get cluster analysis data
    cursor.execute('''
        SELECT cluster_id, size, all_members, "cluster_description [F4]"
        FROM "FINAL_6_CLASSIFIER_cluster_analysis"
        ORDER BY size DESC
    ''')
    clusters = cursor.fetchall()

    # Create new table
    cursor.execute('DROP TABLE IF EXISTS "FINAL_5_networks_community"')
    cursor.execute('''
        CREATE TABLE "FINAL_5_networks_community" (
            cluster_id INTEGER PRIMARY KEY,
            size INTEGER,
            top_10_members TEXT,
            all_members TEXT,
            "cluster_description [F4]" TEXT
        )
    ''')

    for cluster_id, size, all_members_json, description in clusters:
        all_members = json.loads(all_members_json)
        top_10 = json.dumps(all_members[:10])

        cursor.execute('''
            INSERT INTO "FINAL_5_networks_community"
            (cluster_id, size, top_10_members, all_members, "cluster_description [F4]")
            VALUES (?, ?, ?, ?, ?)
        ''', (cluster_id, size, top_10, all_members_json, description))

    conn.commit()
    conn.close()

    print(f"Created FINAL_5_networks_community with {len(clusters)} communities")
    return len(clusters)


def get_discussion_ids_for_person(canonical_name):
    """Get all discussion IDs where a person appears"""
    conn = get_connection()
    cursor = conn.cursor()

    # Get all name variants for this canonical name
    cursor.execute('''
        SELECT name_extracted FROM "FINAL_3_unique_names"
        WHERE "canonical_name [F2d]" = ?
    ''', (canonical_name,))
    variants = [row[0] for row in cursor.fetchall()]

    if not variants:
        conn.close()
        return []

    # Get all thread_ids where any variant appears
    cursor.execute('''
        SELECT thread_id, "names_mentioned [F1]"
        FROM "FINAL_2_CLASSIFIER_name_extraction"
    ''')

    thread_ids = set()
    for row in cursor.fetchall():
        thread_id, names_json = row
        if names_json:
            try:
                names = json.loads(names_json)
                if any(v in names for v in variants):
                    thread_ids.add(thread_id)
            except:
                pass

    conn.close()
    return sorted(list(thread_ids))


def enrich_network_nodes():
    """Add relationship description, cluster description, and discussion IDs to network nodes"""
    print("\nEnriching FINAL_5_network_nodes table...")

    conn = get_connection()
    cursor = conn.cursor()

    # Get relationship descriptions
    cursor.execute('''
        SELECT canonical_name, "relationship_description [F3]"
        FROM "FINAL_4_CLASSIFIER_relationship_description"
    ''')
    relationships = {row[0]: row[1] for row in cursor.fetchall()}
    print(f"Loaded {len(relationships)} relationship descriptions")

    # Get cluster descriptions
    cursor.execute('''
        SELECT cluster_id, "cluster_description [F4]"
        FROM "FINAL_5_networks_community"
    ''')
    cluster_descriptions = {row[0]: row[1] for row in cursor.fetchall()}
    print(f"Loaded {len(cluster_descriptions)} cluster descriptions")

    # Get all nodes
    cursor.execute('''
        SELECT "canonical_name [F2d]", community
        FROM "FINAL_5_network_nodes"
    ''')
    nodes = cursor.fetchall()
    print(f"Processing {len(nodes)} nodes...")

    # Add new columns if they don't exist
    try:
        cursor.execute('ALTER TABLE "FINAL_5_network_nodes" ADD COLUMN "relationship_description [F3]" TEXT')
        print("Added column 'relationship_description [F3]'")
    except:
        print("Column 'relationship_description [F3]' already exists")

    try:
        cursor.execute('ALTER TABLE "FINAL_5_network_nodes" ADD COLUMN "cluster_description [F4]" TEXT')
        print("Added column 'cluster_description [F4]'")
    except:
        print("Column 'cluster_description [F4]' already exists")

    try:
        cursor.execute('ALTER TABLE "FINAL_5_network_nodes" ADD COLUMN "discussion_ids" TEXT')
        print("Added column 'discussion_ids'")
    except:
        print("Column 'discussion_ids' already exists")

    conn.commit()

    # Update each node
    for canonical_name, community in tqdm(nodes, desc="Enriching nodes"):
        # Get relationship description
        rel_desc = relationships.get(canonical_name, "No relationship description available")

        # Get cluster description
        cluster_desc = cluster_descriptions.get(community, "No cluster description available")

        # Get discussion IDs
        discussion_ids = get_discussion_ids_for_person(canonical_name)
        discussion_ids_json = json.dumps(discussion_ids)

        cursor.execute('''
            UPDATE "FINAL_5_network_nodes"
            SET "relationship_description [F3]" = ?,
                "cluster_description [F4]" = ?,
                "discussion_ids" = ?
            WHERE "canonical_name [F2d]" = ?
        ''', (rel_desc, cluster_desc, discussion_ids_json, canonical_name))

    conn.commit()
    conn.close()

    print(f"\nEnriched {len(nodes)} nodes with relationship descriptions, cluster descriptions, and discussion IDs")


def show_sample():
    """Show a sample of the enriched data"""
    conn = get_connection()
    cursor = conn.cursor()

    print("\n" + "=" * 80)
    print("SAMPLE: FINAL_5_networks_community")
    print("=" * 80)

    cursor.execute('''
        SELECT cluster_id, size, top_10_members, "cluster_description [F4]"
        FROM "FINAL_5_networks_community"
        ORDER BY size DESC
        LIMIT 3
    ''')

    for row in cursor.fetchall():
        cluster_id, size, top_10, desc = row
        top_10_list = json.loads(top_10)
        print(f"\nCluster {cluster_id} ({size} members)")
        print(f"Top 10: {', '.join(top_10_list)}")
        print(f"Description: {desc[:200]}...")

    print("\n" + "=" * 80)
    print("SAMPLE: FINAL_5_network_nodes (enriched)")
    print("=" * 80)

    cursor.execute('''
        SELECT "canonical_name [F2d]", occurrences, degree, community,
               "relationship_description [F3]", "cluster_description [F4]", "discussion_ids"
        FROM "FINAL_5_network_nodes"
        ORDER BY degree DESC
        LIMIT 3
    ''')

    for row in cursor.fetchall():
        name, occ, deg, comm, rel_desc, cluster_desc, disc_ids = row
        disc_list = json.loads(disc_ids) if disc_ids else []
        print(f"\n{name}")
        print(f"  Occurrences: {occ}, Degree: {deg}, Community: {comm}")
        print(f"  Relationship: {rel_desc[:150]}...")
        print(f"  Cluster: {cluster_desc[:100]}...")
        print(f"  Discussion IDs ({len(disc_list)}): {disc_list[:5]}...")

    conn.close()


if __name__ == "__main__":
    create_community_table()
    enrich_network_nodes()
    show_sample()
    print("\nDone!")
