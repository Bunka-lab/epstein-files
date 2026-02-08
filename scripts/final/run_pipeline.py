"""
FINAL PIPELINE
Runs the complete classification pipeline on the full dataset.

Pipeline steps:
1. F1: Name extraction (from sender, receiver, CC, body)
2. Create unique names table
3. F2: Name consolidation (last names only)
4. Build network (Leiden communities)
5. F3: Relationship description
6. F4: Cluster analysis

Output tables:
- FINAL_2_CLASSIFIER_name_extraction
- FINAL_3_unique_names
- FINAL_5_network_nodes
- FINAL_5_network_edges
- FINAL_4_CLASSIFIER_relationship_description
- FINAL_6_CLASSIFIER_cluster_analysis

Estimated cost: $15-20 for full dataset (16,447 emails)
"""

import subprocess
import sys
import time
from datetime import datetime


def run_script(name, script_path):
    """Run a Python script and return success status"""
    print(f"\n{'='*70}")
    print(f"STEP: {name}")
    print(f"Script: {script_path}")
    print(f"Started: {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*70}\n")

    start = time.time()

    result = subprocess.run(
        [sys.executable, script_path],
        cwd="/Users/charlesdedampierre/Desktop/Bunka Folder/epstein-files/scripts/final"
    )

    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.1f}s ({elapsed/60:.1f} min)")

    if result.returncode != 0:
        print(f"ERROR: {name} failed with return code {result.returncode}")
        return False

    return True


def main():
    print("=" * 70)
    print("FINAL PIPELINE - Full Dataset Processing")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nThis will process the full dataset (16,447 emails)")
    print("Estimated cost: $15-20")
    print("Estimated time: 30-60 minutes")
    print("\nPipeline steps:")
    print("  1. F1: Name extraction")
    print("  2. Create unique names table")
    print("  3. F2: Name consolidation")
    print("  4. Build network")
    print("  5. F3: Relationship description")
    print("  6. F4: Cluster analysis")

    input("\nPress Enter to start the pipeline...")

    total_start = time.time()
    steps = [
        ("F1: Name Extraction", "run_name_extraction.py"),
        ("Create Unique Names", "create_unique_names.py"),
        ("F2: Name Consolidation", "run_name_consolidation.py"),
        ("Build Network", "build_network.py"),
        ("F3: Relationship Description", "run_relationship_description.py"),
        ("F4: Cluster Analysis", "analyze_clusters.py"),
    ]

    results = []
    for name, script in steps:
        success = run_script(name, script)
        results.append((name, success))
        if not success:
            print(f"\n\nPIPELINE STOPPED: {name} failed")
            break

    total_elapsed = time.time() - total_start

    print("\n" + "=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
    print("\nResults:")
    for name, success in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"  {status} - {name}")

    # Get total cost from database
    import sqlite3
    conn = sqlite3.connect("../../epstein_analysis.db")
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(total_cost) FROM ai_classification_runs WHERE run_id LIKE 'F%'")
    total_cost = cursor.fetchone()[0] or 0
    conn.close()

    print(f"\nTotal API cost: ${total_cost:.4f}")
    print("\nOutput tables created:")
    print("  - FINAL_2_CLASSIFIER_name_extraction")
    print("  - FINAL_3_unique_names")
    print("  - FINAL_5_network_nodes")
    print("  - FINAL_5_network_edges")
    print("  - FINAL_4_CLASSIFIER_relationship_description")
    print("  - FINAL_6_CLASSIFIER_cluster_analysis")
    print("\nVisualization: visualization/FINAL_network.html")


if __name__ == "__main__":
    main()
