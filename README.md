# Epstein Emails Network Analysis

Network analysis of the Epstein email dataset: AI-powered name extraction, multi-pass consolidation, relationship description, community detection, and interactive visualization.

---

## Project Structure

```
epstein-files/
├── data/
│   ├── raw_data/                           # Raw parquet data
│   └── classification/                     # AI classification exports (JSON)
│       ├── F1_name_extraction_final.json
│       ├── F2_name_consolidation_final.json
│       ├── F2b_name_consolidation_v2_final.json
│       ├── F2c_name_consolidation_v3_final.json
│       ├── F2d_name_consolidation_v4_final.json
│       ├── F3_relationship_description_final.json
│       └── F4_cluster_analysis_final.json
│
├── scripts/final/                          # Production pipeline
│   ├── run_pipeline.py                     # Master pipeline runner
│   ├── run_name_extraction.py              # F1: Extract names from emails
│   ├── run_name_consolidation.py           # F2: Initial name consolidation
│   ├── run_name_consolidation_v2.py        # F2b: Phonetic matching
│   ├── run_name_consolidation_v3.py        # F2c: Token-based matching
│   ├── run_name_consolidation_v4.py        # F2d: Suffix matching (Jr., Sr.)
│   ├── create_unique_names.py              # Build unique names table
│   ├── run_relationship_description.py    # F3: Describe relationships
│   ├── build_network.py                    # Build co-occurrence network
│   ├── analyze_clusters.py                 # F4: Analyze communities
│   └── enrich_network_tables.py            # Enrich nodes with metadata
│
├── visualization/
│   └── FINAL_network.html                  # Interactive network (Sigma.js)
│
├── epstein_analysis.db                     # SQLite database
├── .env                                    # ANTHROPIC_KEY
└── README.md
```

---

## Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  F1. NAME EXTRACTION                                                         │
│      run_name_extraction.py                                                  │
│      → Claude Sonnet extracts person names from each email                   │
│      → Output: FINAL_2_CLASSIFIER_name_extraction                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│  F2. NAME CONSOLIDATION (4 passes)                                           │
│      → F2:  Initial consolidation (variants → canonical)                     │
│      → F2b: Phonetic matching (phonetic-only names)                          │
│      → F2c: Token-based matching (reversed names: "Thomas Landon" → "Landon Thomas") │
│      → F2d: Suffix matching ("Landon Thomas" → "Landon Thomas Jr.")          │
│      → Output: FINAL_3_unique_names with columns [F2], [F2b], [F2c], [F2d]   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│  NETWORK BUILDING                                                            │
│      build_network.py                                                        │
│      → Filter: people appearing in 4+ discussions                            │
│      → Co-occurrence edges + Leiden community detection                      │
│      → Output: FINAL_5_network_nodes, FINAL_5_network_edges                  │
│      → Output: visualization/FINAL_network.html                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│  F3. RELATIONSHIP DESCRIPTION                                                │
│      run_relationship_description.py                                         │
│      → Claude Sonnet describes each person's relationship with Epstein       │
│      → Output: FINAL_4_CLASSIFIER_relationship_description                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│  F4. CLUSTER ANALYSIS                                                        │
│      analyze_clusters.py                                                     │
│      → Claude Sonnet analyzes each network community                         │
│      → Output: FINAL_6_CLASSIFIER_cluster_analysis                           │
│      → Output: FINAL_5_networks_community                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│  ENRICHMENT                                                                  │
│      enrich_network_tables.py                                                │
│      → Adds relationship descriptions and discussion IDs to network nodes   │
│      → Output: Updated FINAL_5_network_nodes                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Database Tables

### FINAL Pipeline Tables

| Table | Rows | Description |
|-------|------|-------------|
| `0_raw_data` | 5082 | Original parquet data |
| `FINAL_2_CLASSIFIER_name_extraction` | 5082 | Names mentioned per discussion [F1] |
| `FINAL_3_unique_names` | ~3300 | Unique names with 4 consolidation passes [F2-F2d] |
| `FINAL_4_CLASSIFIER_relationship_description` | 560 | Relationship descriptions [F3] |
| `FINAL_5_network_nodes` | 545 | Network nodes with metadata |
| `FINAL_5_network_edges` | ~2500 | Co-occurrence edges |
| `FINAL_5_networks_community` | 18 | Community descriptions [F4] |
| `FINAL_6_CLASSIFIER_cluster_analysis` | 18 | Cluster analysis results [F4] |
| `ai_classification_runs` | - | AI run metadata with prompts and costs |

### Column Naming Convention

All AI-classified columns are tagged with `[FX]` suffix for traceability:

| Suffix | Classification Run | Description |
|--------|-------------------|-------------|
| `[F1]` | name_extraction | Names mentioned in emails |
| `[F2]` | name_consolidation | Initial consolidation |
| `[F2b]` | name_consolidation_v2 | Phonetic matching |
| `[F2c]` | name_consolidation_v3 | Token-based matching |
| `[F2d]` | name_consolidation_v4 | Suffix matching |
| `[F3]` | relationship_description | Relationship with Epstein |
| `[F4]` | cluster_analysis | Community analysis |

---

## Network Statistics

| Metric | Value |
|--------|-------|
| Total discussions | 5082 |
| People in network | 545 |
| Network edges | ~2500 |
| Communities | 18 |
| Min occurrences filter | 4 discussions |

### Top Communities

| Cluster | Size | Theme | Top Members |
|---------|------|-------|-------------|
| 0 | 105 | Political monitoring (2016-2020) | Donald Trump, Steve Bannon, Robert Mueller |
| 1 | 89 | Legal/crisis management | Darren Indyke, Prince Andrew, Ghislaine Maxwell |
| 2 | 74 | High-profile network (politics, tech, finance) | Bill Clinton, Peter Thiel, Bill Gates |

### Top 10 Most Connected

| Rank | Person | Connections | Community |
|------|--------|-------------|-----------|
| 1 | Donald Trump | 254 | 0 |
| 2 | Bill Clinton | 185 | 2 |
| 3 | Barack Obama | 109 | 3 |
| 4 | Darren K. Indyke | 95 | 1 |
| 5 | Prince Andrew | 89 | 1 |
| 6 | Hillary Clinton | 87 | 0 |
| 7 | Ghislaine Maxwell | 85 | 1 |
| 8 | Steve Bannon | 83 | 0 |
| 9 | Bill Gates | 78 | 2 |
| 10 | Alan Dershowitz | 75 | 1 |

---

## Usage

```bash
# Activate environment
source .venv/bin/activate

# Run the full pipeline
cd scripts/final
python run_pipeline.py

# Or run individual steps
python run_name_extraction.py        # F1
python run_name_consolidation.py     # F2
python run_name_consolidation_v2.py  # F2b
python run_name_consolidation_v3.py  # F2c
python run_name_consolidation_v4.py  # F2d
python create_unique_names.py
python build_network.py
python run_relationship_description.py  # F3
python analyze_clusters.py              # F4
python enrich_network_tables.py
```

### View the network

```bash
# Start local server
python -m http.server 8001

# Open in browser
open http://localhost:8001/visualization/FINAL_network.html
```

---

## AI Classification Costs

| Run | Model | Cost |
|-----|-------|------|
| F1 (name extraction) | Claude Sonnet | ~$8 |
| F2-F2d (consolidation) | Claude Sonnet | ~$1 |
| F3 (relationships) | Claude Sonnet | ~$2 |
| F4 (clusters) | Claude Sonnet | ~$0.07 |
| **Total** | | **~$11** |

---

## Source

Dataset: [notesbymuneeb/epstein-emails](https://huggingface.co/datasets/notesbymuneeb/epstein-emails)
