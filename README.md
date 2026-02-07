# Epstein Emails Network Analysis

Analyse de réseau des emails d'Epstein: extraction de noms, détection de communautés, et exploration interactive des relations.

---

## Structure du Projet

```
epstein-files/
├── data/
│   ├── raw_data/                      # Données brutes (parquet)
│   ├── epstein_discussions_names.json # Discussions avec noms extraits (5082)
│   ├── epstein_discussions_filtered.json # Discussions filtrées (4735)
│   ├── name_matching_table.json       # Table de correspondance des noms
│   ├── names_to_remove.json           # Noms à exclure du réseau
│   ├── journalist_request_ids.json    # IDs des demandes journalistes exclues
│   ├── person_relationships.json      # Relations détaillées avec Epstein
│   └── network_edges.csv              # Arêtes du réseau
│
├── scripts/
│   ├── annotate.py                    # 1. Extraction des noms (Gemini)
│   ├── extract_names.py               # Extraction alternative
│   ├── consolidate_names.py           # 2. Déduplication des noms
│   ├── filter_journalist_requests.py  # 3. Filtrage demandes presse
│   ├── extract_relationships.py       # 4. Analyse des relations (Gemini)
│   └── build_network.py               # 5. Construction du réseau
│
├── network.html                       # Visualisation réseau (Sigma.js)
├── person_connections.html            # Explorateur de connexions
├── .env                               # GEMINI_API_KEY
└── README.md
```

---

## Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. EXTRACTION DES NOMS                                             │
│     scripts/annotate.py                                             │
│     → Utilise Gemini pour extraire senders, receivers, mentioned    │
│     → Sortie: epstein_discussions_names.json                        │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  2. CONSOLIDATION DES NOMS                                          │
│     scripts/consolidate_names.py                                    │
│     → Fusionne les variantes ("Bill" → "Bill Clinton")              │
│     → Sortie: name_matching_table.json                              │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  3. FILTRAGE DES DEMANDES JOURNALISTES                              │
│     scripts/filter_journalist_requests.py                           │
│     → Exclut les demandes d'interview (pas d'interaction directe)   │
│     → Sortie: journalist_request_ids.json                           │
│     → Sortie: epstein_discussions_filtered.json                     │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  4. EXTRACTION DES RELATIONS                                        │
│     scripts/extract_relationships.py                                │
│     → Analyse la nature de chaque relation avec Epstein             │
│     → Sortie: person_relationships.json                             │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  5. CONSTRUCTION DU RÉSEAU                                          │
│     scripts/build_network.py                                        │
│     → Co-occurrences + clustering Leiden                            │
│     → Sortie: network.html, network_edges.csv                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Fichiers de Données

| Fichier | Description | Taille |
|---------|-------------|--------|
| `epstein_discussions_names.json` | Toutes les discussions avec noms extraits | 5082 discussions |
| `epstein_discussions_filtered.json` | Discussions sans demandes presse | 4735 discussions |
| `name_matching_table.json` | Variante → nom canonique | ~2000 mappings |
| `names_to_remove.json` | Noms exclus (génériques, incomplets) | 79 noms |
| `journalist_request_ids.json` | IDs des demandes journalistes | 348 IDs |
| `person_relationships.json` | Description de chaque relation | 490 personnes |
| `network_edges.csv` | Arêtes avec poids | 1818 arêtes |

---

## Visualisations

### 1. Réseau Interactif (`network.html`)

- **416 nœuds** (personnes)
- **1818 arêtes** (co-occurrences)
- **21 communautés** (clustering Leiden)
- Taille des nœuds = nombre d'apparitions
- Couleur = communauté

### 2. Explorateur de Connexions (`person_connections.html`)

- Sélection d'une personne → voir ses connexions
- Affichage de la relation avec Epstein (générée par IA)
- Liens vers les emails sources
- Système de feedback pour validation manuelle

---

## Utilisation

```bash
# Démarrer le serveur local
cd epstein-files
python -m http.server 8001

# Ouvrir dans le navigateur
open http://localhost:8001/network.html
open http://localhost:8001/person_connections.html
```

### Reconstruire le réseau

```bash
# Activer l'environnement
source .venv/bin/activate

# Exécuter le pipeline complet
python scripts/annotate.py                    # ~30 min
python scripts/consolidate_names.py           # ~1 min
python scripts/filter_journalist_requests.py  # ~15 sec
python scripts/extract_relationships.py       # ~10 min
python scripts/build_network.py               # ~5 sec
```

---

## Statistiques du Réseau

| Métrique | Valeur |
|----------|--------|
| Discussions totales | 5082 |
| Après filtrage | 4735 |
| Personnes dans le réseau | 416 |
| Connexions | 1818 |
| Communautés | 21 |

### Top 10 Connexions

| Rang | Personne | Connexions |
|------|----------|------------|
| 1 | Donald Trump | 134 |
| 2 | Bill Clinton | 63 |
| 3 | Lawrence Summers | 50 |
| 4 | Darren Indyke | 49 |
| 5 | Kathy Ruemmler | 46 |
| 6 | Steve Bannon | 46 |
| 7 | Michael Wolff | 43 |
| 8 | Richard Kahn | 40 |
| 9 | Woody Allen | 40 |
| 10 | Barack Obama | 38 |

---

## Source

Dataset: [notesbymuneeb/epstein-emails](https://huggingface.co/datasets/notesbymuneeb/epstein-emails)
