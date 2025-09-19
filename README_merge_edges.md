# PrimeKG and SemMedDB Edge Merger

This script merges PrimeKG and SemMedDB edges in RTX-KG2 format, filtering to only include edges between nodes present in a subset.

## Files

- `merge_primekg_semmeddb_edges.py` - Main script for merging edges
- `primekg_edges.csv` - PrimeKG edges file (uses node indices)
- `primekg_nodes.csv` - PrimeKG nodes file (maps indices to node IDs)
- `connections.csv.gz` - SemMedDB connections file (uses CUI IDs)
- `subset_nodes.csv` - Subset of nodes to filter edges
- `connections_header.csv` - Header for connections file (`:START_ID,:END_ID,:TYPE,frequency`)

## Usage

### Basic Usage
```bash
python merge_primekg_semmeddb_edges.py primekg_edges.csv primekg_nodes.csv connections.csv.gz subset_nodes.csv output_edges.jsonl
```

### Test Mode (Limited Records)
```bash
python merge_primekg_semmeddb_edges.py --test --primekg-limit 100 --semmeddb-limit 1000 primekg_edges.csv primekg_nodes.csv connections.csv.gz subset_nodes.csv test_edges.jsonl
```

### Full Processing
```bash
python merge_primekg_semmeddb_edges.py --primekg-limit 100000 --semmeddb-limit 100000 primekg_edges.csv primekg_nodes.csv connections.csv.gz subset_nodes.csv merged_edges.jsonl
```

## Data Format

### Input Files

**PrimeKG Edges**: `relation,display_relation,x_index,y_index`
- Uses node indices that are mapped to actual node IDs via primekg_nodes.csv

**PrimeKG Nodes**: `node_index,node_id,node_type,node_name,node_source`
- Maps node indices to node IDs for edge processing

**SemMedDB Connections**: `:START_ID,:END_ID,:TYPE,frequency`
- Uses CUI IDs (e.g., C0003725, C0039258)
- Mapped to UMLS CURIEs via subset_nodes.csv

**Subset Nodes**: Contains node mappings with columns:
- `id`: CURIE format (e.g., UMLS:774955)
- `semantic_type`: CUI format (e.g., C0162783)

### Output Format

The script outputs RTX-KG2 JSONL format edges with:
- `subject`: CURIE ID of source node
- `object`: CURIE ID of target node  
- `relation_label`: Human-readable relation name
- `source_predicate`: CURIE of the relation
- `primary_knowledge_source`: Source database (SEMMEDDB: or PRIMEKG:)
- `frequency`: Edge frequency (if available)
- `id`: Unique edge identifier

## Current Status

- ✅ SemMedDB edges: Working (maps CUI IDs to UMLS CURIEs)
- ✅ PrimeKG edges: Working (maps node indices to node IDs via primekg_nodes.csv)

## Example Output

```json
{
  "subject": "UMLS:36508907",
  "object": "UMLS:901998", 
  "relation_label": "PROCESS_OF",
  "source_predicate": "SEMMEDDB:PROCESS_OF",
  "predicate": null,
  "qualified_predicate": null,
  "qualified_object_aspect": null,
  "qualified_object_direction": null,
  "negated": false,
  "publications": [],
  "publications_info": {},
  "update_date": "2025-09-16 17:53:23",
  "primary_knowledge_source": "SEMMEDDB:",
  "domain_range_exclusion": false,
  "id": "UMLS:36508907---SEMMEDDB:PROCESS_OF---None---None---None---UMLS:901998---SEMMEDDB:",
  "frequency": 19573.0
}
```

## Notes

- The script processes both PrimeKG and SemMedDB edges
- PrimeKG edges are mapped from node indices to node IDs using primekg_nodes.csv
- SemMedDB edges are mapped from CUI IDs to UMLS CURIEs using subset_nodes.csv
- The subset_nodes.csv file is used to filter edges to only those connecting nodes in the subset
- Edge frequencies are preserved from the original SemMedDB data
- Both data sources maintain their original relation types and metadata
