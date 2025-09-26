# RTX-KG2 Edge Merging - 4 Knowledge Graphs

This directory demonstrates successful edge merging from 4 biomedical knowledge graphs using RTX-KG2 patterns and NodeNormalization.

## Quick Start

### 1. Generate Aligned Data
```bash
cd /Users/drshika2/RTX-KG2
python create_simple_unmerged_subset.py
```

### 2. Merge Edges
```bash
cd unmerged_simulation_subset
python merge_edges.py --test --limit 1000
```

## Results

| Source | Nodes | Edges | Alignment | Status |
|--------|-------|-------|-----------|--------|
| BioKDE | 1,000 | 2,000 | 100% | ✅ Perfect |
| iKraph | 1,000 | 734 | 100% | ✅ Perfect |
| PrimeKG | 1,000 | 2,000 | 100% | ✅ Perfect |
| SemMedDB | 1,000 | 0 | 100% | ✅ Perfect* |

*SemMedDB has 0 edges because no connections exist between the sampled nodes. The nodes were sampled so that there would be node overlap to test node merging.

**Total Merged Edges: 1,771**

## Files

### Data Files
- `*_nodes.csv` - Node data from each source
- `*_edges.csv` - Edge data from each source (aligned with nodes)
- `combined_four_datasets_union.json` - NodeNormalization output
- `node_mappings.json` - Node ID mappings

### Scripts
- `merge_edges.py` - Main edge merger

### Output
- `merged_edges.jsonl` - Merged edges in RTX-KG2 format
- `merged_edges_stats.json` - Processing statistics

## Usage

### Test Mode (1000 edges per source)
```bash
python merge_edges.py --test --limit 1000
```

### Full Processing
```bash
python merge_edges.py
```

### Custom Limits
```bash
python merge_edges.py --limit 5000
```

## Edge Format

```json
{
  "subject": "ABA:FF",
  "object": "ABA:ZI", 
  "relation_label": "SUBCLASS_OF",
  "source_predicate": "BIOKDE:SUBCLASS_OF",
  "primary_knowledge_source": "BIOKDE:",
  "update_date": "2025-09-26 19:45:45",
  "id": "ABA:FF---BIOKDE:SUBCLASS_OF---None---None---None---ABA:ZI---BIOKDE:"
}
```

## Key Features

✅ **Perfect Data Alignment** - All edges reference existing nodes  
✅ **RTX-KG2 Compatible** - Uses standard RTX-KG2 edge format  
✅ **Source Provenance** - Maintains knowledge source tracking  
✅ **Deduplication** - Prevents duplicate edges  
✅ **NodeNormalization Integration** - Uses normalized node identifiers  