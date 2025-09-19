# Neo4j Demo Subset

This directory contains a small demo subset of the merged PrimeKG and SemMedDB knowledge graph, ready for Neo4j import.

## Files

- `demo_nodes_efficient.jsonl` - 20 nodes from the subset
- `demo_edges_efficient.jsonl` - 63 edges connecting those nodes
- `create_neo4j_demo_efficient.py` - Script to generate the demo subset

## Data Summary

### Nodes (20 total)
- **Source**: All nodes from `subset_nodes.csv` (first 20 entries)
- **Types**: Mix of SemMedDB nodes (UMLS CURIEs) and PrimeKG nodes
- **Format**: RTX-KG2 JSONL format with proper CURIEs, categories, and metadata

### Edges (63 total)
- **Source**: SemMedDB connections only (no PrimeKG edges found connecting the selected nodes)
- **Relations**: Various medical relationships including:
  - `compared_with` (9 edges)
  - `higher_than` (8 edges)
  - Other medical relations
- **Format**: RTX-KG2 JSONL format with frequencies and proper CURIEs

## Data Quality

✅ **100% Valid**: All 63 edges connect nodes that exist in the 20-node subset
✅ **Proper Format**: RTX-KG2 compliant JSONL format
✅ **Rich Metadata**: Includes frequencies, semantic types, and source information

## Usage

### For Neo4j Import
```bash
# Import nodes
neo4j-admin import --nodes=demo_nodes_efficient.jsonl

# Import edges  
neo4j-admin import --relationships=demo_edges_efficient.jsonl
```

### For Analysis
```python
import json

# Load nodes
nodes = []
with open('demo_nodes_efficient.jsonl', 'r') as f:
    for line in f:
        nodes.append(json.loads(line))

# Load edges
edges = []
with open('demo_edges_efficient.jsonl', 'r') as f:
    for line in f:
        edges.append(json.loads(line))
```

## Notes

- This is a small, curated subset designed for demonstration purposes
- All relationships are guaranteed to be valid (both nodes exist in the subset)
- The subset focuses on SemMedDB medical relationships
- PrimeKG edges were not found connecting the selected nodes, indicating the subset may be primarily medical entities

