# Mixed PrimeKG + SemMedDB Demo (CSV Format)

This directory contains a small demo subset of the merged PrimeKG and SemMedDB knowledge graph in CSV format, ready for Neo4j import or other analysis tools.

## Files

- `mixed_demo_nodes.csv` - 20 nodes (10 PrimeKG + 10 SemMedDB)
- `mixed_demo_edges.csv` - 26 edges (4 PrimeKG + 22 SemMedDB)
- `create_mixed_demo_csv.py` - Script to generate the demo subset

## Data Summary

### Nodes (20 total)
- **10 PrimeKG nodes**: Protein/gene entities with `PRIMEKG:` CURIEs
- **10 SemMedDB nodes**: Medical entities with `UMLS:` CURIEs
- **Format**: CSV with columns: id, iri, name, category, description, etc.

### Edges (26 total)
- **4 PrimeKG edges**: Protein-protein interactions (`ppi`)
- **22 SemMedDB edges**: Medical relationships (`compared_with`, `higher_than`, etc.)
- **Format**: CSV with columns: id, subject, object, relation_label, frequency, etc.

## CSV Structure

### Nodes CSV Columns
- `id`: CURIE identifier (e.g., `UMLS:774955`, `PRIMEKG:5297`)
- `iri`: Full IRI for the entity
- `name`: Entity name
- `category`: Biolink category (e.g., `biolink:NamedThing`)
- `description`: Entity description
- `provided_by`: Source database (`SEMMEDDB:` or `PRIMEKG:`)
- `semantic_type`: UMLS semantic type (for SemMedDB)
- `node_type`: Node type (for PrimeKG)

### Edges CSV Columns
- `id`: Unique edge identifier
- `subject`: Source node CURIE
- `object`: Target node CURIE
- `relation_label`: Human-readable relation name
- `source_predicate`: CURIE of the relation
- `primary_knowledge_source`: Source database
- `frequency`: Edge frequency (for SemMedDB edges)

## Usage

### For Neo4j Import
```bash
# Import nodes
neo4j-admin import --nodes=mixed_demo_nodes.csv

# Import edges
neo4j-admin import --relationships=mixed_demo_edges.csv
```

### For Analysis
```python
import pandas as pd

# Load nodes
nodes_df = pd.read_csv('mixed_demo_nodes.csv')
print(f"Nodes: {len(nodes_df)}")
print(f"PrimeKG nodes: {len(nodes_df[nodes_df['provided_by'] == 'PRIMEKG:'])}")
print(f"SemMedDB nodes: {len(nodes_df[nodes_df['provided_by'] == 'SEMMEDDB:'])}")

# Load edges
edges_df = pd.read_csv('mixed_demo_edges.csv')
print(f"Edges: {len(edges_df)}")
print(f"PrimeKG edges: {len(edges_df[edges_df['primary_knowledge_source'] == 'PRIMEKG:'])}")
print(f"SemMedDB edges: {len(edges_df[edges_df['primary_knowledge_source'] == 'SEMMEDDB:'])}")
```

## Data Quality

✅ **100% Valid**: All edges connect nodes that exist in the subset
✅ **CSV Format**: Easy to import into Neo4j, Excel, or other tools
✅ **Rich Metadata**: Includes frequencies, semantic types, and source information
✅ **Mixed Sources**: Demonstrates both PrimeKG and SemMedDB data

## Notes

- This is a small, curated subset designed for demonstration purposes
- All relationships are guaranteed to be valid
- The subset shows how different knowledge sources can be merged
- CSV format makes it easy to work with in various tools and databases

