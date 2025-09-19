# SemMedDB and PrimeKG Node Merger

This directory contains scripts to merge SemMedDB entity data with PrimeKG node data into RTX-KG2 format.

## Files

- `test_merge_semmeddb_primekg.py` - Simple test version (no dependencies on kg2_util)
- `semmeddb_primekg_merge_to_kg_jsonl.py` - Production version integrated with RTX-KG2 system
- `merge_semmeddb_primekg_nodes.py` - Full-featured version with advanced mapping

## Usage

### Test Version (Recommended for initial testing)

```bash
# Test with 100 records from each source
python convert/test_merge_semmeddb_primekg.py entity.gz primekg_nodes.csv test_output.jsonl --test
```

### Production Version (Requires RTX-KG2 dependencies)

```bash
# Test mode (1000 records each)
python convert/semmeddb_primekg_merge_to_kg_jsonl.py entity.gz primekg_nodes.csv merged_nodes.jsonl --test

# Full processing
python convert/semmeddb_primekg_merge_to_kg_jsonl.py entity.gz primekg_nodes.csv merged_nodes.jsonl
```

## Data Sources

### SemMedDB Entity File (entity.gz)
- **Format**: Compressed CSV with entity information
- **Columns**: entity_id, cui, numeric_id, semantic_type, name, aliases, source, frequency, score, rank
- **CURIE Mapping**: Uses UMLS CUI when available, falls back to SemMedDB entity_id
- **Categories**: Mapped from semantic types to Biolink categories
- **Name Field**: Uses the actual entity name (position 4) instead of numeric ID (position 2)

### PrimeKG Nodes File (primekg_nodes.csv)
- **Format**: CSV with node information
- **Columns**: node_index, node_id, node_type, node_name, node_source
- **CURIE Mapping**: Uses PRIMEKG prefix with node_id
- **Categories**: Mapped from node_type to Biolink categories

## Output Format

The merged output follows RTX-KG2 JSONL format with nodes containing:

### Common Fields
- `id`: CURIE identifier
- `name`: Human-readable name
- `category`: Biolink category
- `update_date`: Processing timestamp
- `provided_by`: Source identifier

### SemMedDB-specific Fields
- `description`: Entity definition
- `semmeddb_semantic_type`: UMLS semantic type (CUI codes like "C0162783")
- `synonym`: List of aliases
- `frequency`: Entity frequency score
- `score`: Entity confidence score

### PrimeKG-specific Fields
- `primekg_node_type`: Original PrimeKG node type (e.g., "gene/protein", "drug")
- `primekg_node_source`: Original data source (e.g., "NCBI", "DrugBank")

## Integration with RTX-KG2 Build System

To integrate this into the RTX-KG2 build system:

1. Add the script to the appropriate Snakefile
2. Include the output in the merge_graphs.py process
3. Update configuration files as needed

Example Snakefile rule:
```python
rule SemMedDB_PrimeKG_Merge:
    input:
        semmeddb_file = "entity.gz",
        primekg_file = "primekg_nodes.csv"
    output:
        "kg2-semmeddb-primekg-nodes.jsonl"
    shell:
        "python convert/semmeddb_primekg_merge_to_kg_jsonl.py {input.semmeddb_file} {input.primekg_file} {output}"
```

## Testing

The test version processes only 100 records from each source, making it suitable for:
- Verifying data format compatibility
- Testing CURIE mapping logic
- Validating output structure
- Performance testing with small datasets

## Notes

- Both scripts handle duplicate detection within each source
- Semantic type and node type mappings can be customized in the mapping functions
- The production version uses kg2_util for proper RTX-KG2 integration
- Source nodes are automatically created for both SemMedDB and PrimeKG
- **Field Conflict Resolution**: To avoid conflicts between different data types in the same field:
  - SemMedDB semantic types are stored in `semmeddb_semantic_type` field
  - PrimeKG node types are stored in `primekg_node_type` field  
  - PrimeKG node sources are stored in `primekg_node_source` field
- **Name Field Fix**: SemMedDB entities now use human-readable names (e.g., "Prefrontal Cortex") instead of numeric IDs (e.g., "2784046")
