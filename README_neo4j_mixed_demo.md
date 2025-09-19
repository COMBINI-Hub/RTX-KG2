# Neo4j Mixed Demo Instance

This document describes the Neo4j instance that has been set up with the mixed demo data from `mixed_demo_nodes.csv` and `mixed_demo_edges.csv`.

## Overview

The Neo4j instance contains:
- **20 nodes** from the mixed demo dataset
- **26 relationships** connecting these nodes
- **2 node categories**: `biolink:NamedThing` (10 nodes) and `biolink:Gene` (10 nodes)
- **Multiple relationship types** including `LOCATION_OF`, `same_as`, `NEG_higher_than`, `NEG_same_as`, `lower_than`, etc.

## Access Information

- **URL**: http://localhost:7474
- **Username**: neo4j
- **Password**: password
- **Database**: Default (neo4j)

## Data Structure

### Nodes
All nodes have the label `Node` and contain the following properties:
- `id`: Unique identifier (e.g., "UMLS:774955")
- `name`: Node name (e.g., "2784046")
- `category`: Biolink category (e.g., "biolink:NamedThing")
- `description`: Node description
- `provided_by`: Data source (e.g., "SEMMEDDB:")
- `semantic_type`: UMLS semantic type
- `update_date`: Last update timestamp
- And other properties from the original CSV

### Relationships
All relationships have the type `RELATIONSHIP` and contain:
- `relation_label`: Type of relationship (e.g., "LOCATION_OF")
- `predicate`: Source predicate
- `frequency`: Relationship frequency (when available)
- `negated`: Boolean indicating if relationship is negated
- And other properties from the original CSV

## Sample Queries

### Basic Statistics
```cypher
// Count total nodes
MATCH (n:Node) RETURN count(n) as total_nodes;

// Count total relationships
MATCH ()-[r:RELATIONSHIP]->() RETURN count(r) as total_relationships;
```

### Explore Node Categories
```cypher
MATCH (n:Node) 
RETURN n.category as category, count(n) as count 
ORDER BY count DESC;
```

### Find Sample Relationships
```cypher
MATCH (a:Node)-[r:RELATIONSHIP]->(b:Node) 
RETURN a.name as source, r.relation_label as relationship, b.name as target 
LIMIT 10;
```

### Find Nodes with Most Connections
```cypher
MATCH (n:Node) 
OPTIONAL MATCH (n)-[r:RELATIONSHIP]-() 
RETURN n.name, n.category, count(r) as connections 
ORDER BY connections DESC 
LIMIT 10;
```

## Files Created

1. **`neo4j/load_mixed_demo_data.py`**: Python script to load the mixed demo data
2. **`neo4j/mixed_demo_queries.cypher`**: Collection of useful queries for exploring the data
3. **`neo4j/import_mixed_demo_*.sh`**: Various import scripts (for reference)

## Usage Instructions

1. **Start Neo4j** (if not already running):
   ```bash
   neo4j start
   ```

2. **Access the Neo4j Browser**:
   - Open http://localhost:7474 in your web browser
   - Login with username: `neo4j`, password: `password`

3. **Run Queries**:
   - Copy and paste queries from `mixed_demo_queries.cypher`
   - Or create your own Cypher queries

4. **Explore the Data**:
   - Use the Neo4j browser's visualization features
   - Run queries to understand the data structure
   - Look for patterns and relationships

## Data Sources

The mixed demo data combines:
- **SEMMEDDB**: Semantic MEDLINE database relationships
- **PRIMEKG**: Protein-protein interaction data
- **UMLS**: Unified Medical Language System concepts

## Next Steps

- Explore the data using the provided queries
- Create custom visualizations
- Build applications that query this knowledge graph
- Extend the dataset with additional data sources

## Troubleshooting

If you encounter issues:
1. Check that Neo4j is running: `neo4j status`
2. Verify the database is accessible: `cypher-shell -u neo4j -p password "MATCH (n) RETURN count(n);"`
3. Check the Neo4j logs: `/opt/homebrew/var/log/neo4j/`

## Additional Resources

- [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/)
- [Neo4j Browser Guide](https://neo4j.com/docs/browser-manual/)
- [Biolink Model](https://biolink.github.io/biolink-model/)






