#!/usr/bin/env python3
"""
Load mixed demo data into Neo4j using the Python driver
"""

import pandas as pd
from neo4j import GraphDatabase
import sys

def load_mixed_demo_data():
    # Connect to Neo4j
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))
    
    with driver.session() as session:
        # Clear existing mixed demo data
        print("Clearing existing mixed demo data...")
        session.run("MATCH (n:Node) DETACH DELETE n")
        
        # Load nodes
        print("Loading nodes...")
        nodes_df = pd.read_csv('/Users/drshika2/RTX-KG2/mixed_demo_nodes.csv')
        
        for _, row in nodes_df.iterrows():
            session.run("""
                CREATE (n:Node {
                    id: $id,
                    iri: $iri,
                    name: $name,
                    full_name: $full_name,
                    category: $category,
                    category_label: $category_label,
                    description: $description,
                    provided_by: $provided_by,
                    semantic_type: $semantic_type,
                    node_type: $node_type,
                    node_source: $node_source,
                    synonym: $synonym,
                    update_date: $update_date
                })
            """, 
            id=row['id'],
            iri=row['iri'],
            name=row['name'],
            full_name=row['full_name'],
            category=row['category'],
            category_label=row['category_label'],
            description=row['description'],
            provided_by=row['provided_by'],
            semantic_type=row['semantic_type'],
            node_type=row['node_type'],
            node_source=row['node_source'],
            synonym=row['synonym'],
            update_date=row['update_date']
            )
        
        # Load relationships
        print("Loading relationships...")
        edges_df = pd.read_csv('/Users/drshika2/RTX-KG2/mixed_demo_edges.csv')
        
        for _, row in edges_df.iterrows():
            session.run("""
                MATCH (a:Node {id: $subject})
                MATCH (b:Node {id: $object})
                CREATE (a)-[r:RELATIONSHIP {
                    id: $id,
                    relation_label: $relation_label,
                    source_predicate: $source_predicate,
                    predicate: $predicate,
                    qualified_predicate: $qualified_predicate,
                    qualified_object_aspect: $qualified_object_aspect,
                    qualified_object_direction: $qualified_object_direction,
                    negated: $negated,
                    publications: $publications,
                    publications_info: $publications_info,
                    update_date: $update_date,
                    primary_knowledge_source: $primary_knowledge_source,
                    domain_range_exclusion: $domain_range_exclusion,
                    frequency: $frequency
                }]->(b)
            """,
            id=row['id'],
            subject=row['subject'],
            object=row['object'],
            relation_label=row['relation_label'],
            source_predicate=row['source_predicate'],
            predicate=row['predicate'],
            qualified_predicate=row['qualified_predicate'],
            qualified_object_aspect=row['qualified_object_aspect'],
            qualified_object_direction=row['qualified_object_direction'],
            negated=row['negated'],
            publications=row['publications'],
            publications_info=row['publications_info'],
            update_date=row['update_date'],
            primary_knowledge_source=row['primary_knowledge_source'],
            domain_range_exclusion=row['domain_range_exclusion'],
            frequency=row['frequency']
            )
        
        # Create indexes
        print("Creating indexes...")
        session.run("CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE")
        session.run("CREATE INDEX node_category IF NOT EXISTS FOR (n:Node) ON (n.category)")
        session.run("CREATE INDEX node_name IF NOT EXISTS FOR (n:Node) ON (n.name)")
        session.run("CREATE INDEX edge_predicate IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() ON (r.predicate)")
        session.run("CREATE INDEX edge_relation_label IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() ON (r.relation_label)")
        
        # Show stats
        print("Getting statistics...")
        result = session.run("MATCH (n:Node) RETURN count(n) as total_nodes")
        total_nodes = result.single()['total_nodes']
        
        result = session.run("MATCH ()-[r:RELATIONSHIP]->() RETURN count(r) as total_relationships")
        total_relationships = result.single()['total_relationships']
        
        print(f"Loaded {total_nodes} nodes and {total_relationships} relationships")
        
        # Show some sample data
        print("\nSample nodes:")
        result = session.run("MATCH (n:Node) RETURN n.id, n.name, n.category LIMIT 5")
        for record in result:
            print(f"  {record['n.id']}: {record['n.name']} ({record['n.category']})")
        
        print("\nSample relationships:")
        result = session.run("MATCH (a:Node)-[r:RELATIONSHIP]->(b:Node) RETURN a.name, r.relation_label, b.name LIMIT 5")
        for record in result:
            print(f"  {record['a.name']} --[{record['r.relation_label']}]--> {record['b.name']}")
    
    driver.close()
    print("Data loading completed!")

if __name__ == "__main__":
    load_mixed_demo_data()






