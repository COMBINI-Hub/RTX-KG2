#!/usr/bin/env python3
"""
create_neo4j_demo_efficient.py: Create a demo subset for Neo4j visualization

This script:
1. Selects a small set of specific nodes (e.g., 20-30 nodes)
2. Finds all relationships connecting those nodes
3. Outputs in RTX-KG2 JSONL format ready for Neo4j

Usage: python create_neo4j_demo_efficient.py
"""

import csv
import datetime
import gzip
import kg2_util
import random
import sys
from typing import Dict, Set, List

# Set random seed for reproducible results
random.seed(42)

def date(print_str: str):
    """Print timestamped message"""
    return print(print_str, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def create_demo_nodes_and_edges():
    """Create a demo subset with nodes and edges"""
    date("Creating Neo4j demo subset")
    
    # Step 1: Select a small set of specific nodes from subset_nodes.csv
    selected_nodes = []
    with open('subset_nodes.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 20:  # Select first 20 nodes
                break
            selected_nodes.append(row)
    
    print(f"Selected {len(selected_nodes)} nodes")
    
    # Create mappings for the selected nodes
    node_id_to_data = {}
    cui_to_curie = {}
    
    for node in selected_nodes:
        node_id = node['id']
        node_id_to_data[node_id] = node
        
        # Map CUI to CURIE for SemMedDB
        semantic_type = node.get('semantic_type', '')
        if semantic_type and semantic_type.startswith('C') and len(semantic_type) >= 7:
            cui_to_curie[semantic_type] = node_id
    
    print(f"Created mappings for {len(node_id_to_data)} nodes")
    print(f"Created CUI mappings for {len(cui_to_curie)} nodes")
    
    # Load PrimeKG node mapping
    node_index_to_id = {}
    with open('primekg_nodes.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            node_index_to_id[row['node_index']] = row['node_id']
    
    # Create output files
    nodes_info, edges_info = kg2_util.create_kg2_jsonlines(False)
    nodes_output = nodes_info[0]
    edges_output = edges_info[0]
    
    try:
        # Step 2: Create nodes
        node_count = 0
        for node in selected_nodes:
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Fix category label if needed
            category_label = node.get('category_label', '')
            if category_label == 'named_thing':
                category_label = kg2_util.BIOLINK_CATEGORY_NAMED_THING
            
            kg2_node = kg2_util.make_node(
                id=node['id'],
                iri=node.get('iri', ''),
                name=node.get('name', ''),
                category_label=category_label,
                update_date=update_date,
                provided_by=node.get('provided_by', '')
            )
            
            # Add additional properties
            if node.get('description'):
                kg2_node['description'] = node['description']
            if node.get('semantic_type'):
                kg2_node['semantic_type'] = node['semantic_type']
            if node.get('node_type'):
                kg2_node['node_type'] = node['node_type']
            if node.get('node_source'):
                kg2_node['node_source'] = node['node_source']
            if node.get('synonym'):
                synonyms = [s.strip() for s in node['synonym'].split('|') if s.strip()]
                if synonyms:
                    kg2_node['synonym'] = synonyms
            
            nodes_output.write(kg2_node)
            node_count += 1
        
        print(f"Created {node_count} nodes")
        
        # Step 3: Find PrimeKG edges connecting our selected nodes
        primekg_count = 0
        with open('primekg_edges.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                x_index = row['x_index']
                y_index = row['y_index']
                relation = row['relation']
                display_relation = row['display_relation']
                
                # Get node IDs from indices
                x_node_id = node_index_to_id.get(x_index)
                y_node_id = node_index_to_id.get(y_index)
                
                if not x_node_id or not y_node_id:
                    continue
                
                # Check if both nodes are in our selected set
                x_curie = f"PRIMEKG:{x_node_id}"
                y_curie = f"PRIMEKG:{y_node_id}"
                
                if x_curie not in node_id_to_data or y_curie not in node_id_to_data:
                    continue
                
                # Create edge
                update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                relation_curie = f"PRIMEKG:{relation}"
                
                edge = kg2_util.make_edge(
                    subject_id=x_curie,
                    object_id=y_curie,
                    relation_curie=relation_curie,
                    relation_label=display_relation,
                    primary_knowledge_source="PRIMEKG:",
                    update_date=update_date
                )
                
                edges_output.write(edge)
                primekg_count += 1
        
        print(f"Found {primekg_count} PrimeKG edges")
        
        # Step 4: Find SemMedDB edges connecting our selected nodes
        semmeddb_count = 0
        with gzip.open('connections.csv.gz', 'rt', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) < 3:
                    continue
                    
                start_id = parts[0]
                end_id = parts[1]
                relation_type = parts[2]
                frequency = parts[3] if len(parts) > 3 else '1'
                
                # Check if both CUI IDs map to our selected nodes
                subject_curie = cui_to_curie.get(start_id)
                object_curie = cui_to_curie.get(end_id)
                
                if not subject_curie or not object_curie:
                    continue
                
                # Create edge
                update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                relation_curie = f"SEMMEDDB:{relation_type}"
                
                edge = kg2_util.make_edge(
                    subject_id=subject_curie,
                    object_id=object_curie,
                    relation_curie=relation_curie,
                    relation_label=relation_type,
                    primary_knowledge_source="SEMMEDDB:",
                    update_date=update_date
                )
                
                if frequency and frequency != '1':
                    try:
                        edge['frequency'] = float(frequency)
                    except ValueError:
                        pass
                
                edges_output.write(edge)
                semmeddb_count += 1
        
        print(f"Found {semmeddb_count} SemMedDB edges")
        print(f"Total edges: {primekg_count + semmeddb_count}")
        
    finally:
        kg2_util.close_kg2_jsonlines(nodes_info, edges_info, 'demo_nodes_efficient.jsonl', 'demo_edges_efficient.jsonl')
    
    date("Finished creating Neo4j demo subset")

if __name__ == '__main__':
    create_demo_nodes_and_edges()

