#!/usr/bin/env python3
"""
create_mixed_demo_csv.py: Create a demo subset with BOTH PrimeKG and SemMedDB as CSV files

This script:
1. Selects nodes from both PrimeKG and SemMedDB
2. Finds relationships connecting those nodes
3. Outputs in CSV format ready for Neo4j import

Usage: python create_mixed_demo_csv.py
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

def create_mixed_demo_csv():
    """Create a demo subset with both PrimeKG and SemMedDB as CSV files"""
    date("Creating mixed PrimeKG + SemMedDB demo subset as CSV")
    
    # Step 1: Load all subset nodes and separate by source
    semmeddb_nodes = []
    primekg_nodes = []
    
    with open('subset_nodes.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            provided_by = row.get('provided_by', '')
            if 'SEMMEDDB' in provided_by:
                semmeddb_nodes.append(row)
            elif 'PRIMEKG' in provided_by:
                primekg_nodes.append(row)
    
    print(f"Found {len(semmeddb_nodes)} SemMedDB nodes and {len(primekg_nodes)} PrimeKG nodes in subset")
    
    # Step 2: Select a mix of both types
    selected_semmeddb = semmeddb_nodes[:10]  # First 10 SemMedDB nodes
    selected_primekg = primekg_nodes[:10]    # First 10 PrimeKG nodes
    selected_nodes = selected_semmeddb + selected_primekg
    
    print(f"Selected {len(selected_semmeddb)} SemMedDB nodes and {len(selected_primekg)} PrimeKG nodes")
    
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
    
    # Create CSV output files
    with open('mixed_demo_nodes.csv', 'w', newline='', encoding='utf-8') as nodes_file, \
         open('mixed_demo_edges.csv', 'w', newline='', encoding='utf-8') as edges_file:
        
        # Define CSV headers
        node_fieldnames = [
            'id', 'iri', 'name', 'full_name', 'category', 'category_label',
            'description', 'provided_by', 'semantic_type', 'node_type', 
            'node_source', 'synonym', 'update_date'
        ]
        
        edge_fieldnames = [
            'id', 'subject', 'object', 'relation_label', 'source_predicate',
            'predicate', 'qualified_predicate', 'qualified_object_aspect',
            'qualified_object_direction', 'negated', 'publications',
            'publications_info', 'update_date', 'primary_knowledge_source',
            'domain_range_exclusion', 'frequency'
        ]
        
        nodes_writer = csv.DictWriter(nodes_file, fieldnames=node_fieldnames)
        edges_writer = csv.DictWriter(edges_file, fieldnames=edge_fieldnames)
        
        # Write headers
        nodes_writer.writeheader()
        edges_writer.writeheader()
        
        # Step 3: Create nodes
        node_count = 0
        for node in selected_nodes:
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Fix category label if needed
            category_label = node.get('category_label', '')
            if category_label == 'named_thing':
                category_label = kg2_util.BIOLINK_CATEGORY_NAMED_THING
            
            # Create node row
            node_row = {
                'id': node['id'],
                'iri': node.get('iri', ''),
                'name': node.get('name', ''),
                'full_name': node.get('full_name', ''),
                'category': kg2_util.convert_biolink_category_to_curie(category_label),
                'category_label': category_label,
                'description': node.get('description', ''),
                'provided_by': '|'.join(node.get('provided_by', [])) if isinstance(node.get('provided_by'), list) else node.get('provided_by', ''),
                'semantic_type': node.get('semantic_type', ''),
                'node_type': node.get('node_type', ''),
                'node_source': node.get('node_source', ''),
                'synonym': '|'.join(node.get('synonym', [])) if isinstance(node.get('synonym'), list) else node.get('synonym', ''),
                'update_date': update_date
            }
            
            nodes_writer.writerow(node_row)
            node_count += 1
        
        print(f"Created {node_count} nodes")
        
        # Step 4: Find PrimeKG edges connecting our selected nodes
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
                edge_id = f"{x_curie}---{relation_curie}---None---None---None---{y_curie}---PRIMEKG:"
                
                edge_row = {
                    'id': edge_id,
                    'subject': x_curie,
                    'object': y_curie,
                    'relation_label': display_relation,
                    'source_predicate': relation_curie,
                    'predicate': '',
                    'qualified_predicate': '',
                    'qualified_object_aspect': '',
                    'qualified_object_direction': '',
                    'negated': False,
                    'publications': '',
                    'publications_info': '',
                    'update_date': update_date,
                    'primary_knowledge_source': 'PRIMEKG:',
                    'domain_range_exclusion': False,
                    'frequency': ''
                }
                
                edges_writer.writerow(edge_row)
                primekg_count += 1
        
        print(f"Found {primekg_count} PrimeKG edges")
        
        # Step 5: Find SemMedDB edges connecting our selected nodes
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
                edge_id = f"{subject_curie}---{relation_curie}---None---None---None---{object_curie}---SEMMEDDB:"
                
                edge_row = {
                    'id': edge_id,
                    'subject': subject_curie,
                    'object': object_curie,
                    'relation_label': relation_type,
                    'source_predicate': relation_curie,
                    'predicate': '',
                    'qualified_predicate': '',
                    'qualified_object_aspect': '',
                    'qualified_object_direction': '',
                    'negated': False,
                    'publications': '',
                    'publications_info': '',
                    'update_date': update_date,
                    'primary_knowledge_source': 'SEMMEDDB:',
                    'domain_range_exclusion': False,
                    'frequency': frequency if frequency != '1' else ''
                }
                
                edges_writer.writerow(edge_row)
                semmeddb_count += 1
        
        print(f"Found {semmeddb_count} SemMedDB edges")
        print(f"Total edges: {primekg_count + semmeddb_count}")
    
    date("Finished creating mixed demo subset as CSV")

if __name__ == '__main__':
    create_mixed_demo_csv()

