#!/usr/bin/env python3
"""
test_merge_semmeddb_primekg.py: Simple test version to merge SemMedDB and PrimeKG nodes
"""

import argparse
import csv
import datetime
import gzip
import json
import sys
from typing import Dict, Set, List, Optional

def date(print_str: str):
    """Print timestamped message"""
    return print(print_str, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def make_arg_parser():
    """Create command line argument parser"""
    arg_parser = argparse.ArgumentParser(
        description='Test merge SemMedDB entity data with PrimeKG node data'
    )
    arg_parser.add_argument('--test', dest='test', action='store_true', default=True,
                           help='Run in test mode (process only first 100 records)')
    arg_parser.add_argument('semmeddb_entity_file', type=str,
                           help='Path to SemMedDB entity.gz file')
    arg_parser.add_argument('primekg_nodes_file', type=str,
                           help='Path to PrimeKG nodes.csv file')
    arg_parser.add_argument('output_nodes_file', type=str,
                           help='Path to output nodes JSONL file')
    return arg_parser

def parse_semmeddb_entity_line(line: str) -> Optional[Dict]:
    """Parse a line from SemMedDB entity file"""
    try:
        # Remove quotes and split by comma
        line = line.strip().strip('"')
        parts = line.split('","')
        
        if len(parts) < 10:
            return None
            
        entity_id = parts[0].strip('"')
        cui = parts[1].strip('"')
        name = parts[2].strip('"')
        semantic_type = parts[3].strip('"')
        definition = parts[4].strip('"')
        aliases = parts[5].strip('"')
        source = parts[6].strip('"')
        frequency = parts[7].strip('"')
        score = parts[8].strip('"')
        rank = parts[9].strip('"')
        
        return {
            'entity_id': entity_id,
            'cui': cui,
            'name': name,
            'semantic_type': semantic_type,
            'definition': definition,
            'aliases': aliases,
            'source': source,
            'frequency': frequency,
            'score': score,
            'rank': rank
        }
    except Exception as e:
        print(f"Error parsing SemMedDB line: {e}", file=sys.stderr)
        return None

def create_semmeddb_node(entity_data: Dict, update_date: str) -> Dict:
    """Create a simple node from SemMedDB entity data"""
    # Use CUI as primary identifier if available, otherwise use entity_id
    if entity_data['cui'] and entity_data['cui'] != '':
        node_id = f"UMLS:{entity_data['cui']}"
    else:
        node_id = f"SEMMEDDB:{entity_data['entity_id']}"
    
    node = {
        'id': node_id,
        'name': entity_data['name'],
        'category': 'named_thing',
        'update_date': update_date,
        'provided_by': 'SEMMEDDB:',
        'source': 'SemMedDB'
    }
    
    # Add additional properties
    if entity_data['definition'] and entity_data['definition'] != '':
        node['description'] = entity_data['definition']
    
    if entity_data['semantic_type'] and entity_data['semantic_type'] != '':
        node['semantic_type'] = entity_data['semantic_type']
    
    return node

def create_primekg_node(node_data: Dict, update_date: str) -> Dict:
    """Create a simple node from PrimeKG node data"""
    node_id = f"PRIMEKG:{node_data['node_id']}"
    
    node = {
        'id': node_id,
        'name': node_data['node_name'],
        'category': 'named_thing',
        'update_date': update_date,
        'provided_by': 'PRIMEKG:',
        'source': 'PrimeKG',
        'node_type': node_data['node_type'],
        'node_source': node_data['node_source']
    }
    
    return node

def process_semmeddb_entities(semmeddb_file: str, nodes_output, test_mode: bool) -> Set[str]:
    """Process SemMedDB entity file and write nodes to output"""
    date("Starting SemMedDB entity processing")
    
    processed_entities = set()
    entity_count = 0
    max_entities = 100 if test_mode else 1000
    
    with gzip.open(semmeddb_file, 'rt', encoding='utf-8') as f:
        for line in f:
            if entity_count >= max_entities:
                break
                
            entity_data = parse_semmeddb_entity_line(line)
            if not entity_data:
                continue
            
            # Skip if already processed (avoid duplicates)
            entity_key = entity_data['cui'] if entity_data['cui'] else entity_data['entity_id']
            if entity_key in processed_entities:
                continue
            
            processed_entities.add(entity_key)
            
            # Create and write node
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            node = create_semmeddb_node(entity_data, update_date)
            nodes_output.write(json.dumps(node) + '\n')
            
            entity_count += 1
            if entity_count % 10 == 0:
                print(f"Processed {entity_count} SemMedDB entities")
    
    date(f"Finished SemMedDB entity processing: {entity_count} entities")
    return processed_entities

def process_primekg_nodes(primekg_file: str, nodes_output, test_mode: bool) -> Set[str]:
    """Process PrimeKG nodes file and write nodes to output"""
    date("Starting PrimeKG nodes processing")
    
    processed_nodes = set()
    node_count = 0
    max_nodes = 100 if test_mode else 1000
    
    with open(primekg_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if node_count >= max_nodes:
                break
                
            node_data = {
                'node_index': row['node_index'],
                'node_id': row['node_id'],
                'node_type': row['node_type'],
                'node_name': row['node_name'],
                'node_source': row['node_source']
            }
            
            # Skip if already processed
            if node_data['node_id'] in processed_nodes:
                continue
            
            processed_nodes.add(node_data['node_id'])
            
            # Create and write node
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            node = create_primekg_node(node_data, update_date)
            nodes_output.write(json.dumps(node) + '\n')
            
            node_count += 1
            if node_count % 10 == 0:
                print(f"Processed {node_count} PrimeKG nodes")
    
    date(f"Finished PrimeKG nodes processing: {node_count} nodes")
    return processed_nodes

def main():
    """Main function"""
    date("Starting test_merge_semmeddb_primekg.py")
    
    args = make_arg_parser().parse_args()
    semmeddb_file = args.semmeddb_entity_file
    primekg_file = args.primekg_nodes_file
    output_file = args.output_nodes_file
    test_mode = args.test
    
    # Create output file
    nodes_output = open(output_file, 'w')
    
    try:
        # Process SemMedDB entities
        semmeddb_entities = process_semmeddb_entities(semmeddb_file, nodes_output, test_mode)
        
        # Process PrimeKG nodes
        primekg_nodes = process_primekg_nodes(primekg_file, nodes_output, test_mode)
        
        print(f"Total SemMedDB entities processed: {len(semmeddb_entities)}")
        print(f"Total PrimeKG nodes processed: {len(primekg_nodes)}")
        print(f"Total nodes written: {len(semmeddb_entities) + len(primekg_nodes)}")
        
    finally:
        nodes_output.close()
    
    date("Finished test_merge_semmeddb_primekg.py")

if __name__ == '__main__':
    main()
