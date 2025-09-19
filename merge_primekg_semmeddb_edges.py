#!/usr/bin/env python3
"""
merge_primekg_semmeddb_edges.py: Merges PrimeKG and SemMedDB edges in RTX-KG2 format

This script processes:
- PrimeKG edges.csv file (CSV with edge information using node indices)
- SemMedDB connections files (CSV with edge information using entity IDs)
- Subset nodes.csv file (mapping between node IDs and indices)
- Outputs merged edges in RTX-KG2 JSONL format

Usage: python merge_primekg_semmeddb_edges.py <primekg_edges.csv> <semmeddb_connections.csv.gz> <subset_nodes.csv> <output_edges.jsonl> [--test]
"""

__author__ = 'RTX-KG2 Team'
__copyright__ = 'Oregon State University'
__credits__ = ['RTX-KG2 Team']
__license__ = 'MIT'
__version__ = '1.0.0'
__maintainer__ = 'RTX-KG2 Team'
__email__ = ''
__status__ = 'Production'

import argparse
import csv
import datetime
import gzip
import json
import kg2_util
import sys
from typing import Dict, Set, List, Optional, Tuple

# CURIE prefixes
SEMMEDDB_CURIE_PREFIX = kg2_util.CURIE_PREFIX_SEMMEDDB
PRIMEKG_CURIE_PREFIX = "PRIMEKG"
UMLS_CURIE_PREFIX = kg2_util.CURIE_PREFIX_UMLS

def date(print_str: str):
    """Print timestamped message"""
    return print(print_str, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def make_arg_parser():
    """Create command line argument parser"""
    arg_parser = argparse.ArgumentParser(
        description='Merge PrimeKG and SemMedDB edges in RTX-KG2 format'
    )
    arg_parser.add_argument('--test', dest='test', action='store_true', default=False,
                           help='Run in test mode (process only first 1000 records)')
    arg_parser.add_argument('--primekg-limit', dest='primekg_limit', type=int, default=None,
                           help='Maximum number of PrimeKG edges to process')
    arg_parser.add_argument('--semmeddb-limit', dest='semmeddb_limit', type=int, default=None,
                           help='Maximum number of SemMedDB edges to process')
    arg_parser.add_argument('primekg_edges_file', type=str,
                           help='Path to PrimeKG edges.csv file')
    arg_parser.add_argument('primekg_nodes_file', type=str,
                           help='Path to PrimeKG nodes.csv file')
    arg_parser.add_argument('semmeddb_connections_file', type=str,
                           help='Path to SemMedDB connections.csv.gz file')
    arg_parser.add_argument('subset_nodes_file', type=str,
                           help='Path to subset_nodes.csv file')
    arg_parser.add_argument('output_edges_file', type=str,
                           help='Path to output edges JSONL file')
    return arg_parser

def load_subset_nodes(subset_nodes_file: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Load subset nodes and create mappings:
    - node_id_to_curie: maps various node ID formats to CURIE
    - node_index_to_curie: maps node_index to CURIE (for PrimeKG)
    """
    date("Loading subset nodes")
    
    node_id_to_curie = {}
    node_index_to_curie = {}
    
    with open(subset_nodes_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            node_id = row['id']
            description = row.get('description', '')
            
            # The node_id is already in CURIE format
            curie = node_id
            node_id_to_curie[node_id] = curie
            
            # Extract the raw ID from the CURIE for mapping
            if ':' in node_id:
                raw_id = node_id.split(':', 1)[1]
                node_id_to_curie[raw_id] = curie
            
            # Also map from the semantic_type field which contains CUI
            # The semantic_type field often contains the CUI (e.g., "C0162783")
            semantic_type = row.get('semantic_type', '')
            if semantic_type and semantic_type.startswith('C') and len(semantic_type) >= 7:
                # This looks like a CUI
                node_id_to_curie[semantic_type] = curie
    
    date(f"Loaded {len(node_id_to_curie)} subset nodes")
    return node_id_to_curie, node_index_to_curie

def load_primekg_nodes(primekg_nodes_file: str) -> Dict[str, str]:
    """
    Load PrimeKG nodes and create mapping from node_index to node_id
    """
    date("Loading PrimeKG nodes")
    
    node_index_to_id = {}
    
    with open(primekg_nodes_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            node_index = row['node_index']
            node_id = row['node_id']
            node_index_to_id[node_index] = node_id
    
    date(f"Loaded {len(node_index_to_id)} PrimeKG nodes")
    return node_index_to_id

def process_primekg_edges(primekg_edges_file: str, node_index_to_id: Dict[str, str], 
                         node_id_to_curie: Dict[str, str], edges_output, test_mode: bool, 
                         limit: Optional[int] = None) -> int:
    """Process PrimeKG edges and write to output"""
    date("Starting PrimeKG edges processing")
    
    edge_count = 0
    max_edges = limit if limit is not None else (1000 if test_mode else float('inf'))
    
    with open(primekg_edges_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if edge_count >= max_edges:
                break
                
            x_index = row['x_index']
            y_index = row['y_index']
            relation = row['relation']
            display_relation = row['display_relation']
            
            # Get node IDs from indices
            x_node_id = node_index_to_id.get(x_index)
            y_node_id = node_index_to_id.get(y_index)
            
            # Skip if either node index is not found
            if not x_node_id or not y_node_id:
                continue
            
            # Get CURIE IDs for the nodes
            subject_curie = node_id_to_curie.get(x_node_id)
            object_curie = node_id_to_curie.get(y_node_id)
            
            # Skip if either node is not in our subset
            if not subject_curie or not object_curie:
                continue
            
            # Create edge using kg2_util
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            relation_curie = f"{PRIMEKG_CURIE_PREFIX}:{relation}"
            
            edge = kg2_util.make_edge(
                subject_id=subject_curie,
                object_id=object_curie,
                relation_curie=relation_curie,
                relation_label=display_relation,
                primary_knowledge_source=f"{PRIMEKG_CURIE_PREFIX}:",
                update_date=update_date
            )
            
            edges_output.write(edge)
            edge_count += 1
            
            if edge_count % 10000 == 0:
                print(f"Processed {edge_count} PrimeKG edges")
    
    date(f"Finished PrimeKG edges processing: {edge_count} edges")
    return edge_count

def process_semmeddb_edges(semmeddb_connections_file: str, node_id_to_curie: Dict[str, str],
                          edges_output, test_mode: bool, limit: Optional[int] = None) -> int:
    """Process SemMedDB edges and write to output"""
    date("Starting SemMedDB edges processing")
    
    edge_count = 0
    max_edges = limit if limit is not None else (1000 if test_mode else float('inf'))
    
    with gzip.open(semmeddb_connections_file, 'rt', encoding='utf-8') as f:
        # SemMedDB connections file format: :START_ID,:END_ID,:TYPE,frequency
        for line in f:
            if edge_count >= max_edges:
                break
                
            parts = line.strip().split(',')
            if len(parts) < 3:
                continue
                
            start_id = parts[0]
            end_id = parts[1]
            relation_type = parts[2]
            frequency = parts[3] if len(parts) > 3 else '1'
            
            # Get CURIE IDs for the nodes
            subject_curie = node_id_to_curie.get(start_id)
            object_curie = node_id_to_curie.get(end_id)
            
            # Skip if either node is not in our subset
            if not subject_curie or not object_curie:
                continue
            
            # Create edge using kg2_util
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            relation_curie = f"{SEMMEDDB_CURIE_PREFIX}:{relation_type}"
            
            edge = kg2_util.make_edge(
                subject_id=subject_curie,
                object_id=object_curie,
                relation_curie=relation_curie,
                relation_label=relation_type,
                primary_knowledge_source=f"{SEMMEDDB_CURIE_PREFIX}:",
                update_date=update_date
            )
            
            # Add frequency as additional property if available
            if frequency and frequency != '1':
                try:
                    edge['frequency'] = float(frequency)
                except ValueError:
                    pass
            
            edges_output.write(edge)
            edge_count += 1
            
            if edge_count % 10000 == 0:
                print(f"Processed {edge_count} SemMedDB edges")
    
    date(f"Finished SemMedDB edges processing: {edge_count} edges")
    return edge_count

def main():
    """Main function"""
    date("Starting merge_primekg_semmeddb_edges.py")
    
    args = make_arg_parser().parse_args()
    primekg_edges_file = args.primekg_edges_file
    primekg_nodes_file = args.primekg_nodes_file
    semmeddb_connections_file = args.semmeddb_connections_file
    subset_nodes_file = args.subset_nodes_file
    output_edges_file = args.output_edges_file
    test_mode = args.test
    primekg_limit = args.primekg_limit
    semmeddb_limit = args.semmeddb_limit
    
    # Load subset nodes
    node_id_to_curie, node_index_to_curie = load_subset_nodes(subset_nodes_file)
    
    # Load PrimeKG nodes
    node_index_to_id = load_primekg_nodes(primekg_nodes_file)
    
    # Create output file
    edges_info, _ = kg2_util.create_kg2_jsonlines(test_mode)
    edges_output = edges_info[0]
    
    try:
        # Process PrimeKG edges
        primekg_count = process_primekg_edges(
            primekg_edges_file, node_index_to_id, node_id_to_curie, edges_output, 
            test_mode, primekg_limit
        )
        
        # Process SemMedDB edges
        semmeddb_count = process_semmeddb_edges(
            semmeddb_connections_file, node_id_to_curie, edges_output,
            test_mode, semmeddb_limit
        )
        
        print(f"Total PrimeKG edges processed: {primekg_count}")
        print(f"Total SemMedDB edges processed: {semmeddb_count}")
        print(f"Total edges written: {primekg_count + semmeddb_count}")
        
    finally:
        kg2_util.close_kg2_jsonlines(edges_info, None, output_edges_file, None)
    
    date("Finished merge_primekg_semmeddb_edges.py")

if __name__ == '__main__':
    main()
