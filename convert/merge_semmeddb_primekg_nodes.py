#!/usr/bin/env python3
"""
merge_semmeddb_primekg_nodes.py: Merges SemMedDB entity data with PrimeKG node data into RTX-KG2 format

This script processes:
- SemMedDB entity.gz file (compressed CSV with entity information)
- PrimeKG nodes.csv file (CSV with node information)
- Outputs merged nodes in RTX-KG2 JSONL format

Usage: python merge_semmeddb_primekg_nodes.py <semmeddb_entity.gz> <primekg_nodes.csv> <output_nodes.jsonl> [--test]
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
NCBIGENE_CURIE_PREFIX = kg2_util.CURIE_PREFIX_NCBI_GENE

# IRI mappings
SEMMEDDB_IRI = kg2_util.BASE_URL_SEMMEDDB
PRIMEKG_IRI = "https://primekg.ethz.ch/"

def date(print_str: str):
    """Print timestamped message"""
    return print(print_str, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def make_arg_parser():
    """Create command line argument parser"""
    arg_parser = argparse.ArgumentParser(
        description='Merge SemMedDB entity data with PrimeKG node data into RTX-KG2 format'
    )
    arg_parser.add_argument('--test', dest='test', action='store_true', default=False,
                           help='Run in test mode (process only first 1000 records)')
    arg_parser.add_argument('semmeddb_entity_file', type=str,
                           help='Path to SemMedDB entity.gz file')
    arg_parser.add_argument('primekg_nodes_file', type=str,
                           help='Path to PrimeKG nodes.csv file')
    arg_parser.add_argument('output_nodes_file', type=str,
                           help='Path to output nodes JSONL file')
    return arg_parser

def parse_semmeddb_entity_line(line: str) -> Optional[Dict]:
    """
    Parse a line from SemMedDB entity file
    Expected format: "entity_id","cui","numeric_id","semantic_type","name","aliases","source","frequency","score","rank"
    """
    try:
        # Remove quotes and split by comma
        line = line.strip().strip('"')
        parts = line.split('","')
        
        if len(parts) < 10:
            return None
            
        entity_id = parts[0].strip('"')
        cui = parts[1].strip('"')
        numeric_id = parts[2].strip('"')  # This is actually a numeric ID, not the name
        semantic_type = parts[3].strip('"')
        name = parts[4].strip('"')  # The actual entity name is in position 4
        aliases = parts[5].strip('"')
        source = parts[6].strip('"')
        frequency = parts[7].strip('"')
        score = parts[8].strip('"')
        rank = parts[9].strip('"')
        
        return {
            'entity_id': entity_id,
            'cui': cui,
            'numeric_id': numeric_id,
            'name': name,
            'semantic_type': semantic_type,
            'aliases': aliases,
            'source': source,
            'frequency': frequency,
            'score': score,
            'rank': rank
        }
    except Exception as e:
        print(f"Error parsing SemMedDB line: {e}", file=sys.stderr)
        return None

def parse_primekg_node_line(line: str) -> Optional[Dict]:
    """
    Parse a line from PrimeKG nodes file
    Expected format: node_index,node_id,node_type,node_name,node_source
    """
    try:
        parts = line.strip().split(',')
        if len(parts) < 5:
            return None
            
        return {
            'node_index': parts[0],
            'node_id': parts[1],
            'node_type': parts[2],
            'node_name': parts[3],
            'node_source': parts[4]
        }
    except Exception as e:
        print(f"Error parsing PrimeKG line: {e}", file=sys.stderr)
        return None

def create_semmeddb_node(entity_data: Dict, update_date: str) -> Dict:
    """Create a KG2 node from SemMedDB entity data"""
    # Use CUI as primary identifier if available, otherwise use entity_id
    if entity_data['cui'] and entity_data['cui'] != '':
        node_id = UMLS_CURIE_PREFIX + ':' + entity_data['cui']
        iri = f"https://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/CUI/{entity_data['cui']}"
    else:
        node_id = SEMMEDDB_CURIE_PREFIX + ':' + entity_data['entity_id']
        iri = SEMMEDDB_IRI + '#' + entity_data['entity_id']
    
    # Map semantic type to Biolink category
    category_label = map_semantic_type_to_biolink_category(entity_data['semantic_type'])
    
    # Create node
    node = kg2_util.make_node(
        id=node_id,
        iri=iri,
        name=entity_data['name'],
        category_label=category_label,
        update_date=update_date,
        provided_by=SEMMEDDB_CURIE_PREFIX + ':'
    )
    
    # Add additional properties
    # Note: The 'name' field now contains the actual entity name
    if entity_data['aliases'] and entity_data['aliases'] != '':
        aliases = [alias.strip() for alias in entity_data['aliases'].split('|') if alias.strip()]
        if aliases:
            node['synonym'] = aliases
            # Use the first alias as description if available
            node['description'] = aliases[0]
    
    # Add semantic type as a property (SemMedDB specific)
    if entity_data['semantic_type'] and entity_data['semantic_type'] != '':
        node['semmeddb_semantic_type'] = entity_data['semantic_type']
    
    # Add frequency and score if available
    if entity_data['frequency'] and entity_data['frequency'] != '':
        try:
            node['frequency'] = int(entity_data['frequency'])
        except ValueError:
            pass
    
    if entity_data['score'] and entity_data['score'] != '':
        try:
            node['score'] = float(entity_data['score'])
        except ValueError:
            pass
    
    return node

def create_primekg_node(node_data: Dict, update_date: str) -> Dict:
    """Create a KG2 node from PrimeKG node data"""
    node_id = PRIMEKG_CURIE_PREFIX + ':' + node_data['node_id']
    iri = PRIMEKG_IRI + 'node/' + node_data['node_id']
    
    # Map node type to Biolink category
    category_label = map_primekg_type_to_biolink_category(node_data['node_type'])
    
    # Create node
    node = kg2_util.make_node(
        id=node_id,
        iri=iri,
        name=node_data['node_name'],
        category_label=category_label,
        update_date=update_date,
        provided_by=PRIMEKG_CURIE_PREFIX + ':'
    )
    
    # Add node type and source as properties (PrimeKG specific)
    node['primekg_node_type'] = node_data['node_type']
    node['primekg_node_source'] = node_data['node_source']
    
    return node

def map_semantic_type_to_biolink_category(semantic_type: str) -> str:
    """Map SemMedDB semantic type to Biolink category"""
    if not semantic_type:
        return kg2_util.BIOLINK_CATEGORY_NAMED_THING
    
    # Common semantic type mappings
    semantic_type_mappings = {
        'aapp': kg2_util.BIOLINK_CATEGORY_ANATOMICAL_ENTITY,  # Amino Acid, Peptide, or Protein
        'bact': kg2_util.BIOLINK_CATEGORY_ORGANISM_TAXON,  # Bacterium
        'bpoc': kg2_util.BIOLINK_CATEGORY_ANATOMICAL_ENTITY,  # Body Part, Organ, or Organ Component
        'chem': kg2_util.BIOLINK_CATEGORY_CHEMICAL_ENTITY,  # Chemical
        'diap': kg2_util.BIOLINK_CATEGORY_DISEASE,  # Diagnostic Procedure
        'drdd': kg2_util.BIOLINK_CATEGORY_DRUG,  # Drug Delivery Device
        'dsyn': kg2_util.BIOLINK_CATEGORY_DISEASE,  # Disease or Syndrome
        'ftcn': kg2_util.BIOLINK_CATEGORY_NAMED_THING,  # Functional Concept
        'gngm': kg2_util.BIOLINK_CATEGORY_GENE,  # Gene or Genome
        'imft': kg2_util.BIOLINK_CATEGORY_DRUG,  # Immunologic Factor
        'lbpr': kg2_util.BIOLINK_CATEGORY_NAMED_THING,  # Laboratory Procedure (no specific category)
        'mobd': kg2_util.BIOLINK_CATEGORY_DISEASE,  # Mental or Behavioral Dysfunction
        'neop': kg2_util.BIOLINK_CATEGORY_DISEASE,  # Neoplastic Process
        'npop': kg2_util.BIOLINK_CATEGORY_NAMED_THING,  # Natural Phenomenon or Process
        'orga': kg2_util.BIOLINK_CATEGORY_ORGANISM_TAXON,  # Organism
        'phsu': kg2_util.BIOLINK_CATEGORY_DRUG,  # Pharmacologic Substance
        'sosy': kg2_util.BIOLINK_CATEGORY_DISEASE,  # Sign or Symptom
        'virs': kg2_util.BIOLINK_CATEGORY_ORGANISM_TAXON,  # Virus
    }
    
    return semantic_type_mappings.get(semantic_type.lower(), kg2_util.BIOLINK_CATEGORY_NAMED_THING)

def map_primekg_type_to_biolink_category(node_type: str) -> str:
    """Map PrimeKG node type to Biolink category"""
    if not node_type:
        return kg2_util.BIOLINK_CATEGORY_NAMED_THING
    
    # PrimeKG node type mappings
    type_mappings = {
        'gene/protein': kg2_util.BIOLINK_CATEGORY_GENE,
        'drug': kg2_util.BIOLINK_CATEGORY_DRUG,
        'disease': kg2_util.BIOLINK_CATEGORY_DISEASE,
        'side_effect': kg2_util.BIOLINK_CATEGORY_DISEASE,
        'indication': kg2_util.BIOLINK_CATEGORY_DISEASE,
        'contraindication': kg2_util.BIOLINK_CATEGORY_DISEASE,
        'off_label_use': kg2_util.BIOLINK_CATEGORY_DISEASE,
        'pathway': kg2_util.BIOLINK_CATEGORY_PATHWAY,
        'protein': kg2_util.BIOLINK_CATEGORY_PROTEIN,
        'anatomy': kg2_util.BIOLINK_CATEGORY_ANATOMICAL_ENTITY,
        'symptom': kg2_util.BIOLINK_CATEGORY_DISEASE,
        'phenotype': kg2_util.BIOLINK_CATEGORY_PHENOTYPIC_FEATURE,
    }
    
    return type_mappings.get(node_type.lower(), kg2_util.BIOLINK_CATEGORY_NAMED_THING)

def process_semmeddb_entities(semmeddb_file: str, nodes_output, test_mode: bool) -> Set[str]:
    """Process SemMedDB entity file and write nodes to output"""
    date("Starting SemMedDB entity processing")
    
    processed_entities = set()
    entity_count = 0
    max_entities = 100 if test_mode else 1000  # Limit to 100 for testing
    
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
    max_nodes = 100 if test_mode else 1000  # Limit to 100 for testing
    
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

def create_source_nodes(nodes_output):
    """Create source nodes for both SemMedDB and PrimeKG"""
    update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # SemMedDB source node
    semmeddb_source_node = kg2_util.make_node(
        id=SEMMEDDB_CURIE_PREFIX + ':',
        iri=SEMMEDDB_IRI,
        name='Semantic Medline Database (SemMedDB)',
        category_label=kg2_util.SOURCE_NODE_CATEGORY,
        update_date=update_date,
        provided_by=SEMMEDDB_CURIE_PREFIX + ':'
    )
    nodes_output.write(json.dumps(semmeddb_source_node) + '\n')
    
    # PrimeKG source node
    primekg_source_node = kg2_util.make_node(
        id=PRIMEKG_CURIE_PREFIX + ':',
        iri=PRIMEKG_IRI,
        name='PrimeKG Knowledge Graph',
        category_label=kg2_util.SOURCE_NODE_CATEGORY,
        update_date=update_date,
        provided_by=PRIMEKG_CURIE_PREFIX + ':'
    )
    nodes_output.write(json.dumps(primekg_source_node) + '\n')

def main():
    """Main function"""
    date("Starting merge_semmeddb_primekg_nodes.py")
    
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
        
        # Create source nodes
        create_source_nodes(nodes_output)
        
        print(f"Total SemMedDB entities processed: {len(semmeddb_entities)}")
        print(f"Total PrimeKG nodes processed: {len(primekg_nodes)}")
        print(f"Total nodes written: {len(semmeddb_entities) + len(primekg_nodes) + 2}")  # +2 for source nodes
        
    finally:
        nodes_output.close()
    
    date("Finished merge_semmeddb_primekg_nodes.py")

if __name__ == '__main__':
    main()
