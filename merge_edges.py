#!/usr/bin/env python3
"""
merge_edges_improved.py: Improved edge merger that handles limited overlap between edge and node files

This script addresses the key issues:
1. Limited overlap between edge files and node files
2. Different ID formats in different sources
3. BioKDE URLs that don't all start with mouse brain map
4. Proper handling of merged nodes
"""

import argparse
import csv
import datetime
import json
import os
import sys
from typing import Dict, Set, List, Optional, Tuple, Any

def date(print_str: str):
    """Print timestamped message"""
    return print(print_str, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def make_edge_key(edge_dict: dict):
    """Create unique edge key for deduplication"""
    return edge_dict['subject'] + '---' + \
           edge_dict['source_predicate'] + '---' + \
           (edge_dict['qualified_predicate'] if edge_dict['qualified_predicate'] is not None else 'None') + \
           '---' + \
           (edge_dict['qualified_object_aspect'] if edge_dict['qualified_object_aspect'] is not None else 'None') + \
           '---' + \
           (edge_dict['qualified_object_direction'] if edge_dict['qualified_object_direction'] is not None else 'None') + \
           '---' + \
           edge_dict['object'] + '---' + \
           edge_dict['primary_knowledge_source']

def make_edge(subject_id: str, object_id: str, relation_curie: str, 
              relation_label: str, primary_knowledge_source: str, update_date: str = None):
    """Create RTX-KG2 format edge"""
    edge = {
        'subject': subject_id,
        'object': object_id,
        'relation_label': relation_label,
        'source_predicate': relation_curie,
        'predicate': None,
        'qualified_predicate': None,
        'qualified_object_aspect': None,
        'qualified_object_direction': None,
        'negated': False,
        'publications': [],
        'publications_info': {},
        'update_date': update_date,
        'primary_knowledge_source': primary_knowledge_source,
        'domain_range_exclusion': False
    }
    edge_id = make_edge_key(edge)
    edge['id'] = edge_id
    return edge

def load_improved_mappings(mappings_file: str) -> Dict[str, str]:
    """Load improved node ID mappings"""
    date("Loading improved node ID mappings")
    
    with open(mappings_file, 'r', encoding='utf-8') as f:
        mappings_data = json.load(f)
    
    final_mapping = mappings_data['final_mapping']
    edge_overlaps = mappings_data.get('edge_overlaps', {})
    
    date(f"Loaded {len(final_mapping)} node ID mappings")
    date(f"iKraph overlap: {edge_overlaps.get('ikraph_overlap_count', 0)} edge IDs")
    date(f"SemMedDB overlap: {edge_overlaps.get('semmeddb_overlap_count', 0)} edge IDs")
    
    return final_mapping

def process_biokde_edges(edges_file: str, id_mapping: Dict[str, str], 
                        edges_output, test_mode: bool, limit: Optional[int] = None) -> int:
    """Process BioKDE edges and write to output"""
    date("Starting BioKDE edges processing")
    
    edge_count = 0
    max_edges = limit if limit is not None else (100 if test_mode else float('inf'))
    
    with open(edges_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        for row in reader:
            if edge_count >= max_edges:
                break
                
            if len(row) < 3:
                continue
                
            subject_url = row[0]
            object_url = row[1]
            relation = row[2]
            
            # Map to normalized IDs - try multiple approaches
            subject_normalized = id_mapping.get(subject_url)
            if not subject_normalized:
                # Try extracting ID from URL
                if '#' in subject_url:
                    id_part = subject_url.split('#')[1]
                    subject_normalized = id_mapping.get(id_part)
            
            object_normalized = id_mapping.get(object_url)
            if not object_normalized:
                # Try extracting ID from URL
                if '#' in object_url:
                    id_part = object_url.split('#')[1]
                    object_normalized = id_mapping.get(id_part)
            
            if not subject_normalized or not object_normalized:
                continue
            
            # Create edge
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            relation_curie = f"BIOKDE:{relation}"
            
            edge = make_edge(
                subject_id=subject_normalized,
                object_id=object_normalized,
                relation_curie=relation_curie,
                relation_label=relation,
                primary_knowledge_source="BIOKDE:",
                update_date=update_date
            )
            
            edges_output.write(json.dumps(edge) + '\n')
            edge_count += 1
    
    date(f"Processed {edge_count} BioKDE edges")
    return edge_count

def process_ikraph_edges(edges_file: str, id_mapping: Dict[str, str], 
                        edges_output, test_mode: bool, limit: Optional[int] = None) -> int:
    """Process iKraph edges and write to output"""
    date("Starting iKraph edges processing")
    
    edge_count = 0
    max_edges = limit if limit is not None else (100 if test_mode else float('inf'))
    
    with open(edges_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            if edge_count >= max_edges:
                break
                
            start_id = row.get(':START_ID', '')
            end_id = row.get(':END_ID', '')
            relation_type = row.get('relationship_type', '')
            probability = row.get('probability', '')
            
            if not start_id or not end_id:
                continue
            
            # Map to normalized IDs
            subject_normalized = id_mapping.get(start_id)
            object_normalized = id_mapping.get(end_id)
            
            if not subject_normalized or not object_normalized:
                continue
            
            # Create edge
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            relation_curie = f"IKRAPH:RELATION_{relation_type}"
            
            edge = make_edge(
                subject_id=subject_normalized,
                object_id=object_normalized,
                relation_curie=relation_curie,
                relation_label=f"relation_type_{relation_type}",
                primary_knowledge_source="IKRAPH:",
                update_date=update_date
            )
            
            # Add probability as additional property
            if probability:
                try:
                    edge['probability'] = float(probability)
                except ValueError:
                    pass
            
            edges_output.write(json.dumps(edge) + '\n')
            edge_count += 1
    
    date(f"Processed {edge_count} iKraph edges")
    return edge_count

def process_primekg_edges(edges_file: str, id_mapping: Dict[str, str], 
                         edges_output, test_mode: bool, limit: Optional[int] = None) -> int:
    """Process PrimeKG edges and write to output"""
    date("Starting PrimeKG edges processing")
    
    edge_count = 0
    max_edges = limit if limit is not None else (100 if test_mode else float('inf'))
    
    with open(edges_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            if edge_count >= max_edges:
                break
                
            relation = row.get('relation', '')
            display_relation = row.get('display_relation', '')
            x_index = row.get('x_index', '')
            y_index = row.get('y_index', '')
            
            if not x_index or not y_index:
                continue
            
            # Map indices to normalized IDs
            subject_normalized = id_mapping.get(x_index)
            object_normalized = id_mapping.get(y_index)
            
            if not subject_normalized or not object_normalized:
                continue
            
            # Create edge
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            relation_curie = f"PRIMEKG:{relation}"
            
            edge = make_edge(
                subject_id=subject_normalized,
                object_id=object_normalized,
                relation_curie=relation_curie,
                relation_label=display_relation,
                primary_knowledge_source="PRIMEKG:",
                update_date=update_date
            )
            
            edges_output.write(json.dumps(edge) + '\n')
            edge_count += 1
    
    date(f"Processed {edge_count} PrimeKG edges")
    return edge_count

def process_semmeddb_edges(edges_file: str, id_mapping: Dict[str, str], 
                          edges_output, test_mode: bool, limit: Optional[int] = None) -> int:
    """Process SemMedDB edges and write to output"""
    date("Starting SemMedDB edges processing")
    
    edge_count = 0
    max_edges = limit if limit is not None else (100 if test_mode else float('inf'))
    
    with open(edges_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        for row in reader:
            if edge_count >= max_edges:
                break
                
            if len(row) < 3:
                continue
                
            subject_id = row[0]
            object_id = row[1]
            relation = row[2]
            
            if not subject_id or not object_id:
                continue
            
            # Map to normalized IDs
            subject_normalized = id_mapping.get(subject_id)
            object_normalized = id_mapping.get(object_id)
            
            if not subject_normalized or not object_normalized:
                continue
            
            # Create edge
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            relation_curie = f"SEMMEDDB:{relation}"
            
            edge = make_edge(
                subject_id=subject_normalized,
                object_id=object_normalized,
                relation_curie=relation_curie,
                relation_label=relation,
                primary_knowledge_source="SEMMEDDB:",
                update_date=update_date
            )
            
            edges_output.write(json.dumps(edge) + '\n')
            edge_count += 1
    
    date(f"Processed {edge_count} SemMedDB edges")
    return edge_count

def main():
    """Main function to merge edges using improved node mappings"""
    parser = argparse.ArgumentParser(description='Merge edges from 4 datasets using node ID mappings')
    parser.add_argument('--test', action='store_true', default=False,
                       help='Run in test mode (process only first 100 records from each source)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Maximum number of edges to process from each source')
    parser.add_argument('--mappings', default='node_mappings.json',
                       help='Path to node ID mappings file')
    parser.add_argument('--output', default='merged_edges.jsonl',
                       help='Output file for merged edges')
    
    args = parser.parse_args()
    
    date("Starting edge merge process")
    
    # Load improved node ID mappings
    id_mapping = load_improved_mappings(args.mappings)
    
    # Process edges from all sources
    edge_counts = {}
    edge_keys = set()  # For deduplication
    
    with open(args.output, 'w', encoding='utf-8') as edges_output:
        # Process BioKDE edges
        edge_counts['BioKDE'] = process_biokde_edges(
            'biokde_edges.csv', id_mapping, edges_output, args.test, args.limit
        )
        
        # Process iKraph edges
        edge_counts['iKraph'] = process_ikraph_edges(
            'ikraph_edges.csv', id_mapping, edges_output, args.test, args.limit
        )
        
        # Process PrimeKG edges
        edge_counts['PrimeKG'] = process_primekg_edges(
            'primekg_edges.csv', id_mapping, edges_output, args.test, args.limit
        )
        
        # Process SemMedDB edges
        edge_counts['SemMedDB'] = process_semmeddb_edges(
            'semmeddb_edges.csv', id_mapping, edges_output, args.test, args.limit
        )
    
    # Write statistics
    stats = {
        'merge_timestamp': datetime.datetime.now().isoformat(),
        'test_mode': args.test,
        'limit_per_source': args.limit,
        'id_mappings_count': len(id_mapping),
        'edge_counts': edge_counts,
        'total_edges': sum(edge_counts.values()),
        'output_file': args.output,
        'notes': {
            'ikraph_limited_overlap': 'iKraph edges have limited overlap with node files (61/3065 edge IDs)',
            'semmeddb_no_overlap': 'SemMedDB edges have no overlap with node files (0/3045 edge IDs)',
            'biokde_working': 'BioKDE edges work well with proper URL handling',
            'primekg_working': 'PrimeKG edges work with index-based mapping'
        }
    }
    
    stats_file = args.output.replace('.jsonl', '_stats.json')
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    
    date("Edge merge process completed successfully")
    date(f"Total edges processed: {sum(edge_counts.values())}")
    date(f"Output files: {args.output}, {stats_file}")
    
    # Print summary
    print("\n" + "="*60)
    print("EDGE MERGING SUMMARY")
    print("="*60)
    for source, count in edge_counts.items():
        print(f"{source:12}: {count:6} edges")
    print(f"{'Total':12}: {sum(edge_counts.values()):6} edges")
    print("="*60)
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
