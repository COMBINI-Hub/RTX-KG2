#!/usr/bin/env python3
"""
merge_with_normalization_streaming.py: Merge SemMedDB, PrimeKG, and iKraph using
streaming processing to handle full datasets without memory limits.

Key features:
- Streaming processing to avoid memory limits
- CURIE mapping per dataset (BioKDE placeholder, iKraph, PrimeKG, SemMedDB)
- Name-based fallback normalization for nodes missing CURIEs
- RTX-KG2-like edge structure and deterministic ID for deduplication
- CLI with test/limit flags and configurable input paths
"""

import argparse
import csv
import datetime
import gzip
import io
import json
import logging
import os
import sys
import subprocess
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

# =============================================================================
# CONFIGURATION - Set your base paths here
# =============================================================================
# 
# To use this script with your own data:
# 1. Update the BASE_PATHS below to point to your dataset directories
# 2. Update DATASET_FILES if your file names are different
# 3. Run the script - it will use these paths as defaults
# 4. You can still override individual paths with command-line arguments
#
# Example for different setup:
# BASE_PATHS = {
#     'semmeddb': '/path/to/your/semmeddb/data',
#     'primekg': '/path/to/your/primekg/data', 
#     'ikraph': '/path/to/your/ikraph/data'
# }

# Base paths for each dataset (modify these to match your setup)
BASE_PATHS = {
    'semmeddb': '/Users/drshika2/RTX-KG2/SemMedDB_Data',
    'primekg': '/Users/drshika2/RTX-KG2/PrimeKG_data', 
    'ikraph': '/Users/drshika2/neo4jexploration/iKraph/import'
}

# Dataset-specific file patterns (relative to base paths)
DATASET_FILES = {
    'semmeddb': {
        'entities': 'concept.csv.gz',
        'connections': 'connections.csv.gz'
    },
    'primekg': {
        'nodes': 'primekg_nodes.csv',
        'edges': 'primekg_edges.csv'
    },
    'ikraph': {
        'nodes': [
            'nodes_gene.csv.gz',
            'nodes_disease.csv.gz', 
            'nodes_chemical.csv.gz',
            'nodes_anatomy.csv.gz',
            'nodes_biological process.csv.gz',
            'nodes_cellular component.csv.gz',
            'nodes_molecular function.csv.gz',
            'nodes_pathway.csv.gz',
            'nodes_pharmacologic class.csv.gz',
            'nodes_species.csv.gz',
            'nodes_cellline.csv.gz',
            'nodes_dnamutation.csv.gz'
        ],
        'relationships': [
            'relationships_db.mapped.csv.gz',
            'relationships_pubmed.mapped.csv.gz'
        ]
    }
}


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            # Align log file name with main script for consistent tracking
            logging.FileHandler('merge_with_normalization.log')
        ]
    )
    return logging.getLogger(__name__)

def date(print_str: str):
    return print(print_str, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


# ------------------------- Robust file readers -------------------------

def _open_text(path: str) -> io.TextIOBase:
    """Open text file with gzip support and robust encoding fallback.

    Strategy:
    1) Try utf-8
    2) Try latin-1 (covers most cases)
    3) Try utf-8 with errors='replace' (last resort)
    """
    if path.endswith('.gz'):
        opener = gzip.open
    else:
        opener = open
    
    try:
        return opener(path, 'rt', encoding='utf-8')  # type: ignore[arg-type]
    except UnicodeDecodeError:
        try:
            return opener(path, 'rt', encoding='latin-1')  # type: ignore[arg-type]
        except UnicodeDecodeError:
            return opener(path, 'rt', encoding='utf-8', errors='replace')  # type: ignore[arg-type]


def _csv_reader(path: str, dict_reader: bool = False):
    """Create CSV reader with robust encoding handling"""
    f = _open_text(path)
    if dict_reader:
        reader = csv.DictReader(f)
    else:
        reader = csv.reader(f)
    return f, reader


# ------------------------- Streaming data loaders -------------------------

def load_semmeddb_streaming(entity_path: str, connections_path: str, test: bool, limit: Optional[int], logger, output_dir: str) -> Tuple[str, str]:
    """Load SemMedDB data with streaming to avoid memory limits"""
    logger.info("Loading SemMedDB data (streaming mode)")
    
    # Create output files for streaming
    nodes_file = os.path.join(output_dir, 'semmeddb_nodes.jsonl')
    edges_file = os.path.join(output_dir, 'semmeddb_edges.jsonl')
    
    # Load entities with streaming
    entity_count = 0
    max_entities = limit if limit is not None else (100 if test else float('inf'))
    
    if os.path.exists(entity_path):
        logger.info(f"Loading entities from {entity_path}")
        f, reader = _csv_reader(entity_path)
        try:
            with open(nodes_file, 'w', encoding='utf-8') as out_f:
                for row in reader:
                    if entity_count >= max_entities:
                        break
                    if isinstance(row, list) and len(row) >= 11:
                        # Header in concept.csv.gz: "CUI","name","type","score","aliases","freq1","freq2","start","end","rank","label"
                        entity_id = row[0].strip('"').strip()
                        entity_name = row[1].strip('"')
                        entity_type = row[2].strip('"')
                        node_data = {
                            'id': entity_id,
                            'name': entity_name,
                            'type': entity_type,
                            'source': 'SemMedDB'
                        }
                        out_f.write(json.dumps(node_data) + '\n')
                        entity_count += 1
                        if entity_count % 100000 == 0:
                            logger.info(f"Loaded {entity_count} entities...")
        finally:
            f.close()
        logger.info(f"Loaded {entity_count} SemMedDB entities")

    # Load connections with streaming
    edge_count = 0
    max_edges = limit if limit is not None else (100 if test else float('inf'))
    
    if os.path.exists(connections_path):
        logger.info(f"Loading connections from {connections_path}")
        f, reader = _csv_reader(connections_path)
        try:
            with open(edges_file, 'w', encoding='utf-8') as out_f:
                for row in reader:
                    if edge_count >= max_edges:
                        break
                    if isinstance(row, list) and len(row) >= 4:
                        # Format: sentence_id, cui, relation, frequency
                        subject_id, object_id, relation, frequency = row[1], row[1], row[2], row[3]
                        edge_data = {
                            'subject': subject_id,
                            'object': object_id,
                            'relation': relation,
                            'frequency': frequency,
                            'source': 'SemMedDB'
                        }
                        out_f.write(json.dumps(edge_data) + '\n')
                        edge_count += 1
                        if edge_count % 100000 == 0:
                            logger.info(f"Loaded {edge_count} connections...")
        finally:
            f.close()
        logger.info(f"Loaded {edge_count} SemMedDB connections")

    logger.info(f"SemMedDB summary: {entity_count} nodes, {edge_count} edges")
    return nodes_file, edges_file


def load_primekg_streaming(nodes_csv: str, edges_csv: str, test: bool, limit: Optional[int], logger, output_dir: str) -> Tuple[str, str]:
    """Load PrimeKG data with streaming"""
    logger.info("Loading PrimeKG data (streaming mode)")
    
    nodes_file = os.path.join(output_dir, 'primekg_nodes.jsonl')
    edges_file = os.path.join(output_dir, 'primekg_edges.jsonl')
    
    # Load nodes with streaming
    node_count = 0
    max_nodes = limit if limit is not None else (100 if test else float('inf'))
    
    if os.path.exists(nodes_csv):
        logger.info(f"Loading PrimeKG nodes from {nodes_csv}")
        f_nodes, reader_nodes = _csv_reader(nodes_csv, dict_reader=True)
        try:
            with open(nodes_file, 'w', encoding='utf-8') as out_f:
                for row in reader_nodes:
                    if node_count >= max_nodes:
                        break
                    # Use node_index as the exported ID to align with edges x_index/y_index
                    node_data = {
                        'id': row.get('node_index', ''),
                        'name': row.get('node_name', ''),
                        'type': row.get('node_type', ''),
                        'source': 'PrimeKG'
                    }
                    out_f.write(json.dumps(node_data) + '\n')
                    node_count += 1
                    if node_count % 10000 == 0:
                        logger.info(f"Loaded {node_count} PrimeKG nodes...")
        finally:
            f_nodes.close()
        logger.info(f"Loaded {node_count} PrimeKG nodes")

    # Load edges with streaming
    edge_count = 0
    max_edges = limit if limit is not None else (100 if test else float('inf'))
    
    if os.path.exists(edges_csv):
        logger.info(f"Loading PrimeKG edges from {edges_csv}")
        f_edges, reader_edges = _csv_reader(edges_csv, dict_reader=True)
        try:
            with open(edges_file, 'w', encoding='utf-8') as out_f:
                for row in reader_edges:
                    if edge_count >= max_edges:
                        break
                    edge_data = {
                        'subject': row.get('x_index', ''),
                        'object': row.get('y_index', ''),
                        'relation': row.get('relation', ''),
                        'source': 'PrimeKG'
                    }
                    out_f.write(json.dumps(edge_data) + '\n')
                    edge_count += 1
                    if edge_count % 10000 == 0:
                        logger.info(f"Loaded {edge_count} PrimeKG edges...")
        finally:
            f_edges.close()
        logger.info(f"Loaded {edge_count} PrimeKG edges")

    logger.info(f"PrimeKG summary: {node_count} nodes, {edge_count} edges")
    return nodes_file, edges_file


def load_ikraph_streaming(ikraph_dir: str, relationship_files: List[str], test: bool, limit: Optional[int], logger, output_dir: str) -> Tuple[str, str]:
    """Load iKraph data with streaming"""
    logger.info("Loading iKraph data (streaming mode)")
    
    nodes_file = os.path.join(output_dir, 'ikraph_nodes.jsonl')
    edges_file = os.path.join(output_dir, 'ikraph_edges.jsonl')
    
    # Load nodes with streaming
    node_count = 0
    max_nodes = limit if limit is not None else (100 if test else float('inf'))
    
    node_files = DATASET_FILES['ikraph']['nodes']
    with open(nodes_file, 'w', encoding='utf-8') as out_f:
        for node_file in node_files:
            file_path = os.path.join(ikraph_dir, node_file)
            if os.path.exists(file_path):
                logger.info(f"Loading nodes from {node_file}")
                f, reader = _csv_reader(file_path, dict_reader=True)
                try:
                    for row in reader:
                        if node_count >= max_nodes:
                            break
                        node_data = {
                            'id': row.get('biokdeid:ID', ''),
                            'name': row.get('official_name', '') or row.get('common_name', ''),
                            'type': row.get('type', ''),
                            'source': 'iKraph'
                        }
                        out_f.write(json.dumps(node_data) + '\n')
                        node_count += 1
                        if node_count % 10000 == 0:
                            logger.info(f"Loaded {node_count} iKraph nodes...")
                finally:
                    f.close()
                if node_count >= max_nodes:
                    break

    # Load relationships with streaming
    edge_count = 0
    max_edges = limit if limit is not None else (100 if test else float('inf'))
    
    with open(edges_file, 'w', encoding='utf-8') as out_f:
        for rel_file in relationship_files:
            if os.path.exists(rel_file):
                logger.info(f"Loading relationships from {os.path.basename(rel_file)}")
                f, reader = _csv_reader(rel_file, dict_reader=True)
                try:
                    for row in reader:
                        if edge_count >= max_edges:
                            break
                        edge_data = {
                            'subject': row.get(':START_ID', ''),
                            'object': row.get(':END_ID', ''),
                            'relation': row.get('relationship_type', ''),
                            'probability': row.get('probability', ''),
                            'source': 'iKraph'
                        }
                        out_f.write(json.dumps(edge_data) + '\n')
                        edge_count += 1
                        if edge_count % 10000 == 0:
                            logger.info(f"Loaded {edge_count} iKraph edges...")
                finally:
                    f.close()
                if edge_count >= max_edges:
                    break

    logger.info(f"iKraph summary: {node_count} nodes, {edge_count} edges")
    return nodes_file, edges_file


# ------------------------- Streaming merge functions -------------------------

def merge_streaming_data(output_dir: str, logger) -> Tuple[str, str]:
    """Merge all streaming data files into final outputs with source aggregation and edge frequency"""
    logger.info("Merging streaming data files")
    
    # Final output files
    final_nodes_file = os.path.join(output_dir, 'nodes_neo4j.csv')
    final_edges_file = os.path.join(output_dir, 'relationships_neo4j.csv')
    
    # Collect all node and edge files
    node_files = [
        os.path.join(output_dir, 'semmeddb_nodes.jsonl'),
        os.path.join(output_dir, 'primekg_nodes.jsonl'),
        os.path.join(output_dir, 'ikraph_nodes.jsonl')
    ]
    
    edge_files = [
        os.path.join(output_dir, 'semmeddb_edges.jsonl'),
        os.path.join(output_dir, 'primekg_edges.jsonl'),
        os.path.join(output_dir, 'ikraph_edges.jsonl')
    ]
    
    # Merge nodes with source aggregation and type-first labeling
    logger.info("Merging nodes with source aggregation and type-first labeling")
    merged_nodes = {}
    for node_file in node_files:
        if os.path.exists(node_file):
            with open(node_file, 'r', encoding='utf-8') as f:
                for line in f:
                    node = json.loads(line.strip())
                    nid = node.get('id', '')
                    if not nid:
                        continue
                    entry = merged_nodes.get(nid)
                    if entry is None:
                        merged_nodes[nid] = {
                            'id': nid,
                            'name': node.get('name', ''),
                            'type': node.get('type', ''),
                            'source': node.get('source', ''),
                            'sources': set([s for s in [node.get('source', '')] if s])
                        }
                    else:
                        # prefer first non-empty name/type
                        if not entry.get('name') and node.get('name'):
                            entry['name'] = node.get('name')
                        if not entry.get('type') and node.get('type'):
                            entry['type'] = node.get('type')
                        src = node.get('source', '')
                        if src:
                            entry['sources'].add(src)

    with open(final_nodes_file, 'w', encoding='utf-8', newline='') as out_f:
        writer = csv.writer(out_f)
        writer.writerow([':ID', 'name', 'type', 'sources:string[]', ':LABEL'])
        for nid, node in merged_nodes.items():
            # Create meaningful labels prioritizing biological type
            labels = []
            node_type = (node.get('type', '') or '').lower()
            if 'gene' in node_type or 'protein' in node_type:
                labels.append('Gene')
            elif 'disease' in node_type:
                labels.append('Disease')
            elif 'chemical' in node_type or 'drug' in node_type:
                labels.append('Chemical')
            elif 'anatomy' in node_type:
                labels.append('Anatomy')
            elif 'pathway' in node_type:
                labels.append('Pathway')
            elif 'species' in node_type:
                labels.append('Species')
            elif 'cell' in node_type:
                labels.append('Cell')
            elif 'mutation' in node_type:
                labels.append('Mutation')
            elif 'biological process' in node_type:
                labels.append('BiologicalProcess')
            elif 'cellular component' in node_type:
                labels.append('CellularComponent')
            elif 'molecular function' in node_type:
                labels.append('MolecularFunction')
            elif 'pharmacologic class' in node_type:
                labels.append('PharmacologicClass')
            elif 'dna mutation' in node_type:
                labels.append('Mutation')
            else:
                labels.append('Entity')

            # Add source-specific labels from all sources
            sources_list = sorted(list(node['sources'])) if isinstance(node.get('sources'), set) else []
            for s in sources_list:
                if 'SemMedDB' in s:
                    labels.append('SemMedDB')
                elif 'PrimeKG' in s:
                    labels.append('PrimeKG')
                elif 'iKraph' in s:
                    labels.append('iKraph')
            if len(sources_list) > 1:
                labels.append('MergedNode')
            label_string = ';'.join(labels)

            writer.writerow([nid, node.get('name', ''), node.get('type', ''), ';'.join(sources_list), label_string])
    
    # Prepare node ID maps for remapping edge endpoints to existing merged node IDs
    node_id_set = set(merged_nodes.keys())
    lower_to_id = {nid.lower(): nid for nid in merged_nodes.keys()}

    def _normalize_id(raw_id: str) -> str:
        if raw_id is None:
            return ''
        sid = str(raw_id).strip().strip('"').strip("'")
        return sid

    def _remap_id(raw_id: str) -> str:
        sid = _normalize_id(raw_id)
        if not sid or sid == '0':
            return ''
        if sid in node_id_set:
            return sid
        lid = sid.lower()
        return lower_to_id.get(lid, '')

    # Merge edges with aggregation (sources + frequency) and meaningful relationship types
    logger.info("Merging edges with aggregation (sources/frequency) and endpoint remapping")
    agg = {}
    for edge_file in edge_files:
        if os.path.exists(edge_file):
            with open(edge_file, 'r', encoding='utf-8') as f:
                for line in f:
                    edge = json.loads(line.strip())
                    # Remap endpoints to merged node IDs when necessary
                    s = _remap_id(edge.get('subject', ''))
                    o = _remap_id(edge.get('object', ''))
                    if not s or not o:
                        continue
                    rel = (edge.get('relation', '') or '').strip()
                    src = edge.get('source', '')
                    key = (s, o, rel)
                    if key not in agg:
                        agg[key] = {'subject': s, 'object': o, 'relation': rel, 'sources': set([s for s in [src] if s]), 'frequency': 1}
                    else:
                        agg[key]['frequency'] += 1
                        if src:
                            agg[key]['sources'].add(src)

    with open(final_edges_file, 'w', encoding='utf-8', newline='') as out_f:
        writer = csv.writer(out_f)
        writer.writerow([':START_ID', ':END_ID', ':TYPE', 'relation_label', 'source_predicate', 'sources:string[]', 'frequency', 'update_date', 'id', 'probability'])
        for (_s, _o, rel), rec in agg.items():
            rel_label = (rel or '').lower()
            rel_type = 'RELATIONSHIP'
            if 'ppi' in rel_label or 'protein' in rel_label:
                rel_type = 'PROTEIN_INTERACTION'
            elif 'regulates' in rel_label:
                rel_type = 'REGULATES'
            elif 'interacts' in rel_label:
                rel_type = 'INTERACTS'
            elif 'association' in rel_label:
                rel_type = 'ASSOCIATION'
            elif 'causes' in rel_label:
                rel_type = 'CAUSES'
            elif 'treats' in rel_label:
                rel_type = 'TREATS'
            elif 'correlation' in rel_label:
                rel_type = 'CORRELATION'
            elif 'participates' in rel_label:
                rel_type = 'PARTICIPATES'
            elif 'binds' in rel_label:
                rel_type = 'BINDS'
            elif 'localizes' in rel_label:
                rel_type = 'LOCALIZES'
            elif 'presents' in rel_label:
                rel_type = 'PRESENTS'
            elif 'resembles' in rel_label:
                rel_type = 'RESEMBLES'
            elif 'palliates' in rel_label:
                rel_type = 'PALLIATES'
            elif 'includes' in rel_label:
                rel_type = 'INCLUDES'
            elif 'covaries' in rel_label:
                rel_type = 'COVARIES'
            elif 'negative' in rel_label:
                rel_type = 'NEGATIVE_CORRELATION'
            elif 'positive' in rel_label:
                rel_type = 'POSITIVE_CORRELATION'

            sources_list = sorted(list(rec['sources']))
            # Populate columns; source_predicate and id empty in streaming aggregation
            writer.writerow([
                rec['subject'],
                rec['object'],
                rel_type,
                rec['relation'],
                rec['relation'],
                ';'.join(sources_list),
                rec['frequency'],
                '',
                '',
                ''
            ])
    
    return final_nodes_file, final_edges_file


def run_neo4j_admin_import(nodes_csv: str, rels_csv: str, db_name: str, overwrite: bool, offheap_memory: str, array_delimiter: str, trim_strings: bool, logger) -> int:
    """Run neo4j-admin database import with provided CSVs and options.

    This mirrors the successful command we used during the final load step.
    """
    cmd = [
        'neo4j-admin', 'database', 'import', 'full'
    ]
    # Database name (e.g., 'neo4j')
    cmd.append(db_name)
    # Flags
    if overwrite:
        cmd.append('--overwrite-destination')
    cmd.extend([
        f"--nodes={nodes_csv}",
        f"--relationships={rels_csv}",
        f"--trim-strings={'true' if trim_strings else 'false'}",
        f"--array-delimiter={array_delimiter}",
        f"--max-off-heap-memory={offheap_memory}"
    ])

    logger.info("neo4j-admin import command:")
    logger.info(' '.join(cmd))

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        logger.info(f"neo4j-admin stdout:\n{proc.stdout}")
        if proc.stderr:
            logger.warning(f"neo4j-admin stderr:\n{proc.stderr}")
        if proc.returncode != 0:
            logger.error(f"neo4j-admin import failed with exit code {proc.returncode}")
        return proc.returncode
    except FileNotFoundError:
        logger.error("neo4j-admin not found in PATH. Please ensure Neo4j is installed and neo4j-admin is accessible.")
        return 127


def main() -> int:
    parser = argparse.ArgumentParser(description='Merge KGs using streaming processing (no memory limits)')
    
    # Use configuration paths as defaults
    semmed_entity_default = os.path.join(BASE_PATHS['semmeddb'], DATASET_FILES['semmeddb']['entities'])
    semmed_connections_default = os.path.join(BASE_PATHS['semmeddb'], DATASET_FILES['semmeddb']['connections'])
    primekg_nodes_default = os.path.join(BASE_PATHS['primekg'], DATASET_FILES['primekg']['nodes'])
    primekg_edges_default = os.path.join(BASE_PATHS['primekg'], DATASET_FILES['primekg']['edges'])
    ikraph_dir_default = BASE_PATHS['ikraph']
    ikraph_relationships_default = [os.path.join(BASE_PATHS['ikraph'], f) for f in DATASET_FILES['ikraph']['relationships']]
    
    parser.add_argument('--semmed-entity', default=semmed_entity_default, help='SemMedDB entity file (gz or plain)')
    parser.add_argument('--semmed-connections', default=semmed_connections_default, help='SemMedDB connections file (gz or plain)')
    parser.add_argument('--primekg-nodes', default=primekg_nodes_default, help='PrimeKG nodes CSV')
    parser.add_argument('--primekg-edges', default=primekg_edges_default, help='PrimeKG edges CSV')
    parser.add_argument('--ikraph-dir', default=ikraph_dir_default, help='iKraph import directory containing nodes_*.csv.gz')
    parser.add_argument('--ikraph-relationships', nargs='+', default=ikraph_relationships_default, help='iKraph relationship files (gz or plain)')
    parser.add_argument('--output-dir', default='streaming_merge_output', help='Output directory')
    parser.add_argument('--test', action='store_true', default=False, help='Test mode: limit to ~100 records per source')
    parser.add_argument('--limit', type=int, default=None, help='Explicit limit per source (overrides test default)')
    parser.add_argument('--verbose', action='store_true', default=False, help='Verbose logging')
    # Optional Neo4j import step (via neo4j-admin)
    parser.add_argument('--neo4j-import', action='store_true', default=False, help='After merge, run neo4j-admin database import with the generated CSVs')
    parser.add_argument('--neo4j-db', default='neo4j', help='Neo4j database name for import (default: neo4j)')
    parser.add_argument('--neo4j-overwrite', action='store_true', default=True, help='Use --overwrite-destination for import')
    parser.add_argument('--neo4j-offheap', default='1G', help='neo4j-admin --max-off-heap-memory value (e.g., 512M, 1G)')
    parser.add_argument('--neo4j-array-delimiter', default=';', help='Array delimiter passed to neo4j-admin (default: ;)')
    parser.add_argument('--neo4j-trim-strings', action='store_true', default=True, help='Pass --trim-strings=true to neo4j-admin')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)
    # Align start message with main script wording
    logger.info("Starting merge with normalization (robust encodings)")
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    try:
        # Load all datasets with streaming
        sem_nodes_file, sem_edges_file = load_semmeddb_streaming(
            args.semmed_entity, args.semmed_connections, args.test, args.limit, logger, args.output_dir
        )
        
        pk_nodes_file, pk_edges_file = load_primekg_streaming(
            args.primekg_nodes, args.primekg_edges, args.test, args.limit, logger, args.output_dir
        )
        
        ik_nodes_file, ik_edges_file = load_ikraph_streaming(
            args.ikraph_dir, args.ikraph_relationships, args.test, args.limit, logger, args.output_dir
        )
        
        # Merge all streaming data
        final_nodes_file, final_edges_file = merge_streaming_data(args.output_dir, logger)
        
        logger.info("Streaming merge completed successfully")
        logger.info(f"Final outputs: {final_nodes_file}, {final_edges_file}")

        # Optional Neo4j import step
        if args.neo4j_import:
            logger.info("Starting neo4j-admin database import using generated CSVs")
            rc = run_neo4j_admin_import(
                nodes_csv=os.path.abspath(final_nodes_file),
                rels_csv=os.path.abspath(final_edges_file),
                db_name=args.neo4j_db,
                overwrite=bool(args.neo4j_overwrite),
                offheap_memory=args.neo4j_offheap,
                array_delimiter=args.neo4j_array_delimiter,
                trim_strings=bool(args.neo4j_trim_strings),
                logger=logger,
            )
            if rc != 0:
                return rc

        return 0
        
    except Exception as e:
        logger.error(f"Error during streaming merge process: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())




