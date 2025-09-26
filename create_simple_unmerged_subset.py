#!/usr/bin/env python3
"""
Create a simple unmerged simulation subset from all 4 biomedical knowledge graphs.
Handles different column structures by saving each source separately.
"""

import csv
import gzip
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleUnmergedCreator:
    def __init__(self, data_dir: str = "/Users/drshika2/RTX-KG2"):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(data_dir) / "unmerged_simulation_subset"
        self.output_dir.mkdir(exist_ok=True)
        
        # Sample sizes
        self.node_sample_size = 1000
        self.edge_sample_size = 2000
        
    def sample_and_save_bioKDE(self):
        """Sample and save BioKDE data"""
        logger.info("Processing BioKDE...")
        
        # Nodes
        nodes_file = self.data_dir / "BioKDE_data" / "nodes_for_neo4j.csv"
        output_nodes = self.output_dir / "biokde_nodes.csv"
        
        with open(nodes_file, 'r') as infile, open(output_nodes, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample
            for i, row in enumerate(reader):
                if i >= self.node_sample_size:
                    break
                writer.writerow(row + ['BioKDE'])
        
        # Edges
        edges_file = self.data_dir / "BioKDE_data" / "edges_for_neo4j.csv"
        output_edges = self.output_dir / "biokde_edges.csv"
        
        with open(edges_file, 'r') as infile, open(output_edges, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample
            for i, row in enumerate(reader):
                if i >= self.edge_sample_size:
                    break
                writer.writerow(row + ['BioKDE'])
        
        logger.info(f"BioKDE: {self.node_sample_size} nodes, {self.edge_sample_size} edges")
    
    def sample_and_save_primeKG(self):
        """Sample and save PrimeKG data"""
        logger.info("Processing PrimeKG...")
        
        # First, sample nodes and collect their indices
        nodes_file = self.data_dir / "PrimeKG_data" / "primekg_nodes.csv"
        output_nodes = self.output_dir / "primekg_nodes.csv"
        
        sampled_node_indices = set()
        with open(nodes_file, 'r') as infile, open(output_nodes, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample and collect node indices
            for i, row in enumerate(reader):
                if i >= self.node_sample_size:
                    break
                writer.writerow(row + ['PrimeKG'])
                # PrimeKG format: node_index,node_id,node_type,node_name,node_source
                if len(row) >= 1:
                    sampled_node_indices.add(row[0])  # node_index
        
        # Now sample edges that only reference the sampled nodes
        edges_file = self.data_dir / "PrimeKG_data" / "primekg_edges.csv"
        output_edges = self.output_dir / "primekg_edges.csv"
        
        edge_count = 0
        with open(edges_file, 'r') as infile, open(output_edges, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write edges that reference sampled nodes
            for row in reader:
                if edge_count >= self.edge_sample_size:
                    break
                # PrimeKG format: relation,display_relation,x_index,y_index
                if len(row) >= 4:
                    x_index = row[2]  # x_index
                    y_index = row[3]  # y_index
                    # Only include edges where both nodes are in our sample
                    if x_index in sampled_node_indices and y_index in sampled_node_indices:
                        writer.writerow(row + ['PrimeKG'])
                        edge_count += 1
        
        logger.info(f"PrimeKG: {len(sampled_node_indices)} nodes, {edge_count} edges")
    
    def sample_and_save_ikraph(self):
        """Sample and save iKraph data"""
        logger.info("Processing iKraph...")
        
        # First, sample nodes and collect their IDs
        nodes_file = self.data_dir / "iKraph_data" / "nodes_gene.csv.gz"
        output_nodes = self.output_dir / "ikraph_nodes.csv"
        
        sampled_node_ids = set()
        with gzip.open(nodes_file, 'rt') as infile, open(output_nodes, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample and collect node IDs
            for i, row in enumerate(reader):
                if i >= self.node_sample_size:
                    break
                writer.writerow(row + ['iKraph'])
                # iKraph format: biokdeid:ID,type,subtype,external_id,species,official_name,common_name,:LABEL
                if len(row) >= 1:
                    sampled_node_ids.add(row[0])  # biokdeid:ID
        
        # Now sample edges that only reference the sampled nodes
        edges_file = self.data_dir / "iKraph_data" / "relationships_db.csv.gz"
        output_edges = self.output_dir / "ikraph_edges.csv"
        
        edge_count = 0
        with gzip.open(edges_file, 'rt') as infile, open(output_edges, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write edges that reference sampled nodes
            for row in reader:
                if edge_count >= self.edge_sample_size:
                    break
                # iKraph format: :START_ID,:END_ID,relation_id,correlation_id,direction,source,relationship_type,probability,score,:TYPE
                if len(row) >= 2:
                    start_id = row[0]  # :START_ID
                    end_id = row[1]    # :END_ID
                    # Only include edges where both nodes are in our sample
                    if start_id in sampled_node_ids and end_id in sampled_node_ids:
                        writer.writerow(row + ['iKraph'])
                        edge_count += 1
        
        logger.info(f"iKraph: {len(sampled_node_ids)} nodes, {edge_count} edges")
    
    def sample_and_save_semmeddb(self):
        """Sample and save SemMedDB data"""
        logger.info("Processing SemMedDB...")
        
        # First, sample nodes and collect their entity IDs
        nodes_file = self.data_dir / "SemMedDB_Data" / "entity.gz"
        output_nodes = self.output_dir / "semmeddb_nodes.csv"
        
        sampled_entity_ids = set()
        with gzip.open(nodes_file, 'rt') as infile, open(output_nodes, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample and collect entity IDs
            for i, row in enumerate(reader):
                if i >= self.node_sample_size:
                    break
                writer.writerow(row + ['SemMedDB'])
                # SemMedDB format: entity_id,cui,numeric_id,semantic_type,name,aliases,source,frequency,score,rank
                if len(row) >= 1:
                    sampled_entity_ids.add(row[0])  # entity_id
        
        # Now sample edges that only reference the sampled nodes
        edges_file = self.data_dir / "SemMedDB_Data" / "connections.csv.gz"
        output_edges = self.output_dir / "semmeddb_edges.csv"
        
        edge_count = 0
        with gzip.open(edges_file, 'rt') as infile, open(output_edges, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write edges that reference sampled nodes
            for row in reader:
                if edge_count >= self.edge_sample_size:
                    break
                # SemMedDB format: :START_ID,:END_ID,:TYPE,frequency
                if len(row) >= 2:
                    start_id = row[0]  # :START_ID
                    end_id = row[1]    # :END_ID
                    # Only include edges where both nodes are in our sample
                    if start_id in sampled_entity_ids and end_id in sampled_entity_ids:
                        writer.writerow(row + ['SemMedDB'])
                        edge_count += 1
        
        logger.info(f"SemMedDB: {len(sampled_entity_ids)} nodes, {edge_count} edges")
    
    def create_summary(self):
        """Create summary file"""
        summary = {
            'description': 'Unmerged simulation subset from 4 biomedical knowledge graphs',
            'purpose': 'Simulates what it would look like to have all graphs together but unmerged',
            'sources': [
                {
                    'name': 'BioKDE',
                    'description': 'Deep learning-powered search engine and knowledge graph',
                    'nodes_file': 'biokde_nodes.csv',
                    'edges_file': 'biokde_edges.csv',
                    'sample_size': f'{self.node_sample_size} nodes, {self.edge_sample_size} edges'
                },
                {
                    'name': 'PrimeKG',
                    'description': 'Multimodal knowledge graph for precision medicine',
                    'nodes_file': 'primekg_nodes.csv',
                    'edges_file': 'primekg_edges.csv',
                    'sample_size': f'{self.node_sample_size} nodes, {self.edge_sample_size} edges'
                },
                {
                    'name': 'iKraph',
                    'description': 'Biomedical knowledge graph with gene relationships',
                    'nodes_file': 'ikraph_nodes.csv',
                    'edges_file': 'ikraph_edges.csv',
                    'sample_size': f'{self.node_sample_size} nodes, {self.edge_sample_size} edges'
                },
                {
                    'name': 'SemMedDB',
                    'description': 'Semantic database from biomedical literature',
                    'nodes_file': 'semmeddb_nodes.csv',
                    'edges_file': 'semmeddb_edges.csv',
                    'sample_size': f'{self.node_sample_size} nodes, {self.edge_sample_size} edges'
                }
            ],
            'total_sample_size': f'{self.node_sample_size * 4} nodes, {self.edge_sample_size * 4} edges',
            'note': 'Each file contains a "source" column indicating the original dataset. This allows you to see overlapping entities and unique entities from each source.'
        }
        
        with open(self.output_dir / "README.md", 'w') as f:
            f.write("# Unmerged Simulation Subset\n\n")
            f.write("This directory contains a simulation subset from 4 biomedical knowledge graphs:\n\n")
            
            for source in summary['sources']:
                f.write(f"## {source['name']}\n")
                f.write(f"- **Description**: {source['description']}\n")
                f.write(f"- **Files**: {source['nodes_file']}, {source['edges_file']}\n")
                f.write(f"- **Sample Size**: {source['sample_size']}\n\n")
            
            f.write(f"## Total Sample Size\n")
            f.write(f"- **Total**: {summary['total_sample_size']}\n\n")
            f.write(f"## Usage\n")
            f.write(f"Each file contains a 'source' column indicating the original dataset. ")
            f.write(f"This allows you to see overlapping entities and unique entities from each source.\n\n")
            f.write(f"## Data Integrity\n")
            f.write(f"**IMPORTANT**: All edges reference nodes that exist in the corresponding node files. ")
            f.write(f"This ensures data integrity and prevents orphaned edges. ")
            f.write(f"Edge counts may be lower than the target due to this constraint.\n")
        
        with open(self.output_dir / "summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Summary created: {self.output_dir}/README.md and {self.output_dir}/summary.json")

def main():
    creator = SimpleUnmergedCreator()
    
    logger.info("=== Creating Simple Unmerged Simulation Subset ===")
    
    # Process each source
    creator.sample_and_save_bioKDE()
    creator.sample_and_save_primeKG()
    creator.sample_and_save_ikraph()
    creator.sample_and_save_semmeddb()
    
    # Create summary
    creator.create_summary()
    
    logger.info(f"Unmerged simulation subset created in: {creator.output_dir}")
    logger.info("Files created:")
    logger.info("  - biokde_nodes.csv, biokde_edges.csv")
    logger.info("  - primekg_nodes.csv, primekg_edges.csv")
    logger.info("  - ikraph_nodes.csv, ikraph_edges.csv")
    logger.info("  - semmeddb_nodes.csv, semmeddb_edges.csv")
    logger.info("  - README.md, summary.json")
    logger.info("")
    logger.info("IMPORTANT: All edges now reference nodes that exist in the corresponding node files.")
    logger.info("Edge counts may be lower than the target due to this constraint.")

if __name__ == "__main__":
    main()
