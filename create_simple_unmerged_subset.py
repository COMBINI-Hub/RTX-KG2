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
        
        # Nodes
        nodes_file = self.data_dir / "PrimeKG_data" / "primekg_nodes.csv"
        output_nodes = self.output_dir / "primekg_nodes.csv"
        
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
                writer.writerow(row + ['PrimeKG'])
        
        # Edges
        edges_file = self.data_dir / "PrimeKG_data" / "primekg_edges.csv"
        output_edges = self.output_dir / "primekg_edges.csv"
        
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
                writer.writerow(row + ['PrimeKG'])
        
        logger.info(f"PrimeKG: {self.node_sample_size} nodes, {self.edge_sample_size} edges")
    
    def sample_and_save_ikraph(self):
        """Sample and save iKraph data"""
        logger.info("Processing iKraph...")
        
        # Nodes
        nodes_file = self.data_dir / "iKraph_data" / "nodes_gene.csv.gz"
        output_nodes = self.output_dir / "ikraph_nodes.csv"
        
        with gzip.open(nodes_file, 'rt') as infile, open(output_nodes, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample
            for i, row in enumerate(reader):
                if i >= self.node_sample_size:
                    break
                writer.writerow(row + ['iKraph'])
        
        # Edges
        edges_file = self.data_dir / "iKraph_data" / "relationships_db.csv.gz"
        output_edges = self.output_dir / "ikraph_edges.csv"
        
        with gzip.open(edges_file, 'rt') as infile, open(output_edges, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample
            for i, row in enumerate(reader):
                if i >= self.edge_sample_size:
                    break
                writer.writerow(row + ['iKraph'])
        
        logger.info(f"iKraph: {self.node_sample_size} nodes, {self.edge_sample_size} edges")
    
    def sample_and_save_semmeddb(self):
        """Sample and save SemMedDB data"""
        logger.info("Processing SemMedDB...")
        
        # Nodes
        nodes_file = self.data_dir / "SemMedDB_Data" / "entity.gz"
        output_nodes = self.output_dir / "semmeddb_nodes.csv"
        
        with gzip.open(nodes_file, 'rt') as infile, open(output_nodes, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample
            for i, row in enumerate(reader):
                if i >= self.node_sample_size:
                    break
                writer.writerow(row + ['SemMedDB'])
        
        # Edges
        edges_file = self.data_dir / "SemMedDB_Data" / "connections.csv.gz"
        output_edges = self.output_dir / "semmeddb_edges.csv"
        
        with gzip.open(edges_file, 'rt') as infile, open(output_edges, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header
            header = next(reader)
            writer.writerow(header + ['source'])
            
            # Write sample
            for i, row in enumerate(reader):
                if i >= self.edge_sample_size:
                    break
                writer.writerow(row + ['SemMedDB'])
        
        logger.info(f"SemMedDB: {self.node_sample_size} nodes, {self.edge_sample_size} edges")
    
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
            f.write(f"This allows you to see overlapping entities and unique entities from each source.\n")
        
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

if __name__ == "__main__":
    main()
