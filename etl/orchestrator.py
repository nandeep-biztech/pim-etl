"""
ETL Orchestrator
Main orchestrator that coordinates the entire ETL pipeline
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import json
import os

from etl.base import ETLPipelineFactory, ETLResult, ETLStatus
from etl.extractors.midocean_extractor import MidOceanExtractor
from etl.transformers.midocean_transformer import MidOceanTransformer
from etl.loaders.mongodb_loader import MongoDBLoader


class ETLOrchestrator:
    """
    Main ETL orchestrator that manages the entire pipeline
    """
    
    def __init__(self, config_file: str = "config/etl_config.json"):
        self.config_file = config_file
        
        # Initialize basic logger first
        self.logger = logging.getLogger("ETLOrchestrator")
        
        # Set up basic logging if not already configured
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO)
        
        # Load configuration
        self.config = self._load_config()
        
        # Setup full logging configuration
        self._setup_logging()
    
    def run_full_sync(self, supplier_id: str = None) -> Dict[str, ETLResult]:
        """
        Run full ETL sync for one or all suppliers
        """
        results = {}
        
        suppliers = [supplier_id] if supplier_id else self.config.get('suppliers', {}).keys()
        
        for sid in suppliers:
            try:
                self.logger.info(f"Starting full sync for supplier: {sid}")
                
                # Get supplier configuration
                supplier_config = self.config['suppliers'].get(sid)
                if not supplier_config:
                    self.logger.error(f"No configuration found for supplier: {sid}")
                    continue
                
                # Add supplier_id to config
                supplier_config['supplier_id'] = sid
                
                # Create pipeline
                pipeline = self._create_pipeline(supplier_config)
                
                # Run sync
                result = pipeline.run_full_sync()
                results[sid] = result
                
                self.logger.info(
                    f"Sync completed for {sid}: {result.status.value} "
                    f"({result.success_count} success, {result.error_count} errors)"
                )
                
            except Exception as e:
                self.logger.error(f"Failed to sync supplier {sid}: {e}")
                results[sid] = ETLResult(
                    status=ETLStatus.FAILED,
                    error_count=1,
                    errors=[str(e)]
                )
        
        return results
    
    def run_incremental_sync(self, supplier_id: str = None, since: datetime = None) -> Dict[str, ETLResult]:
        """
        Run incremental ETL sync for one or all suppliers
        """
        results = {}
        
        suppliers = [supplier_id] if supplier_id else self.config.get('suppliers', {}).keys()
        
        for sid in suppliers:
            try:
                self.logger.info(f"Starting incremental sync for supplier: {sid}")
                
                # Get supplier configuration
                supplier_config = self.config['suppliers'].get(sid)
                if not supplier_config:
                    self.logger.error(f"No configuration found for supplier: {sid}")
                    continue
                
                # Add supplier_id to config
                supplier_config['supplier_id'] = sid
                
                # Create pipeline
                pipeline = self._create_pipeline(supplier_config)
                
                # Run incremental sync
                result = pipeline.run_incremental_sync(since)
                results[sid] = result
                
                self.logger.info(
                    f"Incremental sync completed for {sid}: {result.status.value} "
                    f"({result.success_count} success, {result.error_count} errors)"
                )
                
            except Exception as e:
                self.logger.error(f"Failed to incremental sync supplier {sid}: {e}")
                results[sid] = ETLResult(
                    status=ETLStatus.FAILED,
                    error_count=1,
                    errors=[str(e)]
                )
        
        return results
    
    def validate_all_connections(self) -> Dict[str, bool]:
        """
        Validate connections for all suppliers
        """
        results = {}
        
        for supplier_id, supplier_config in self.config.get('suppliers', {}).items():
            try:
                supplier_config['supplier_id'] = supplier_id
                pipeline = self._create_pipeline(supplier_config)
                
                results[supplier_id] = pipeline._validate_connections()
                
            except Exception as e:
                self.logger.error(f"Failed to validate connections for {supplier_id}: {e}")
                results[supplier_id] = False
        
        return results
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get status of all pipelines
        """
        status = {
            'timestamp': datetime.utcnow().isoformat(),
            'suppliers': {},
            'database': {}
        }
        
        # Check supplier connections
        connections = self.validate_all_connections()
        for supplier_id, is_connected in connections.items():
            status['suppliers'][supplier_id] = {
                'connected': is_connected,
                'last_checked': datetime.utcnow().isoformat()
            }
        
        # Check database connection
        try:
            loader_config = self.config.get('database', {})
            loader = MongoDBLoader(loader_config)
            
            stats = loader.get_collection_stats()
            status['database'] = {
                'connected': True,
                'stats': stats
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get database status: {e}")
            status['database'] = {
                'connected': False,
                'error': str(e)
            }
        
        return status
    
    def _create_pipeline(self, supplier_config: Dict[str, Any]):
        """
        Create ETL pipeline for a supplier
        """
        supplier_id = supplier_config.get('supplier_id')
        
        # Special handling for MidOcean (multiple API endpoints)
        if supplier_id == 'midocean':
            return self._create_midocean_pipeline(supplier_config)
        
        # Use factory for other suppliers
        loader_config = self.config.get('database', {})
        batch_size = supplier_config.get('batch_size', 100)
        
        return ETLPipelineFactory.create_pipeline(
            supplier_config,
            loader_config,
            batch_size
        )
    
    def _create_midocean_pipeline(self, supplier_config: Dict[str, Any]):
        """
        Create specialized MidOcean pipeline with multiple endpoints
        """
        from etl.base import ETLPipeline
        
        # Create components
        extractor = MidOceanExtractor(supplier_config)
        transformer = MidOceanTransformer(supplier_config)
        loader = MongoDBLoader(self.config.get('database', {}))
        
        # Load additional data for transformer
        print_data = extractor.get_print_data()
        pricing_data = extractor.get_pricing_data()
        print_pricing_data = extractor.get_print_pricing_data()
        
        transformer.set_additional_data(
            pricing_data=pricing_data,
            print_data=print_data,
            print_pricing=print_pricing_data
        )
        
        # Create pipeline
        batch_size = supplier_config.get('batch_size', 100)
        
        return ETLPipeline(extractor, transformer, loader, batch_size)
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from file
        """
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                self.logger.warning(f"Config file not found: {self.config_file}")
                return self._get_default_config()
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """
        Get default configuration
        """
        return {
            "database": {
                "type": "mongodb",
                "connection_string": "mongodb://localhost:27017/",
                "database": "product_catalog",
                "collection": "products",
                "batch_size": 1000
            },
            "suppliers": {
                "midocean": {
                    "supplier_name": "MidOcean",
                    "api": {
                        "api_key": "",
                        "language": "en",
                        "use_sample_data": True,
                        "sample_data_path": "sample data/MidOcean Sample Data.json"
                    },
                    "batch_size": 100
                }
            },
            "logging": {
                "level": "INFO",
                "file": "logs/etl.log",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        }
    
    def _setup_logging(self):
        """
        Setup logging configuration
        """
        logging_config = self.config.get('logging', {})
        
        # Create logs directory if it doesn't exist
        log_file = logging_config.get('file', 'logs/etl.log')
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, logging_config.get('level', 'INFO')),
            format=logging_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def create_sample_config(self):
        """
        Create a sample configuration file
        """
        config = self._get_default_config()
        
        # Create config directory if it doesn't exist
        config_dir = os.path.dirname(self.config_file)
        if config_dir and not os.path.exists(config_dir):
            os.makedirs(config_dir)
        
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        
        self.logger.info(f"Sample configuration created: {self.config_file}")


def main():
    """
    Main entry point for ETL orchestrator
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Orchestrator')
    parser.add_argument('--config', default='config/etl_config.json', 
                       help='Configuration file path')
    parser.add_argument('--supplier', help='Specific supplier to sync')
    parser.add_argument('--action', choices=['sync', 'incremental', 'validate', 'status', 'create-config'], 
                       default='sync', help='Action to perform')
    parser.add_argument('--since', help='For incremental sync: date since (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = ETLOrchestrator(args.config)
    
    if args.action == 'create-config':
        orchestrator.create_sample_config()
        return
    
    if args.action == 'sync':
        results = orchestrator.run_full_sync(args.supplier)
        
        # Print results
        for supplier_id, result in results.items():
            print(f"\n{supplier_id}: {result.status.value}")
            print(f"  Processed: {result.processed_count}")
            print(f"  Success: {result.success_count}")
            print(f"  Errors: {result.error_count}")
            if result.duration:
                print(f"  Duration: {result.duration:.2f}s")
            
            if result.errors:
                print("  Error details:")
                for error in result.errors[:5]:  # Show first 5 errors
                    print(f"    - {error}")
    
    elif args.action == 'incremental':
        since = None
        if args.since:
            since = datetime.strptime(args.since, '%Y-%m-%d')
        
        results = orchestrator.run_incremental_sync(args.supplier, since)
        
        # Print results (same as sync)
        for supplier_id, result in results.items():
            print(f"\n{supplier_id}: {result.status.value}")
            print(f"  Processed: {result.processed_count}")
            print(f"  Success: {result.success_count}")
            print(f"  Errors: {result.error_count}")
            if result.duration:
                print(f"  Duration: {result.duration:.2f}s")
    
    elif args.action == 'validate':
        results = orchestrator.validate_all_connections()
        
        print("\nConnection validation results:")
        for supplier_id, is_connected in results.items():
            status = "✓ Connected" if is_connected else "✗ Failed"
            print(f"  {supplier_id}: {status}")
    
    elif args.action == 'status':
        status = orchestrator.get_pipeline_status()
        
        print(f"\nPipeline Status ({status['timestamp']})")
        print("\nSuppliers:")
        for supplier_id, supplier_status in status['suppliers'].items():
            connected = "✓" if supplier_status['connected'] else "✗"
            print(f"  {supplier_id}: {connected}")
        
        print("\nDatabase:")
        db_status = status['database']
        if db_status['connected']:
            stats = db_status.get('stats', {})
            print(f"  ✓ Connected")
            print(f"  Total products: {stats.get('total_documents', 0)}")
            print(f"  Storage size: {stats.get('storage_size', 0)} bytes")
        else:
            print(f"  ✗ Failed: {db_status.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main() 