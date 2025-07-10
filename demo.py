#!/usr/bin/env python3
"""
Demo script for the Generalized ETL System
Shows how to use the ETL pipeline with sample data
"""

import json
import os
import sys
from datetime import datetime

# Add the project root to the path
sys.path.insert(0, os.path.abspath('.'))

from etl.orchestrator import ETLOrchestrator
from etl.loaders.mongodb_loader import MongoDBLoader

def main():
    """
    Main demo function
    """
    print("🚀 Generalized ETL System Demo")
    print("=" * 50)
    
    # Create orchestrator
    orchestrator = ETLOrchestrator()
    
    # Create sample configuration if it doesn't exist
    if not os.path.exists('config/etl_config.json'):
        print("📝 Creating sample configuration...")
        orchestrator.create_sample_config()
    
    # Validate connections
    print("\n🔍 Validating connections...")
    connections = orchestrator.validate_all_connections()
    
    for supplier_id, is_connected in connections.items():
        status = "✅ Connected" if is_connected else "❌ Failed"
        print(f"  {supplier_id}: {status}")
    
    # Check if all connections are valid
    if not all(connections.values()):
        print("\n⚠️  Some connections failed. Please check your configuration.")
        print("For MongoDB, ensure it's running on localhost:27017")
        print("For sample data, ensure the 'sample data' directory exists")
        return
    
    # Get pipeline status
    print("\n📊 Getting pipeline status...")
    status = orchestrator.get_pipeline_status()
    
    db_status = status['database']
    if db_status['connected']:
        stats = db_status.get('stats', {})
        print(f"  📦 Database connected")
        print(f"  📋 Total products: {stats.get('total_documents', 0)}")
        print(f"  💾 Storage size: {stats.get('storage_size', 0)} bytes")
    else:
        print(f"  ❌ Database failed: {db_status.get('error', 'Unknown error')}")
    
    # Run full sync
    print("\n⚡ Running full ETL sync...")
    results = orchestrator.run_full_sync()
    
    # Print results
    print("\n📈 Sync Results:")
    for supplier_id, result in results.items():
        print(f"\n🏪 {supplier_id.upper()}:")
        print(f"  Status: {result.status.value}")
        print(f"  Processed: {result.processed_count}")
        print(f"  Success: {result.success_count}")
        print(f"  Errors: {result.error_count}")
        if result.duration:
            print(f"  Duration: {result.duration:.2f}s")
        
        if result.errors:
            print("  Error details:")
            for error in result.errors[:3]:  # Show first 3 errors
                print(f"    - {error}")
    
    # Show sample products
    print("\n🎯 Sample Products in Database:")
    try:
        config = orchestrator.config
        loader = MongoDBLoader(config.get('database', {}))
        
        # Get a few sample products
        sample_products = list(loader.collection.find({}).limit(3))
        
        for i, product in enumerate(sample_products, 1):
            print(f"\n📦 Product {i}:")
            print(f"  ID: {product.get('product_id', 'N/A')}")
            print(f"  Name: {product.get('name', 'N/A')}")
            print(f"  Supplier: {product.get('supplier', {}).get('name', 'N/A')}")
            print(f"  Variants: {len(product.get('variants', []))}")
            print(f"  Printable: {product.get('is_printable', False)}")
            print(f"  Categories: {len(product.get('categories', []))}")
            
        print(f"\n✅ Demo completed successfully!")
        print(f"📊 Total products in database: {len(sample_products) if sample_products else 0}")
        
    except Exception as e:
        print(f"❌ Error accessing database: {e}")
    
    # Print usage instructions
    print("\n" + "=" * 50)
    print("🎯 Usage Instructions:")
    print("1. Install requirements: pip install -r requirements.txt")
    print("2. Start MongoDB: mongod")
    print("3. Run ETL sync: python -m etl.orchestrator --action sync")
    print("4. Check status: python -m etl.orchestrator --action status")
    print("5. Validate connections: python -m etl.orchestrator --action validate")
    print("")
    print("📁 Configuration file: config/etl_config.json")
    print("📋 Log file: logs/etl.log")
    print("🗄️  Database: MongoDB -> product_catalog -> products")


if __name__ == "__main__":
    main() 