"""
MongoDB Loader Implementation
Handles loading transformed products into MongoDB with upsert capabilities
"""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import logging
from pymongo import MongoClient, errors as mongo_errors, ReplaceOne
from pymongo.collection import Collection
from pymongo.database import Database

from etl.base import BaseLoader, ETLResult, ETLStatus, etl_component
from schemas.unified_product_schema import Product, create_product_index_config

@etl_component("loader")
class MongoDBLoader(BaseLoader):
    """
    MongoDB implementation of BaseLoader
    Handles product upserts, indexing, and batch operations
    """
    
    LOADER_TYPE = "mongodb"
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        
        # MongoDB connection settings
        self.connection_string = connection_config.get("connection_string", "mongodb://localhost:27017/")
        self.database_name = connection_config.get("database", "product_catalog")
        self.collection_name = connection_config.get("collection", "products")
        self.batch_size = connection_config.get("batch_size", 1000)
        
        # Connection objects
        self.client: Optional[MongoClient] = None
        self.database: Optional[Database] = None
        self.collection: Optional[Collection] = None
        
        # Connect to MongoDB
        self._connect()
    
    def _connect(self):
        """Establish MongoDB connection"""
        try:
            self.client = MongoClient(self.connection_string)
            self.database = self.client[self.database_name]
            self.collection = self.database[self.collection_name]
            
            # Test connection
            self.client.admin.command('ping')
            self.logger.info(f"Connected to MongoDB: {self.database_name}.{self.collection_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def setup_database(self) -> bool:
        """
        Setup MongoDB collection with proper indexes
        """
        try:
            if self.collection is None:
                self._connect()
            
            # Create indexes for optimal query performance
            index_configs = create_product_index_config()
            
            for index_config in index_configs:
                key = index_config["key"]
                options = {k: v for k, v in index_config.items() if k != "key"}
                
                try:
                    if isinstance(key, list):
                        # Compound index
                        self.collection.create_index(key, **options)
                    else:
                        # Single field index
                        self.collection.create_index(key, **options)
                    
                    self.logger.debug(f"Created index: {key}")
                    
                except mongo_errors.OperationFailure as e:
                    if "already exists" not in str(e):
                        self.logger.warning(f"Failed to create index {key}: {e}")
            
            self.logger.info("Database setup completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}")
            return False
    
    def load_products(self, products: List[Union[Product, Dict[str, Any]]]) -> ETLResult:
        """
        Load a batch of products using bulk upsert operations
        """
        result = ETLResult(
            status=ETLStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        
        if not products:
            result.status = ETLStatus.SUCCESS
            result.end_time = datetime.utcnow()
            return result
        
        try:
            # Prepare bulk operations
            bulk_operations = []
            
            for product in products:
                try:
                    # Convert product to dict for MongoDB
                    if hasattr(product, 'dict'):
                        # Pydantic model
                        product_dict = product.dict(by_alias=True)
                        product_id = product.product_id
                    else:
                        # Already a dictionary
                        product_dict = product.copy()
                        product_id = product_dict.get("product_id")
                    
                    # Ensure datetime objects are properly serialized
                    product_dict["updated_at"] = datetime.utcnow()
                    
                    # Convert any datetime objects to ISO format strings
                    self._serialize_datetime_fields(product_dict)
                    
                    # Create upsert operation
                    operation = ReplaceOne(
                        {"product_id": product_id},
                        product_dict,
                        upsert=True
                    )
                    bulk_operations.append(operation)
                    
                except Exception as e:
                    prod_id = getattr(product, 'product_id', product.get('product_id', 'unknown') if isinstance(product, dict) else 'unknown')
                    self.logger.error(f"Failed to prepare product {prod_id}: {e}")
                    result.error_count += 1
                    result.errors.append(f"Product {prod_id}: {str(e)}")
            
            # Execute bulk operations in batches
            if bulk_operations:
                self.logger.info(f"Executing {len(bulk_operations)} bulk operations")
                
                for i in range(0, len(bulk_operations), self.batch_size):
                    batch = bulk_operations[i:i + self.batch_size]
                    
                    try:
                        bulk_result = self.collection.bulk_write(batch, ordered=False)
                        
                        # Count successful operations
                        success_count = (
                            bulk_result.upserted_count + 
                            bulk_result.modified_count + 
                            bulk_result.matched_count
                        )
                        result.success_count += success_count
                        
                        self.logger.info(f"Processed batch: {len(batch)} operations, {success_count} successful")
                        self.logger.debug(f"Bulk result: upserted={bulk_result.upserted_count}, modified={bulk_result.modified_count}, matched={bulk_result.matched_count}")
                        
                    except mongo_errors.BulkWriteError as e:
                        # Handle partial failures
                        for error in e.details.get("writeErrors", []):
                            result.error_count += 1
                            result.errors.append(f"Bulk write error: {error.get('errmsg', 'Unknown error')}")
                        
                        # Count successful operations from bulk result
                        if hasattr(e, 'details') and 'nUpserted' in e.details:
                            result.success_count += e.details.get('nUpserted', 0)
                            result.success_count += e.details.get('nModified', 0)
                        
                        self.logger.warning(f"Bulk write partial failure: {len(e.details.get('writeErrors', []))} errors")
            
            # Determine final status
            if result.error_count == 0:
                result.status = ETLStatus.SUCCESS
            elif result.success_count > 0:
                result.status = ETLStatus.PARTIAL_SUCCESS
            else:
                result.status = ETLStatus.FAILED
            
            self.logger.info(
                f"Load completed: {result.success_count} success, {result.error_count} errors"
            )
            
        except Exception as e:
            self.logger.error(f"Load operation failed: {e}")
            result.status = ETLStatus.FAILED
            result.errors.append(str(e))
        
        finally:
            result.end_time = datetime.utcnow()
        
        return result
    
    def upsert_product(self, product: Product) -> bool:
        """
        Insert or update a single product
        """
        try:
            product_dict = product.dict(by_alias=True)
            product_dict["updated_at"] = datetime.utcnow()
            
            result = self.collection.replace_one(
                {"product_id": product.product_id},
                product_dict,
                upsert=True
            )
            
            success = result.upserted_id is not None or result.modified_count > 0
            
            if success:
                self.logger.debug(f"Upserted product: {product.product_id}")
            else:
                self.logger.warning(f"No changes for product: {product.product_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to upsert product {product.product_id}: {e}")
            return False
    
    def delete_products(self, product_ids: List[str]) -> int:
        """
        Delete products by IDs
        """
        try:
            if not product_ids:
                return 0
            
            result = self.collection.delete_many(
                {"product_id": {"$in": product_ids}}
            )
            
            deleted_count = result.deleted_count
            self.logger.info(f"Deleted {deleted_count} products")
            
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to delete products: {e}")
            return 0
    
    def get_product(self, product_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single product by ID
        """
        try:
            product = self.collection.find_one({"product_id": product_id})
            return product
            
        except Exception as e:
            self.logger.error(f"Failed to get product {product_id}: {e}")
            return None
    
    def get_products_by_supplier(self, supplier_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve all products for a supplier
        """
        try:
            products = list(self.collection.find({"supplier.id": supplier_id}))
            return products
            
        except Exception as e:
            self.logger.error(f"Failed to get products for supplier {supplier_id}: {e}")
            return []
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """
        Get collection statistics
        """
        try:
            stats = self.database.command("collStats", self.collection_name)
            
            # Get count by supplier
            supplier_counts = list(self.collection.aggregate([
                {"$group": {"_id": "$supplier.id", "count": {"$sum": 1}}}
            ]))
            
            # Get status distribution
            status_counts = list(self.collection.aggregate([
                {"$group": {"_id": "$status", "count": {"$sum": 1}}}
            ]))
            
            return {
                "total_documents": stats.get("count", 0),
                "storage_size": stats.get("storageSize", 0),
                "index_size": stats.get("totalIndexSize", 0),
                "supplier_counts": {item["_id"]: item["count"] for item in supplier_counts},
                "status_counts": {item["_id"]: item["count"] for item in status_counts},
                "last_updated": datetime.utcnow()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get collection stats: {e}")
            return {}
    
    def create_backup(self, backup_collection: str = None) -> bool:
        """
        Create a backup of the products collection
        """
        try:
            if not backup_collection:
                backup_collection = f"{self.collection_name}_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            # Copy all documents to backup collection
            pipeline = [{"$out": backup_collection}]
            list(self.collection.aggregate(pipeline))
            
            self.logger.info(f"Created backup collection: {backup_collection}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create backup: {e}")
            return False
    
    def cleanup_old_products(self, supplier_id: str, cutoff_date: datetime) -> int:
        """
        Remove products that haven't been updated since cutoff_date
        """
        try:
            result = self.collection.delete_many({
                "supplier.id": supplier_id,
                "updated_at": {"$lt": cutoff_date}
            })
            
            deleted_count = result.deleted_count
            self.logger.info(f"Cleaned up {deleted_count} old products for supplier {supplier_id}")
            
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old products: {e}")
            return 0
    
    def validate_connection(self) -> bool:
        """
        Validate MongoDB connection and setup
        """
        try:
            if not self.client:
                self._connect()
            
            # Test database connection
            self.client.admin.command('ping')
            
            # Setup database if needed
            return self.setup_database()
            
        except Exception as e:
            self.logger.error(f"MongoDB connection validation failed: {e}")
            return False
    
    def _serialize_datetime_fields(self, data):
        """Recursively serialize datetime objects to ISO format strings"""
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, datetime):
                    data[key] = value.isoformat()
                elif isinstance(value, (dict, list)):
                    self._serialize_datetime_fields(value)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, datetime):
                    data[i] = item.isoformat()
                elif isinstance(item, (dict, list)):
                    self._serialize_datetime_fields(item)
    
    def close_connection(self):
        """
        Close MongoDB connection
        """
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")
    
    def __del__(self):
        """Cleanup on destruction"""
        self.close_connection() 