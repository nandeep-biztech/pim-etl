"""
Base ETL Framework
Abstract classes and interfaces for building supplier-specific ETL pipelines
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generator, Type
import logging
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from schemas.unified_product_schema import Product, Supplier

class ETLStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"

@dataclass
class ETLResult:
    """Result object for ETL operations"""
    status: ETLStatus
    processed_count: int = 0
    success_count: int = 0
    error_count: int = 0
    errors: List[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

class BaseExtractor(ABC):
    """
    Abstract base class for data extractors
    Handles fetching raw data from supplier APIs
    """
    
    def __init__(self, supplier_config: Dict[str, Any]):
        self.supplier_config = supplier_config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.supplier_id = supplier_config.get("supplier_id")
        self.api_config = supplier_config.get("api", {})
    
    @abstractmethod
    def extract_products(self) -> Generator[Dict[str, Any], None, None]:
        """
        Extract product data from supplier API
        Yields raw product data dictionaries
        """
        pass
    
    @abstractmethod
    def extract_pricing(self, product_codes: List[str] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Extract pricing data from supplier API
        Yields raw pricing data dictionaries
        """
        pass
    
    @abstractmethod
    def extract_stock(self, product_codes: List[str] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Extract stock data from supplier API
        Yields raw stock data dictionaries
        """
        pass
    
    def validate_connection(self) -> bool:
        """
        Validate connection to supplier API
        Returns True if connection is successful
        """
        try:
            # Basic connection test - can be overridden
            test_data = list(self.extract_products())
            return len(test_data) >= 0
        except Exception as e:
            self.logger.error(f"Connection validation failed: {e}")
            return False

class BaseTransformer(ABC):
    """
    Abstract base class for data transformers
    Handles converting supplier-specific data to unified schema
    """
    
    def __init__(self, supplier_config: Dict[str, Any]):
        self.supplier_config = supplier_config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.supplier_id = supplier_config.get("supplier_id")
        self.supplier_name = supplier_config.get("supplier_name")
    
    @abstractmethod
    def transform_product(self, raw_data: Dict[str, Any]) -> Product:
        """
        Transform raw product data to unified Product schema
        """
        pass
    
    @abstractmethod
    def create_supplier_info(self) -> Supplier:
        """
        Create supplier information object
        """
        pass
    
    def transform_batch(self, raw_products: List[Dict[str, Any]]) -> List[Product]:
        """
        Transform a batch of raw products
        """
        transformed_products = []
        for raw_product in raw_products:
            try:
                product = self.transform_product(raw_product)
                transformed_products.append(product)
            except Exception as e:
                self.logger.error(f"Failed to transform product {raw_product.get('id', 'unknown')}: {e}")
        
        return transformed_products
    
    def validate_transformed_product(self, product: Product) -> bool:
        """
        Validate transformed product against schema
        """
        try:
            # Pydantic validation
            product.dict()
            return True
        except Exception as e:
            self.logger.error(f"Product validation failed: {e}")
            return False

class BaseLoader(ABC):
    """
    Abstract base class for data loaders
    Handles loading transformed data into target system (MongoDB)
    """
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.connection_config = connection_config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
    
    @abstractmethod
    def load_products(self, products: List[Product]) -> ETLResult:
        """
        Load products into target database
        """
        pass
    
    @abstractmethod
    def upsert_product(self, product: Product) -> bool:
        """
        Insert or update a single product
        """
        pass
    
    @abstractmethod
    def delete_products(self, product_ids: List[str]) -> int:
        """
        Delete products by IDs
        Returns count of deleted products
        """
        pass
    
    @abstractmethod
    def setup_database(self) -> bool:
        """
        Setup database schema, indexes, etc.
        """
        pass
    
    def validate_connection(self) -> bool:
        """
        Validate connection to target database
        """
        try:
            return self.setup_database()
        except Exception as e:
            self.logger.error(f"Database connection validation failed: {e}")
            return False

class ETLPipeline:
    """
    Main ETL Pipeline orchestrator
    Coordinates extraction, transformation, and loading
    """
    
    def __init__(
        self,
        extractor: BaseExtractor,
        transformer: BaseTransformer,
        loader: BaseLoader,
        batch_size: int = 100
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.batch_size = batch_size
        self.logger = logging.getLogger("ETLPipeline")
    
    def run_full_sync(self) -> ETLResult:
        """
        Run complete ETL pipeline
        """
        result = ETLResult(
            status=ETLStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        
        try:
            self.logger.info("Starting full ETL sync")
            
            # Validate connections
            if not self._validate_connections():
                result.status = ETLStatus.FAILED
                result.errors.append("Connection validation failed")
                return result
            
            # Process products in batches
            batch = []
            for raw_product in self.extractor.extract_products():
                batch.append(raw_product)
                result.processed_count += 1
                
                if len(batch) >= self.batch_size:
                    self._process_batch(batch, result)
                    batch = []
            
            # Process remaining batch
            if batch:
                self._process_batch(batch, result)
            
            # Determine final status
            if result.error_count == 0:
                result.status = ETLStatus.SUCCESS
            elif result.success_count > 0:
                result.status = ETLStatus.PARTIAL_SUCCESS
            else:
                result.status = ETLStatus.FAILED
            
            self.logger.info(f"ETL completed: {result.success_count} success, {result.error_count} errors")
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            result.status = ETLStatus.FAILED
            result.errors.append(str(e))
        
        finally:
            result.end_time = datetime.utcnow()
        
        return result
    
    def run_incremental_sync(self, since: datetime = None) -> ETLResult:
        """
        Run incremental ETL sync
        Override in specific implementations
        """
        self.logger.warning("Incremental sync not implemented, running full sync")
        return self.run_full_sync()
    
    def _validate_connections(self) -> bool:
        """
        Validate all connections before starting ETL
        """
        try:
            extractor_valid = self.extractor.validate_connection()
            loader_valid = self.loader.validate_connection()
            
            if not extractor_valid:
                self.logger.error("Extractor connection validation failed")
            if not loader_valid:
                self.logger.error("Loader connection validation failed")
            
            return extractor_valid and loader_valid
        
        except Exception as e:
            self.logger.error(f"Connection validation error: {e}")
            return False
    
    def _process_batch(self, raw_batch: List[Dict[str, Any]], result: ETLResult):
        """
        Process a batch of raw products
        """
        try:
            # Transform batch
            transformed_products = self.transformer.transform_batch(raw_batch)
            
            # Load batch
            load_result = self.loader.load_products(transformed_products)
            
            # Update results
            result.success_count += load_result.success_count
            result.error_count += load_result.error_count
            result.errors.extend(load_result.errors)
            
        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}")
            result.error_count += len(raw_batch)
            result.errors.append(f"Batch processing error: {e}")

class ETLPipelineFactory:
    """
    Factory for creating ETL pipelines for different suppliers
    """
    
    _extractors: Dict[str, Type[BaseExtractor]] = {}
    _transformers: Dict[str, Type[BaseTransformer]] = {}
    _loaders: Dict[str, Type[BaseLoader]] = {}
    
    @classmethod
    def register_extractor(cls, supplier_id: str, extractor_class: Type[BaseExtractor]):
        """Register an extractor for a supplier"""
        cls._extractors[supplier_id] = extractor_class
    
    @classmethod
    def register_transformer(cls, supplier_id: str, transformer_class: Type[BaseTransformer]):
        """Register a transformer for a supplier"""
        cls._transformers[supplier_id] = transformer_class
    
    @classmethod
    def register_loader(cls, loader_type: str, loader_class: Type[BaseLoader]):
        """Register a loader by type"""
        cls._loaders[loader_type] = loader_class
    
    @classmethod
    def create_pipeline(
        cls,
        supplier_config: Dict[str, Any],
        loader_config: Dict[str, Any],
        batch_size: int = 100
    ) -> ETLPipeline:
        """
        Create ETL pipeline for a supplier
        """
        supplier_id = supplier_config.get("supplier_id")
        loader_type = loader_config.get("type", "mongodb")
        
        # Get registered classes
        extractor_class = cls._extractors.get(supplier_id)
        transformer_class = cls._transformers.get(supplier_id)
        loader_class = cls._loaders.get(loader_type)
        
        if not extractor_class:
            raise ValueError(f"No extractor registered for supplier: {supplier_id}")
        if not transformer_class:
            raise ValueError(f"No transformer registered for supplier: {supplier_id}")
        if not loader_class:
            raise ValueError(f"No loader registered for type: {loader_type}")
        
        # Create instances
        extractor = extractor_class(supplier_config)
        transformer = transformer_class(supplier_config)
        loader = loader_class(loader_config)
        
        return ETLPipeline(extractor, transformer, loader, batch_size)

# Utility decorators
def etl_component(component_type: str, supplier_id: str = None):
    """
    Decorator to automatically register ETL components
    """
    def decorator(cls):
        if component_type == "extractor" and supplier_id:
            ETLPipelineFactory.register_extractor(supplier_id, cls)
        elif component_type == "transformer" and supplier_id:
            ETLPipelineFactory.register_transformer(supplier_id, cls)
        elif component_type == "loader":
            loader_type = getattr(cls, 'LOADER_TYPE', 'default')
            ETLPipelineFactory.register_loader(loader_type, cls)
        return cls
    return decorator 