"""
Unified Product Schema for ETL Pipeline
Supports all supplier data formats (MidOcean, Laltex, Preseli, etc.)
"""

from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum

class PriceType(str, Enum):
    UNIT = "unit"
    SETUP = "setup"
    ADDITIONAL = "additional"
    SHIPPING = "shipping"

class PrintTechnique(str, Enum):
    SCREEN_PRINT = "screen_print"
    PAD_PRINT = "pad_print"
    EMBROIDERY = "embroidery"
    LASER_ENGRAVING = "laser_engraving"
    DIGITAL_PRINT = "digital_print"
    FULL_COLOR = "full_color"
    DEBOSSING = "debossing"
    SUBLIMATION = "sublimation"
    TRANSFER = "transfer"

class DimensionUnit(str, Enum):
    MM = "mm"
    CM = "cm"
    M = "m"
    INCH = "in"

class WeightUnit(str, Enum):
    G = "g"
    KG = "kg"
    LB = "lb"
    OZ = "oz"

class Currency(str, Enum):
    GBP = "GBP"
    EUR = "EUR"
    USD = "USD"

class ProductStatus(str, Enum):
    ACTIVE = "active"
    DISCONTINUED = "discontinued"
    OUT_OF_STOCK = "out_of_stock"

# Core Data Models
class Dimensions(BaseModel):
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    diameter: Optional[float] = None
    unit: DimensionUnit = DimensionUnit.MM

class Weight(BaseModel):
    value: Optional[float] = None
    unit: WeightUnit = WeightUnit.G

class Price(BaseModel):
    value: float
    currency: Currency = Currency.GBP
    min_quantity: int = 1
    max_quantity: Optional[int] = None
    type: PriceType = PriceType.UNIT
    description: Optional[str] = None
    valid_until: Optional[datetime] = None

class Image(BaseModel):
    url: str
    type: Optional[str] = None  # front, back, side, detail, ambiant, etc.
    description: Optional[str] = None
    color_variant: Optional[str] = None

class PrintPosition(BaseModel):
    id: str
    name: str
    max_width: Optional[float] = None
    max_height: Optional[float] = None
    max_area: Optional[float] = None
    unit: DimensionUnit = DimensionUnit.MM
    techniques: List[PrintTechnique] = []
    max_colors: Optional[int] = None
    coordinates: Optional[Dict[str, Any]] = None  # For print position mapping
    images: List[Image] = []

class PrintOption(BaseModel):
    technique: PrintTechnique
    position: str
    max_colors: int = 1
    setup_charge: Optional[float] = None
    prices: List[Price] = []
    lead_time: Optional[str] = None
    is_default: bool = False

class ColorVariant(BaseModel):
    code: str
    name: str
    hex_color: Optional[str] = None
    pms_color: Optional[str] = None
    images: List[Image] = []
    status: ProductStatus = ProductStatus.ACTIVE

class StockInfo(BaseModel):
    available: int = 0
    due_ins: List[Dict[str, Any]] = []  # quantity and expected date
    last_updated: Optional[datetime] = None

class Variant(BaseModel):
    sku: str
    variant_id: Optional[str] = None
    color: Optional[ColorVariant] = None
    size: Optional[str] = None
    material_variant: Optional[str] = None
    dimensions: Optional[Dimensions] = None
    weight: Optional[Weight] = None
    prices: List[Price] = []
    stock: Optional[StockInfo] = None
    images: List[Image] = []
    status: ProductStatus = ProductStatus.ACTIVE
    gtin: Optional[str] = None

class Category(BaseModel):
    id: Optional[str] = None
    name: str
    level: int = 1
    parent_id: Optional[str] = None

class ShippingOption(BaseModel):
    service_type: str
    service_name: str
    cost: float
    currency: Currency = Currency.GBP
    conditions: Optional[Dict[str, Any]] = None

class Supplier(BaseModel):
    id: str
    name: str
    api_version: Optional[str] = None
    contact_info: Optional[Dict[str, Any]] = None

# Main Product Schema
class Product(BaseModel):
    # Core identifiers
    product_id: str = Field(..., description="Unique product identifier")
    supplier: Supplier
    supplier_product_code: str
    
    # Basic product information
    name: str
    title: Optional[str] = None
    short_description: Optional[str] = None
    long_description: Optional[str] = None
    keywords: List[str] = []
    
    # Categorization
    categories: List[Category] = []
    brand: Optional[str] = None
    
    # Physical properties
    dimensions: Optional[Dimensions] = None
    weight: Optional[Weight] = None
    material: Optional[str] = None
    colors_available: List[str] = []
    
    # Product variants (colors, sizes, etc.)
    variants: List[Variant] = []
    
    # Pricing information
    base_prices: List[Price] = []  # Base product pricing
    
    # Print/customization options
    is_printable: bool = False
    print_positions: List[PrintPosition] = []
    print_options: List[PrintOption] = []
    
    # Media
    images: List[Image] = []
    artwork_templates: List[str] = []
    
    # Logistics
    minimum_order_quantity: int = 1
    carton_quantity: Optional[int] = None
    lead_time: Optional[str] = None
    shipping_options: List[ShippingOption] = []
    
    # Compliance and origin
    country_of_origin: Optional[str] = None
    tariff_code: Optional[str] = None
    commodity_code: Optional[str] = None
    
    # Status and metadata
    status: ProductStatus = ProductStatus.ACTIVE
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_sync: datetime = Field(default_factory=datetime.utcnow)
    
    # Raw data for debugging
    raw_data: Optional[Dict[str, Any]] = None
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

# MongoDB Collection Schemas
class ProductCollection(BaseModel):
    """
    MongoDB collection structure for products
    """
    products: List[Product]
    
    class Config:
        collection_name = "products"
        indexes = [
            "product_id",
            "supplier.id",
            "supplier_product_code",
            "name",
            "categories.name",
            "status",
            "updated_at"
        ]

# Utility functions for schema validation
def create_product_index_config():
    """
    MongoDB index configuration for optimal query performance
    """
    return [
        {"key": "product_id", "unique": True},
        {"key": "supplier.id"},
        {"key": "supplier_product_code"},
        {"key": "name", "text": True},
        {"key": "categories.name"},
        {"key": "status"},
        {"key": "updated_at"},
        {"key": "variants.sku"},
        {"key": "is_printable"},
        {"key": "minimum_order_quantity"},
        # Compound indexes for common queries
        {"key": [("supplier.id", 1), ("status", 1)]},
        {"key": [("categories.name", 1), ("status", 1)]},
        {"key": [("is_printable", 1), ("status", 1)]}
    ] 