"""
MidOcean Supplier Transformer
Handles MidOcean's complex API structure with products, pricing, print data, and print pricing
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import re
from decimal import Decimal

from etl.base import BaseTransformer, etl_component
from schemas.unified_product_schema import (
    Product, Supplier, Category, Variant, ColorVariant, Image, 
    Dimensions, Weight, Price, PrintPosition, PrintOption, PrintTechnique,
    DimensionUnit, WeightUnit, Currency, ProductStatus, PriceType
)

@etl_component("transformer", "midocean")
class MidOceanTransformer(BaseTransformer):
    """
    MidOcean API data transformer
    Handles products, pricing, print data, and print pricing endpoints
    """
    
    def __init__(self, supplier_config: Dict[str, Any]):
        super().__init__(supplier_config)
        
        # MidOcean specific mappings
        self.print_technique_mapping = {
            'B': PrintTechnique.DEBOSSING,
            'E': PrintTechnique.EMBROIDERY,
            'L0': PrintTechnique.LASER_ENGRAVING,
            'L1': PrintTechnique.LASER_ENGRAVING,
            'L2': PrintTechnique.LASER_ENGRAVING,
            'L3': PrintTechnique.LASER_ENGRAVING,
            'L4': PrintTechnique.LASER_ENGRAVING,
            'L5': PrintTechnique.LASER_ENGRAVING,
            'L6': PrintTechnique.LASER_ENGRAVING,
            'L7': PrintTechnique.LASER_ENGRAVING,
            'P0': PrintTechnique.PAD_PRINT,
            'P1': PrintTechnique.PAD_PRINT,
            'P2': PrintTechnique.PAD_PRINT,
            'P3': PrintTechnique.PAD_PRINT,
            'P4': PrintTechnique.PAD_PRINT,
            'P5': PrintTechnique.PAD_PRINT,
            'P6': PrintTechnique.PAD_PRINT,
            'P7': PrintTechnique.PAD_PRINT,
            'PD0': PrintTechnique.DIGITAL_PRINT,
            'PD1': PrintTechnique.DIGITAL_PRINT,
            'PD2': PrintTechnique.DIGITAL_PRINT,
            'PD3': PrintTechnique.DIGITAL_PRINT,
            'PD4': PrintTechnique.DIGITAL_PRINT,
            'PD5': PrintTechnique.DIGITAL_PRINT,
            'PD6': PrintTechnique.DIGITAL_PRINT,
            'PD7': PrintTechnique.DIGITAL_PRINT,
            'S0': PrintTechnique.SCREEN_PRINT,
            'S1': PrintTechnique.SCREEN_PRINT,
            'S2': PrintTechnique.SCREEN_PRINT,
            'S3': PrintTechnique.SCREEN_PRINT,
            'S4': PrintTechnique.SCREEN_PRINT,
            'S5': PrintTechnique.SCREEN_PRINT,
            'S6': PrintTechnique.SCREEN_PRINT,
            'S7': PrintTechnique.SCREEN_PRINT,
            'ST': PrintTechnique.SCREEN_PRINT,
            'ST0': PrintTechnique.SCREEN_PRINT,
            'ST1': PrintTechnique.SCREEN_PRINT,
            'ST2': PrintTechnique.SCREEN_PRINT,
            'RS0': PrintTechnique.SCREEN_PRINT,
            'RS1': PrintTechnique.SCREEN_PRINT,
            'RS2': PrintTechnique.SCREEN_PRINT,
            'RS3': PrintTechnique.SCREEN_PRINT,
            'RS4': PrintTechnique.SCREEN_PRINT,
            'RS5': PrintTechnique.SCREEN_PRINT,
            'RS6': PrintTechnique.SCREEN_PRINT,
            'RS7': PrintTechnique.SCREEN_PRINT,
            'RD0': PrintTechnique.DIGITAL_PRINT,
            'RD1': PrintTechnique.DIGITAL_PRINT,
            'RD2': PrintTechnique.DIGITAL_PRINT,
            'RD3': PrintTechnique.DIGITAL_PRINT,
            'T1': PrintTechnique.TRANSFER,
            'TD': PrintTechnique.TRANSFER,
            'TD1': PrintTechnique.TRANSFER,
            'TDT': PrintTechnique.TRANSFER,
            'TT': PrintTechnique.TRANSFER,
            'TR': PrintTechnique.TRANSFER,
            'TS': PrintTechnique.SUBLIMATION,
            'TS1': PrintTechnique.SUBLIMATION,
            'TS2': PrintTechnique.SUBLIMATION,
            'TS3': PrintTechnique.SUBLIMATION,
            'TS4': PrintTechnique.SUBLIMATION,
            'TSM': PrintTechnique.SUBLIMATION,
            'TST': PrintTechnique.SUBLIMATION,
            'TC': PrintTechnique.TRANSFER,
            'RL': PrintTechnique.LASER_ENGRAVING,
        }
        
        # Store additional API data
        self.print_data = {}
        self.print_pricing = {}
        self.pricing_data = {}
    
    def create_supplier_info(self) -> Supplier:
        """Create MidOcean supplier information"""
        return Supplier(
            id="midocean",
            name="MidOcean",
            api_version="2.0",
            contact_info={
                "website": "https://www.midocean.com",
                "api_base": "https://api.midocean.com/gateway/"
            }
        )
    
    def set_additional_data(self, pricing_data: Dict[str, Any] = None, 
                          print_data: Dict[str, Any] = None,
                          print_pricing: Dict[str, Any] = None):
        """Set additional data from other API endpoints"""
        if pricing_data:
            self.pricing_data = pricing_data
        if print_data:
            self.print_data = print_data
        if print_pricing:
            self.print_pricing = print_pricing
    
    def transform_product(self, raw_data: Dict[str, Any]) -> Product:
        """Transform raw MidOcean product data to unified Product schema"""
        try:
            # Extract basic product information
            master_code = raw_data.get('master_code', '')
            product_id = f"midocean_{master_code}"
            
            # Basic product info
            name = raw_data.get('product_name', '')
            short_description = raw_data.get('short_description', '')
            long_description = raw_data.get('long_description', '')
            
            # Dimensions
            dimensions = self._extract_dimensions(raw_data)
            weight = self._extract_weight(raw_data)
            
            # Categories
            categories = self._extract_categories(raw_data)
            
            # Variants (color variations)
            variants = self._extract_variants(raw_data)
            
            # Images from variants
            images = self._extract_main_images(raw_data)
            
            # Print positions and options
            print_positions = self._extract_print_positions(raw_data)
            print_options = self._extract_print_options(raw_data)
            
            # Base pricing
            base_prices = self._extract_base_prices(raw_data)
            
            # Create product
            product = Product(
                product_id=product_id,
                supplier=self.create_supplier_info(),
                supplier_product_code=master_code,
                name=name,
                short_description=short_description,
                long_description=long_description,
                categories=categories,
                dimensions=dimensions,
                weight=weight,
                material=raw_data.get('material', ''),
                variants=variants,
                base_prices=base_prices,
                is_printable=raw_data.get('printable', '').lower() == 'yes',
                print_positions=print_positions,
                print_options=print_options,
                images=images,
                minimum_order_quantity=1,
                carton_quantity=self._parse_int(raw_data.get('outer_carton_quantity')),
                country_of_origin=raw_data.get('country_of_origin', ''),
                tariff_code=raw_data.get('commodity_code', ''),
                status=ProductStatus.ACTIVE,
                brand=raw_data.get('brand', ''),
                raw_data=raw_data
            )
            
            return product
            
        except Exception as e:
            self.logger.error(f"Failed to transform MidOcean product {raw_data.get('master_code', 'unknown')}: {e}")
            raise
    
    def _extract_dimensions(self, raw_data: Dict[str, Any]) -> Optional[Dimensions]:
        """Extract dimensions from raw product data"""
        try:
            length = self._parse_float(raw_data.get('length'))
            width = self._parse_float(raw_data.get('width'))
            height = self._parse_float(raw_data.get('height'))
            
            if not any([length, width, height]):
                return None
            
            # Get unit (default to cm)
            unit_str = raw_data.get('length_unit', 'cm').lower()
            unit = DimensionUnit.CM if unit_str == 'cm' else DimensionUnit.MM
            
            return Dimensions(
                length=length,
                width=width,
                height=height,
                unit=unit
            )
        except Exception as e:
            self.logger.warning(f"Failed to extract dimensions: {e}")
            return None
    
    def _extract_weight(self, raw_data: Dict[str, Any]) -> Optional[Weight]:
        """Extract weight from raw product data"""
        try:
            # Try gross weight first, then net weight
            weight_value = self._parse_float(raw_data.get('gross_weight')) or \
                          self._parse_float(raw_data.get('net_weight'))
            
            if not weight_value:
                return None
            
            # Get unit (default to kg)
            unit_str = raw_data.get('gross_weight_unit', 'kg').lower()
            unit = WeightUnit.KG if unit_str == 'kg' else WeightUnit.G
            
            return Weight(value=weight_value, unit=unit)
        except Exception as e:
            self.logger.warning(f"Failed to extract weight: {e}")
            return None
    
    def _extract_categories(self, raw_data: Dict[str, Any]) -> List[Category]:
        """Extract categories from product data"""
        categories = []
        
        # Main category
        if raw_data.get('product_class'):
            categories.append(Category(
                name=raw_data['product_class'],
                level=1
            ))
        
        # Sub-categories from variants
        variants = raw_data.get('variants', [])
        if variants:
            variant = variants[0]  # Use first variant for category info
            
            if variant.get('category_level1'):
                categories.append(Category(
                    name=variant['category_level1'],
                    level=1
                ))
            
            if variant.get('category_level2'):
                categories.append(Category(
                    name=variant['category_level2'],
                    level=2
                ))
            
            if variant.get('category_level3'):
                categories.append(Category(
                    name=variant['category_level3'],
                    level=3
                ))
        
        return categories
    
    def _extract_variants(self, raw_data: Dict[str, Any]) -> List[Variant]:
        """Extract product variants from raw data"""
        variants = []
        
        for variant_data in raw_data.get('variants', []):
            try:
                # Color variant
                color_variant = ColorVariant(
                    code=variant_data.get('color_code', ''),
                    name=variant_data.get('color_description', ''),
                    hex_color=None,  # Not provided in API
                    pms_color=variant_data.get('pms_color', ''),
                    images=self._extract_variant_images(variant_data)
                )
                
                # Variant pricing
                variant_prices = self._extract_variant_prices(variant_data)
                
                variant = Variant(
                    sku=variant_data.get('sku', ''),
                    variant_id=variant_data.get('variant_id', ''),
                    color=color_variant,
                    prices=variant_prices,
                    images=self._extract_variant_images(variant_data),
                    gtin=variant_data.get('gtin', ''),
                    status=self._get_variant_status(variant_data)
                )
                
                variants.append(variant)
                
            except Exception as e:
                self.logger.warning(f"Failed to extract variant {variant_data.get('sku', 'unknown')}: {e}")
        
        return variants
    
    def _extract_variant_images(self, variant_data: Dict[str, Any]) -> List[Image]:
        """Extract images from variant data"""
        images = []
        
        for asset in variant_data.get('digital_assets', []):
            if asset.get('type') == 'image':
                images.append(Image(
                    url=asset.get('url', ''),
                    type=asset.get('subtype', ''),
                    description=asset.get('subtype', '').replace('_', ' ').title()
                ))
        
        return images
    
    def _extract_main_images(self, raw_data: Dict[str, Any]) -> List[Image]:
        """Extract main product images"""
        images = []
        
        # Get images from first variant if available
        variants = raw_data.get('variants', [])
        if variants:
            return self._extract_variant_images(variants[0])
        
        return images
    
    def _extract_variant_prices(self, variant_data: Dict[str, Any]) -> List[Price]:
        """Extract pricing for a variant"""
        prices = []
        
        # Get price from pricing data if available
        sku = variant_data.get('sku', '')
        if sku in self.pricing_data:
            price_data = self.pricing_data[sku]
            
            try:
                # Parse price (comes as string with comma decimal separator)
                price_str = price_data.get('price', '0').replace(',', '.')
                price_value = float(price_str)
                
                # Parse valid until date
                valid_until = None
                if price_data.get('valid_until'):
                    valid_until = datetime.strptime(price_data['valid_until'], '%Y-%m-%d')
                
                price = Price(
                    value=price_value,
                    currency=Currency.GBP,  # MidOcean uses GBP
                    min_quantity=1,
                    type=PriceType.UNIT,
                    valid_until=valid_until
                )
                
                prices.append(price)
                
            except Exception as e:
                self.logger.warning(f"Failed to parse price for {sku}: {e}")
        
        return prices
    
    def _extract_base_prices(self, raw_data: Dict[str, Any]) -> List[Price]:
        """Extract base pricing from variants"""
        prices = []
        
        # Get first variant price as base price
        variants = raw_data.get('variants', [])
        if variants:
            variant_prices = self._extract_variant_prices(variants[0])
            if variant_prices:
                prices.append(variant_prices[0])
        
        return prices
    
    def _extract_print_positions(self, raw_data: Dict[str, Any]) -> List[PrintPosition]:
        """Extract print positions from raw data"""
        positions = []
        
        # Get print positions from print data
        master_code = raw_data.get('master_code', '')
        print_product_data = None
        
        # Find print data for this product
        for product in self.print_data.get('products', []):
            if product.get('master_code') == master_code:
                print_product_data = product
                break
        
        if not print_product_data:
            return positions
        
        for pos_data in print_product_data.get('printing_positions', []):
            try:
                # Extract print techniques
                techniques = []
                for tech_data in pos_data.get('printing_techniques', []):
                    tech_id = tech_data.get('id', '')
                    if tech_id in self.print_technique_mapping:
                        techniques.append(self.print_technique_mapping[tech_id])
                
                # Extract position images
                position_images = []
                for img_data in pos_data.get('images', []):
                    position_images.append(Image(
                        url=img_data.get('print_position_image_with_area', ''),
                        type='print_position',
                        description=f"Print position: {pos_data.get('position_id', '')}"
                    ))
                
                position = PrintPosition(
                    id=pos_data.get('position_id', ''),
                    name=pos_data.get('position_id', ''),
                    max_width=pos_data.get('max_print_size_width'),
                    max_height=pos_data.get('max_print_size_height'),
                    unit=DimensionUnit.MM,  # MidOcean uses mm
                    techniques=techniques,
                    images=position_images
                )
                
                positions.append(position)
                
            except Exception as e:
                self.logger.warning(f"Failed to extract print position {pos_data.get('position_id', 'unknown')}: {e}")
        
        return positions
    
    def _extract_print_options(self, raw_data: Dict[str, Any]) -> List[PrintOption]:
        """Extract print options with pricing"""
        options = []
        
        # Get print pricing data
        for tech_data in self.print_pricing.get('print_techniques', []):
            try:
                tech_id = tech_data.get('id', '')
                if tech_id not in self.print_technique_mapping:
                    continue
                
                technique = self.print_technique_mapping[tech_id]
                
                # Extract setup cost
                setup_cost = self._parse_float(tech_data.get('setup', '0').replace(',', '.'))
                
                # Extract pricing tiers
                prices = []
                for cost_data in tech_data.get('var_costs', []):
                    for scale in cost_data.get('scales', []):
                        try:
                            price_value = self._parse_float(scale.get('price', '0').replace(',', '.'))
                            min_qty = self._parse_int(scale.get('minimum_quantity', '1').replace('.', ''))
                            
                            if price_value and min_qty:
                                prices.append(Price(
                                    value=price_value,
                                    currency=Currency.GBP,
                                    min_quantity=min_qty,
                                    type=PriceType.UNIT
                                ))
                        except Exception as e:
                            self.logger.warning(f"Failed to parse print price scale: {e}")
                
                option = PrintOption(
                    technique=technique,
                    position="various",  # MidOcean doesn't specify position in pricing
                    setup_charge=setup_cost,
                    prices=prices,
                    is_default=False
                )
                
                options.append(option)
                
            except Exception as e:
                self.logger.warning(f"Failed to extract print option {tech_data.get('id', 'unknown')}: {e}")
        
        return options
    
    def _get_variant_status(self, variant_data: Dict[str, Any]) -> ProductStatus:
        """Determine variant status"""
        # Check discontinuation date
        discontinued_date = variant_data.get('discontinued_date')
        if discontinued_date and discontinued_date != '2099-12-31':
            return ProductStatus.DISCONTINUED
        
        # Check PLC status
        plc_status = variant_data.get('plc_status_description', '').upper()
        if 'DISCONTINUED' in plc_status:
            return ProductStatus.DISCONTINUED
        
        return ProductStatus.ACTIVE
    
    def _parse_float(self, value: Any) -> Optional[float]:
        """Safely parse float value"""
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                # Handle European decimal format (comma as decimal separator)
                value = value.replace(',', '.')
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_int(self, value: Any) -> Optional[int]:
        """Safely parse integer value"""
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                # Remove thousands separators
                value = value.replace('.', '').replace(',', '')
            return int(value)
        except (ValueError, TypeError):
            return None 