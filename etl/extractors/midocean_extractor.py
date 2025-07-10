"""
MidOcean Extractor
Handles extraction from MidOcean's multiple API endpoints
"""

import json
import os
from typing import Dict, List, Any, Generator
import requests
from datetime import datetime

from etl.base import BaseExtractor, etl_component

@etl_component("extractor", "midocean")
class MidOceanExtractor(BaseExtractor):
    """
    MidOcean API extractor
    Handles products, pricing, print data, and print pricing endpoints
    """
    
    def __init__(self, supplier_config: Dict[str, Any]):
        super().__init__(supplier_config)
        
        # API endpoints
        self.endpoints = {
            'products': 'https://api.midocean.com/gateway/products/2.0',
            'pricelist': 'https://api.midocean.com/gateway/pricelist/2.0',
            'printdata': 'https://api.midocean.com/gateway/printdata/1.0',
            'printpricelist': 'https://api.midocean.com/gateway/printpricelist/2.0'
        }
        
        # API credentials
        self.api_key = self.api_config.get('api_key', '')
        self.language = self.api_config.get('language', 'en')
        
        # Use sample data for testing
        self.use_sample_data = self.api_config.get('use_sample_data', True)
        self.sample_data_path = self.api_config.get('sample_data_path', 'sample data/MidOcean Sample Data.json')
        
        # Cache for additional API data
        self._print_data = None
        self._pricing_data = None
        self._print_pricing_data = None
    
    def extract_products(self) -> Generator[Dict[str, Any], None, None]:
        """Extract product data from MidOcean API"""
        if self.use_sample_data:
            yield from self._extract_products_from_sample()
        else:
            yield from self._extract_products_from_api()
    
    def extract_pricing(self, product_codes: List[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Extract pricing data from MidOcean API"""
        if self.use_sample_data:
            yield from self._extract_pricing_from_sample()
        else:
            yield from self._extract_pricing_from_api()
    
    def extract_stock(self, product_codes: List[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Extract stock data - MidOcean doesn't have a dedicated stock endpoint"""
        # MidOcean doesn't have a separate stock endpoint
        return
        yield  # Make this a generator
    
    def get_print_data(self) -> Dict[str, Any]:
        """Get print data (techniques, positions, etc.)"""
        if self._print_data is None:
            if self.use_sample_data:
                self._print_data = self._get_print_data_from_sample()
            else:
                self._print_data = self._get_print_data_from_api()
        
        return self._print_data
    
    def get_pricing_data(self) -> Dict[str, Any]:
        """Get pricing data"""
        if self._pricing_data is None:
            if self.use_sample_data:
                self._pricing_data = self._get_pricing_data_from_sample()
            else:
                self._pricing_data = self._get_pricing_data_from_api()
        
        return self._pricing_data
    
    def get_print_pricing_data(self) -> Dict[str, Any]:
        """Get print pricing data"""
        if self._print_pricing_data is None:
            if self.use_sample_data:
                self._print_pricing_data = self._get_print_pricing_data_from_sample()
            else:
                self._print_pricing_data = self._get_print_pricing_data_from_api()
        
        return self._print_pricing_data
    
    def _extract_products_from_sample(self) -> Generator[Dict[str, Any], None, None]:
        """Extract products from sample data file"""
        try:
            sample_data = self._load_sample_data()
            
            # Parse products from sample data
            for line in sample_data.split('\n'):
                if line.strip().startswith('[{') and 'master_code' in line:
                    # Found the products JSON line
                    products_json = line.strip()
                    if products_json.endswith(','):
                        products_json = products_json[:-1]  # Remove trailing comma
                    
                    products = json.loads(products_json)
                    
                    for product in products:
                        yield product
                    
                    return
                    
        except Exception as e:
            self.logger.error(f"Failed to extract products from sample data: {e}")
    
    def _extract_pricing_from_sample(self) -> Generator[Dict[str, Any], None, None]:
        """Extract pricing from sample data file"""
        try:
            sample_data = self._load_sample_data()
            
            # Parse pricing from sample data
            for line in sample_data.split('\n'):
                if '"currency":"GBP"' in line and '"price":[' in line:
                    pricing_json = line.strip()
                    if pricing_json.endswith(','):
                        pricing_json = pricing_json[:-1]
                    
                    pricing_data = json.loads(pricing_json)
                    
                    for price_item in pricing_data.get('price', []):
                        yield price_item
                    
                    return
                    
        except Exception as e:
            self.logger.error(f"Failed to extract pricing from sample data: {e}")
    
    def _extract_products_from_api(self) -> Generator[Dict[str, Any], None, None]:
        """Extract products from MidOcean API"""
        try:
            url = self.endpoints['products']
            params = {'language': self.language}
            headers = {'Authorization': f'Bearer {self.api_key}'}
            
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            products = response.json()
            
            for product in products:
                yield product
                
        except Exception as e:
            self.logger.error(f"Failed to extract products from API: {e}")
    
    def _extract_pricing_from_api(self) -> Generator[Dict[str, Any], None, None]:
        """Extract pricing from MidOcean API"""
        try:
            url = self.endpoints['pricelist']
            headers = {'Authorization': f'Bearer {self.api_key}'}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            pricing_data = response.json()
            
            for price_item in pricing_data.get('price', []):
                yield price_item
                
        except Exception as e:
            self.logger.error(f"Failed to extract pricing from API: {e}")
    
    def _get_print_data_from_sample(self) -> Dict[str, Any]:
        """Get print data from sample file"""
        try:
            sample_data = self._load_sample_data()
            
            # Look for print data section
            for line in sample_data.split('\n'):
                if '"printing_technique_descriptions":[' in line:
                    print_data_json = line.strip()
                    if print_data_json.endswith(','):
                        print_data_json = print_data_json[:-1]
                    
                    return json.loads(print_data_json)
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Failed to get print data from sample: {e}")
            return {}
    
    def _get_pricing_data_from_sample(self) -> Dict[str, Any]:
        """Get pricing data from sample file as SKU -> price mapping"""
        try:
            pricing_map = {}
            
            for price_item in self._extract_pricing_from_sample():
                sku = price_item.get('sku')
                if sku:
                    pricing_map[sku] = price_item
            
            return pricing_map
            
        except Exception as e:
            self.logger.error(f"Failed to get pricing data from sample: {e}")
            return {}
    
    def _get_print_pricing_data_from_sample(self) -> Dict[str, Any]:
        """Get print pricing data from sample file"""
        try:
            sample_data = self._load_sample_data()
            
            # Look for print pricing section
            lines = sample_data.split('\n')
            for i, line in enumerate(lines):
                if '"print_manipulations":[' in line:
                    # Found print pricing data - it's usually the last large JSON object
                    print_pricing_json = line.strip()
                    if print_pricing_json.endswith(','):
                        print_pricing_json = print_pricing_json[:-1]
                    
                    return json.loads(print_pricing_json)
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Failed to get print pricing data from sample: {e}")
            return {}
    
    def _get_print_data_from_api(self) -> Dict[str, Any]:
        """Get print data from MidOcean API"""
        try:
            url = self.endpoints['printdata']
            headers = {'Authorization': f'Bearer {self.api_key}'}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            self.logger.error(f"Failed to get print data from API: {e}")
            return {}
    
    def _get_pricing_data_from_api(self) -> Dict[str, Any]:
        """Get pricing data from MidOcean API as SKU -> price mapping"""
        try:
            pricing_map = {}
            
            for price_item in self._extract_pricing_from_api():
                sku = price_item.get('sku')
                if sku:
                    pricing_map[sku] = price_item
            
            return pricing_map
            
        except Exception as e:
            self.logger.error(f"Failed to get pricing data from API: {e}")
            return {}
    
    def _get_print_pricing_data_from_api(self) -> Dict[str, Any]:
        """Get print pricing data from MidOcean API"""
        try:
            url = self.endpoints['printpricelist']
            headers = {'Authorization': f'Bearer {self.api_key}'}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            self.logger.error(f"Failed to get print pricing data from API: {e}")
            return {}
    
    def _load_sample_data(self) -> str:
        """Load sample data from file"""
        try:
            if os.path.exists(self.sample_data_path):
                with open(self.sample_data_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                self.logger.error(f"Sample data file not found: {self.sample_data_path}")
                return ""
        except Exception as e:
            self.logger.error(f"Failed to load sample data: {e}")
            return ""
    
    def validate_connection(self) -> bool:
        """Validate connection to MidOcean API or sample data"""
        try:
            if self.use_sample_data:
                # Check if sample data file exists and is readable
                sample_data = self._load_sample_data()
                return len(sample_data) > 0
            else:
                # Test API connection
                url = self.endpoints['products']
                params = {'language': self.language}
                headers = {'Authorization': f'Bearer {self.api_key}'}
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                return response.status_code == 200
                
        except Exception as e:
            self.logger.error(f"Connection validation failed: {e}")
            return False 