# Generalized ETL System for Supplier Product Catalogs

A flexible and scalable ETL (Extract, Transform, Load) pipeline designed to handle different supplier APIs with varying response formats, normalizing them into a unified MongoDB schema.

## 🎯 Overview

This system solves the challenge of integrating product catalogs from multiple suppliers with different API formats. It provides:

- **Unified Schema**: All supplier data is normalized into a consistent MongoDB structure
- **Flexible Architecture**: Easy to add new suppliers with minimal code changes
- **Comprehensive Data Model**: Handles products, variants, pricing, print options, images, and more
- **Robust Error Handling**: Graceful handling of API failures and data inconsistencies
- **Configurable Pipeline**: JSON-based configuration for different environments

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   EXTRACTORS    │    │  TRANSFORMERS   │    │    LOADERS      │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │  MidOcean   │ │────│ │  MidOcean   │ │────│ │  MongoDB    │ │
│ │  Extractor  │ │    │ │ Transformer │ │    │ │   Loader    │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │                 │
│ │   Laltex    │ │────│ │   Laltex    │ │────│                 │
│ │  Extractor  │ │    │ │ Transformer │ │    │                 │
│ └─────────────┘ │    │ └─────────────┘ │    │                 │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │                 │
│ │   Preseli   │ │────│ │   Preseli   │ │────│                 │
│ │  Extractor  │ │    │ │ Transformer │ │    │                 │
│ └─────────────┘ │    │ └─────────────┘ │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📊 Unified Data Schema

The system normalizes all supplier data into a comprehensive schema:

### Core Product Structure
- **Basic Info**: Name, descriptions, categories, brand
- **Physical Properties**: Dimensions, weight, materials
- **Variants**: Color/size variations with individual pricing
- **Pricing**: Tiered pricing with currency and validity periods
- **Print Options**: Available printing techniques and positions
- **Media**: Images, artwork templates, documents
- **Logistics**: MOQ, lead times, shipping options
- **Compliance**: Origin country, tariff codes, certifications

### MongoDB Collections
- `products`: Main product catalog
- Optimized indexes for efficient querying
- Support for complex aggregations and reporting

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- MongoDB 4.0+
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd etl
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start MongoDB**
   ```bash
   mongod
   ```

4. **Run the demo**
   ```bash
   python demo.py
   ```

### Basic Usage

1. **Create configuration**
   ```bash
   python -m etl.orchestrator --action create-config
   ```

2. **Validate connections**
   ```bash
   python -m etl.orchestrator --action validate
   ```

3. **Run full sync**
   ```bash
   python -m etl.orchestrator --action sync
   ```

4. **Check status**
   ```bash
   python -m etl.orchestrator --action status
   ```

## 📁 Project Structure

```
etl/
├── schemas/
│   └── unified_product_schema.py    # Pydantic models for unified schema
├── etl/
│   ├── base.py                      # Abstract base classes
│   ├── extractors/
│   │   └── midocean_extractor.py    # MidOcean API extractor
│   ├── transformers/
│   │   └── midocean_transformer.py  # MidOcean data transformer
│   ├── loaders/
│   │   └── mongodb_loader.py        # MongoDB loader
│   └── orchestrator.py              # Main ETL orchestrator
├── sample data/                     # Sample data files
├── config/
│   └── etl_config.json             # Configuration file
├── logs/                           # Log files
├── demo.py                         # Demo script
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

## ⚙️ Configuration

Configuration is managed through `config/etl_config.json`:

```json
{
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
        "api_key": "your-api-key",
        "language": "en",
        "use_sample_data": true,
        "sample_data_path": "sample data/MidOcean Sample Data.json"
      },
      "batch_size": 100
    }
  },
  "logging": {
    "level": "INFO",
    "file": "logs/etl.log"
  }
}
```

## 🔧 Adding New Suppliers

The system is designed for easy extension. To add a new supplier:

1. **Create Extractor**
   ```python
   from etl.base import BaseExtractor, etl_component
   
   @etl_component("extractor", "newsupplier")
   class NewSupplierExtractor(BaseExtractor):
       def extract_products(self):
           # Implementation
           pass
   ```

2. **Create Transformer**
   ```python
   from etl.base import BaseTransformer, etl_component
   
   @etl_component("transformer", "newsupplier")
   class NewSupplierTransformer(BaseTransformer):
       def transform_product(self, raw_data):
           # Implementation
           pass
   ```

3. **Add Configuration**
   ```json
   {
     "suppliers": {
       "newsupplier": {
         "supplier_name": "New Supplier",
         "api": {
           "api_key": "key",
           "base_url": "https://api.newsupplier.com"
         }
       }
     }
   }
   ```

## 📊 Sample Data

The system includes sample data from multiple suppliers:

- **MidOcean**: Comprehensive product catalog with print options
- **Laltex**: UK-based supplier with regional shipping
- **Preseli**: Eco-friendly promotional products

This allows for testing and development without requiring API access.

## 🎯 Key Features

### Multi-Endpoint Support
- Handles suppliers with multiple API endpoints (products, pricing, print data)
- Intelligent data correlation across endpoints
- Caching for performance optimization

### Flexible Transformation
- Field mapping and normalization
- Data type conversion and validation
- Missing data handling
- Multi-language support

### Robust Error Handling
- Graceful API failure handling
- Partial success support
- Detailed error reporting and logging
- Automatic retry mechanisms

### Performance Optimization
- Batch processing for large datasets
- Database indexing for fast queries
- Memory-efficient streaming
- Configurable batch sizes

## 📈 Monitoring and Logging

The system provides comprehensive monitoring:

- **Structured Logging**: JSON-formatted logs for analysis
- **Performance Metrics**: Processing times and throughput
- **Error Tracking**: Detailed error reporting
- **Status Dashboard**: Real-time pipeline status

## 🔒 Security

- API key management through configuration
- Data validation and sanitization
- Secure database connections
- Audit logging for compliance

## 🚀 Production Deployment

For production use:

1. **Environment Variables**: Use environment variables for sensitive data
2. **Monitoring**: Set up log aggregation and monitoring
3. **Scheduling**: Use cron jobs or scheduling systems
4. **Backup**: Regular database backups
5. **Scaling**: MongoDB sharding for large datasets

## 📋 API Reference

### ETL Orchestrator Commands

```bash
# Full sync
python -m etl.orchestrator --action sync [--supplier supplier_id]

# Incremental sync
python -m etl.orchestrator --action incremental [--since 2023-01-01]

# Validate connections
python -m etl.orchestrator --action validate

# Get status
python -m etl.orchestrator --action status

# Create configuration
python -m etl.orchestrator --action create-config
```

### MongoDB Queries

```javascript
// Find products by supplier
db.products.find({"supplier.id": "midocean"})

// Find printable products
db.products.find({"is_printable": true})

// Find products by category
db.products.find({"categories.name": "Office & Writing"})

// Aggregate by supplier
db.products.aggregate([
  {"$group": {"_id": "$supplier.id", "count": {"$sum": 1}}}
])
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests and documentation
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
- Check the logs in `logs/etl.log`
- Review the configuration in `config/etl_config.json`
- Ensure MongoDB is running and accessible
- Verify sample data files exist in `sample data/`

## 🔮 Future Enhancements

- **Web Dashboard**: Real-time monitoring interface
- **Data Validation**: Advanced data quality checks
- **Incremental Updates**: Smart change detection
- **Multi-Database Support**: Support for other databases
- **API Rate Limiting**: Intelligent rate limiting
- **Data Lineage**: Track data transformations
- **Automated Testing**: Comprehensive test suite 