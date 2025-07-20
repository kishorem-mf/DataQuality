# DataQuality - Unity Catalog SQL Agent

A simplified, clean architecture for natural language to SQL conversion using Databricks Unity Catalog.

## 🏗️ Architecture

Simple, production-ready design focused on Unity Catalog metadata management:

```
DataQuality/
├── agents/                     # Core agent framework
│   ├── base_agent.py          # Base agent classes and framework
│   ├── unity_sql_agent.py     # 🎯 Main Unity SQL Agent
│   └── __init__.py            # Package initialization
├── config/                     # Configuration management
│   ├── unity_catalog_config.json  # 🗂️ Unity Catalog metadata
│   └── azure_openai_config.json   # LLM service settings
├── services/                   # Core services
│   ├── llm_service.py         # OpenAI/Azure LLM integration
│   └── __init__.py            # Package initialization
├── DataQuality_AI_CaseStudy.md    # 📄 Comprehensive case study
├── DataQuality_AI_CaseStudy.pdf   # 📄 PDF case study (789KB)
├── PROJECT_STRUCTURE.md           # Architecture documentation
├── main.py                         # 🚀 Main application entry
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation
```

## 🎯 Core Features

### ✅ **Simple Query Flow**
1. **Natural Language Input**: "show completeness metric for md_location table string columns"
2. **Unity Catalog Metadata**: Loads catalogs/schemas/tables from configuration
3. **SQL Generation**: AI converts to safe SQL using real schema metadata
4. **Databricks Execution**: Direct execution on Databricks warehouse
5. **Formatted Response**: Clean, structured results with actual data

### ✅ **Configuration-Driven Architecture**
```json
{
  "catalogs": ["sop_da"],
  "schemas": ["sop_da.sop_da"],
  "tables": [
    "sop_da.sop_da.md_customer",
    "sop_da.sop_da.md_location",
    "sop_da.sop_da.md_product"
  ]
}
```

### ✅ **Security & Safety**
- SQL injection prevention
- Forbidden operation blocking (DROP, DELETE, etc.)
- Automatic LIMIT clause addition
- Query validation before execution
- Schema validation to prevent column hallucination

### ✅ **Smart Schema Handling**
- **Real-time Schema Fetching**: Uses DESCRIBE to get actual table metadata
- **Fallback Mechanisms**: Automatic DESCRIBE when schema unavailable
- **Column Validation**: Prevents LLM from inventing non-existent columns
- **Error Handling**: Comprehensive error handling for metadata failures

## 🚀 Quick Start

### Command Line Interface
```bash
# Ask about available tables
python main.py sql-query "show me all available tables"

# Query specific data with real column names
python main.py sql-query "show completeness metric for md_location table string columns"

# Assess a specific table
python main.py assess sop_da.sop_da.md_customer

# Process natural language queries
python main.py query "show me customer data overview"

# Check system health
python main.py health
```

### Python API
```python
from main import ask_sql_question, assess_table

# Natural language SQL queries
result = ask_sql_question("show me customer data from md_customer table")
print(result['sql'])        # Generated SQL with real column names
print(result['summary'])    # Human-readable summary
print(result['results'])    # Actual query results from Databricks

# Table assessment
assessment = assess_table("sop_da.sop_da.md_customer")
print(assessment['status']) # Assessment status
print(assessment['results']) # Assessment data
```

## 📊 Current Configuration

### **Catalogs**: 1
- `sop_da` - SOP data analytics

### **Schemas**: 1
- `sop_da.sop_da`

### **Tables**: 14
- `md_customer`, `md_location`, `md_product`, `md_resource`
- `md_salesorder`, `md_transaction`, `chat_history`, `cpu_data`
- `event_logs`, `md_location_resource`, `md_location_source`
- `md_production_source_header`, `md_production_source_item`
- `md_production_source_resource`

All tables properly configured with fully qualified names from Unity Catalog.

## 🔧 Setup

### 1. **Environment Variables**
```bash
DATABRICKS_SERVER_HOSTNAME=your-databricks-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-personal-access-token
```

### 2. **Configuration Files**
Update `config/unity_catalog_config.json` with your Unity Catalog metadata:
```json
{
  "catalogs": ["your_catalog"],
  "schemas": ["your_catalog.your_schema"],
  "tables": ["your_catalog.your_schema.your_table"]
}
```

### 3. **Dependencies**
```bash
pip install -r requirements.txt
```

## 🎉 Key Improvements

### **Problem Solved**: Column Hallucination
- **Before**: LLM generated SQL with non-existent columns like `location_name`
- **After**: Uses real schema metadata to generate SQL with actual columns like `location_id`, `location`, `location_region`

### **Clean Architecture**
1. **Simple & Maintainable**: Easy to understand and extend
2. **Configuration-Driven**: All metadata externalized to config files
3. **Unity Catalog Native**: Direct integration with Databricks metadata
4. **Production Ready**: Safety, logging, error handling, schema validation
5. **No Legacy Dependencies**: Removed PySpark requirements, BELIV references

### **Enhanced Reliability**
- **Schema Validation**: Prevents SQL generation with wrong column names
- **Fallback Mechanisms**: Automatic DESCRIBE when metadata unavailable
- **Comprehensive Error Handling**: Graceful handling of metadata fetch failures
- **Real-time Verification**: Live schema fetching from Unity Catalog

## 📝 Example Results

**Query**: "show completeness metric for md_location table string columns"

**Generated SQL**: 
```sql
SELECT 'location_id' AS column_name, 
       (COUNT(location_id) / (SELECT COUNT(*) FROM sop_da.sop_da.md_location)) AS completeness_metric 
FROM sop_da.sop_da.md_location 
UNION ALL 
SELECT 'location' AS column_name, 
       (COUNT(location) / (SELECT COUNT(*) FROM sop_da.sop_da.md_location)) AS completeness_metric 
FROM sop_da.sop_da.md_location
-- ... (continues for all string columns)
```

**Results**: Actual completeness metrics using real column names from Unity Catalog schema.

The system now reliably converts natural language to SQL using verified schema metadata, eliminating column hallucination and ensuring accurate query execution.