# Databricks Data Quality Agent System

A comprehensive, AI-powered data quality assessment system designed specifically for Databricks environments with Unity Catalog integration.

## 🎯 Overview

This system transforms traditional procedural data quality code into an intelligent, agent-based architecture that provides:

- **Automated daily data quality assessments**
- **AI-powered insights and recommendations**
- **Natural language query interface**
- **Adaptive thresholds and intelligent decision-making**
- **Scalable, cost-effective processing**

## 🏗️ Architecture

### Agent-Based Design

The system uses specialized agents that work together:

- **DataQualityOrchestrator**: Coordinates all quality assessment activities
- **TableDiscoveryAgent**: Discovers and analyzes table schemas with AI insights
- **MetricComputationAgent**: Computes quality metrics with adaptive thresholds
- **NLPQueryAgent**: Processes natural language queries about data quality
- **ResultsWriterAgent**: Manages results storage in Delta tables

### Azure OpenAI Integration

- **LLMService**: Centralized AI service management
- **Intelligent Analysis**: AI-powered schema analysis and quality insights
- **Adaptive Behavior**: Agents adjust based on data characteristics
- **Natural Language Processing**: Conversational queries about data quality

## 📋 Features

### Core Capabilities

- ✅ **Completeness Analysis**: Null value detection and analysis
- ✅ **Uniqueness Validation**: Duplicate detection and key analysis
- ✅ **Validity Checks**: Outlier detection and rare category analysis
- ✅ **Consistency Monitoring**: Format and pattern validation
- ✅ **Accuracy Assessment**: Data correctness evaluation
- ✅ **Timeliness Tracking**: Data freshness analysis

### AI-Enhanced Features

- 🤖 **Intelligent Column Classification**: Automatic semantic understanding
- 🤖 **Adaptive Thresholds**: AI-driven quality thresholds
- 🤖 **Root Cause Analysis**: AI explanations for quality issues
- 🤖 **Recommendations**: Automated improvement suggestions
- 🤖 **Natural Language Queries**: Conversational data quality interface

### Enterprise Features

- 📊 **Delta Lake Storage**: Persistent results with history tracking
- 📊 **Unity Catalog Integration**: Seamless metadata access
- 📊 **Cost Optimization**: Intelligent sampling and resource management
- 📊 **Scalable Processing**: Distributed Spark processing
- 📊 **Monitoring & Alerting**: Built-in health checks and notifications

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd DataQuality

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Configure your environment variables in `.env`:

```env
# Azure OpenAI Configuration
AZURE_OPENAI_API_KEY=your_api_key_here
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-02-01
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4

# Databricks Configuration
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-databricks-token
```

### 3. Update Configuration Files

Edit the configuration files in the `config/` directory:

- `tables_config.json`: Specify tables to assess
- `metrics_config.json`: Configure metrics and thresholds
- `agent_config.json`: Set agent behavior parameters

### 4. Run Assessment

```python
from main import DataQualityApp

# Initialize the application
app = DataQualityApp()

# Run full assessment
results = app.run_full_assessment()

# Ask natural language questions
response = app.process_nlp_query("What is the completeness for table orders?")
```

## 📖 Usage Examples

### Command Line Interface

```bash
# Run full assessment
python main.py full-assessment

# Assess specific tables
python main.py full-assessment --tables beliv_prd.sales.orders beliv_prd.inventory.stock

# Single table assessment
python main.py single-table beliv_prd.catalogs.tipo_cambio

# Natural language queries
python main.py query "What is the completeness score for table orders?"

# Check system health
python main.py health

# Get usage statistics
python main.py stats
```

### Databricks Notebook

```python
# Databricks notebook source
from main import run_quality_assessment, ask_quality_question

# Quick assessment
results = run_quality_assessment(
    tables=["beliv_prd.sales.orders", "beliv_prd.inventory.stock"]
)

# Ask questions
answer = ask_quality_question("Show me all categorical columns in table orders")
```

### Natural Language Queries

The system supports conversational queries:

```python
queries = [
    "What is the completeness score for table sales.orders?",
    "Show me all key columns in inventory.stock",
    "List all categorical columns in beliv_prd.sales.orders",
    "What timestamp columns are available in table X?",
    "Which tables have data quality issues?",
    "What are the top recommendations for improving data quality?"
]

for query in queries:
    response = app.process_nlp_query(query)
    print(f"Q: {query}")
    print(f"A: {response['nlp_result']['response']['message']}")
```

## 📊 Results Storage

Results are stored in Delta tables with the following schema:

```sql
CREATE TABLE dq_metrics.daily_results (
    run_id STRING,
    table_name STRING,
    column_name STRING,
    metric_name STRING,
    metric_value DOUBLE,
    metric_value_str STRING,
    threshold_value DOUBLE,
    status STRING,  -- PASS, FAIL, WARNING
    run_timestamp TIMESTAMP,
    run_date STRING,  -- Partition column
    metadata STRING,  -- JSON metadata
    insights STRING,  -- AI insights
    recommendations STRING  -- AI recommendations
) PARTITIONED BY (run_date)
```

## 🔧 Configuration

### Tables Configuration (`config/tables_config.json`)

```json
{
  "tables": [
    "beliv_prd.sales.orders",
    "beliv_prd.inventory.stock"
  ],
  "sampling_rate": 0.01,
  "max_tables_per_run": 10,
  "ai_enabled": true,
  "parallel_processing": true,
  "exclude_columns": ["ingest_date", "report_date"]
}
```

### Metrics Configuration (`config/metrics_config.json`)

```json
{
  "default_metrics": ["completeness", "uniqueness", "validity", "consistency"],
  "table_specific_config": {
    "beliv_prd.sales.orders": {
      "metrics": ["completeness", "uniqueness", "accuracy"],
      "columns": {
        "completeness": ["order_id", "customer_id"],
        "uniqueness": ["order_id"]
      },
      "thresholds": {
        "completeness": 0.95,
        "uniqueness": 0.99
      },
      "ai_adaptive_thresholds": true
    }
  }
}
```

## 🤖 AI Features

### Schema Analysis

The system uses AI to analyze table schemas:

```python
# AI analyzes columns and provides insights
insights = {
    "table_purpose": "Customer orders transaction data",
    "column_classifications": {
        "order_id": "key",
        "customer_id": "key", 
        "product_name": "categorical",
        "order_amount": "numerical",
        "order_date": "temporal"
    },
    "data_quality_concerns": [
        "Monitor order_id uniqueness",
        "Check for null customer_id values"
    ],
    "recommended_metrics": {
        "order_id": ["uniqueness", "completeness"],
        "customer_id": ["completeness", "validity"]
    }
}
```

### Adaptive Thresholds

AI adjusts quality thresholds based on data patterns:

```python
# AI-generated adaptive thresholds
thresholds = {
    "completeness_threshold": 0.98,  # Higher than default due to critical data
    "uniqueness_threshold": 0.995,  # Very high for key columns
    "validity_threshold": 0.92,     # Adjusted for business context
    "reasoning": "Increased thresholds due to financial data criticality"
}
```

### Quality Insights

AI provides actionable insights:

```python
insights = """
Overall data quality assessment: GOOD (92.3%)

Critical issues requiring attention:
- customer_id column has 5.2% null values (exceeded threshold)
- order_amount shows 12 outliers above 99th percentile

Recommendations:
1. Implement customer_id validation at source
2. Review order_amount outliers for data entry errors
3. Consider adding referential integrity checks

Business impact: High - affects customer analytics and reporting
"""
```

## 📅 Scheduling

### Databricks Jobs

The system includes a complete Databricks job configuration:

```json
{
  "name": "Daily Data Quality Assessment",
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  },
  "email_notifications": {
    "on_failure": ["data-team@company.com"]
  }
}
```

### Automated Execution

```python
# Schedule daily assessment
def daily_quality_check():
    app = DataQualityApp()
    results = app.run_full_assessment()
    
    # Send alerts for failures
    if results.get('status') == 'failed':
        send_alert(results)
    
    return results
```

## 🔍 Monitoring

### Health Checks

```python
# System health monitoring
health = app.get_health_status()
print(f"System Status: {health['status']}")

# Component health
for component, status in health['components'].items():
    print(f"{component}: {status['status']}")
```

### Usage Statistics

```python
# Track AI usage and costs
stats = app.get_usage_stats()
print(f"AI Requests: {stats['llm_usage']['total_requests']}")
print(f"Total Tokens: {stats['llm_usage']['total_tokens']}")
print(f"Failed Requests: {stats['llm_usage']['failed_requests']}")
```

## 🔒 Security

### Authentication

- Azure OpenAI API key management
- Databricks token authentication
- Unity Catalog access controls

### Data Privacy

- Sensitive data never leaves Databricks environment
- Configurable AI interaction logging
- Audit trails for all operations

## 💰 Cost Optimization

### Intelligent Sampling

```python
# Automatic sampling for large tables
sampling_config = {
    "sampling_rate": 0.01,  # 1% sample for large tables
    "min_rows_for_sampling": 1000000,
    "adaptive_sampling": True  # AI adjusts based on data patterns
}
```

### Resource Management

- Serverless compute optimization
- Cluster autoscaling configuration
- Cost-aware execution strategies

## 🧪 Testing

### Unit Tests

```bash
# Run unit tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_agents.py::test_orchestrator
```

### Integration Tests

```bash
# Test with sample data
python -m pytest tests/integration/

# End-to-end test
python tests/e2e_test.py
```

## 📚 API Reference

### DataQualityApp

```python
class DataQualityApp:
    def run_full_assessment(self, tables: List[str] = None) -> Dict[str, Any]
    def run_single_table_assessment(self, table_name: str) -> Dict[str, Any]
    def process_nlp_query(self, query: str) -> Dict[str, Any]
    def get_health_status(self) -> Dict[str, Any]
    def get_usage_stats(self) -> Dict[str, Any]
```

### LLMService

```python
class LLMService:
    def complete(self, prompt: str, **kwargs) -> str
    def parse_json_response(self, prompt: str, **kwargs) -> Dict[str, Any]
    def analyze_schema(self, table_name: str, columns: List[Dict]) -> Dict[str, Any]
    def generate_quality_insights(self, metrics_data: Dict) -> str
```

## 🛠️ Troubleshooting

### Common Issues

1. **Azure OpenAI Connection Issues**
   ```bash
   # Check configuration
   python -c "from services.llm_service import LLMService; print(LLMService().health_check())"
   ```

2. **Databricks Authentication**
   ```bash
   # Verify token
   python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().sql('SELECT 1').show()"
   ```

3. **Configuration Errors**
   ```bash
   # Validate configuration
   python -c "from main import DataQualityApp; print(DataQualityApp().get_health_status())"
   ```

### Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Run with verbose output
app = DataQualityApp()
results = app.run_full_assessment()
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built on Apache Spark and Delta Lake
- Powered by Azure OpenAI
- Designed for Databricks Unity Catalog
- Inspired by modern data quality practices

---

**Ready to transform your data quality monitoring with AI?** Get started today! 🚀