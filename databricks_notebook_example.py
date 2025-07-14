# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Agent System - Example Usage
# MAGIC 
# MAGIC This notebook demonstrates how to use the AI-powered Data Quality Agent system in Databricks.
# MAGIC 
# MAGIC ## Features
# MAGIC - **Agent-based Architecture**: Intelligent agents that adapt to your data
# MAGIC - **Azure OpenAI Integration**: AI-powered insights and recommendations
# MAGIC - **Natural Language Queries**: Ask questions about your data quality
# MAGIC - **Delta Lake Storage**: Persistent storage of quality metrics
# MAGIC - **Automated Scheduling**: Daily quality assessments

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages (if not already installed)
# %pip install openai langchain langchain-openai python-dotenv tenacity pydantic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Import the main application
from main import DataQualityApp, run_quality_assessment, ask_quality_question

# Initialize the application
app = DataQualityApp(config_dir="config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Health Check

# COMMAND ----------

# Check system health
health_status = app.get_health_status()
print("System Health Status:")
print(f"Overall Status: {health_status['status']}")
print(f"Timestamp: {health_status['timestamp']}")

for component, status in health_status['components'].items():
    print(f"- {component}: {status['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Data Quality Assessment

# COMMAND ----------

# Run full assessment for all configured tables
print("Running full data quality assessment...")
full_results = app.run_full_assessment()

print(f"Assessment Status: {full_results.get('status', 'Unknown')}")
print(f"Execution Time: {full_results.get('execution_time', 0):.2f} seconds")

# Display summary
if 'summary' in full_results:
    print("\nSummary:")
    print(full_results['summary'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single Table Assessment

# COMMAND ----------

# Assess a specific table
table_name = "beliv_prd.catalogs.tipo_cambio"
print(f"Assessing table: {table_name}")

single_result = app.run_single_table_assessment(table_name)
print(f"Assessment completed with status: {single_result.get('status', 'Unknown')}")

# Display results
if 'metrics_results' in single_result:
    metrics = single_result['metrics_results'].get(table_name, {})
    print(f"\nMetrics for {table_name}:")
    for metric_type, metric_data in metrics.get('metrics', {}).items():
        print(f"  {metric_type}: {metric_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Natural Language Queries

# COMMAND ----------

# Ask questions about data quality using natural language
queries = [
    "What is the completeness score for table beliv_prd.catalogs.tipo_cambio?",
    "Show me all categorical columns in beliv_prd.sales.orders",
    "List timestamp columns in beliv_prd.inventory.stock",
    "What are the key columns in beliv_prd.commercial.facturacion_explosionada?"
]

for query in queries:
    print(f"\nQuery: {query}")
    response = app.process_nlp_query(query)
    
    if 'nlp_result' in response:
        nlp_result = response['nlp_result']
        print(f"Response: {nlp_result.get('response', {}).get('message', 'No response')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Usage Statistics

# COMMAND ----------

# Get usage statistics
stats = app.get_usage_stats()
print("Usage Statistics:")
print(f"LLM Requests: {stats.get('llm_usage', {}).get('total_requests', 0)}")
print(f"Active Jobs: {stats.get('active_jobs', 0)}")
print(f"Timestamp: {stats.get('timestamp', 'Unknown')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convenience Functions

# COMMAND ----------

# Use convenience functions for quick assessments
print("Using convenience functions:")

# Quick assessment
quick_results = run_quality_assessment(
    tables=["beliv_prd.catalogs.tipo_cambio"], 
    config_dir="config"
)

print(f"Quick assessment status: {quick_results.get('status', 'Unknown')}")

# COMMAND ----------

# Ask a question using convenience function
question = "What is the data quality summary for all tables?"
answer = ask_quality_question(question, config_dir="config")

print(f"Question: {question}")
print(f"Answer: {answer.get('nlp_result', {}).get('response', {}).get('message', 'No answer')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Usage

# COMMAND ----------

# Get detailed metrics for analysis
from agents.results_writer import ResultsWriterAgent

# Initialize results writer
results_writer = ResultsWriterAgent(
    config=app.config.get('agent_config', {}).get('results_writer', {}),
    llm_service=app.llm_service
)

# Query stored results
stored_results = results_writer.query_results(
    filters={
        'table_name': 'beliv_prd.catalogs.tipo_cambio',
        'status': 'PASS'
    }
)

if stored_results.get('status') == 'success':
    print(f"Found {stored_results.get('record_count', 0)} records")
    
    # Display sample results
    sample_results = stored_results.get('sample_results', [])
    if sample_results:
        print("\nSample Results:")
        for result in sample_results[:3]:  # Show first 3
            print(f"- {result['metric_name']}: {result['metric_value']} ({result['status']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Summary

# COMMAND ----------

# Get summary of all results
table_summary = results_writer.get_table_summary()

if table_summary.get('status') == 'success':
    print(f"Total records in results table: {table_summary.get('total_records', 0)}")
    
    # Show summary by table and status
    summary_data = table_summary.get('summary_by_table_and_status', [])
    if summary_data:
        print("\nSummary by Table and Status:")
        for item in summary_data:
            print(f"- {item['table_name']}: {item['status']} ({item['count']} records)")
    
    # Show latest run info
    latest_run = table_summary.get('latest_run')
    if latest_run:
        print(f"\nLatest Run: {latest_run['run_id']} at {latest_run['run_timestamp']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduling and Automation
# MAGIC 
# MAGIC This system can be scheduled to run daily using Databricks Jobs:
# MAGIC 
# MAGIC 1. **Daily Assessment**: Automatically assess all configured tables
# MAGIC 2. **Alert Generation**: Send notifications for quality issues
# MAGIC 3. **Trend Analysis**: Track quality metrics over time
# MAGIC 4. **AI Insights**: Get intelligent recommendations for improvement
# MAGIC 
# MAGIC See `databricks_job_config.json` for the complete job configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Commands for Different Use Cases

# COMMAND ----------

# 1. Monitor specific tables with custom thresholds
custom_assessment = app.run_full_assessment(
    tables=["beliv_prd.sales.orders", "beliv_prd.inventory.stock"]
)

# 2. Get completeness metrics for all tables
completeness_query = app.process_nlp_query(
    "Show me the completeness scores for all tables"
)

# 3. Find tables with data quality issues
issues_query = app.process_nlp_query(
    "Which tables have data quality issues?"
)

# 4. Get recommendations for improvement
recommendations_query = app.process_nlp_query(
    "What are the top recommendations for improving data quality?"
)

print("Custom assessment completed")
print("Queries processed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC 
# MAGIC The Data Quality Agent system provides:
# MAGIC 
# MAGIC - **Automated Assessment**: Daily quality checks without manual intervention
# MAGIC - **AI-Powered Insights**: Intelligent analysis and recommendations
# MAGIC - **Natural Language Interface**: Easy querying for business users
# MAGIC - **Scalable Architecture**: Agent-based design for large-scale data
# MAGIC - **Delta Lake Integration**: Persistent storage and historical tracking
# MAGIC 
# MAGIC The system is designed to be plug-and-play for any Databricks workspace with Unity Catalog.