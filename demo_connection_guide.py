"""
Databricks Data Quality System - Connection Guide and Demo
This script demonstrates how to set up and use the system with actual Databricks credentials.
"""

def show_connection_setup_guide():
    """Show detailed setup guide for Databricks connection"""
    
    print("🔧 Databricks Data Quality System - Setup Guide")
    print("=" * 55)
    
    print("\n1️⃣ Get Databricks Credentials")
    print("-" * 30)
    print("Step 1: Get your Databricks workspace URL")
    print("   • Go to your Databricks workspace")
    print("   • Copy the URL (e.g., https://your-workspace.cloud.databricks.com)")
    print("   • Remove 'https://' for the hostname")
    
    print("\nStep 2: Create a Personal Access Token")
    print("   • In Databricks, click your profile > Settings")
    print("   • Go to 'Access tokens' or 'Developer' > 'Access tokens'")
    print("   • Click 'Generate new token'")
    print("   • Set a name (e.g., 'Data Quality System')")
    print("   • Set expiry (e.g., 90 days)")
    print("   • Copy the token (you won't see it again!)")
    
    print("\nStep 3: Get SQL Warehouse HTTP Path")
    print("   • Go to 'SQL Warehouses' in Databricks")
    print("   • Click on your warehouse")
    print("   • Go to 'Connection details'")
    print("   • Copy the 'HTTP path' (e.g., /sql/1.0/warehouses/abc123)")
    
    print("\n2️⃣ Update Configuration")
    print("-" * 25)
    print("Update your .env file with:")
    print()
    print("DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com")
    print("DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id")
    print("DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...")
    print()
    print("AZURE_OPENAI_API_KEY=your-azure-openai-key")
    print("AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/")
    print("AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4")
    
    print("\n3️⃣ Test Connection")
    print("-" * 18)
    print("Run the test script:")
    print("   python run_sample_query.py")
    print()
    print("Or use the main application:")
    print("   python main.py health")

def show_sample_usage_examples():
    """Show sample usage examples"""
    
    print("\n4️⃣ Sample Usage Examples")
    print("-" * 26)
    
    print("\n🔍 Command Line Interface:")
    print("   python main.py full-assessment")
    print("   python main.py single-table beliv_prd.sales.orders")
    print("   python main.py query 'What is the completeness for table orders?'")
    print("   python main.py health")
    print("   python main.py stats")
    
    print("\n🤖 Python API:")
    print("""
from main import DataQualityApp

# Initialize the app
app = DataQualityApp()

# Check system health
health = app.get_health_status()
print(f"System status: {health['status']}")

# Run full assessment
results = app.run_full_assessment()
print(f"Assessment status: {results.get('status')}")

# Ask natural language questions
response = app.process_nlp_query("What is the completeness for table sales.orders?")
print(f"Response: {response['nlp_result']['response']['message']}")
""")
    
    print("\n🗣️ Natural Language Queries:")
    queries = [
        "What is the completeness score for table sales.orders?",
        "Show me all categorical columns in inventory.stock",
        "List timestamp columns in beliv_prd.commercial.orders",
        "Which tables have data quality issues?",
        "What are the key columns in customer_data?",
        "Show me tables with poor uniqueness scores"
    ]
    
    for query in queries:
        print(f"   • \"{query}\"")

def show_configuration_examples():
    """Show configuration examples"""
    
    print("\n5️⃣ Configuration Examples")
    print("-" * 26)
    
    print("\n📋 tables_config.json:")
    print("""
{
  "tables": [
    "your_catalog.your_schema.orders",
    "your_catalog.your_schema.customers", 
    "your_catalog.your_schema.products"
  ],
  "sampling_rate": 0.01,
  "max_tables_per_run": 10,
  "ai_enabled": true,
  "parallel_processing": true
}
""")
    
    print("\n📊 metrics_config.json:")
    print("""
{
  "default_metrics": ["completeness", "uniqueness", "validity"],
  "table_specific_config": {
    "your_catalog.your_schema.orders": {
      "metrics": ["completeness", "uniqueness", "accuracy"],
      "columns": {
        "completeness": ["order_id", "customer_id", "order_date"],
        "uniqueness": ["order_id"],
        "accuracy": ["order_date", "total_amount"]
      },
      "thresholds": {
        "completeness": 0.95,
        "uniqueness": 0.99,
        "accuracy": 0.98
      }
    }
  }
}
""")

def show_expected_output():
    """Show expected output examples"""
    
    print("\n6️⃣ Expected Output Examples")
    print("-" * 28)
    
    print("\n✅ Successful Connection:")
    print("""
🔗 Testing Basic Connection...
✅ Connection successful!
✅ Found 5 databases
  - default
  - your_catalog
  - sample_data
  - information_schema
  - hive_metastore
""")
    
    print("\n📊 Sample Data Quality Results:")
    print("""
📊 Analyzing table: your_catalog.your_schema.orders
✅ Table has 1,234,567 rows
✅ Table has 12 columns

📈 Basic Data Quality Metrics:
  - order_id: 0.00% null values
    Uniqueness: 100.00%
  - customer_id: 2.34% null values
    Uniqueness: 45.67%
  - order_date: 0.12% null values
    Uniqueness: 78.90%
""")
    
    print("\n💬 Natural Language Query Response:")
    print("""
Query: "What is the completeness score for table orders?"
Response: ✅ your_catalog.your_schema.orders: 97.8% complete

The orders table shows good overall completeness with:
- order_id: 100% complete
- customer_id: 97.7% complete  
- order_date: 99.9% complete
""")
    
    print("\n🎯 AI Insights Example:")
    print("""
Overall data quality assessment: GOOD (94.5%)

Critical issues requiring attention:
- customer_id column has 2.3% null values
- Detected 15 duplicate orders in the last week

Recommendations:
1. Implement customer_id validation at data ingestion
2. Add unique constraints on order_id
3. Review duplicate detection logic in ETL pipeline

Business impact: Medium - affects customer analytics accuracy
""")

def show_troubleshooting_tips():
    """Show troubleshooting tips"""
    
    print("\n7️⃣ Troubleshooting Tips")
    print("-" * 23)
    
    print("\n❌ Common Issues:")
    
    print("\n1. Connection Errors:")
    print("   • Check your workspace URL (no https://)")
    print("   • Verify your access token is valid")
    print("   • Ensure SQL warehouse is running")
    print("   • Check network connectivity")
    
    print("\n2. Permission Errors:")
    print("   • Verify token has workspace access")
    print("   • Check Unity Catalog permissions")
    print("   • Ensure you can access the configured tables")
    
    print("\n3. Configuration Issues:")
    print("   • Validate JSON syntax in config files")
    print("   • Check table names are fully qualified")
    print("   • Verify Azure OpenAI credentials")
    
    print("\n4. Performance Issues:")
    print("   • Increase sampling rate for large tables")
    print("   • Use smaller batch sizes")
    print("   • Check cluster size and configuration")
    
    print("\n🔧 Debug Commands:")
    print("   # Test basic connection")
    print("   python run_sample_query.py")
    print()
    print("   # Check system health")
    print("   python main.py health")
    print()
    print("   # Test with single table")
    print("   python main.py single-table your_catalog.your_schema.small_table")
    print()
    print("   # Enable debug logging")
    print("   python main.py --verbose health")

def main():
    """Main function to show complete setup guide"""
    
    show_connection_setup_guide()
    show_sample_usage_examples()
    show_configuration_examples()
    show_expected_output()
    show_troubleshooting_tips()
    
    print("\n🚀 Ready to Get Started?")
    print("=" * 25)
    print("1. Update your .env file with actual Databricks credentials")
    print("2. Configure your tables in config/tables_config.json")
    print("3. Run: python run_sample_query.py")
    print("4. If successful, run: python main.py full-assessment")
    print("\n🎉 Enjoy your AI-powered data quality monitoring!")

if __name__ == "__main__":
    main()