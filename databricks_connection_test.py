"""
Databricks Connection Test Script
Run this script to test connection to your Databricks workspace and execute sample queries.
"""

import os
import sys
from pyspark.sql import SparkSession
from databricks.sql import connect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_databricks_connection():
    """Test basic connection to Databricks using SQL connector"""
    
    # Get connection parameters from environment
    server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    http_path = os.getenv('DATABRICKS_HTTP_PATH')
    access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    print("🔗 Testing Databricks Connection...")
    print(f"Server: {server_hostname}")
    print(f"HTTP Path: {http_path}")
    print(f"Token: {'*' * 20 if access_token else 'Not provided'}")
    
    if not all([server_hostname, http_path, access_token]):
        print("❌ Missing required connection parameters in .env file")
        print("Please update the following in your .env file:")
        if not server_hostname:
            print("- DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com")
        if not http_path:
            print("- DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id")
        if not access_token:
            print("- DATABRICKS_ACCESS_TOKEN=your-databricks-token")
        return False
    
    try:
        # Test SQL connection
        with connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            cursor = connection.cursor()
            
            # Test basic query
            cursor.execute("SELECT 1 as test_connection")
            result = cursor.fetchall()
            print(f"✅ SQL Connection successful: {result}")
            
            # Test database access
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            print(f"✅ Found {len(databases)} databases")
            
            # Show first few databases
            for db in databases[:5]:
                print(f"  - {db[0]}")
            
            if len(databases) > 5:
                print(f"  ... and {len(databases) - 5} more")
            
            cursor.close()
            return True
            
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return False

def test_spark_connection():
    """Test Spark connection in Databricks environment"""
    
    print("\n🔗 Testing Spark Connection...")
    
    try:
        # Try to get existing Spark session (works in Databricks)
        spark = SparkSession.getActiveSession()
        
        if spark is None:
            print("No active Spark session found, creating new one...")
            spark = SparkSession.builder \
                .appName("DataQualityConnectionTest") \
                .getOrCreate()
        
        print("✅ Spark session obtained successfully")
        
        # Test basic Spark SQL
        test_df = spark.sql("SELECT 1 as test_value, 'Hello Databricks' as message")
        result = test_df.collect()
        print(f"✅ Spark SQL test: {result}")
        
        # Test catalog access
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            print(f"✅ Found {len(databases)} databases via Spark")
            
            # Show first few databases
            for db in databases[:5]:
                print(f"  - {db['databaseName']}")
            
            if len(databases) > 5:
                print(f"  ... and {len(databases) - 5} more")
                
        except Exception as e:
            print(f"⚠️ Catalog access limited: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Spark connection failed: {str(e)}")
        return False

def run_sample_data_quality_query():
    """Run a sample data quality query"""
    
    print("\n📊 Running Sample Data Quality Query...")
    
    try:
        # Try Spark first (preferred in Databricks)
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder.appName("DataQualityTest").getOrCreate()
            
            # Check if we can access the tables from config
            from main import DataQualityApp
            app = DataQualityApp()
            tables_config = app.config.get('tables_config', {})
            sample_tables = tables_config.get('tables', [])
            
            if sample_tables:
                sample_table = sample_tables[0]
                print(f"Testing with table: {sample_table}")
                
                # Try to access the table
                try:
                    df = spark.table(sample_table)
                    row_count = df.count()
                    column_count = len(df.columns)
                    
                    print(f"✅ Table {sample_table} accessed successfully")
                    print(f"  - Rows: {row_count:,}")
                    print(f"  - Columns: {column_count}")
                    
                    # Show schema
                    print("  - Schema:")
                    for field in df.schema.fields[:5]:  # Show first 5 columns
                        print(f"    * {field.name}: {field.dataType}")
                    
                    if len(df.schema.fields) > 5:
                        print(f"    ... and {len(df.schema.fields) - 5} more columns")
                    
                    # Sample data quality check
                    print("\n📈 Sample Data Quality Analysis:")
                    
                    # Check for null values in first few columns
                    for field in df.schema.fields[:3]:
                        col_name = field.name
                        null_count = df.filter(df[col_name].isNull()).count()
                        null_percentage = (null_count / row_count) * 100 if row_count > 0 else 0
                        print(f"  - {col_name}: {null_percentage:.2f}% null values")
                    
                    return True
                    
                except Exception as e:
                    print(f"❌ Cannot access table {sample_table}: {str(e)}")
                    print("This might be due to permissions or the table doesn't exist")
            
            # Fallback: Try system tables
            print("\n🔍 Trying to access system information...")
            
            try:
                # Try to get current database info
                current_db = spark.sql("SELECT current_database()").collect()[0][0]
                print(f"✅ Current database: {current_db}")
                
                # Try to list tables in current database
                tables = spark.sql(f"SHOW TABLES IN {current_db}").collect()
                print(f"✅ Found {len(tables)} tables in {current_db}")
                
                if tables:
                    print("Available tables:")
                    for table in tables[:5]:
                        print(f"  - {table['tableName']}")
                    
                    # Try to analyze first available table
                    if len(tables) > 0:
                        first_table = tables[0]['tableName']
                        try:
                            df = spark.table(f"{current_db}.{first_table}")
                            sample_count = df.count()
                            print(f"✅ Sample table {first_table} has {sample_count:,} rows")
                        except Exception as e:
                            print(f"⚠️ Cannot analyze {first_table}: {str(e)}")
                
            except Exception as e:
                print(f"⚠️ System information access limited: {str(e)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Spark query failed: {str(e)}")
            return False
            
    except Exception as e:
        print(f"❌ Sample query failed: {str(e)}")
        return False

def test_data_quality_app():
    """Test the data quality application"""
    
    print("\n🤖 Testing Data Quality App...")
    
    try:
        from main import DataQualityApp
        
        app = DataQualityApp()
        
        # Test health check
        health = app.get_health_status()
        print(f"✅ App Health Status: {health['status']}")
        
        for component, status in health['components'].items():
            print(f"  - {component}: {status['status']}")
        
        # Test configuration
        print("\n📋 Configuration Summary:")
        tables_config = app.config.get('tables_config', {})
        print(f"  - Tables configured: {len(tables_config.get('tables', []))}")
        print(f"  - AI enabled: {tables_config.get('ai_enabled', False)}")
        print(f"  - Sampling rate: {tables_config.get('sampling_rate', 'Not set')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data Quality App test failed: {str(e)}")
        return False

def main():
    """Main test function"""
    
    print("🚀 Databricks Connection and Data Quality Test")
    print("=" * 50)
    
    # Test 1: Basic connection
    sql_success = test_databricks_connection()
    
    # Test 2: Spark connection
    spark_success = test_spark_connection()
    
    # Test 3: Sample query
    query_success = run_sample_data_quality_query()
    
    # Test 4: Data Quality App
    app_success = test_data_quality_app()
    
    # Summary
    print("\n📊 Test Summary:")
    print("=" * 50)
    print(f"SQL Connection: {'✅ PASS' if sql_success else '❌ FAIL'}")
    print(f"Spark Connection: {'✅ PASS' if spark_success else '❌ FAIL'}")
    print(f"Sample Query: {'✅ PASS' if query_success else '❌ FAIL'}")
    print(f"Data Quality App: {'✅ PASS' if app_success else '❌ FAIL'}")
    
    if all([sql_success, spark_success, query_success, app_success]):
        print("\n🎉 All tests passed! Your Databricks connection is ready.")
        print("You can now run the full data quality assessment.")
    else:
        print("\n⚠️ Some tests failed. Please check the error messages above.")
        print("Common issues:")
        print("- Verify your .env file has correct Databricks credentials")
        print("- Ensure your Databricks token has necessary permissions")
        print("- Check that the configured tables exist and are accessible")

if __name__ == "__main__":
    main()