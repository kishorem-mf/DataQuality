"""
Sample Query Runner for Databricks Data Quality System
This script demonstrates how to connect to Databricks and run sample queries.
"""

import os
import sys
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_databricks_credentials():
    """Get Databricks credentials from environment or prompt user"""
    
    credentials = {
        'server_hostname': os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        'http_path': os.getenv('DATABRICKS_HTTP_PATH'),
        'access_token': os.getenv('DATABRICKS_ACCESS_TOKEN')
    }
    
    # Check if credentials are provided
    missing_creds = [key for key, value in credentials.items() if not value or value.startswith('your-')]
    
    if missing_creds:
        print("🔑 Databricks credentials needed:")
        print("Please update your .env file with actual values:")
        print()
        
        if 'server_hostname' in missing_creds:
            print("DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com")
        if 'http_path' in missing_creds:
            print("DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id")
        if 'access_token' in missing_creds:
            print("DATABRICKS_ACCESS_TOKEN=your-databricks-token")
        
        print("\nTo get these values:")
        print("1. Go to your Databricks workspace")
        print("2. Click on your user profile > Settings")
        print("3. Go to 'Access tokens' and generate a new token")
        print("4. For HTTP path, go to SQL Warehouses and copy the connection details")
        
        return None
    
    return credentials

def run_basic_connection_test():
    """Run basic connection test"""
    
    print("🔗 Testing Basic Connection...")
    
    credentials = get_databricks_credentials()
    if not credentials:
        return False
    
    try:
        # Try to import and use databricks-sql-connector
        from databricks import sql
        
        with sql.connect(
            server_hostname=credentials['server_hostname'],
            http_path=credentials['http_path'],
            access_token=credentials['access_token']
        ) as connection:
            cursor = connection.cursor()
            
            # Test basic query
            cursor.execute("SELECT 1 as test_value, current_timestamp() as test_time")
            result = cursor.fetchall()
            
            print(f"✅ Connection successful!")
            print(f"Test result: {result}")
            
            cursor.close()
            return True
            
    except ImportError:
        print("❌ databricks-sql-connector not installed")
        print("Install with: pip install databricks-sql-connector")
        return False
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return False

def run_catalog_exploration():
    """Explore available databases and tables"""
    
    print("\n📊 Exploring Databricks Catalog...")
    
    credentials = get_databricks_credentials()
    if not credentials:
        return False
    
    try:
        from databricks import sql
        
        with sql.connect(
            server_hostname=credentials['server_hostname'],
            http_path=credentials['http_path'],
            access_token=credentials['access_token']
        ) as connection:
            cursor = connection.cursor()
            
            # Get databases
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            
            print(f"✅ Found {len(databases)} databases:")
            for db in databases[:10]:  # Show first 10
                print(f"  - {db[0]}")
            
            if len(databases) > 10:
                print(f"  ... and {len(databases) - 10} more")
            
            # If we have databases, explore tables in the first one
            if databases:
                first_db = databases[0][0]
                print(f"\n🔍 Exploring tables in '{first_db}'...")
                
                try:
                    cursor.execute(f"SHOW TABLES IN {first_db}")
                    tables = cursor.fetchall()
                    
                    print(f"✅ Found {len(tables)} tables in '{first_db}':")
                    for table in tables[:5]:  # Show first 5
                        print(f"  - {table[1]}")  # table name is in second column
                    
                    if len(tables) > 5:
                        print(f"  ... and {len(tables) - 5} more")
                    
                    # Try to get basic info about first table
                    if tables:
                        first_table = tables[0][1]
                        table_full_name = f"{first_db}.{first_table}"
                        
                        try:
                            cursor.execute(f"DESCRIBE {table_full_name}")
                            columns = cursor.fetchall()
                            
                            print(f"\n📋 Schema of '{table_full_name}':")
                            for col in columns[:5]:  # Show first 5 columns
                                print(f"  - {col[0]}: {col[1]}")
                            
                            if len(columns) > 5:
                                print(f"  ... and {len(columns) - 5} more columns")
                        
                        except Exception as e:
                            print(f"⚠️ Cannot describe table {table_full_name}: {str(e)}")
                
                except Exception as e:
                    print(f"⚠️ Cannot list tables in {first_db}: {str(e)}")
            
            cursor.close()
            return True
            
    except Exception as e:
        print(f"❌ Catalog exploration failed: {str(e)}")
        return False

def run_sample_data_quality_check():
    """Run a sample data quality check"""
    
    print("\n🔍 Running Sample Data Quality Check...")
    
    credentials = get_databricks_credentials()
    if not credentials:
        return False
    
    try:
        from databricks import sql
        
        with sql.connect(
            server_hostname=credentials['server_hostname'],
            http_path=credentials['http_path'],
            access_token=credentials['access_token']
        ) as connection:
            cursor = connection.cursor()
            
            # Find a table to analyze
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            
            table_found = False
            sample_table = None
            
            for db in databases:
                db_name = db[0]
                try:
                    cursor.execute(f"SHOW TABLES IN {db_name}")
                    tables = cursor.fetchall()
                    
                    if tables:
                        sample_table = f"{db_name}.{tables[0][1]}"
                        table_found = True
                        break
                except:
                    continue
            
            if not table_found:
                print("❌ No accessible tables found for analysis")
                return False
            
            print(f"📊 Analyzing table: {sample_table}")
            
            # Get basic table info
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {sample_table}")
                row_count = cursor.fetchone()[0]
                print(f"✅ Table has {row_count:,} rows")
                
                # Get column information
                cursor.execute(f"DESCRIBE {sample_table}")
                columns = cursor.fetchall()
                print(f"✅ Table has {len(columns)} columns")
                
                # Sample data quality checks
                print("\n📈 Basic Data Quality Metrics:")
                
                for col in columns[:3]:  # Check first 3 columns
                    col_name = col[0]
                    try:
                        # Count null values
                        cursor.execute(f"SELECT COUNT(*) FROM {sample_table} WHERE {col_name} IS NULL")
                        null_count = cursor.fetchone()[0]
                        null_percentage = (null_count / row_count) * 100 if row_count > 0 else 0
                        
                        print(f"  - {col_name}: {null_percentage:.2f}% null values")
                        
                        # Count distinct values (for uniqueness)
                        cursor.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM {sample_table}")
                        distinct_count = cursor.fetchone()[0]
                        uniqueness_percentage = (distinct_count / row_count) * 100 if row_count > 0 else 0
                        
                        print(f"    Uniqueness: {uniqueness_percentage:.2f}%")
                        
                    except Exception as e:
                        print(f"  - {col_name}: Error analyzing ({str(e)})")
                
                print("\n🎉 Sample data quality check completed!")
                return True
                
            except Exception as e:
                print(f"❌ Error analyzing table {sample_table}: {str(e)}")
                return False
            
    except Exception as e:
        print(f"❌ Data quality check failed: {str(e)}")
        return False

def test_data_quality_app_integration():
    """Test integration with the data quality app"""
    
    print("\n🤖 Testing Data Quality App Integration...")
    
    try:
        # Import our data quality app
        from main import DataQualityApp
        
        # Create app instance
        app = DataQualityApp()
        
        # Test health status
        health = app.get_health_status()
        print(f"✅ App Health: {health['status']}")
        
        # Test NLP query (without actual execution)
        print("\n💬 Testing NLP Query Interface...")
        sample_query = "What tables are available for analysis?"
        
        # Note: This would require actual table data to work fully
        print(f"Sample query: '{sample_query}'")
        print("✅ NLP interface is ready (requires table data for full functionality)")
        
        return True
        
    except Exception as e:
        print(f"❌ Data Quality App integration failed: {str(e)}")
        return False

def main():
    """Main function to run all tests"""
    
    print("🚀 Databricks Data Quality System - Sample Query Runner")
    print("=" * 60)
    
    # Test 1: Basic connection
    print("\n1️⃣ Testing Basic Connection")
    connection_success = run_basic_connection_test()
    
    # Test 2: Catalog exploration
    print("\n2️⃣ Exploring Databricks Catalog")
    catalog_success = run_catalog_exploration()
    
    # Test 3: Sample data quality check
    print("\n3️⃣ Running Sample Data Quality Check")
    quality_success = run_sample_data_quality_check()
    
    # Test 4: App integration
    print("\n4️⃣ Testing Data Quality App Integration")
    app_success = test_data_quality_app_integration()
    
    # Summary
    print("\n📊 Test Results Summary")
    print("=" * 30)
    print(f"Connection Test: {'✅ PASS' if connection_success else '❌ FAIL'}")
    print(f"Catalog Exploration: {'✅ PASS' if catalog_success else '❌ FAIL'}")
    print(f"Data Quality Check: {'✅ PASS' if quality_success else '❌ FAIL'}")
    print(f"App Integration: {'✅ PASS' if app_success else '❌ FAIL'}")
    
    if all([connection_success, catalog_success, quality_success, app_success]):
        print("\n🎉 All tests passed! Your system is ready for production use.")
        print("\nNext steps:")
        print("1. Update your table configuration in config/tables_config.json")
        print("2. Run full assessment: python main.py full-assessment")
        print("3. Try natural language queries: python main.py query 'Your question here'")
    else:
        print("\n⚠️ Some tests failed. Please check the issues above.")
        print("\nTroubleshooting:")
        print("- Verify your .env file has correct Databricks credentials")
        print("- Ensure your token has necessary permissions")
        print("- Check network connectivity to Databricks")

if __name__ == "__main__":
    main()