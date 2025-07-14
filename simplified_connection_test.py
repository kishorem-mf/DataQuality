"""
Simplified Databricks Connection Test - Token-Based Authentication
This script demonstrates different ways to connect to Databricks using just the access token.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_token_only_connection():
    """Test connection using only Databricks access token"""
    
    print("🔑 Testing Token-Only Connection...")
    
    access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    if not access_token or access_token.startswith('your-'):
        print("❌ DATABRICKS_ACCESS_TOKEN is required")
        print("Please set it in your .env file:")
        print("DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...")
        print("\nTo get your token:")
        print("1. Go to your Databricks workspace")
        print("2. Click your profile > Settings > Access tokens")
        print("3. Generate new token")
        return False
    
    print(f"✅ Token found: {access_token[:10]}...")
    return True

def test_with_pyspark_session():
    """Test using PySpark session (works in Databricks runtime)"""
    
    print("\n🔥 Testing with PySpark Session...")
    
    try:
        from pyspark.sql import SparkSession
        
        # This works when running inside Databricks
        spark = SparkSession.getActiveSession()
        
        if spark is None:
            print("⚠️ No active Spark session (this is normal outside Databricks)")
            print("Creating new Spark session...")
            spark = SparkSession.builder \
                .appName("DataQualityTest") \
                .getOrCreate()
        
        # Test basic functionality
        test_df = spark.sql("SELECT 1 as test_value")
        result = test_df.collect()
        print(f"✅ Spark session working: {result}")
        
        # Test catalog access
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            print(f"✅ Found {len(databases)} databases")
            for db in databases[:3]:
                print(f"  - {db['databaseName']}")
        except Exception as e:
            print(f"⚠️ Limited catalog access: {str(e)}")
        
        return True
        
    except ImportError:
        print("❌ PySpark not available (install with: pip install pyspark)")
        return False
    except Exception as e:
        print(f"❌ PySpark session failed: {str(e)}")
        return False

def test_databricks_connect():
    """Test using Databricks Connect (token-based)"""
    
    print("\n🔌 Testing Databricks Connect...")
    
    access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    
    if not access_token:
        print("❌ DATABRICKS_ACCESS_TOKEN required")
        return False
    
    if not server_hostname:
        print("❌ DATABRICKS_SERVER_HOSTNAME required for Databricks Connect")
        print("Set it in .env: DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com")
        return False
    
    try:
        # For Databricks Connect, we can use the token directly
        from databricks.connect import DatabricksSession
        
        spark = DatabricksSession.builder \
            .serverHostname(server_hostname) \
            .accessToken(access_token) \
            .getOrCreate()
        
        # Test basic query
        test_df = spark.sql("SELECT 1 as test_value")
        result = test_df.collect()
        print(f"✅ Databricks Connect working: {result}")
        
        return True
        
    except ImportError:
        print("❌ Databricks Connect not available")
        print("Install with: pip install databricks-connect")
        return False
    except Exception as e:
        print(f"❌ Databricks Connect failed: {str(e)}")
        return False

def test_sql_connector_minimal():
    """Test SQL connector with minimal configuration"""
    
    print("\n📊 Testing SQL Connector (Minimal)...")
    
    access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    if not access_token:
        print("❌ DATABRICKS_ACCESS_TOKEN required")
        return False
    
    # Try to auto-detect server hostname from token if not provided
    server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    
    if not server_hostname:
        print("⚠️ DATABRICKS_SERVER_HOSTNAME not set")
        print("Attempting to use token-based discovery...")
        
        # In a real scenario, you could parse the token or use API to discover
        # For now, we'll prompt the user
        print("Please set DATABRICKS_SERVER_HOSTNAME in .env file")
        return False
    
    try:
        from databricks import sql
        
        # Try different connection methods
        connection_configs = [
            {
                'server_hostname': server_hostname,
                'access_token': access_token,
                'http_path': os.getenv('DATABRICKS_HTTP_PATH', '/sql/1.0/warehouses/default')
            },
            {
                'server_hostname': server_hostname,
                'access_token': access_token,
                # Try without http_path
            }
        ]
        
        for i, config in enumerate(connection_configs):
            try:
                print(f"  Trying connection method {i+1}...")
                with sql.connect(**config) as connection:
                    cursor = connection.cursor()
                    cursor.execute("SELECT 1 as test_value")
                    result = cursor.fetchall()
                    print(f"✅ SQL Connector working: {result}")
                    cursor.close()
                    return True
            except Exception as e:
                print(f"  Method {i+1} failed: {str(e)}")
                continue
        
        return False
        
    except ImportError:
        print("❌ databricks-sql-connector not available")
        print("Install with: pip install databricks-sql-connector")
        return False
    except Exception as e:
        print(f"❌ SQL Connector failed: {str(e)}")
        return False

def show_minimal_env_setup():
    """Show minimal .env setup"""
    
    print("\n📝 Minimal .env Setup")
    print("=" * 25)
    print("For most use cases, you only need:")
    print()
    print("# Required")
    print("DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...")
    print()
    print("# Optional (but recommended)")
    print("DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com")
    print("DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id")
    print()
    print("# For Azure OpenAI features")
    print("AZURE_OPENAI_API_KEY=your-azure-openai-key")
    print("AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/")
    print()
    print("Authentication Methods (in order of preference):")
    print("1. 🔥 PySpark Session (inside Databricks) - token automatic")
    print("2. 🔌 Databricks Connect - token + hostname")
    print("3. 📊 SQL Connector - token + hostname + http_path")

def test_current_environment():
    """Test what's possible with current environment"""
    
    print("\n🔍 Testing Current Environment...")
    
    # Check what's available
    access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    http_path = os.getenv('DATABRICKS_HTTP_PATH')
    
    print(f"Access Token: {'✅ Set' if access_token and not access_token.startswith('your-') else '❌ Missing'}")
    print(f"Server Hostname: {'✅ Set' if server_hostname and not server_hostname.startswith('your-') else '❌ Missing'}")
    print(f"HTTP Path: {'✅ Set' if http_path and not http_path.startswith('/sql/1.0/warehouses/your-') else '❌ Missing'}")
    
    # Check available packages
    packages = ['pyspark', 'databricks.sql', 'databricks.connect']
    available_packages = []
    
    for package in packages:
        try:
            __import__(package)
            available_packages.append(package)
            print(f"Package {package}: ✅ Available")
        except ImportError:
            print(f"Package {package}: ❌ Not installed")
    
    print(f"\nRecommended approach based on your setup:")
    
    if access_token and not access_token.startswith('your-'):
        if 'pyspark' in available_packages:
            print("✅ Use PySpark session (best for Databricks notebooks)")
        elif 'databricks.connect' in available_packages and server_hostname:
            print("✅ Use Databricks Connect (good for local development)")
        elif 'databricks.sql' in available_packages and server_hostname:
            print("✅ Use SQL Connector (good for SQL queries)")
        else:
            print("⚠️ Install required packages")
    else:
        print("❌ Set DATABRICKS_ACCESS_TOKEN first")

def main():
    """Main function to test all connection methods"""
    
    print("🚀 Databricks Connection Test - Token-Based Authentication")
    print("=" * 60)
    
    # Test 1: Check token
    token_ok = test_token_only_connection()
    
    # Test 2: Current environment
    test_current_environment()
    
    # Test 3: PySpark session
    pyspark_ok = test_with_pyspark_session()
    
    # Test 4: Databricks Connect
    connect_ok = test_databricks_connect()
    
    # Test 5: SQL Connector
    sql_ok = test_sql_connector_minimal()
    
    # Show minimal setup
    show_minimal_env_setup()
    
    # Summary
    print("\n📊 Connection Test Results")
    print("=" * 30)
    print(f"Token Available: {'✅ PASS' if token_ok else '❌ FAIL'}")
    print(f"PySpark Session: {'✅ PASS' if pyspark_ok else '❌ FAIL'}")
    print(f"Databricks Connect: {'✅ PASS' if connect_ok else '❌ FAIL'}")
    print(f"SQL Connector: {'✅ PASS' if sql_ok else '❌ FAIL'}")
    
    success_count = sum([token_ok, pyspark_ok, connect_ok, sql_ok])
    
    if success_count > 0:
        print(f"\n🎉 {success_count} connection method(s) working!")
        print("You can proceed with the data quality assessment.")
    else:
        print("\n⚠️ No connection methods working.")
        print("Please check your token and try again.")

if __name__ == "__main__":
    main()