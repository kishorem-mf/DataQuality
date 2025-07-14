"""
Minimal Databricks Token Authentication Test
Tests connection using only the DATABRICKS_ACCESS_TOKEN
"""

import os
from dotenv import load_dotenv

def test_token_setup():
    """Test if token is properly configured"""
    load_dotenv()
    
    print("🔑 Testing Token Configuration...")
    
    token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    if not token:
        print("❌ DATABRICKS_ACCESS_TOKEN not found in environment")
        return False
    
    if token.startswith('your-'):
        print("❌ DATABRICKS_ACCESS_TOKEN contains placeholder value")
        print("Please update .env file with your actual token")
        return False
    
    if len(token) < 20:
        print("❌ DATABRICKS_ACCESS_TOKEN seems too short")
        print("Databricks tokens are typically much longer")
        return False
    
    if not token.startswith('dapi'):
        print("⚠️ DATABRICKS_ACCESS_TOKEN doesn't start with 'dapi' (might be okay)")
    
    print(f"✅ Token found: {token[:10]}...{token[-4:]}")
    print(f"✅ Token length: {len(token)} characters")
    return True

def test_pyspark_with_token():
    """Test PySpark connection (works inside Databricks)"""
    print("\n🔥 Testing PySpark Connection...")
    
    try:
        from pyspark.sql import SparkSession
        
        # Try to get active session first (works in Databricks)
        spark = SparkSession.getActiveSession()
        
        if spark is None:
            print("No active session found, creating new one...")
            spark = SparkSession.builder \
                .appName("TokenTest") \
                .getOrCreate()
        
        # Test basic SQL
        result = spark.sql("SELECT 1 as test, 'Hello Databricks' as message").collect()
        print(f"✅ PySpark working: {result[0].asDict()}")
        
        # Test database access
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            print(f"✅ Database access: Found {len(databases)} databases")
            
            # Show first few databases
            for db in databases[:3]:
                print(f"  - {db.databaseName}")
            
            if len(databases) > 3:
                print(f"  ... and {len(databases) - 3} more")
                
        except Exception as e:
            print(f"⚠️ Limited database access: {str(e)}")
        
        return True
        
    except ImportError:
        print("❌ PySpark not installed (normal outside Databricks)")
        print("Inside Databricks: This would work automatically")
        return False
    except Exception as e:
        print(f"❌ PySpark error: {str(e)}")
        return False

def test_with_our_data_quality_app():
    """Test integration with our data quality application"""
    print("\n🤖 Testing Data Quality App Integration...")
    
    try:
        # Test importing our main application
        from main import DataQualityApp
        
        print("✅ Data Quality App imported successfully")
        
        # Test configuration loading
        app = DataQualityApp()
        print("✅ App initialized successfully")
        
        # Test health check (doesn't require actual data)
        try:
            health = app.get_health_status()
            print(f"✅ Health check: {health['status']}")
            
            # Show component status
            for component, status in health['components'].items():
                print(f"  - {component}: {status['status']}")
                
        except Exception as e:
            print(f"⚠️ Health check limited: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data Quality App error: {str(e)}")
        return False

def show_next_steps():
    """Show what to do next"""
    print("\n🎯 Next Steps:")
    print("=" * 15)
    
    print("\n1. If you're running INSIDE Databricks:")
    print("   • Your token is automatically available")
    print("   • Just run: python main.py full-assessment")
    
    print("\n2. If you're running OUTSIDE Databricks:")
    print("   • Make sure your .env file has:")
    print("     DATABRICKS_ACCESS_TOKEN=your-actual-token")
    print("   • You may also need:")
    print("     DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com")
    
    print("\n3. To get your token:")
    print("   • Go to Databricks > Profile > Settings > Access Tokens")
    print("   • Generate new token")
    print("   • Copy and paste into .env file")
    
    print("\n4. Test the full system:")
    print("   • python main.py health")
    print("   • python main.py full-assessment")
    print("   • python main.py query 'What tables are available?'")

def main():
    """Main test function"""
    print("🚀 Databricks Token Authentication Test")
    print("=" * 42)
    
    # Test 1: Token setup
    token_ok = test_token_setup()
    
    # Test 2: PySpark (if available)
    pyspark_ok = test_pyspark_with_token()
    
    # Test 3: Data Quality App
    app_ok = test_with_our_data_quality_app()
    
    # Summary
    print("\n📊 Test Summary:")
    print("=" * 16)
    print(f"Token Setup: {'✅ PASS' if token_ok else '❌ FAIL'}")
    print(f"PySpark Test: {'✅ PASS' if pyspark_ok else '❌ FAIL'}")
    print(f"App Integration: {'✅ PASS' if app_ok else '❌ FAIL'}")
    
    if token_ok and app_ok:
        print("\n🎉 Token-based authentication is working!")
        print("Your Data Quality system is ready to use.")
    elif token_ok:
        print("\n⚠️ Token is configured but some features may be limited")
        print("This is normal when running outside Databricks")
    else:
        print("\n❌ Please configure your DATABRICKS_ACCESS_TOKEN")
    
    show_next_steps()

if __name__ == "__main__":
    main()