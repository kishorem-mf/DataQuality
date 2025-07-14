"""
Check what packages are available and what needs to be installed
"""

import sys
import importlib

def check_package(package_name, description=""):
    """Check if a package is available"""
    try:
        importlib.import_module(package_name)
        print(f"✅ {package_name} - {description}")
        return True
    except ImportError:
        print(f"❌ {package_name} - {description}")
        return False

def main():
    print("📦 Checking Package Requirements for VS Code Development")
    print("=" * 58)
    
    # Core packages
    print("\n🔧 Core Dependencies:")
    core_packages = [
        ("pyspark", "Apache Spark for Python"),
        ("delta", "Delta Lake support"),
        ("openai", "OpenAI API client"),
        ("langchain", "LangChain framework"),
        ("langchain_openai", "LangChain OpenAI integration"),
        ("dotenv", "Environment variable management"),
        ("pandas", "Data manipulation"),
        ("numpy", "Numerical computing"),
        ("tenacity", "Retry logic"),
        ("pydantic", "Data validation")
    ]
    
    core_available = 0
    for package, desc in core_packages:
        if check_package(package, desc):
            core_available += 1
    
    # Databricks packages
    print("\n🔌 Databricks Connectivity:")
    databricks_packages = [
        ("databricks", "Databricks SQL connector"),
        ("databricks.connect", "Databricks Connect"),
        ("databricks.sql", "Databricks SQL client")
    ]
    
    databricks_available = 0
    for package, desc in databricks_packages:
        if check_package(package, desc):
            databricks_available += 1
    
    # Azure packages
    print("\n☁️ Azure Integration:")
    azure_packages = [
        ("azure", "Azure SDK"),
        ("azure.identity", "Azure authentication")
    ]
    
    azure_available = 0
    for package, desc in azure_packages:
        if check_package(package, desc):
            azure_available += 1
    
    # System info
    print("\n🐍 Python Environment:")
    print(f"✅ Python version: {sys.version}")
    print(f"✅ Python executable: {sys.executable}")
    
    # Summary
    print("\n📊 Summary:")
    print("=" * 10)
    print(f"Core packages: {core_available}/{len(core_packages)}")
    print(f"Databricks packages: {databricks_available}/{len(databricks_packages)}")
    print(f"Azure packages: {azure_available}/{len(azure_packages)}")
    
    total_available = core_available + databricks_available + azure_available
    total_packages = len(core_packages) + len(databricks_packages) + len(azure_packages)
    
    print(f"\nOverall: {total_available}/{total_packages} packages available")
    
    # Recommendations
    print("\n💡 Recommendations:")
    print("=" * 15)
    
    if core_available < len(core_packages):
        print("❌ Install core packages: pip install -r requirements.txt")
    
    if databricks_available == 0:
        print("❌ Install Databricks packages:")
        print("   pip install databricks-sql-connector")
        print("   pip install databricks-connect")
    
    if azure_available == 0:
        print("⚠️ Install Azure packages (optional for AI features):")
        print("   pip install azure-identity")
    
    if total_available == total_packages:
        print("🎉 All packages are available! You're ready to develop.")
    elif total_available >= len(core_packages) + 1:
        print("✅ Most packages available. You can start development.")
    else:
        print("⚠️ Several packages missing. Run setup first.")
    
    # Next steps
    print("\n🚀 Next Steps:")
    print("=" * 12)
    print("1. If packages are missing, run: ./setup_vscode.sh")
    print("2. Update .env file with your Databricks token")
    print("3. Test connection: python test_token_auth.py")
    print("4. Open VS Code: code .")
    print("5. Install VS Code extensions: Python, Pylance, Jupyter")

if __name__ == "__main__":
    main()