#!/bin/bash

# VS Code Setup Script for Databricks Data Quality System
# Run this script to set up your local development environment

echo "🚀 Setting up VS Code for Databricks Data Quality System"
echo "========================================================="

# Check if Python is installed
echo "📋 Checking Python installation..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8+ first."
    exit 1
fi

python_version=$(python3 --version 2>&1)
echo "✅ Found: $python_version"

# Create virtual environment
echo "🔧 Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "📦 Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📦 Installing dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo "✅ Dependencies installed from requirements.txt"
else
    echo "⚠️ requirements.txt not found, installing core packages..."
    pip install pyspark>=3.4.0 delta-spark>=2.4.0 openai>=1.0.0 langchain>=0.1.0 langchain-openai>=0.1.0 python-dotenv>=1.0.0 pandas>=1.5.0 numpy>=1.24.0 databricks-sql-connector>=2.9.0 azure-identity>=1.15.0 tenacity>=8.2.0 pydantic>=2.0.0
fi

# Install Databricks Connect
echo "🔌 Installing Databricks Connect..."
pip install databricks-connect>=13.0.0

# Create VS Code configuration directory
echo "📁 Creating VS Code configuration..."
mkdir -p .vscode

# Create VS Code settings.json
cat > .vscode/settings.json << 'EOF'
{
    "python.defaultInterpreterPath": "./venv/bin/python",
    "python.envFile": "${workspaceFolder}/.env",
    "python.terminal.activateEnvironment": true,
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "jupyter.notebookFileRoot": "${workspaceFolder}",
    "files.exclude": {
        "**/__pycache__": true,
        "**/*.pyc": true
    },
    "python.analysis.autoImportCompletions": true,
    "python.analysis.typeCheckingMode": "basic"
}
EOF

# Create VS Code launch.json
cat > .vscode/launch.json << 'EOF'
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Data Quality Full Assessment",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/main.py",
            "args": ["full-assessment"],
            "console": "integratedTerminal",
            "envFile": "${workspaceFolder}/.env"
        },
        {
            "name": "Data Quality Health Check",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/main.py",
            "args": ["health"],
            "console": "integratedTerminal",
            "envFile": "${workspaceFolder}/.env"
        },
        {
            "name": "Data Quality Single Table",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/main.py",
            "args": ["single-table", "your_catalog.schema.table"],
            "console": "integratedTerminal",
            "envFile": "${workspaceFolder}/.env"
        },
        {
            "name": "Test Token Auth",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/test_token_auth.py",
            "console": "integratedTerminal",
            "envFile": "${workspaceFolder}/.env"
        }
    ]
}
EOF

echo "✅ VS Code configuration created"

# Create .gitignore if it doesn't exist
if [ ! -f ".gitignore" ]; then
    cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual Environment
venv/
env/
ENV/

# Environment variables
.env
.env.local
.env.*.local

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Logs
*.log
logs/

# Databricks
.databrickscfg
EOF
    echo "✅ .gitignore created"
fi

# Test installation
echo "🧪 Testing installation..."
python -c "import pyspark; print('✅ PySpark OK')"
python -c "import databricks; print('✅ Databricks OK')"
python -c "import openai; print('✅ OpenAI OK')"
python -c "import langchain; print('✅ LangChain OK')"

echo ""
echo "🎉 Setup Complete!"
echo "=================="
echo ""
echo "Next steps:"
echo "1. Update your .env file with Databricks credentials:"
echo "   DATABRICKS_ACCESS_TOKEN=your-token-here"
echo "   DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com"
echo ""
echo "2. Open VS Code in this directory:"
echo "   code ."
echo ""
echo "3. Test your connection:"
echo "   python test_token_auth.py"
echo ""
echo "4. Run the system:"
echo "   python main.py health"
echo ""
echo "5. For debugging, use F5 in VS Code or the Run and Debug panel"
echo ""
echo "🔧 VS Code Extensions to install:"
echo "   - Python (Microsoft)"
echo "   - Pylance (Microsoft)"
echo "   - Jupyter (Microsoft)"
echo "   - Databricks (Databricks)"
echo ""
echo "Happy coding! 🚀"