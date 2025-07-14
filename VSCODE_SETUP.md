# 🖥️ VS Code External Development Setup

## What You Need to Run the Data Quality System from VS Code

### 1. 🐍 Python Environment Setup

#### **Option A: Using pyenv (Recommended)**
```bash
# Install Python 3.8+ 
pyenv install 3.10.14
pyenv global 3.10.14

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

#### **Option B: Using conda**
```bash
conda create -n databricks-dq python=3.10
conda activate databricks-dq
```

### 2. 📦 Install Required Packages

```bash
# Install core dependencies
pip install -r requirements.txt

# Or install manually:
pip install pyspark>=3.4.0
pip install delta-spark>=2.4.0
pip install openai>=1.0.0
pip install langchain>=0.1.0
pip install langchain-openai>=0.1.0
pip install python-dotenv>=1.0.0
pip install pandas>=1.5.0
pip install numpy>=1.24.0
pip install databricks-sql-connector>=2.9.0
pip install azure-identity>=1.15.0
pip install tenacity>=8.2.0
pip install pydantic>=2.0.0

# For external Databricks connection
pip install databricks-connect>=13.0.0
```

### 3. 🔧 VS Code Extensions

Install these VS Code extensions:
- **Python** (Microsoft)
- **Pylance** (Microsoft) 
- **Jupyter** (Microsoft)
- **Python Environment Manager** (donjayamanne)
- **Databricks** (Databricks) - Optional but helpful

### 4. 🔑 Databricks Authentication

#### **Get Your Credentials:**

1. **Databricks Personal Access Token:**
   - Go to your Databricks workspace
   - Profile → Settings → Access Tokens
   - Generate new token
   - Copy the token

2. **Workspace Information:**
   - Your workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
   - Remove `https://` for hostname: `your-workspace.cloud.databricks.com`

3. **Optional - SQL Warehouse HTTP Path:**
   - Go to SQL Warehouses → Your warehouse → Connection details
   - Copy HTTP path (e.g., `/sql/1.0/warehouses/abc123`)

#### **Update .env File:**
```env
# Required
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com

# Optional (for SQL warehouse connections)
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id

# Optional (for AI features)
AZURE_OPENAI_API_KEY=your-azure-openai-key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
```

### 5. 🏗️ Databricks Connect Setup

#### **Configure Databricks Connect:**
```bash
# Configure connection
databricks-connect configure

# Or set environment variables
export DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
export DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...
export DATABRICKS_CLUSTER_ID=your-cluster-id  # Optional
```

#### **Test Connection:**
```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
spark.sql("SELECT 1").show()
```

### 6. 📝 VS Code Configuration

#### **Create .vscode/settings.json:**
```json
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
    }
}
```

#### **Create .vscode/launch.json:**
```json
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
        }
    ]
}
```

### 7. 🧪 Test Your Setup

#### **Step 1: Test Python Environment**
```bash
# Activate environment
source venv/bin/activate

# Test basic imports
python -c "import pyspark; print('PySpark OK')"
python -c "import databricks; print('Databricks OK')"
python -c "import openai; print('OpenAI OK')"
```

#### **Step 2: Test Databricks Connection**
```bash
python test_token_auth.py
```

#### **Step 3: Test Data Quality App**
```bash
python main.py health
```

### 8. 📁 Project Structure for VS Code

```
DataQuality/
├── .vscode/
│   ├── settings.json
│   └── launch.json
├── venv/                    # Virtual environment
├── config/
│   ├── tables_config.json
│   └── metrics_config.json
├── agents/
├── services/
├── .env                     # Your credentials
├── main.py
├── requirements.txt
└── README.md
```

### 9. 🔍 Common Issues & Solutions

#### **Issue 1: "No module named 'pyspark'"**
```bash
# Solution: Install in virtual environment
pip install pyspark>=3.4.0
```

#### **Issue 2: "Cannot connect to Databricks"**
```bash
# Solution: Check credentials
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print(os.getenv('DATABRICKS_ACCESS_TOKEN'))"
```

#### **Issue 3: "Permission denied"**
```bash
# Solution: Verify token permissions
# Go to Databricks → Profile → Settings → Access Tokens
# Check token hasn't expired
```

#### **Issue 4: "Java not found" (for PySpark)**
```bash
# Solution: Install Java
# macOS: brew install openjdk@11
# Ubuntu: sudo apt install openjdk-11-jdk
# Windows: Download from Oracle
```

### 10. 🚀 Running the System

#### **From VS Code Terminal:**
```bash
# Full assessment
python main.py full-assessment

# Single table
python main.py single-table your_catalog.schema.table

# Natural language query
python main.py query "What is the completeness for table orders?"

# Health check
python main.py health
```

#### **From VS Code Debugger:**
- Press F5 to run with debugger
- Set breakpoints in your code
- Choose "Data Quality Full Assessment" from run configurations

#### **Interactive Development:**
```python
# In VS Code Python terminal
from main import DataQualityApp

app = DataQualityApp()
health = app.get_health_status()
print(health)
```

### 11. 🎯 Recommended Development Workflow

1. **Start VS Code** in the project directory
2. **Activate virtual environment** (should happen automatically)
3. **Test connection** with `python test_token_auth.py`
4. **Run health check** with `python main.py health`
5. **Develop/test** individual components
6. **Debug** using VS Code debugger
7. **Run full assessment** when ready

### 12. 📊 Expected VS Code Experience

#### **Terminal Output:**
```bash
✅ Token found: dapi123456...
✅ PySpark working: {'test': 1, 'message': 'Hello Databricks'}
✅ Database access: Found 5 databases
✅ Health check: healthy
```

#### **IntelliSense/Autocomplete:**
- Full code completion for all classes and methods
- Type hints for better development experience
- Error detection and suggestions

#### **Debugging:**
- Set breakpoints in agent code
- Inspect variables and execution flow
- Step through AI service calls

### 13. 💡 Pro Tips

1. **Use virtual environment** - keeps dependencies isolated
2. **Configure VS Code Python interpreter** - points to your venv
3. **Use .env file** - keeps credentials secure
4. **Test incrementally** - start with connection, then health, then full assessment
5. **Use debugger** - helps understand the agent workflow
6. **Keep tokens secure** - add .env to .gitignore

---

## 🎉 You're Ready!

Once you complete these steps, you'll be able to:
- ✅ Run the full data quality system from VS Code
- ✅ Debug and develop new features
- ✅ Test individual components
- ✅ Use natural language queries
- ✅ Monitor system health

The key requirement is having your **Databricks token** and **proper Python environment** setup!