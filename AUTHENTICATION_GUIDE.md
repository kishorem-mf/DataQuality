# Databricks Authentication Guide

## 🔑 You're Right - Token is Often Enough!

The `DATABRICKS_ACCESS_TOKEN` is indeed the primary credential needed for most Databricks connections. Here's a breakdown of the different authentication methods:

## 🎯 Authentication Methods (Choose One)

### **Method 1: TOKEN ONLY** ⭐ (Simplest)
```env
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...
```

**When to use:**
- ✅ Running inside Databricks notebooks
- ✅ Using Databricks Connect with default settings
- ✅ Most common use case

**What it provides:**
- Authentication to your Databricks workspace
- Access to Unity Catalog
- Access to all compute resources you have permissions for

---

### **Method 2: TOKEN + HOSTNAME** (Recommended for External)
```env
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
```

**When to use:**
- ✅ Running from external environments (local laptop, CI/CD)
- ✅ Using Databricks Connect
- ✅ When you need to specify which workspace to connect to

**What it provides:**
- Everything from Method 1
- Explicit workspace targeting
- Better for external applications

---

### **Method 3: FULL CONNECTION** (For SQL Warehouses)
```env
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
```

**When to use:**
- ✅ Connecting to specific SQL Warehouses
- ✅ Using databricks-sql-connector
- ✅ When you need SQL-specific features

**What it provides:**
- Everything from Method 2
- Direct SQL Warehouse connection
- Optimized for SQL workloads

---

## 🚀 How to Get Your Token

1. **Go to your Databricks workspace**
2. **Click your profile (top right) > Settings**
3. **Navigate to:**
   - **Databricks < 13.x**: User Settings > Access Tokens
   - **Databricks >= 13.x**: Developer > Access Tokens
4. **Click "Generate new token"**
5. **Set:**
   - **Name**: "Data Quality System"
   - **Lifetime**: 90 days (or as needed)
6. **Copy the token** (you won't see it again!)

## 🔧 Testing Your Connection

### **Quick Test** (Token Only)
```python
import os
from dotenv import load_dotenv

load_dotenv()
token = os.getenv('DATABRICKS_ACCESS_TOKEN')

if token and not token.startswith('your-'):
    print("✅ Token is configured")
else:
    print("❌ Please set DATABRICKS_ACCESS_TOKEN in .env file")
```

### **Full Test** (All Methods)
```bash
python simplified_connection_test.py
```

## 🎯 For Your Data Quality System

### **Minimum Required:**
```env
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...
```

### **Recommended Setup:**
```env
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
```

### **Full Setup** (with AI features):
```env
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
AZURE_OPENAI_API_KEY=your-azure-openai-key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
```

## 🔍 Connection Types by Environment

### **Inside Databricks Notebook:**
```python
# This works automatically with just the token
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
```

### **External Python Application:**
```python
# Needs token + hostname
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder \
    .serverHostname(hostname) \
    .accessToken(token) \
    .getOrCreate()
```

### **SQL Queries Only:**
```python
# Needs token + hostname + http_path
from databricks import sql
with sql.connect(
    server_hostname=hostname,
    http_path=http_path,
    access_token=token
) as connection:
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
```

## 🛡️ Security Best Practices

1. **Never commit tokens to git**
2. **Use environment variables (.env file)**
3. **Set appropriate token expiration**
4. **Rotate tokens regularly**
5. **Use minimal required permissions**

## 🚨 Common Issues

### **"Token not found" Error**
```bash
# Check if token is set
echo $DATABRICKS_ACCESS_TOKEN

# Or in Python
import os
print(os.getenv('DATABRICKS_ACCESS_TOKEN'))
```

### **"Permission denied" Error**
- Verify token hasn't expired
- Check workspace permissions
- Ensure Unity Catalog access

### **"Connection refused" Error**
- Check workspace hostname
- Verify network connectivity
- Ensure SQL warehouse is running (if using Method 3)

## 🎉 Quick Start

1. **Get your token** (steps above)
2. **Update .env file:**
   ```env
   DATABRICKS_ACCESS_TOKEN=your-actual-token-here
   ```
3. **Test connection:**
   ```bash
   python simplified_connection_test.py
   ```
4. **Run data quality assessment:**
   ```bash
   python main.py full-assessment
   ```

## 💡 Pro Tips

- **Start with Method 1** (token only) - it works in most cases
- **Add hostname** if you get connection errors
- **Add HTTP path** only if you specifically need SQL Warehouse features
- **The token is the most important credential** - everything else can often be auto-detected

---

**You're absolutely right - in most cases, just the `DATABRICKS_ACCESS_TOKEN` is sufficient!** 🎯