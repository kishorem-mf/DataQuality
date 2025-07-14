# 🚀 Quick Start - Token Authentication

## You're Right! Token is Often Enough

The `DATABRICKS_ACCESS_TOKEN` is indeed the primary credential needed. Here's the simplified approach:

## 🔑 Step 1: Get Your Token

1. Go to your Databricks workspace
2. Click your profile (top right) → **Settings**
3. Navigate to **Access Tokens** (or **Developer** → **Access Tokens**)
4. Click **"Generate new token"**
5. Set name: "Data Quality System"
6. Set lifetime: 90 days
7. **Copy the token** (you won't see it again!)

## 📝 Step 2: Update .env File

Replace the placeholder in your `.env` file:

```env
# Just this line is often enough!
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef...

# Optional (add if you get connection errors)
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com

# Optional (for AI features)
AZURE_OPENAI_API_KEY=your-azure-openai-key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
```

## 🧪 Step 3: Test Connection

```bash
python test_token_auth.py
```

## 🎯 Step 4: Run Data Quality Assessment

### **Inside Databricks Notebook:**
```python
# This works automatically with just the token
from main import DataQualityApp

app = DataQualityApp()
results = app.run_full_assessment()
```

### **Command Line:**
```bash
python main.py full-assessment
```

### **Natural Language Queries:**
```bash
python main.py query "What is the completeness for table orders?"
```

## 🔍 What the Token Provides

Your `DATABRICKS_ACCESS_TOKEN` gives you:

✅ **Authentication** to your Databricks workspace  
✅ **Access to Unity Catalog** (all databases and tables you have permissions for)  
✅ **Compute access** (clusters, SQL warehouses, etc.)  
✅ **Schema discovery** (automatic table and column analysis)  
✅ **Data quality metrics** (completeness, uniqueness, validity, etc.)  

## 📊 Different Environments

### **Inside Databricks (Recommended):**
- Token is automatically available
- PySpark session is pre-configured
- Unity Catalog access is seamless
- Just run the code!

### **Outside Databricks (Advanced):**
- Need to install PySpark: `pip install pyspark`
- May need hostname: `DATABRICKS_SERVER_HOSTNAME=...`
- Connection setup is more complex

## 🎉 Expected Results

Once your token is configured, you'll see:

```bash
✅ Token found: dapi123456...
✅ PySpark working: {'test': 1, 'message': 'Hello Databricks'}
✅ Database access: Found 5 databases
✅ Health check: healthy
```

## 💡 Pro Tips

1. **Start simple** - just set the token
2. **Add hostname** only if you get connection errors  
3. **The token is the key** - everything else can be auto-detected
4. **Best experience** is inside Databricks notebooks
5. **For production**, consider using Databricks Jobs (included in config)

## 🛠️ Troubleshooting

### **"Token not found" Error:**
- Check `.env` file exists
- Verify token is not placeholder value
- Ensure no extra spaces around token

### **"Permission denied" Error:**
- Verify token hasn't expired
- Check workspace permissions
- Ensure Unity Catalog access

### **"Connection refused" Error:**
- Add `DATABRICKS_SERVER_HOSTNAME` to `.env`
- Check network connectivity
- Verify workspace URL is correct

## 🎯 Summary

**You're absolutely correct!** For most use cases, just the `DATABRICKS_ACCESS_TOKEN` is sufficient. The system will:

1. **Auto-detect** your workspace environment
2. **Connect** to Unity Catalog automatically  
3. **Discover** your tables and schemas
4. **Analyze** data quality with AI insights
5. **Provide** natural language query interface

**Next step:** Just add your actual token to the `.env` file and run the system! 🚀