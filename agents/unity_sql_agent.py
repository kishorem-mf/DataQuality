"""
Unity Catalog SQL Agent
Simple, clean architecture for natural language to SQL conversion using Databricks Unity Catalog.
Manages catalogs, schemas, and tables from configuration files.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from dotenv import load_dotenv

try:
    from databricks import sql as databricks_sql
    DATABRICKS_SQL_AVAILABLE = True
except ImportError:
    DATABRICKS_SQL_AVAILABLE = False

from .base_agent import AIEnabledAgent
from services.llm_service import LLMService


class UnityCatalogManager:
    """Manages Unity Catalog metadata from configuration"""
    
    def __init__(self):
        load_dotenv()
        self.config = self._load_catalog_config()
        self.catalogs = self.config.get('catalogs', [])
        self.schemas = self.config.get('schemas', [])
        self.tables = self.config.get('tables', [])
    
    def _load_catalog_config(self) -> Dict[str, Any]:
        """Load catalog configuration from file"""
        try:
            config_path = os.path.join('config', 'unity_catalog_config.json')
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return json.load(f)
            else:
                # Fallback: load from existing tables_config.json
                tables_config_path = os.path.join('config', 'tables_config.json')
                if os.path.exists(tables_config_path):
                    with open(tables_config_path, 'r') as f:
                        tables_config = json.load(f)
                        return self._extract_catalog_info(tables_config.get('tables', []))
                return {'catalogs': [], 'schemas': [], 'tables': []}
        except Exception as e:
            logging.warning(f"Could not load catalog config: {str(e)}")
            return {'catalogs': [], 'schemas': [], 'tables': []}
    
    def _extract_catalog_info(self, tables: List[str]) -> Dict[str, Any]:
        """Extract catalog, schema, table info from table list"""
        catalogs = set()
        schemas = set()
        
        for table in tables:
            parts = table.split('.')
            if len(parts) == 3:  # catalog.schema.table
                catalogs.add(parts[0])
                schemas.add(f"{parts[0]}.{parts[1]}")
            elif len(parts) == 2:  # schema.table
                schemas.add(parts[0])
        
        return {
            'catalogs': list(catalogs),
            'schemas': list(schemas),
            'tables': tables
        }
    
    def get_catalogs(self) -> List[str]:
        """Get available catalogs"""
        return self.catalogs
    
    def get_schemas(self, catalog: str = None) -> List[str]:
        """Get schemas, optionally filtered by catalog"""
        if catalog:
            return [s for s in self.schemas if s.startswith(f"{catalog}.")]
        return self.schemas
    
    def get_tables(self, schema: str = None) -> List[str]:
        """Get tables, optionally filtered by schema"""
        if schema:
            return [t for t in self.tables if t.startswith(f"{schema}.")]
        return self.tables
    
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get metadata for a specific table using DESCRIBE"""
        try:
            if not DATABRICKS_SQL_AVAILABLE:
                return {'error': 'Databricks SQL connector not available'}
            
            connection = databricks_sql.connect(
                server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
                http_path=os.getenv('DATABRICKS_HTTP_PATH'),
                access_token=os.getenv('DATABRICKS_ACCESS_TOKEN')
            )
            
            cursor = connection.cursor()
            cursor.execute(f"DESCRIBE {table_name}")
            results = cursor.fetchall()
            
            columns = []
            for row in results:
                if row[0] and not row[0].startswith('#'):  # Skip partition info
                    columns.append({
                        'name': row[0],
                        'data_type': row[1],
                        'comment': row[2] if len(row) > 2 else ''
                    })
            
            cursor.close()
            connection.close()
            
            return {
                'table_name': table_name,
                'columns': columns,
                'column_count': len(columns),
                'fetched_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {'error': str(e), 'table_name': table_name}


class SQLSafetyValidator:
    """Validates SQL queries for safety"""
    
    def __init__(self):
        self.forbidden_operations = [
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 
            'UPDATE', 'GRANT', 'REVOKE'
        ]
    
    def is_safe(self, sql: str) -> Dict[str, Any]:
        """Check if SQL query is safe to execute"""
        sql_upper = sql.upper().strip()
        
        # Check for forbidden operations
        for op in self.forbidden_operations:
            if f' {op} ' in f' {sql_upper} ' or sql_upper.startswith(f'{op} '):
                return {
                    'safe': False,
                    'reason': f'Forbidden operation: {op}',
                    'severity': 'high'
                }
        
        # Ensure LIMIT for SELECT queries
        if 'SELECT' in sql_upper and 'LIMIT' not in sql_upper:
            if not any(agg in sql_upper for agg in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(', 'GROUP BY']):
                return {
                    'safe': False,
                    'reason': 'Missing LIMIT clause',
                    'severity': 'low',
                    'suggestion': 'Add LIMIT clause to prevent large result sets'
                }
        
        return {'safe': True, 'reason': 'Query is safe'}


class DatabricksExecutor:
    """Executes SQL queries on Databricks"""
    
    def __init__(self):
        load_dotenv()
        self.connection_params = {
            'server_hostname': os.getenv('DATABRICKS_SERVER_HOSTNAME'),
            'http_path': os.getenv('DATABRICKS_HTTP_PATH'),
            'access_token': os.getenv('DATABRICKS_ACCESS_TOKEN')
        }
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query and return results"""
        if not DATABRICKS_SQL_AVAILABLE:
            return {
                'success': False,
                'error': 'Databricks SQL connector not available'
            }
        
        if not all(self.connection_params.values()):
            return {
                'success': False,
                'error': 'Missing Databricks connection parameters'
            }
        
        try:
            connection = databricks_sql.connect(**self.connection_params)
            cursor = connection.cursor()
            
            start_time = datetime.now()
            cursor.execute(sql)
            results = cursor.fetchall()
            execution_time = (datetime.now() - start_time).total_seconds()
            
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            cursor.close()
            connection.close()
            
            return {
                'success': True,
                'results': results,
                'column_names': column_names,
                'row_count': len(results),
                'execution_time': execution_time,
                'sql': sql
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'sql': sql
            }
    
    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            result = self.execute("SELECT 1 as test")
            return result.get('success', False)
        except:
            return False


class UnitySQLAgent(AIEnabledAgent):
    """
    Unity Catalog SQL Agent
    Simple architecture for natural language to SQL conversion using Unity Catalog metadata.
    """
    
    def __init__(self, config: Dict[str, Any], llm_service: LLMService):
        super().__init__("UnitySQLAgent", config, llm_service)
        
        self.catalog_manager = UnityCatalogManager()
        self.safety_validator = SQLSafetyValidator()
        self.executor = DatabricksExecutor()
        self.max_rows = config.get('max_rows', 100)
    
    def _validate_inputs(self, **kwargs) -> None:
        """Validate input parameters"""
        if 'query' not in kwargs:
            raise ValueError("Natural language query is required")
        
        if not isinstance(kwargs['query'], str) or not kwargs['query'].strip():
            raise ValueError("Query must be a non-empty string")
    
    def _execute_logic(self, **kwargs) -> Dict[str, Any]:
        """Main execution logic"""
        query = kwargs['query'].strip()
        self.logger.info(f"Processing query: {query}")
        
        try:
            # Step 1: Generate SQL from natural language
            sql_result = self._generate_sql(query)
            if 'error' in sql_result:
                return self._format_error_response(sql_result['error'], query)
            
            # Step 2: Validate SQL safety
            safety_check = self.safety_validator.is_safe(sql_result['sql'])
            if not safety_check['safe']:
                if safety_check['severity'] == 'low':
                    # Auto-fix missing LIMIT
                    sql_result['sql'] = self._add_limit_clause(sql_result['sql'])
                    sql_result['auto_fixed'] = True
                else:
                    return self._format_error_response(
                        f"Unsafe query: {safety_check['reason']}", query
                    )
            
            # Step 3: Execute SQL
            execution_result = self.executor.execute(sql_result['sql'])
            
            # Step 4: Format response
            if execution_result['success']:
                return self._format_success_response(execution_result, sql_result, query)
            else:
                return self._format_error_response(execution_result['error'], query, sql_result['sql'])
                
        except Exception as e:
            self.logger.error(f"Query processing failed: {str(e)}")
            return self._format_error_response(str(e), query)
    
    def _generate_sql(self, query: str) -> Dict[str, Any]:
        """Generate SQL from natural language query"""
        try:
            # Build context from Unity Catalog
            context = self._build_catalog_context(query)
            
            # Validate that we have schema information if querying specific tables
            validation_result = self._validate_schema_availability(query, context)
            if not validation_result['valid']:
                # Try fallback mechanism: automatically get schema for relevant tables
                fallback_result = self._try_schema_fallback(query)
                if fallback_result['success']:
                    # Update context with fallback schema information
                    context = fallback_result['context']
                    self.logger.info("Successfully applied schema fallback mechanism")
                else:
                    return {
                        'error': validation_result['error'],
                        'suggestion': validation_result.get('suggestion', 'Try using DESCRIBE table_name to get schema information first'),
                        'fallback_attempted': True,
                        'fallback_error': fallback_result.get('error', 'Unknown fallback error')
                    }
            
            # Create prompt
            prompt = f"""
You are a SQL expert working with Databricks Unity Catalog.

User Query: "{query}"

Available Catalogs: {', '.join(self.catalog_manager.get_catalogs())}
Available Schemas: {', '.join(self.catalog_manager.get_schemas())}
Available Tables: {', '.join(self.catalog_manager.get_tables())}

{context}

Generate safe SQL following these rules:
1. Use Databricks SQL (Spark SQL) syntax
2. Use fully qualified table names (catalog.schema.table)
3. Add LIMIT {self.max_rows} for SELECT queries (unless aggregating)
4. ONLY use tables from the available tables list above
5. ONLY use column names that are explicitly provided in the table schema above
6. DO NOT guess, assume, or invent column names that are not listed
7. If no schema is available for a table, use DESCRIBE table_name first
8. No destructive operations (DROP, DELETE, ALTER, etc.)

CRITICAL: Use ONLY the exact column names provided in the schema information above. Do not make up column names.

Return JSON format:
{{
  "sql": "your SQL query here",
  "explanation": "what this query does",
  "confidence": 0.95
}}
"""
            
            response = self.llm_service.parse_json_response(prompt)
            
            # Additional validation of generated SQL
            if 'sql' in response:
                sql_validation = self._validate_generated_sql(response['sql'], context)
                if not sql_validation['valid']:
                    return {
                        'error': f"Generated SQL validation failed: {sql_validation['error']}",
                        'sql': response['sql'],
                        'suggestion': sql_validation.get('suggestion', 'Review the query and try again')
                    }
            
            return response
            
        except Exception as e:
            return {'error': f"Failed to generate SQL: {str(e)}"}
    
    def _build_catalog_context(self, query: str) -> str:
        """Build relevant context from Unity Catalog based on query"""
        query_lower = query.lower()
        
        # Check if asking for metadata
        if any(phrase in query_lower for phrase in ['show tables', 'list tables', 'tables available']):
            return "User wants to see available tables. Use SHOW TABLES or similar commands."
        
        # Find relevant tables based on query keywords
        relevant_tables = []
        for table in self.catalog_manager.get_tables():
            table_parts = table.split('.')
            table_name = table_parts[-1] if table_parts else table
            
            if any(word in query_lower for word in table_name.lower().split('_')):
                relevant_tables.append(table)
        
        if relevant_tables:
            context_parts = []
            
            # Get schema for all relevant tables, up to 3 to avoid token limit
            for table in relevant_tables[:3]:
                self.logger.info(f"Fetching metadata for table: {table}")
                try:
                    metadata = self.catalog_manager.get_table_metadata(table)
                    
                    if 'error' in metadata:
                        self.logger.warning(f"Failed to fetch metadata for {table}: {metadata['error']}")
                        # Still include table name so LLM knows it exists
                        context_parts.append(f"Table: {table} (schema unavailable)")
                        continue
                    
                    columns = metadata.get('columns', [])
                    if not columns:
                        self.logger.warning(f"No columns found for table {table}")
                        context_parts.append(f"Table: {table} (no columns found)")
                        continue
                    
                    # Build detailed column information
                    columns_info = []
                    for col in columns:
                        col_name = col.get('name', 'unknown')
                        col_type = col.get('data_type', 'unknown')
                        comment = col.get('comment', '')
                        if comment:
                            columns_info.append(f"{col_name} ({col_type}) - {comment}")
                        else:
                            columns_info.append(f"{col_name} ({col_type})")
                    
                    table_context = f"Table: {table}\nColumns: {', '.join(columns_info)}"
                    context_parts.append(table_context)
                    
                    self.logger.info(f"Successfully fetched {len(columns)} columns for {table}")
                    
                except Exception as e:
                    self.logger.error(f"Exception fetching metadata for {table}: {str(e)}")
                    context_parts.append(f"Table: {table} (metadata fetch failed: {str(e)})")
            
            if context_parts:
                return "\n\n".join(context_parts)
            else:
                return f"Tables found: {', '.join(relevant_tables)} but no schema metadata available. Use only these exact table names."
        
        return "Use only the available tables from the list above. Do not assume column names."
    
    def _validate_schema_availability(self, query: str, context: str) -> Dict[str, Any]:
        """Validate that schema information is available for data queries"""
        query_lower = query.lower()
        
        # Skip validation for metadata queries
        if any(phrase in query_lower for phrase in ['show tables', 'list tables', 'describe']):
            return {'valid': True}
        
        # Check if this looks like a data query that needs column information
        data_query_indicators = ['select', 'completeness', 'metric', 'count', 'sum', 'avg', 'where', 'group by']
        is_data_query = any(indicator in query_lower for indicator in data_query_indicators)
        
        if is_data_query:
            # Check if we have schema information in context
            if 'schema unavailable' in context or 'no columns found' in context or 'metadata fetch failed' in context:
                return {
                    'valid': False,
                    'error': 'Schema information unavailable for data query',
                    'suggestion': 'Please run DESCRIBE on the table first to get schema information'
                }
            
            # Check if context has actual column information
            if 'Columns:' not in context and 'Use only the available tables' in context:
                return {
                    'valid': False,
                    'error': 'No column schema available for the requested tables',
                    'suggestion': 'Use DESCRIBE table_name to get column information first'
                }
        
        return {'valid': True}
    
    def _validate_generated_sql(self, sql: str, context: str) -> Dict[str, Any]:
        """Validate generated SQL against available schema"""
        try:
            import re
            
            # Extract column names from the SQL
            sql_upper = sql.upper()
            
            # Simple regex to find column references (this is basic validation)
            # Look for SELECT statements and extract potential column names
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_upper, re.DOTALL)
            if not select_match:
                return {'valid': True}  # No SELECT found, skip validation
            
            select_clause = select_match.group(1)
            
            # Skip validation for aggregate functions and wildcards
            if any(keyword in select_clause for keyword in ['*', 'COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                return {'valid': True}
            
            # Extract available columns from context
            available_columns = []
            if 'Columns:' in context:
                # Parse columns from context
                lines = context.split('\n')
                for line in lines:
                    if line.startswith('Columns:'):
                        # Extract column names from the format "col_name (type)"
                        columns_text = line.replace('Columns:', '').strip()
                        column_matches = re.findall(r'(\w+)\s*\([^)]+\)', columns_text)
                        available_columns.extend(column_matches)
            
            if not available_columns:
                return {'valid': True}  # No schema available, can't validate
            
            # Basic validation - check if SQL contains column names not in schema
            # This is a simplified check and could be enhanced
            potential_columns = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', select_clause)
            
            # Filter out SQL keywords and common functions
            sql_keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'AS', 'NULL', 'IS', 'NOT'}
            potential_columns = [col for col in potential_columns if col.upper() not in sql_keywords]
            
            # Check for columns that might not exist
            unknown_columns = []
            for col in potential_columns:
                if col.lower() not in [ac.lower() for ac in available_columns]:
                    unknown_columns.append(col)
            
            if unknown_columns:
                return {
                    'valid': False,
                    'error': f"Unknown columns detected: {', '.join(unknown_columns)}",
                    'suggestion': f"Use only these available columns: {', '.join(available_columns)}"
                }
            
            return {'valid': True}
            
        except Exception as e:
            # If validation fails, allow the SQL to proceed
            self.logger.warning(f"SQL validation failed: {str(e)}")
            return {'valid': True}
    
    def _try_schema_fallback(self, query: str) -> Dict[str, Any]:
        """Try to get schema information using fallback mechanism"""
        try:
            query_lower = query.lower()
            
            # Find tables mentioned in query
            fallback_tables = []
            for table in self.catalog_manager.get_tables():
                table_parts = table.split('.')
                table_name = table_parts[-1] if table_parts else table
                
                if any(word in query_lower for word in table_name.lower().split('_')):
                    fallback_tables.append(table)
            
            if not fallback_tables:
                return {
                    'success': False,
                    'error': 'No relevant tables found for fallback schema retrieval'
                }
            
            # Try to get schema using DESCRIBE for the primary table
            primary_table = fallback_tables[0]
            self.logger.info(f"Attempting schema fallback for table: {primary_table}")
            
            # Use the executor to run DESCRIBE query
            describe_sql = f"DESCRIBE {primary_table}"
            describe_result = self.executor.execute(describe_sql)
            
            if not describe_result.get('success', False):
                return {
                    'success': False,
                    'error': f"Failed to describe table {primary_table}: {describe_result.get('error', 'Unknown error')}"
                }
            
            # Process DESCRIBE results to build context
            results = describe_result.get('results', [])
            columns = []
            
            for row in results:
                if len(row) >= 2 and row[0] and not row[0].startswith('#'):
                    col_name = row[0]
                    col_type = row[1]
                    comment = row[2] if len(row) > 2 else ''
                    
                    if comment:
                        columns.append(f"{col_name} ({col_type}) - {comment}")
                    else:
                        columns.append(f"{col_name} ({col_type})")
            
            if not columns:
                return {
                    'success': False,
                    'error': f"No valid columns found in DESCRIBE result for {primary_table}"
                }
            
            # Build fallback context
            fallback_context = f"Table: {primary_table} (schema retrieved via fallback)\nColumns: {', '.join(columns)}"
            
            self.logger.info(f"Fallback schema retrieval successful for {primary_table} with {len(columns)} columns")
            
            return {
                'success': True,
                'context': fallback_context,
                'table': primary_table,
                'columns_found': len(columns)
            }
            
        except Exception as e:
            self.logger.error(f"Schema fallback mechanism failed: {str(e)}")
            return {
                'success': False,
                'error': f"Schema fallback failed: {str(e)}"
            }
    
    def _add_limit_clause(self, sql: str) -> str:
        """Add LIMIT clause to SQL query"""
        sql = sql.strip().rstrip(';')
        return f"{sql} LIMIT {self.max_rows}"
    
    def _format_success_response(self, execution_result: Dict[str, Any], 
                                sql_result: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Format successful response"""
        results = execution_result['results']
        column_names = execution_result['column_names']
        
        # Generate summary
        row_count = len(results)
        if row_count == 0:
            summary = "No data found for your query."
        elif row_count == 1 and len(column_names) == 1:
            summary = f"Result: {results[0][0]}"
        else:
            summary = f"Found {row_count} records."
        
        return {
            'success': True,
            'query': query,
            'sql': sql_result['sql'],
            'explanation': sql_result.get('explanation', ''),
            'summary': summary,
            'results': {
                'data': results[:20],  # Limit display
                'columns': column_names,
                'total_rows': row_count,
                'execution_time': execution_result['execution_time']
            },
            'metadata': {
                'confidence': sql_result.get('confidence', 0.8),
                'auto_fixed': sql_result.get('auto_fixed', False),
                'processed_at': datetime.now().isoformat()
            }
        }
    
    def _format_error_response(self, error: str, query: str, sql: str = None) -> Dict[str, Any]:
        """Format error response with helpful suggestions"""
        suggestions = []
        
        error_lower = error.lower()
        if 'table' in error_lower and 'not found' in error_lower:
            suggestions.extend([
                "Check table name spelling",
                "Verify you have access to this table",
                "Use fully qualified names: catalog.schema.table"
            ])
        elif 'schema' in error_lower and 'not found' in error_lower:
            suggestions.extend([
                "Check schema name spelling", 
                "Verify schema exists in your catalog"
            ])
        else:
            suggestions.extend([
                "Try rephrasing your question",
                "Ask: 'What tables are available?'",
                "Use simpler queries"
            ])
        
        return {
            'success': False,
            'query': query,
            'sql': sql,
            'error': error,
            'suggestions': suggestions,
            'available_tables': self.catalog_manager.get_tables()[:5],  # Show some examples
            'processed_at': datetime.now().isoformat()
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get agent health status"""
        return {
            'status': 'healthy' if self.executor.test_connection() else 'degraded',
            'catalogs_count': len(self.catalog_manager.get_catalogs()),
            'schemas_count': len(self.catalog_manager.get_schemas()),
            'tables_count': len(self.catalog_manager.get_tables()),
            'database_connected': self.executor.test_connection(),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_available_tables(self) -> List[str]:
        """Get available tables"""
        return self.catalog_manager.get_tables()