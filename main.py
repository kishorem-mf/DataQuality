"""
Main Application Entry Point for Databricks Data Quality Agent
Provides both programmatic API and command-line interface for data quality assessment.
"""

import os
import sys
import json
import argparse
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.llm_service import LLMService
from agents.unity_sql_agent import UnitySQLAgent

class DataQualityApp:
    """Main application class for the Data Quality Agent system"""
    
    def __init__(self, config_dir: str = "config"):
        # Load environment variables
        load_dotenv()
        
        self.config_dir = config_dir
        self.config = self._load_configurations()
        self.llm_service = self._initialize_llm_service()
        self.unity_sql_agent = self._initialize_unity_sql_agent()
        self.logger = self._setup_logging()
    
    def _load_configurations(self) -> Dict[str, Any]:
        """Load all configuration files"""
        config = {}
        
        config_files = [
            'azure_openai_config.json',
            'unity_catalog_config.json'
        ]
        
        for config_file in config_files:
            config_path = os.path.join(self.config_dir, config_file)
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config[config_file.replace('.json', '')] = json.load(f)
            else:
                print(f"Warning: Configuration file {config_file} not found")
        
        return config
    
    def _initialize_llm_service(self) -> LLMService:
        """Initialize the LLM service"""
        try:
            config_path = os.path.join(self.config_dir, 'azure_openai_config.json')
            return LLMService(config_path)
        except Exception as e:
            print(f"Failed to initialize LLM service: {str(e)}")
            raise
    
    def _initialize_unity_sql_agent(self) -> UnitySQLAgent:
        """Initialize the Unity SQL Agent"""
        unity_config = self.config.get('unity_catalog_config', {})
        # Add default settings
        unity_config.setdefault('max_rows', 100)
        unity_config.setdefault('query_timeout', 60)
        return UnitySQLAgent(unity_config, self.llm_service)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup application logging"""
        logger = logging.getLogger('DataQualityApp')
        log_level = self.config.get('agent_config', {}).get('logging', {}).get('level', 'INFO')
        logger.setLevel(getattr(logging, log_level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def run_table_assessment(self, table_name: str) -> Dict[str, Any]:
        """
        Run basic data quality assessment for a single table
        
        Args:
            table_name: Name of the table to assess
            
        Returns:
            Assessment results
        """
        self.logger.info(f"Starting table assessment for {table_name}")
        
        try:
            # Basic assessment queries
            queries = [
                f"SELECT COUNT(*) as total_rows FROM {table_name}",
                f"SELECT COUNT(DISTINCT *) as distinct_rows FROM {table_name}"
            ]
            
            results = []
            for query in queries:
                result = self.unity_sql_agent.execute(query=query)
                if result.status.name == 'COMPLETED':
                    results.append(result.data)
            
            return {
                'status': 'completed',
                'table': table_name,
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Assessment failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    
    
    def process_sql_query(self, query: str) -> Dict[str, Any]:
        """
        Process SQL natural language query using Unity SQL Agent
        
        Args:
            query: Natural language query to convert to SQL
            
        Returns:
            SQL query results with formatted response
        """
        self.logger.info(f"Processing Unity SQL query: {query}")
        
        try:
            result = self.unity_sql_agent.execute(query=query)
            
            self.logger.info(f"Unity SQL query processed with status: {result.status}")
            return result.data
            
        except Exception as e:
            self.logger.error(f"Unity SQL query processing failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get system health status"""
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'components': {}
        }
        
        # Check LLM service health
        try:
            llm_health = self.llm_service.health_check()
            health_status['components']['llm_service'] = llm_health
        except Exception as e:
            health_status['components']['llm_service'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
            health_status['status'] = 'degraded'
        
        # Check core components
        health_status['components']['core'] = {
            'status': 'healthy',
            'architecture': 'simplified_unity_catalog'
        }
        
        # Check Unity SQL agent health
        try:
            unity_sql_health = self.unity_sql_agent.get_health_status()
            health_status['components']['unity_sql_agent'] = unity_sql_health
            if unity_sql_health.get('status') != 'healthy':
                health_status['status'] = 'degraded'
        except Exception as e:
            health_status['components']['unity_sql_agent'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
            health_status['status'] = 'degraded'
        
        # Check configuration
        required_configs = ['unity_catalog_config']
        missing_configs = [cfg for cfg in required_configs if cfg not in self.config]
        
        if missing_configs:
            health_status['components']['configuration'] = {
                'status': 'unhealthy',
                'missing_configs': missing_configs
            }
            health_status['status'] = 'degraded'
        else:
            health_status['components']['configuration'] = {
                'status': 'healthy'
            }
        
        return health_status
    
    def execute_describe_query(self, table_name: str, save_to_file: bool = False) -> Dict[str, Any]:
        """
        Execute DESCRIBE query on a table and return structured results
        
        Args:
            table_name: Name of the table to describe
            save_to_file: Whether to save results to a JSON file
            
        Returns:
            Structured query results with schema information
        """
        self.logger.info(f"Executing DESCRIBE query for table: {table_name}")
        
        # Try multiple authentication approaches
        methods = [
            self._try_sql_connector_auth,
            self._try_pyspark_auth,
            self._try_requests_auth
        ]
        
        for i, method in enumerate(methods, 1):
            try:
                self.logger.info(f"Trying authentication method {i}: {method.__name__}")
                result = method(table_name)
                
                if result['status'] == 'success':
                    # Save to file if requested
                    if save_to_file:
                        filename = f"describe_{table_name.replace('.', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        with open(filename, 'w') as f:
                            json.dump(result, f, indent=2, default=str)
                        result['saved_to_file'] = filename
                        self.logger.info(f"Results saved to: {filename}")
                    
                    return result
                else:
                    self.logger.warning(f"Method {i} failed: {result.get('error', 'Unknown error')}")
                    continue
                    
            except Exception as e:
                self.logger.warning(f"Method {i} failed with exception: {str(e)}")
                continue
        
        # If all methods failed, return comprehensive error
        return {
            'status': 'failed',
            'table_name': table_name,
            'error': 'All authentication methods failed',
            'timestamp': datetime.now().isoformat(),
            'troubleshooting': {
                'methods_tried': [method.__name__ for method in methods],
                'suggestions': [
                    'Try running the query directly in Databricks workspace',
                    'Check SQL warehouse permissions and status',
                    'Verify token has appropriate scopes',
                    'Consider using a personal access token with broader permissions'
                ],
                'manual_query': f"DESCRIBE {table_name}"
            }
        }
    
    def _try_sql_connector_auth(self, table_name: str) -> Dict[str, Any]:
        """Try databricks-sql-connector authentication"""
        try:
            from databricks import sql
        except ImportError:
            return {
                'status': 'failed',
                'error': 'databricks-sql-connector not installed'
            }
        
        server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
        http_path = os.getenv('DATABRICKS_HTTP_PATH')
        access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
        
        if not all([server_hostname, http_path, access_token]):
            return {
                'status': 'failed',
                'error': 'Missing connection parameters'
            }
        
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        
        cursor = connection.cursor()
        query = f"DESCRIBE {table_name}"
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return self._process_describe_results(table_name, query, results)
    
    def _try_pyspark_auth(self, table_name: str) -> Dict[str, Any]:
        """Try PySpark authentication (works in Databricks runtime)"""
        try:
            from pyspark.sql import SparkSession
            
            # This works when running inside Databricks
            spark = SparkSession.builder.appName("DataQualityDescribe").getOrCreate()
            
            # Try to describe the table using Spark SQL
            df = spark.sql(f"DESCRIBE {table_name}")
            results = df.collect()
            
            # Convert to expected format
            formatted_results = []
            for row in results:
                formatted_results.append((row.col_name, row.data_type, row.comment))
            
            return self._process_describe_results(table_name, f"DESCRIBE {table_name}", formatted_results)
            
        except Exception as e:
            return {
                'status': 'failed',
                'error': f'PySpark method failed: {str(e)}'
            }
    
    def _try_requests_auth(self, table_name: str) -> Dict[str, Any]:
        """Try REST API authentication"""
        try:
            import requests
            
            server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
            access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
            
            if not all([server_hostname, access_token]):
                return {
                    'status': 'failed',
                    'error': 'Missing connection parameters for REST API'
                }
            
            # Try using Databricks SQL REST API
            url = f"https://{server_hostname}/api/2.0/sql/statements"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'statement': f"DESCRIBE {table_name}",
                'warehouse_id': os.getenv('DATABRICKS_HTTP_PATH', '').split('/')[-1] if os.getenv('DATABRICKS_HTTP_PATH') else None
            }
            
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                # This would need to be processed based on the actual API response format
                return {
                    'status': 'failed',
                    'error': 'REST API method needs implementation for result processing'
                }
            else:
                return {
                    'status': 'failed',
                    'error': f'REST API failed: {response.status_code} - {response.text}'
                }
                
        except Exception as e:
            return {
                'status': 'failed',
                'error': f'REST API method failed: {str(e)}'
            }
    
    def _process_describe_results(self, table_name: str, query: str, results: List[Tuple]) -> Dict[str, Any]:
        """Process DESCRIBE query results into structured format"""
        columns = []
        numeric_columns = []
        categorical_columns = []
        temporal_columns = []
        text_columns = []
        
        numeric_types = ['int', 'bigint', 'float', 'double', 'decimal', 'numeric']
        temporal_types = ['date', 'timestamp']
        
        for row in results:
            col_name = row[0]
            data_type = row[1]
            comment = row[2] if len(row) > 2 else ''
            
            column_info = {
                'name': col_name,
                'data_type': data_type,
                'comment': comment,
                'nullable': True  # Default, would need additional query to determine
            }
            columns.append(column_info)
            
            # Classify columns by type
            data_type_lower = data_type.lower()
            if any(num_type in data_type_lower for num_type in numeric_types):
                numeric_columns.append(col_name)
            elif any(temp_type in data_type_lower for temp_type in temporal_types):
                temporal_columns.append(col_name)
            elif 'string' in data_type_lower or 'varchar' in data_type_lower:
                text_columns.append(col_name)
        
        # Build structured response
        return {
            'status': 'success',
            'table_name': table_name,
            'query_executed': query,
            'execution_time': datetime.now().isoformat(),
            'schema_info': {
                'total_columns': len(columns),
                'columns': columns,
                'column_classification': {
                    'numeric': numeric_columns,
                    'temporal': temporal_columns,
                    'text': text_columns,
                    'categorical': categorical_columns
                }
            },
            'summary': {
                'numeric_columns_count': len(numeric_columns),
                'temporal_columns_count': len(temporal_columns),
                'text_columns_count': len(text_columns),
                'numeric_columns': numeric_columns
            }
        }
    
    def execute_describe_query_demo(self, table_name: str, save_to_file: bool = False) -> Dict[str, Any]:
        """
        Demo version of describe query that returns mock results for testing
        
        Args:
            table_name: Name of the table to describe
            save_to_file: Whether to save results to a JSON file
            
        Returns:
            Mock structured query results with schema information
        """
        self.logger.info(f"Executing DEMO DESCRIBE query for table: {table_name}")
        
        # Mock schema data based on common e-commerce/sales table structure
        mock_columns = [
            {'name': 'order_id', 'data_type': 'bigint', 'comment': 'Primary key'},
            {'name': 'customer_id', 'data_type': 'bigint', 'comment': 'Foreign key to customer'},
            {'name': 'product_id', 'data_type': 'string', 'comment': 'Product identifier'},
            {'name': 'order_date', 'data_type': 'date', 'comment': 'Order date'},
            {'name': 'quantity', 'data_type': 'int', 'comment': 'Quantity ordered'},
            {'name': 'unit_price', 'data_type': 'decimal(10,2)', 'comment': 'Price per unit'},
            {'name': 'total_amount', 'data_type': 'decimal(12,2)', 'comment': 'Total order amount'},
            {'name': 'discount_rate', 'data_type': 'double', 'comment': 'Discount percentage'},
            {'name': 'sales_rep', 'data_type': 'string', 'comment': 'Sales representative'},
            {'name': 'region', 'data_type': 'string', 'comment': 'Sales region'},
            {'name': 'status', 'data_type': 'string', 'comment': 'Order status'},
            {'name': 'created_timestamp', 'data_type': 'timestamp', 'comment': 'Record creation time'}
        ]
        
        # Classify columns
        numeric_columns = []
        temporal_columns = []
        text_columns = []
        
        numeric_types = ['int', 'bigint', 'float', 'double', 'decimal']
        temporal_types = ['date', 'timestamp']
        
        for col in mock_columns:
            data_type_lower = col['data_type'].lower()
            if any(num_type in data_type_lower for num_type in numeric_types):
                numeric_columns.append(col['name'])
            elif any(temp_type in data_type_lower for temp_type in temporal_types):
                temporal_columns.append(col['name'])
            elif 'string' in data_type_lower:
                text_columns.append(col['name'])
        
        # Build result
        result = {
            'status': 'success',
            'mode': 'demo',
            'table_name': table_name,
            'query_executed': f"DESCRIBE {table_name}",
            'execution_time': datetime.now().isoformat(),
            'schema_info': {
                'total_columns': len(mock_columns),
                'columns': mock_columns,
                'column_classification': {
                    'numeric': numeric_columns,
                    'temporal': temporal_columns,
                    'text': text_columns,
                    'categorical': []
                }
            },
            'summary': {
                'numeric_columns_count': len(numeric_columns),
                'temporal_columns_count': len(temporal_columns),
                'text_columns_count': len(text_columns),
                'numeric_columns': numeric_columns
            },
            'note': 'This is mock data for demonstration. For real data, ensure proper Databricks SQL Warehouse access.'
        }
        
        # Save to file if requested
        if save_to_file:
            filename = f"describe_demo_{table_name.replace('.', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            result['saved_to_file'] = filename
            self.logger.info(f"Demo results saved to: {filename}")
        
        self.logger.info(f"Demo DESCRIBE query completed for {table_name}")
        return result
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get usage statistics"""
        try:
            llm_stats = self.llm_service.get_usage_stats()
            
            return {
                'llm_usage': {
                    'total_requests': llm_stats.total_requests,
                    'total_tokens': llm_stats.total_tokens,
                    'failed_requests': llm_stats.failed_requests,
                    'average_response_time': llm_stats.average_response_time
                },
                'unity_sql_agent': {
                    'status': 'active'
                },
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

def create_cli_parser():
    """Create command-line argument parser"""
    parser = argparse.ArgumentParser(description='Databricks Data Quality Agent')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Table assessment command
    assess_parser = subparsers.add_parser('assess', help='Run basic table assessment')
    assess_parser.add_argument('table_name', help='Name of the table to assess')
    
    # Natural language query command
    query_parser = subparsers.add_parser('query', help='Process natural language query')
    query_parser.add_argument('query', help='Natural language query about data')
    
    # SQL query command
    sql_parser = subparsers.add_parser('sql-query', help='Process natural language SQL query')
    sql_parser.add_argument('query', help='Natural language query to convert to SQL')
    sql_parser.add_argument('--format', choices=['auto', 'table', 'summary'], default='auto', help='Response format')
    sql_parser.add_argument('--max-rows', type=int, help='Maximum rows to return')
    
    # Health check command
    subparsers.add_parser('health', help='Check system health')
    
    # Usage stats command
    subparsers.add_parser('stats', help='Get usage statistics')
    
    # Describe table command
    describe_parser = subparsers.add_parser('describe', help='Get table schema information')
    describe_parser.add_argument('table_name', help='Name of the table to describe')
    describe_parser.add_argument('--save', action='store_true', help='Save results to JSON file')
    
    # Demo describe command
    demo_parser = subparsers.add_parser('describe-demo', help='Get demo table schema information (mock data)')
    demo_parser.add_argument('table_name', help='Name of the table to describe')
    demo_parser.add_argument('--save', action='store_true', help='Save results to JSON file')
    
    # Configuration options
    parser.add_argument('--config-dir', default='config', help='Configuration directory path')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    return parser

def main():
    """Main entry point for command-line interface"""
    parser = create_cli_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Setup logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize application
        app = DataQualityApp(config_dir=args.config_dir)
        
        # Execute command
        if args.command == 'assess':
            result = app.run_table_assessment(args.table_name)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'query':
            result = app.process_sql_query(args.query)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'sql-query':
            result = app.process_sql_query(args.query)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'health':
            result = app.get_health_status()
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'stats':
            result = app.get_usage_stats()
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'describe':
            result = app.execute_describe_query(args.table_name, save_to_file=args.save)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'describe-demo':
            result = app.execute_describe_query_demo(args.table_name, save_to_file=args.save)
            print(json.dumps(result, indent=2, default=str))
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

# Example usage functions for Databricks notebooks
def create_data_quality_app(config_dir: str = "config") -> DataQualityApp:
    """
    Create and return a DataQualityApp instance for use in Databricks notebooks
    
    Args:
        config_dir: Path to configuration directory
        
    Returns:
        DataQualityApp instance
    """
    return DataQualityApp(config_dir)

def assess_table(table_name: str, config_dir: str = "config") -> Dict[str, Any]:
    """
    Convenience function to assess a single table
    
    Args:
        table_name: Name of the table to assess
        config_dir: Path to configuration directory
        
    Returns:
        Assessment results
    """
    app = create_data_quality_app(config_dir)
    return app.run_table_assessment(table_name)

def ask_sql_question(question: str, config_dir: str = "config") -> Dict[str, Any]:
    """
    Convenience function to ask natural language SQL questions using Unity Catalog
    
    Args:
        question: Natural language SQL query
        config_dir: Path to configuration directory
        
    Returns:
        SQL query results
    """
    app = create_data_quality_app(config_dir)
    return app.process_sql_query(question)

if __name__ == "__main__":
    main()