"""
Main Application Entry Point for Databricks Data Quality Agent
Provides both programmatic API and command-line interface for data quality assessment.
"""

import os
import sys
import json
import argparse
import logging
from typing import Dict, Any, Optional
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.llm_service import LLMService
from agents.orchestrator import DataQualityOrchestrator
from agents.nlp_query import NLPQueryAgent

class DataQualityApp:
    """Main application class for the Data Quality Agent system"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = config_dir
        self.config = self._load_configurations()
        self.llm_service = self._initialize_llm_service()
        self.orchestrator = self._initialize_orchestrator()
        self.nlp_agent = self._initialize_nlp_agent()
        self.logger = self._setup_logging()
    
    def _load_configurations(self) -> Dict[str, Any]:
        """Load all configuration files"""
        config = {}
        
        config_files = [
            'azure_openai_config.json',
            'tables_config.json',
            'metrics_config.json',
            'agent_config.json'
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
    
    def _initialize_orchestrator(self) -> DataQualityOrchestrator:
        """Initialize the orchestrator agent"""
        orchestrator_config = self.config.get('agent_config', {}).get('orchestrator', {})
        return DataQualityOrchestrator(orchestrator_config, self.llm_service)
    
    def _initialize_nlp_agent(self) -> NLPQueryAgent:
        """Initialize the NLP query agent"""
        nlp_config = self.config.get('agent_config', {}).get('nlp_query', {})
        return NLPQueryAgent(nlp_config, self.llm_service)
    
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
    
    def run_full_assessment(self, tables: Optional[list] = None) -> Dict[str, Any]:
        """
        Run full data quality assessment
        
        Args:
            tables: Optional list of specific tables to assess
            
        Returns:
            Assessment results
        """
        self.logger.info("Starting full data quality assessment")
        
        try:
            # Use provided tables or default from config
            if tables:
                tables_config = {'tables': tables}
            else:
                tables_config = self.config.get('tables_config', {})
            
            metrics_config = self.config.get('metrics_config', {})
            
            # Execute assessment
            result = self.orchestrator.execute(
                tables_config=tables_config,
                metrics_config=metrics_config,
                job_type='full_assessment'
            )
            
            self.logger.info(f"Full assessment completed with status: {result.status}")
            return result.data
            
        except Exception as e:
            self.logger.error(f"Full assessment failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_single_table_assessment(self, table_name: str) -> Dict[str, Any]:
        """
        Run assessment for a single table
        
        Args:
            table_name: Name of the table to assess
            
        Returns:
            Assessment results
        """
        self.logger.info(f"Starting single table assessment for {table_name}")
        
        try:
            tables_config = self.config.get('tables_config', {})
            metrics_config = self.config.get('metrics_config', {})
            
            result = self.orchestrator.execute(
                tables_config=tables_config,
                metrics_config=metrics_config,
                job_type='single_table',
                table_name=table_name
            )
            
            self.logger.info(f"Single table assessment completed with status: {result.status}")
            return result.data
            
        except Exception as e:
            self.logger.error(f"Single table assessment failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def process_nlp_query(self, query: str) -> Dict[str, Any]:
        """
        Process natural language query
        
        Args:
            query: Natural language query about data quality
            
        Returns:
            Query response
        """
        self.logger.info(f"Processing NLP query: {query}")
        
        try:
            tables_config = self.config.get('tables_config', {})
            metrics_config = self.config.get('metrics_config', {})
            
            result = self.orchestrator.execute(
                tables_config=tables_config,
                metrics_config=metrics_config,
                job_type='nlp_query',
                query=query
            )
            
            self.logger.info(f"NLP query processed with status: {result.status}")
            return result.data
            
        except Exception as e:
            self.logger.error(f"NLP query processing failed: {str(e)}")
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
        
        # Check orchestrator health
        try:
            orchestrator_status = self.orchestrator.get_status()
            health_status['components']['orchestrator'] = {
                'status': 'healthy',
                'current_status': orchestrator_status.value
            }
        except Exception as e:
            health_status['components']['orchestrator'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
            health_status['status'] = 'degraded'
        
        # Check configuration
        required_configs = ['tables_config', 'metrics_config']
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
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get usage statistics"""
        try:
            llm_stats = self.llm_service.get_usage_stats()
            active_jobs = self.orchestrator.list_active_jobs()
            
            return {
                'llm_usage': {
                    'total_requests': llm_stats.total_requests,
                    'total_tokens': llm_stats.total_tokens,
                    'failed_requests': llm_stats.failed_requests,
                    'average_response_time': llm_stats.average_response_time
                },
                'active_jobs': len(active_jobs),
                'job_ids': active_jobs,
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
    
    # Full assessment command
    full_parser = subparsers.add_parser('full-assessment', help='Run full data quality assessment')
    full_parser.add_argument('--tables', nargs='+', help='Specific tables to assess')
    
    # Single table assessment command
    single_parser = subparsers.add_parser('single-table', help='Assess a single table')
    single_parser.add_argument('table_name', help='Name of the table to assess')
    
    # NLP query command
    query_parser = subparsers.add_parser('query', help='Process natural language query')
    query_parser.add_argument('query', help='Natural language query about data quality')
    
    # Health check command
    subparsers.add_parser('health', help='Check system health')
    
    # Usage stats command
    subparsers.add_parser('stats', help='Get usage statistics')
    
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
        if args.command == 'full-assessment':
            result = app.run_full_assessment(tables=args.tables)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'single-table':
            result = app.run_single_table_assessment(args.table_name)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'query':
            result = app.process_nlp_query(args.query)
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'health':
            result = app.get_health_status()
            print(json.dumps(result, indent=2, default=str))
        
        elif args.command == 'stats':
            result = app.get_usage_stats()
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

def run_quality_assessment(tables: list = None, config_dir: str = "config") -> Dict[str, Any]:
    """
    Convenience function to run data quality assessment
    
    Args:
        tables: Optional list of tables to assess
        config_dir: Path to configuration directory
        
    Returns:
        Assessment results
    """
    app = create_data_quality_app(config_dir)
    return app.run_full_assessment(tables)

def ask_quality_question(question: str, config_dir: str = "config") -> Dict[str, Any]:
    """
    Convenience function to ask natural language questions about data quality
    
    Args:
        question: Natural language question
        config_dir: Path to configuration directory
        
    Returns:
        Query response
    """
    app = create_data_quality_app(config_dir)
    return app.process_nlp_query(question)

if __name__ == "__main__":
    main()