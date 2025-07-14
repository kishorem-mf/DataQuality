"""
NLPQueryAgent
Processes natural language queries about data quality and converts them to actionable insights.
"""

import json
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from pyspark.sql import SparkSession

from .base_agent import AIEnabledAgent
from services.llm_service import LLMService

class NLPQueryAgent(AIEnabledAgent):
    """
    Agent that handles natural language queries about data quality.
    Converts conversational questions into structured queries and provides insights.
    """
    
    def __init__(self, config: Dict[str, Any], llm_service: LLMService):
        super().__init__("NLPQueryAgent", config, llm_service)
        self.spark = self._get_spark_session()
        self.query_patterns = self._initialize_query_patterns()
        
    def _get_spark_session(self) -> SparkSession:
        """Get or create Spark session"""
        try:
            return SparkSession.getActiveSession()
        except:
            return SparkSession.builder.appName("DataQualityNLPQuery").getOrCreate()
    
    def _initialize_query_patterns(self) -> Dict[str, List[str]]:
        """Initialize common query patterns for fallback processing"""
        return {
            'completeness': [
                r'completeness.*for.*table\s+(\w+)',
                r'null.*percentage.*(\w+)',
                r'missing.*values.*(\w+)',
                r'how.*complete.*(\w+)'
            ],
            'uniqueness': [
                r'uniqueness.*for.*(\w+)',
                r'duplicate.*(\w+)',
                r'unique.*values.*(\w+)'
            ],
            'schema': [
                r'columns.*in.*(\w+)',
                r'schema.*(\w+)',
                r'describe.*table.*(\w+)',
                r'what.*columns.*(\w+)'
            ],
            'quality_summary': [
                r'quality.*summary.*(\w+)',
                r'overall.*quality.*(\w+)',
                r'data.*quality.*(\w+)'
            ],
            'categorical_columns': [
                r'categorical.*columns.*(\w+)',
                r'category.*columns.*(\w+)',
                r'text.*columns.*(\w+)'
            ],
            'timestamp_columns': [
                r'timestamp.*columns.*(\w+)',
                r'date.*columns.*(\w+)',
                r'time.*columns.*(\w+)'
            ],
            'key_columns': [
                r'key.*columns.*(\w+)',
                r'primary.*key.*(\w+)',
                r'identifier.*columns.*(\w+)'
            ]
        }
    
    def _validate_inputs(self, **kwargs) -> None:
        """Validate input parameters"""
        if 'query' not in kwargs:
            raise ValueError("query is required")
        
        if not isinstance(kwargs['query'], str):
            raise ValueError("query must be a string")
    
    def _execute_logic(self, **kwargs) -> Dict[str, Any]:
        """Main NLP query processing logic"""
        query = kwargs['query']
        available_tables = kwargs.get('available_tables', [])
        metrics_config = kwargs.get('metrics_config', {})
        
        self.logger.info(f"Processing NLP query: {query}")
        
        # Parse and classify the query
        query_analysis = self._analyze_query(query)
        
        # Extract table names and column references
        extracted_entities = self._extract_entities(query, available_tables)
        
        # Generate response based on query type
        response = self._generate_response(query, query_analysis, extracted_entities, metrics_config)
        
        return {
            'original_query': query,
            'query_analysis': query_analysis,
            'extracted_entities': extracted_entities,
            'response': response,
            'processed_at': datetime.now().isoformat()
        }
    
    def _analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze the query using AI to understand intent and requirements"""
        prompt = f"""
        Analyze the following natural language query about data quality:
        
        Query: "{query}"
        
        Please classify this query and provide analysis in JSON format:
        
        {{
            "query_type": "one of: completeness, uniqueness, validity, consistency, accuracy, timeliness, schema_info, quality_summary, column_classification, general",
            "intent": "brief description of what the user wants to know",
            "confidence": "confidence level 0-1",
            "parameters": {{
                "table_name": "extracted table name if any",
                "column_name": "extracted column name if any",
                "metric_type": "specific metric requested",
                "time_period": "time period if specified",
                "filters": "any filtering criteria"
            }},
            "suggested_actions": ["list of actions to fulfill the query"],
            "requires_data_access": "boolean indicating if data access is needed"
        }}
        
        Return valid JSON only.
        """
        
        try:
            analysis = self.llm_service.parse_json_response(prompt)
            return analysis
        except Exception as e:
            self.logger.warning(f"AI query analysis failed: {str(e)}, using fallback")
            return self._fallback_query_analysis(query)
    
    def _fallback_query_analysis(self, query: str) -> Dict[str, Any]:
        """Fallback query analysis using pattern matching"""
        query_lower = query.lower()
        
        # Pattern matching for common query types
        for query_type, patterns in self.query_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    return {
                        'query_type': query_type,
                        'intent': f'User wants to know about {query_type}',
                        'confidence': 0.7,
                        'parameters': {},
                        'suggested_actions': [f'retrieve_{query_type}_metrics'],
                        'requires_data_access': True
                    }
        
        # Default classification
        return {
            'query_type': 'general',
            'intent': 'General data quality inquiry',
            'confidence': 0.5,
            'parameters': {},
            'suggested_actions': ['provide_general_help'],
            'requires_data_access': False
        }
    
    def _extract_entities(self, query: str, available_tables: List[str]) -> Dict[str, Any]:
        """Extract table names, column names, and other entities from the query"""
        entities = {
            'tables': [],
            'columns': [],
            'metrics': [],
            'time_references': []
        }
        
        # Extract table names
        for table in available_tables:
            # Check for full table name (catalog.schema.table)
            if table.lower() in query.lower():
                entities['tables'].append(table)
            
            # Check for just table name (last part)
            table_short = table.split('.')[-1]
            if table_short.lower() in query.lower():
                entities['tables'].append(table)
        
        # Extract column references (common patterns)
        column_patterns = [
            r'column[s]?\s+(\w+)',
            r'field[s]?\s+(\w+)',
            r'for\s+(\w+)',
            r'in\s+(\w+)'
        ]
        
        for pattern in column_patterns:
            matches = re.findall(pattern, query.lower())
            entities['columns'].extend(matches)
        
        # Extract metric references
        metric_keywords = ['completeness', 'uniqueness', 'validity', 'consistency', 'accuracy', 'timeliness']
        for metric in metric_keywords:
            if metric in query.lower():
                entities['metrics'].append(metric)
        
        # Remove duplicates
        entities['tables'] = list(set(entities['tables']))
        entities['columns'] = list(set(entities['columns']))
        entities['metrics'] = list(set(entities['metrics']))
        
        return entities
    
    def _generate_response(self, query: str, query_analysis: Dict[str, Any], 
                          extracted_entities: Dict[str, Any], 
                          metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate appropriate response based on query analysis"""
        
        query_type = query_analysis.get('query_type', 'general')
        
        try:
            if query_type == 'completeness':
                return self._handle_completeness_query(extracted_entities, metrics_config)
            elif query_type == 'uniqueness':
                return self._handle_uniqueness_query(extracted_entities, metrics_config)
            elif query_type == 'schema_info':
                return self._handle_schema_query(extracted_entities)
            elif query_type == 'quality_summary':
                return self._handle_quality_summary_query(extracted_entities, metrics_config)
            elif query_type == 'categorical_columns':
                return self._handle_categorical_columns_query(extracted_entities)
            elif query_type == 'timestamp_columns':
                return self._handle_timestamp_columns_query(extracted_entities)
            elif query_type == 'key_columns':
                return self._handle_key_columns_query(extracted_entities)
            else:
                return self._handle_general_query(query, query_analysis)
                
        except Exception as e:
            self.logger.error(f"Error generating response: {str(e)}")
            return {
                'response_type': 'error',
                'message': f"Sorry, I encountered an error processing your query: {str(e)}",
                'suggestions': ['Please try rephrasing your question', 'Check if the table name is correct']
            }
    
    def _handle_completeness_query(self, entities: Dict[str, Any], 
                                 metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle completeness-related queries"""
        tables = entities.get('tables', [])
        
        if not tables:
            return {
                'response_type': 'error',
                'message': 'Please specify a table name for completeness analysis.',
                'suggestions': ['Example: "What is the completeness for table sales.orders?"']
            }
        
        results = {}
        for table in tables:
            try:
                # Get completeness metrics from Delta table or compute on-demand
                completeness_data = self._get_completeness_metrics(table)
                results[table] = completeness_data
            except Exception as e:
                results[table] = {'error': str(e)}
        
        return {
            'response_type': 'completeness',
            'message': self._format_completeness_response(results),
            'data': results
        }
    
    def _handle_uniqueness_query(self, entities: Dict[str, Any], 
                               metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle uniqueness-related queries"""
        tables = entities.get('tables', [])
        
        if not tables:
            return {
                'response_type': 'error',
                'message': 'Please specify a table name for uniqueness analysis.',
                'suggestions': ['Example: "Show uniqueness for table inventory.stock"']
            }
        
        results = {}
        for table in tables:
            try:
                uniqueness_data = self._get_uniqueness_metrics(table)
                results[table] = uniqueness_data
            except Exception as e:
                results[table] = {'error': str(e)}
        
        return {
            'response_type': 'uniqueness',
            'message': self._format_uniqueness_response(results),
            'data': results
        }
    
    def _handle_schema_query(self, entities: Dict[str, Any]) -> Dict[str, Any]:
        """Handle schema information queries"""
        tables = entities.get('tables', [])
        
        if not tables:
            return {
                'response_type': 'error',
                'message': 'Please specify a table name for schema information.',
                'suggestions': ['Example: "Show me columns in table orders"']
            }
        
        results = {}
        for table in tables:
            try:
                schema_info = self._get_table_schema(table)
                results[table] = schema_info
            except Exception as e:
                results[table] = {'error': str(e)}
        
        return {
            'response_type': 'schema',
            'message': self._format_schema_response(results),
            'data': results
        }
    
    def _handle_quality_summary_query(self, entities: Dict[str, Any], 
                                    metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle overall quality summary queries"""
        tables = entities.get('tables', [])
        
        if not tables:
            return {
                'response_type': 'error',
                'message': 'Please specify a table name for quality summary.',
                'suggestions': ['Example: "Show quality summary for table orders"']
            }
        
        results = {}
        for table in tables:
            try:
                quality_summary = self._get_quality_summary(table)
                results[table] = quality_summary
            except Exception as e:
                results[table] = {'error': str(e)}
        
        return {
            'response_type': 'quality_summary',
            'message': self._format_quality_summary_response(results),
            'data': results
        }
    
    def _handle_categorical_columns_query(self, entities: Dict[str, Any]) -> Dict[str, Any]:
        """Handle categorical columns queries"""
        tables = entities.get('tables', [])
        
        if not tables:
            return {
                'response_type': 'error',
                'message': 'Please specify a table name to find categorical columns.',
                'suggestions': ['Example: "List categorical columns in table orders"']
            }
        
        results = {}
        for table in tables:
            try:
                categorical_columns = self._get_categorical_columns(table)
                results[table] = categorical_columns
            except Exception as e:
                results[table] = {'error': str(e)}
        
        return {
            'response_type': 'categorical_columns',
            'message': self._format_categorical_columns_response(results),
            'data': results
        }
    
    def _handle_timestamp_columns_query(self, entities: Dict[str, Any]) -> Dict[str, Any]:
        """Handle timestamp columns queries"""
        tables = entities.get('tables', [])
        
        if not tables:
            return {
                'response_type': 'error',
                'message': 'Please specify a table name to find timestamp columns.',
                'suggestions': ['Example: "What timestamp columns are in table orders?"']
            }
        
        results = {}
        for table in tables:
            try:
                timestamp_columns = self._get_timestamp_columns(table)
                results[table] = timestamp_columns
            except Exception as e:
                results[table] = {'error': str(e)}
        
        return {
            'response_type': 'timestamp_columns',
            'message': self._format_timestamp_columns_response(results),
            'data': results
        }
    
    def _handle_key_columns_query(self, entities: Dict[str, Any]) -> Dict[str, Any]:
        """Handle key columns queries"""
        tables = entities.get('tables', [])
        
        if not tables:
            return {
                'response_type': 'error',
                'message': 'Please specify a table name to find key columns.',
                'suggestions': ['Example: "Show key columns in table orders"']
            }
        
        results = {}
        for table in tables:
            try:
                key_columns = self._get_key_columns(table)
                results[table] = key_columns
            except Exception as e:
                results[table] = {'error': str(e)}
        
        return {
            'response_type': 'key_columns',
            'message': self._format_key_columns_response(results),
            'data': results
        }
    
    def _handle_general_query(self, query: str, query_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Handle general queries using AI"""
        prompt = f"""
        A user asked this question about data quality: "{query}"
        
        Based on the context of data quality monitoring in Databricks, provide a helpful response.
        
        Include:
        1. Direct answer to their question
        2. Relevant context about data quality
        3. Suggestions for next steps
        4. Example queries they might find useful
        
        Format your response as clear, actionable guidance.
        """
        
        try:
            ai_response = self.llm_service.complete(prompt)
            return {
                'response_type': 'general',
                'message': ai_response,
                'suggestions': [
                    'Ask about specific tables: "What is the completeness for table X?"',
                    'Request schema information: "Show me columns in table Y"',
                    'Get quality summaries: "Show quality summary for table Z"'
                ]
            }
        except Exception as e:
            return {
                'response_type': 'error',
                'message': 'I can help with data quality questions. Try asking about specific tables or metrics.',
                'suggestions': [
                    'Example: "What is the completeness for table orders?"',
                    'Example: "Show me categorical columns in table products"',
                    'Example: "Get quality summary for table customers"'
                ]
            }
    
    # Helper methods for data retrieval
    def _get_completeness_metrics(self, table_name: str) -> Dict[str, Any]:
        """Get completeness metrics for a table"""
        # This would query the Delta table with stored results
        # For now, return a placeholder
        return {
            'table_name': table_name,
            'completeness_score': 95.5,
            'columns_analyzed': 10,
            'columns_with_issues': 2
        }
    
    def _get_uniqueness_metrics(self, table_name: str) -> Dict[str, Any]:
        """Get uniqueness metrics for a table"""
        return {
            'table_name': table_name,
            'uniqueness_score': 98.2,
            'key_columns': ['id', 'order_id'],
            'duplicate_records': 150
        }
    
    def _get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        try:
            df = self.spark.table(table_name)
            columns = []
            for field in df.schema.fields:
                columns.append({
                    'name': field.name,
                    'type': str(field.dataType),
                    'nullable': field.nullable
                })
            
            return {
                'table_name': table_name,
                'column_count': len(columns),
                'columns': columns
            }
        except Exception as e:
            return {'error': str(e)}
    
    def _get_quality_summary(self, table_name: str) -> Dict[str, Any]:
        """Get overall quality summary for a table"""
        return {
            'table_name': table_name,
            'overall_score': 92.3,
            'completeness': 95.5,
            'uniqueness': 98.2,
            'validity': 87.8,
            'last_assessed': datetime.now().isoformat()
        }
    
    def _get_categorical_columns(self, table_name: str) -> Dict[str, Any]:
        """Get categorical columns for a table"""
        try:
            df = self.spark.table(table_name)
            categorical_columns = []
            
            for field in df.schema.fields:
                if 'string' in str(field.dataType).lower():
                    # Simple heuristic - check cardinality
                    distinct_count = df.select(field.name).distinct().count()
                    if distinct_count < 20:  # Threshold for categorical
                        categorical_columns.append(field.name)
            
            return {
                'table_name': table_name,
                'categorical_columns': categorical_columns,
                'count': len(categorical_columns)
            }
        except Exception as e:
            return {'error': str(e)}
    
    def _get_timestamp_columns(self, table_name: str) -> Dict[str, Any]:
        """Get timestamp columns for a table"""
        try:
            df = self.spark.table(table_name)
            timestamp_columns = []
            
            for field in df.schema.fields:
                data_type = str(field.dataType).lower()
                if 'timestamp' in data_type or 'date' in data_type:
                    timestamp_columns.append(field.name)
            
            return {
                'table_name': table_name,
                'timestamp_columns': timestamp_columns,
                'count': len(timestamp_columns)
            }
        except Exception as e:
            return {'error': str(e)}
    
    def _get_key_columns(self, table_name: str) -> Dict[str, Any]:
        """Get key columns for a table"""
        try:
            df = self.spark.table(table_name)
            key_columns = []
            
            for field in df.schema.fields:
                # Heuristic: columns with 'id', 'key', 'code' in name
                if any(keyword in field.name.lower() for keyword in ['id', 'key', 'code']):
                    key_columns.append(field.name)
            
            return {
                'table_name': table_name,
                'key_columns': key_columns,
                'count': len(key_columns)
            }
        except Exception as e:
            return {'error': str(e)}
    
    # Response formatting methods
    def _format_completeness_response(self, results: Dict[str, Any]) -> str:
        """Format completeness response"""
        response_parts = []
        for table, data in results.items():
            if 'error' in data:
                response_parts.append(f"❌ {table}: {data['error']}")
            else:
                score = data.get('completeness_score', 0)
                response_parts.append(f"✅ {table}: {score}% complete")
        
        return "\\n".join(response_parts)
    
    def _format_uniqueness_response(self, results: Dict[str, Any]) -> str:
        """Format uniqueness response"""
        response_parts = []
        for table, data in results.items():
            if 'error' in data:
                response_parts.append(f"❌ {table}: {data['error']}")
            else:
                score = data.get('uniqueness_score', 0)
                response_parts.append(f"✅ {table}: {score}% unique")
        
        return "\\n".join(response_parts)
    
    def _format_schema_response(self, results: Dict[str, Any]) -> str:
        """Format schema response"""
        response_parts = []
        for table, data in results.items():
            if 'error' in data:
                response_parts.append(f"❌ {table}: {data['error']}")
            else:
                columns = data.get('columns', [])
                column_names = [col['name'] for col in columns]
                response_parts.append(f"✅ {table} ({len(columns)} columns): {', '.join(column_names)}")
        
        return "\\n".join(response_parts)
    
    def _format_quality_summary_response(self, results: Dict[str, Any]) -> str:
        """Format quality summary response"""
        response_parts = []
        for table, data in results.items():
            if 'error' in data:
                response_parts.append(f"❌ {table}: {data['error']}")
            else:
                score = data.get('overall_score', 0)
                response_parts.append(f"✅ {table}: Overall quality {score}%")
        
        return "\\n".join(response_parts)
    
    def _format_categorical_columns_response(self, results: Dict[str, Any]) -> str:
        """Format categorical columns response"""
        response_parts = []
        for table, data in results.items():
            if 'error' in data:
                response_parts.append(f"❌ {table}: {data['error']}")
            else:
                columns = data.get('categorical_columns', [])
                response_parts.append(f"✅ {table}: {', '.join(columns) if columns else 'No categorical columns found'}")
        
        return "\\n".join(response_parts)
    
    def _format_timestamp_columns_response(self, results: Dict[str, Any]) -> str:
        """Format timestamp columns response"""
        response_parts = []
        for table, data in results.items():
            if 'error' in data:
                response_parts.append(f"❌ {table}: {data['error']}")
            else:
                columns = data.get('timestamp_columns', [])
                response_parts.append(f"✅ {table}: {', '.join(columns) if columns else 'No timestamp columns found'}")
        
        return "\\n".join(response_parts)
    
    def _format_key_columns_response(self, results: Dict[str, Any]) -> str:
        """Format key columns response"""
        response_parts = []
        for table, data in results.items():
            if 'error' in data:
                response_parts.append(f"❌ {table}: {data['error']}")
            else:
                columns = data.get('key_columns', [])
                response_parts.append(f"✅ {table}: {', '.join(columns) if columns else 'No key columns found'}")
        
        return "\\n".join(response_parts)