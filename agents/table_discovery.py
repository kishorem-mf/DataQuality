"""
TableDiscoveryAgent
Discovers and analyzes table schemas from Unity Catalog with AI-powered insights.
"""

import json
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType

from .base_agent import AIEnabledAgent
from services.llm_service import LLMService

class TableDiscoveryAgent(AIEnabledAgent):
    """
    Agent responsible for discovering and analyzing table schemas from Unity Catalog.
    Uses AI to provide semantic understanding of table structure and relationships.
    """
    
    def __init__(self, config: Dict[str, Any], llm_service: LLMService):
        super().__init__("TableDiscoveryAgent", config, llm_service)
        self.spark = self._get_spark_session()
        
    def _get_spark_session(self) -> SparkSession:
        """Get or create Spark session"""
        try:
            return SparkSession.getActiveSession()
        except:
            return SparkSession.builder.appName("DataQualityTableDiscovery").getOrCreate()
    
    def _validate_inputs(self, **kwargs) -> None:
        """Validate input parameters"""
        table_name = kwargs.get('table_name')
        if not table_name:
            raise ValueError("table_name is required")
        
        if not isinstance(table_name, str):
            raise ValueError("table_name must be a string")
    
    def _execute_logic(self, **kwargs) -> Dict[str, Any]:
        """Main table discovery logic"""
        table_name = kwargs['table_name']
        
        self.logger.info(f"Starting table discovery for {table_name}")
        
        # Get basic table information
        table_info = self._get_table_info(table_name)
        
        # Get schema information
        schema_info = self._get_schema_info(table_name)
        
        # Classify columns
        column_classification = self._classify_columns(table_name, schema_info)
        
        # Get sample data for AI analysis
        sample_data = self._get_sample_data(table_name)
        
        # Generate AI insights
        ai_insights = self._generate_ai_insights(table_name, schema_info, sample_data)
        
        # Calculate table statistics
        table_stats = self._calculate_table_stats(table_name, schema_info)
        
        return {
            'table_name': table_name,
            'table_info': table_info,
            'schema_info': schema_info,
            'column_classification': column_classification,
            'sample_data': sample_data,
            'ai_insights': ai_insights,
            'table_stats': table_stats,
            'discovery_timestamp': self._get_timestamp()
        }
    
    def _get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get basic table information from Unity Catalog"""
        try:
            # Get table metadata
            table_df = self.spark.table(table_name)
            
            # Get table properties
            describe_result = self.spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            properties = {}
            
            for row in describe_result.collect():
                if row.col_name and row.data_type:
                    properties[row.col_name] = row.data_type
            
            return {
                'name': table_name,
                'row_count': table_df.count(),
                'column_count': len(table_df.columns),
                'properties': properties,
                'is_partitioned': 'Partition Information' in properties,
                'table_type': properties.get('Type', 'Unknown')
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {str(e)}")
            return {
                'name': table_name,
                'error': str(e)
            }
    
    def _get_schema_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed schema information"""
        try:
            df = self.spark.table(table_name)
            schema = df.schema
            
            columns = []
            for field in schema.fields:
                column_info = {
                    'name': field.name,
                    'data_type': str(field.dataType),
                    'nullable': field.nullable,
                    'metadata': field.metadata
                }
                columns.append(column_info)
            
            return {
                'columns': columns,
                'total_columns': len(columns),
                'schema_json': schema.json()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get schema info for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def _classify_columns(self, table_name: str, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """Classify columns into categories using rule-based logic and AI"""
        if 'error' in schema_info:
            return {'error': schema_info['error']}
        
        try:
            df = self.spark.table(table_name)
            columns = schema_info['columns']
            
            classification = {
                'numerical': [],
                'categorical': [],
                'text': [],
                'temporal': [],
                'key_columns': [],
                'foreign_keys': []
            }
            
            # Rule-based classification
            for col_info in columns:
                col_name = col_info['name']
                data_type = col_info['data_type']
                
                # Skip excluded columns
                if col_name in self.config.get('exclude_columns', []):
                    continue
                
                # Temporal columns
                if 'date' in data_type.lower() or 'timestamp' in data_type.lower():
                    classification['temporal'].append(col_name)
                
                # Numerical columns
                elif 'int' in data_type.lower() or 'float' in data_type.lower() or 'double' in data_type.lower():
                    classification['numerical'].append(col_name)
                
                # String columns - need to determine if categorical or text
                elif 'string' in data_type.lower():
                    # Check cardinality to determine if categorical
                    distinct_count = df.select(col_name).distinct().count()
                    total_count = df.count()
                    
                    if total_count > 0:
                        cardinality_ratio = distinct_count / total_count
                        threshold = self.config.get('cardinality_threshold', 0.1)
                        
                        if cardinality_ratio < threshold or distinct_count < 20:
                            classification['categorical'].append(col_name)
                        else:
                            classification['text'].append(col_name)
                    else:
                        classification['text'].append(col_name)
                
                # Identify potential key columns
                if 'id' in col_name.lower() or 'key' in col_name.lower() or 'code' in col_name.lower():
                    classification['key_columns'].append(col_name)
            
            # Use AI for enhanced classification if enabled
            if self.config.get('use_ai_classification', True):
                ai_classification = self._ai_classify_columns(table_name, columns, classification)
                classification.update(ai_classification)
            
            return classification
            
        except Exception as e:
            self.logger.error(f"Failed to classify columns for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def _ai_classify_columns(self, table_name: str, columns: List[Dict[str, Any]], 
                           initial_classification: Dict[str, List[str]]) -> Dict[str, Any]:
        """Use AI to enhance column classification"""
        try:
            # Prepare column information for AI
            column_summary = []
            for col in columns:
                column_summary.append({
                    'name': col['name'],
                    'type': col['data_type'],
                    'nullable': col['nullable']
                })
            
            prompt = f"""
            Analyze the following database table columns and enhance the classification:
            
            Table: {table_name}
            Columns: {json.dumps(column_summary, indent=2)}
            
            Current Classification: {json.dumps(initial_classification, indent=2)}
            
            Please provide enhanced classification with:
            1. business_keys: Columns that serve as business identifiers
            2. foreign_keys: Columns that reference other tables
            3. measures: Columns containing measurable values
            4. dimensions: Columns used for grouping/filtering
            5. quality_indicators: Columns critical for data quality
            
            Also identify:
            - potential_relationships: Suspected relationships with other tables
            - data_quality_priority: Priority level (high/medium/low) for quality monitoring
            
            Return JSON format only.
            """
            
            ai_response = self.llm_service.parse_json_response(prompt)
            
            return {
                'ai_enhanced': True,
                'business_keys': ai_response.get('business_keys', []),
                'foreign_keys': ai_response.get('foreign_keys', []),
                'measures': ai_response.get('measures', []),
                'dimensions': ai_response.get('dimensions', []),
                'quality_indicators': ai_response.get('quality_indicators', []),
                'potential_relationships': ai_response.get('potential_relationships', []),
                'data_quality_priority': ai_response.get('data_quality_priority', {})
            }
            
        except Exception as e:
            self.logger.warning(f"AI classification failed for {table_name}: {str(e)}")
            return {'ai_enhanced': False, 'error': str(e)}
    
    def _get_sample_data(self, table_name: str) -> Dict[str, Any]:
        """Get sample data for AI analysis"""
        try:
            df = self.spark.table(table_name)
            sample_size = self.config.get('sample_size', 100)
            
            # Get sample data
            sample_df = df.sample(withReplacement=False, fraction=0.01, seed=42).limit(sample_size)
            
            # Convert to list of dictionaries
            sample_data = []
            for row in sample_df.collect():
                sample_data.append(row.asDict())
            
            return {
                'sample_size': len(sample_data),
                'data': sample_data[:10],  # Limit to first 10 rows for AI analysis
                'total_sampled': len(sample_data)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get sample data for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def _generate_ai_insights(self, table_name: str, schema_info: Dict[str, Any], 
                            sample_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate AI insights about the table"""
        if not self.config.get('ai_schema_analysis', True):
            return {'enabled': False}
        
        try:
            # Prepare data for AI analysis
            columns = schema_info.get('columns', [])
            sample = sample_data.get('data', [])
            
            insights = self.llm_service.analyze_schema(table_name, columns)
            
            # Add sample data analysis
            if sample:
                sample_prompt = f"""
                Based on this sample data from table {table_name}:
                {json.dumps(sample[:5], indent=2)}
                
                Provide additional insights about:
                1. Data patterns and trends
                2. Potential data quality issues
                3. Business context and usage
                4. Recommended monitoring approaches
                
                Return insights as descriptive text.
                """
                
                sample_insights = self.llm_service.complete(sample_prompt)
                insights['sample_analysis'] = sample_insights
            
            return insights
            
        except Exception as e:
            self.logger.warning(f"AI insights generation failed for {table_name}: {str(e)}")
            return {'enabled': True, 'error': str(e)}
    
    def _calculate_table_stats(self, table_name: str, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate basic table statistics"""
        try:
            df = self.spark.table(table_name)
            
            stats = {
                'row_count': df.count(),
                'column_count': len(df.columns),
                'estimated_size_mb': 0,  # Would need to calculate from storage
                'partition_count': 0,
                'last_modified': None
            }
            
            # Calculate null percentages for each column
            null_stats = {}
            for col_name in df.columns:
                if col_name not in self.config.get('exclude_columns', []):
                    null_count = df.filter(df[col_name].isNull()).count()
                    null_percentage = (null_count / stats['row_count']) * 100 if stats['row_count'] > 0 else 0
                    null_stats[col_name] = {
                        'null_count': null_count,
                        'null_percentage': round(null_percentage, 2)
                    }
            
            stats['null_statistics'] = null_stats
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to calculate table stats for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def get_table_relationships(self, table_name: str) -> Dict[str, Any]:
        """Discover potential relationships with other tables"""
        try:
            # This would require analyzing foreign key patterns
            # For now, return a placeholder
            return {
                'relationships': [],
                'potential_joins': [],
                'dependency_score': 0
            }
        except Exception as e:
            self.logger.error(f"Failed to get relationships for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def validate_table_access(self, table_name: str) -> bool:
        """Validate that the table is accessible"""
        try:
            df = self.spark.table(table_name)
            df.count()  # Trigger action to check access
            return True
        except Exception as e:
            self.logger.error(f"Table access validation failed for {table_name}: {str(e)}")
            return False