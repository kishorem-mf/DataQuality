"""
ResultsWriterAgent
Manages writing data quality results to Delta tables with optimized performance.
"""

import json
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from delta.tables import DeltaTable

from .base_agent import AIEnabledAgent
from services.llm_service import LLMService

class ResultsWriterAgent(AIEnabledAgent):
    """
    Agent responsible for writing data quality results to Delta tables.
    Handles schema management, data transformation, and performance optimization.
    """
    
    def __init__(self, config: Dict[str, Any], llm_service: LLMService):
        super().__init__("ResultsWriterAgent", config, llm_service)
        self.spark = self._get_spark_session()
        self.results_schema = self._define_results_schema()
        
    def _get_spark_session(self) -> SparkSession:
        """Get or create Spark session"""
        try:
            return SparkSession.getActiveSession()
        except:
            return SparkSession.builder.appName("DataQualityResultsWriter").getOrCreate()
    
    def _define_results_schema(self) -> StructType:
        """Define the schema for the results Delta table"""
        return StructType([
            StructField("run_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), True),
            StructField("metric_name", StringType(), False),
            StructField("metric_value", DoubleType(), True),
            StructField("metric_value_str", StringType(), True),
            StructField("threshold_value", DoubleType(), True),
            StructField("status", StringType(), False),  # PASS, FAIL, WARNING
            StructField("run_timestamp", TimestampType(), False),
            StructField("run_date", StringType(), False),  # Partition column
            StructField("metadata", StringType(), True),  # JSON string for additional info
            StructField("insights", StringType(), True),  # AI-generated insights
            StructField("recommendations", StringType(), True)  # AI-generated recommendations
        ])
    
    def _validate_inputs(self, **kwargs) -> None:
        """Validate input parameters"""
        if 'metrics_results' not in kwargs:
            raise ValueError("metrics_results is required")
    
    def _execute_logic(self, **kwargs) -> Dict[str, Any]:
        """Main results writing logic"""
        metrics_results = kwargs['metrics_results']
        run_id = kwargs.get('run_id', str(uuid.uuid4()))
        
        self.logger.info(f"Starting results writing for run {run_id}")
        
        # Transform metrics results to Delta table format
        transformed_data = self._transform_metrics_data(metrics_results, run_id)
        
        # Create DataFrame from transformed data
        results_df = self._create_results_dataframe(transformed_data)
        
        # Write to Delta table
        write_result = self._write_to_delta_table(results_df)
        
        # Optimize table if configured
        if self.config.get('optimize_after_write', True):
            self._optimize_delta_table()
        
        # Generate write summary
        write_summary = self._generate_write_summary(write_result, len(transformed_data))
        
        return {
            'run_id': run_id,
            'records_written': len(transformed_data),
            'tables_processed': len(metrics_results),
            'write_result': write_result,
            'write_summary': write_summary,
            'delta_table_path': self._get_delta_table_path()
        }
    
    def _transform_metrics_data(self, metrics_results: Dict[str, Any], run_id: str) -> List[Dict[str, Any]]:
        """Transform metrics results into Delta table format"""
        transformed_records = []
        current_time = datetime.now()
        run_date = current_time.date().isoformat()
        
        for table_name, table_results in metrics_results.items():
            if 'error' in table_results:
                # Handle error cases
                error_record = {
                    'run_id': run_id,
                    'table_name': table_name,
                    'column_name': None,
                    'metric_name': 'table_processing_error',
                    'metric_value': None,
                    'metric_value_str': table_results['error'],
                    'threshold_value': None,
                    'status': 'FAIL',
                    'run_timestamp': current_time,
                    'run_date': run_date,
                    'metadata': json.dumps({'error': table_results['error']}),
                    'insights': None,
                    'recommendations': None
                }
                transformed_records.append(error_record)
                continue
            
            # Process metrics for each table
            metrics_data = table_results.get('metrics', {})
            adaptive_thresholds = table_results.get('adaptive_thresholds', {})
            quality_insights = table_results.get('quality_insights', {})
            
            # Process each metric type
            for metric_type, metric_results in metrics_data.items():
                if isinstance(metric_results, dict):
                    for column_name, column_metrics in metric_results.items():
                        if isinstance(column_metrics, dict) and 'error' not in column_metrics:
                            # Transform column metrics
                            records = self._transform_column_metrics(
                                run_id, table_name, column_name, metric_type, 
                                column_metrics, adaptive_thresholds, quality_insights,
                                current_time, run_date
                            )
                            transformed_records.extend(records)
        
        return transformed_records
    
    def _transform_column_metrics(self, run_id: str, table_name: str, column_name: str, 
                                 metric_type: str, column_metrics: Dict[str, Any], 
                                 adaptive_thresholds: Dict[str, Any], 
                                 quality_insights: Dict[str, Any],
                                 current_time: datetime, run_date: str) -> List[Dict[str, Any]]:
        """Transform column metrics to individual records"""
        records = []
        
        # Map metric types to specific metric names
        metric_mappings = {
            'completeness': ['completeness_percentage', 'null_count', 'non_null_count'],
            'uniqueness': ['uniqueness_percentage', 'distinct_count', 'duplicate_count'],
            'validity': ['validity_percentage', 'outlier_count', 'outlier_percentage'],
            'consistency': ['consistency_percentage'],
            'accuracy': ['accuracy_percentage'],
            'timeliness': ['timeliness_percentage', 'days_since_latest']
        }
        
        metrics_to_extract = metric_mappings.get(metric_type, [])
        
        for metric_name in metrics_to_extract:
            if metric_name in column_metrics:
                metric_value = column_metrics[metric_name]
                
                # Convert metric value to float if possible
                try:
                    metric_value_float = float(metric_value) if metric_value is not None else None
                except (ValueError, TypeError):
                    metric_value_float = None
                
                # Get threshold value
                threshold_key = f"{metric_type}_threshold"
                threshold_value = adaptive_thresholds.get(threshold_key) if adaptive_thresholds else None
                
                # Determine status
                status = self._determine_status(metric_name, metric_value_float, threshold_value)
                
                # Create record
                record = {
                    'run_id': run_id,
                    'table_name': table_name,
                    'column_name': column_name,
                    'metric_name': metric_name,
                    'metric_value': metric_value_float,
                    'metric_value_str': str(metric_value) if metric_value is not None else None,
                    'threshold_value': threshold_value,
                    'status': status,
                    'run_timestamp': current_time,
                    'run_date': run_date,
                    'metadata': json.dumps({
                        'metric_type': metric_type,
                        'full_metrics': column_metrics,
                        'computation_metadata': {}
                    }),
                    'insights': quality_insights.get('ai_insights', '') if quality_insights else None,
                    'recommendations': self._generate_recommendations(metric_name, metric_value_float, status)
                }
                
                records.append(record)
        
        return records
    
    def _determine_status(self, metric_name: str, metric_value: Optional[float], 
                         threshold_value: Optional[float]) -> str:
        """Determine the status (PASS/FAIL/WARNING) based on metric value and threshold"""
        if metric_value is None:
            return 'FAIL'
        
        if threshold_value is None:
            # Use default thresholds
            default_thresholds = {
                'completeness_percentage': 95.0,
                'uniqueness_percentage': 95.0,
                'validity_percentage': 90.0,
                'consistency_percentage': 90.0,
                'accuracy_percentage': 95.0,
                'timeliness_percentage': 85.0
            }
            threshold_value = default_thresholds.get(metric_name, 90.0)
        
        # For percentage metrics, higher is better
        if 'percentage' in metric_name:
            if metric_value >= threshold_value:
                return 'PASS'
            elif metric_value >= threshold_value * 0.8:  # 80% of threshold
                return 'WARNING'
            else:
                return 'FAIL'
        
        # For count metrics, lower is usually better (like outlier_count)
        if 'count' in metric_name and 'outlier' in metric_name:
            if metric_value <= threshold_value:
                return 'PASS'
            elif metric_value <= threshold_value * 1.2:  # 20% above threshold
                return 'WARNING'
            else:
                return 'FAIL'
        
        # Default case
        return 'PASS'
    
    def _generate_recommendations(self, metric_name: str, metric_value: Optional[float], 
                                status: str) -> str:
        """Generate recommendations based on metric performance"""
        if status == 'PASS':
            return f"Good quality: {metric_name} is within acceptable range"
        
        recommendations = {
            'completeness_percentage': 'Consider investigating null value sources and implementing data validation',
            'uniqueness_percentage': 'Check for duplicate data sources and implement deduplication logic',
            'validity_percentage': 'Review outlier detection rules and data entry processes',
            'consistency_percentage': 'Standardize data formats and implement validation rules',
            'accuracy_percentage': 'Verify data source accuracy and implement quality checks',
            'timeliness_percentage': 'Review data pipeline schedules and implement freshness monitoring'
        }
        
        base_recommendation = recommendations.get(metric_name, 'Review data quality processes')
        
        if status == 'WARNING':
            return f"Attention needed: {base_recommendation}"
        else:  # FAIL
            return f"Critical issue: {base_recommendation}"
    
    def _create_results_dataframe(self, transformed_data: List[Dict[str, Any]]):
        """Create Spark DataFrame from transformed data"""
        if not transformed_data:
            # Create empty DataFrame with schema
            return self.spark.createDataFrame([], self.results_schema)
        
        # Create DataFrame
        df = self.spark.createDataFrame(transformed_data, self.results_schema)
        
        # Add any additional transformations
        df = df.withColumn("run_timestamp", F.current_timestamp())
        
        return df
    
    def _write_to_delta_table(self, results_df) -> Dict[str, Any]:
        """Write results to Delta table"""
        try:
            delta_table_path = self._get_delta_table_path()
            
            # Check if table exists
            if self._delta_table_exists():
                # Append to existing table
                results_df.write \
                    .format("delta") \
                    .mode("append") \
                    .partitionBy("run_date") \
                    .save(delta_table_path)
                
                write_mode = "append"
            else:
                # Create new table
                results_df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .partitionBy("run_date") \
                    .save(delta_table_path)
                
                write_mode = "create"
            
            # Get write statistics
            row_count = results_df.count()
            
            self.logger.info(f"Successfully wrote {row_count} records to Delta table")
            
            return {
                'status': 'success',
                'write_mode': write_mode,
                'rows_written': row_count,
                'delta_table_path': delta_table_path
            }
            
        except Exception as e:
            self.logger.error(f"Failed to write to Delta table: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def _get_delta_table_path(self) -> str:
        """Get the Delta table path"""
        table_name = self.config.get('delta_table_name', 'dq_metrics.daily_results')
        
        # If it's a full table name (catalog.schema.table), use it directly
        if '.' in table_name:
            return f"/tmp/delta/{table_name.replace('.', '/')}"
        else:
            return f"/tmp/delta/{table_name}"
    
    def _delta_table_exists(self) -> bool:
        """Check if Delta table exists"""
        try:
            delta_table_path = self._get_delta_table_path()
            DeltaTable.forPath(self.spark, delta_table_path)
            return True
        except:
            return False
    
    def _optimize_delta_table(self) -> None:
        """Optimize Delta table for better performance"""
        try:
            delta_table_path = self._get_delta_table_path()
            
            # Optimize table
            self.spark.sql(f"OPTIMIZE delta.`{delta_table_path}`")
            
            # Vacuum old files if configured
            vacuum_retention_hours = self.config.get('vacuum_retention_hours', 168)  # 7 days
            self.spark.sql(f"VACUUM delta.`{delta_table_path}` RETAIN {vacuum_retention_hours} HOURS")
            
            self.logger.info("Delta table optimization completed")
            
        except Exception as e:
            self.logger.warning(f"Delta table optimization failed: {str(e)}")
    
    def _generate_write_summary(self, write_result: Dict[str, Any], records_count: int) -> str:
        """Generate human-readable write summary"""
        if write_result['status'] == 'success':
            return f"""
            ✅ Data Quality Results Written Successfully
            
            📊 Records: {records_count}
            📁 Table: {write_result['delta_table_path']}
            🔄 Mode: {write_result['write_mode']}
            
            Results are now available for querying and analysis.
            """
        else:
            return f"""
            ❌ Data Quality Results Write Failed
            
            Error: {write_result.get('error', 'Unknown error')}
            
            Please check the logs for more details.
            """
    
    def query_results(self, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Query results from Delta table"""
        try:
            delta_table_path = self._get_delta_table_path()
            
            if not self._delta_table_exists():
                return {'error': 'Results table does not exist'}
            
            # Read from Delta table
            df = self.spark.read.format("delta").load(delta_table_path)
            
            # Apply filters if provided
            if filters:
                if 'table_name' in filters:
                    df = df.filter(F.col('table_name') == filters['table_name'])
                if 'run_date' in filters:
                    df = df.filter(F.col('run_date') == filters['run_date'])
                if 'status' in filters:
                    df = df.filter(F.col('status') == filters['status'])
            
            # Get sample results
            results = df.limit(100).collect()
            
            return {
                'status': 'success',
                'record_count': df.count(),
                'sample_results': [row.asDict() for row in results]
            }
            
        except Exception as e:
            self.logger.error(f"Failed to query results: {str(e)}")
            return {'error': str(e)}
    
    def get_table_summary(self) -> Dict[str, Any]:
        """Get summary statistics from results table"""
        try:
            delta_table_path = self._get_delta_table_path()
            
            if not self._delta_table_exists():
                return {'error': 'Results table does not exist'}
            
            df = self.spark.read.format("delta").load(delta_table_path)
            
            # Calculate summary statistics
            summary = df.groupBy('table_name', 'status').count().collect()
            
            # Get latest run information
            latest_run = df.select('run_id', 'run_timestamp').orderBy(F.desc('run_timestamp')).first()
            
            return {
                'status': 'success',
                'total_records': df.count(),
                'summary_by_table_and_status': [row.asDict() for row in summary],
                'latest_run': latest_run.asDict() if latest_run else None
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table summary: {str(e)}")
            return {'error': str(e)}