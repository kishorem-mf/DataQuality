"""
MetricComputationAgent
Computes data quality metrics with adaptive thresholds and AI-powered insights.
"""

import json
import uuid
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType

from .base_agent import AIEnabledAgent
from services.llm_service import LLMService

class MetricComputationAgent(AIEnabledAgent):
    """
    Agent responsible for computing data quality metrics with adaptive thresholds.
    Converts procedural metric calculation logic into intelligent, context-aware processing.
    """
    
    def __init__(self, config: Dict[str, Any], llm_service: LLMService):
        super().__init__("MetricComputationAgent", config, llm_service)
        self.spark = self._get_spark_session()
        
    def _get_spark_session(self) -> SparkSession:
        """Get or create Spark session"""
        try:
            return SparkSession.getActiveSession()
        except:
            return SparkSession.builder.appName("DataQualityMetricComputation").getOrCreate()
    
    def _validate_inputs(self, **kwargs) -> None:
        """Validate input parameters"""
        required_params = ['table_name', 'schema_info']
        for param in required_params:
            if param not in kwargs:
                raise ValueError(f"{param} is required")
    
    def _execute_logic(self, **kwargs) -> Dict[str, Any]:
        """Main metric computation logic"""
        table_name = kwargs['table_name']
        schema_info = kwargs['schema_info']
        metrics_config = kwargs.get('metrics_config', {})
        
        self.logger.info(f"Starting metric computation for {table_name}")
        
        # Get table data
        df = self.spark.table(table_name)
        
        # Apply sampling if configured
        sampling_rate = self.config.get('sampling_rate', 0.01)
        if sampling_rate < 1.0:
            df = df.sample(withReplacement=False, fraction=sampling_rate, seed=42)
            self.logger.info(f"Applied sampling rate {sampling_rate} to {table_name}")
        
        # Get column classifications
        column_classification = schema_info.get('column_classification', {})
        
        # Compute metrics
        metrics_results = {}
        
        # Completeness metrics
        if 'completeness' in metrics_config.get('metrics', []):
            completeness_results = self._compute_completeness_metrics(
                df, table_name, column_classification, metrics_config
            )
            metrics_results['completeness'] = completeness_results
        
        # Uniqueness metrics
        if 'uniqueness' in metrics_config.get('metrics', []):
            uniqueness_results = self._compute_uniqueness_metrics(
                df, table_name, column_classification, metrics_config
            )
            metrics_results['uniqueness'] = uniqueness_results
        
        # Validity metrics
        if 'validity' in metrics_config.get('metrics', []):
            validity_results = self._compute_validity_metrics(
                df, table_name, column_classification, metrics_config
            )
            metrics_results['validity'] = validity_results
        
        # Consistency metrics
        if 'consistency' in metrics_config.get('metrics', []):
            consistency_results = self._compute_consistency_metrics(
                df, table_name, column_classification, metrics_config
            )
            metrics_results['consistency'] = consistency_results
        
        # Accuracy metrics
        if 'accuracy' in metrics_config.get('metrics', []):
            accuracy_results = self._compute_accuracy_metrics(
                df, table_name, column_classification, metrics_config
            )
            metrics_results['accuracy'] = accuracy_results
        
        # Timeliness metrics
        if 'timeliness' in metrics_config.get('metrics', []):
            timeliness_results = self._compute_timeliness_metrics(
                df, table_name, column_classification, metrics_config
            )
            metrics_results['timeliness'] = timeliness_results
        
        # Generate adaptive thresholds using AI
        adaptive_thresholds = self._generate_adaptive_thresholds(
            table_name, metrics_results, schema_info
        )
        
        # Generate quality insights
        quality_insights = self._generate_quality_insights(
            table_name, metrics_results, adaptive_thresholds
        )
        
        return {
            'table_name': table_name,
            'metrics': metrics_results,
            'adaptive_thresholds': adaptive_thresholds,
            'quality_insights': quality_insights,
            'computation_metadata': {
                'sampling_rate': sampling_rate,
                'computed_at': datetime.now().isoformat(),
                'row_count': df.count(),
                'column_count': len(df.columns)
            }
        }
    
    def _compute_completeness_metrics(self, df, table_name: str, 
                                    column_classification: Dict[str, Any], 
                                    metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Compute completeness metrics (null percentage)"""
        self.logger.info(f"Computing completeness metrics for {table_name}")
        
        completeness_results = {}
        total_count = df.count()
        
        # Get columns to analyze
        columns_to_analyze = metrics_config.get('columns', {}).get('completeness', [])
        if not columns_to_analyze:
            # Use all columns if not specified
            columns_to_analyze = df.columns
        
        # Exclude system columns
        exclude_columns = self.config.get('exclude_columns', ['ingest_date', 'report_date'])
        columns_to_analyze = [col for col in columns_to_analyze if col not in exclude_columns]
        
        for column in columns_to_analyze:
            try:
                # Calculate null count
                null_count = df.filter(df[column].isNull()).count()
                
                # Calculate completeness percentage
                completeness_percentage = ((total_count - null_count) / total_count) * 100 if total_count > 0 else 0
                
                completeness_results[column] = {
                    'completeness_percentage': round(completeness_percentage, 2),
                    'null_count': null_count,
                    'total_count': total_count,
                    'non_null_count': total_count - null_count
                }
                
            except Exception as e:
                self.logger.error(f"Failed to compute completeness for {column}: {str(e)}")
                completeness_results[column] = {'error': str(e)}
        
        return completeness_results
    
    def _compute_uniqueness_metrics(self, df, table_name: str, 
                                  column_classification: Dict[str, Any], 
                                  metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Compute uniqueness metrics (duplicate percentage)"""
        self.logger.info(f"Computing uniqueness metrics for {table_name}")
        
        uniqueness_results = {}
        total_count = df.count()
        
        # Get columns to analyze
        columns_to_analyze = metrics_config.get('columns', {}).get('uniqueness', [])
        if not columns_to_analyze:
            # Use key columns if available
            columns_to_analyze = column_classification.get('key_columns', [])
        
        for column in columns_to_analyze:
            try:
                # Calculate distinct count
                distinct_count = df.select(column).distinct().count()
                
                # Calculate uniqueness percentage
                uniqueness_percentage = (distinct_count / total_count) * 100 if total_count > 0 else 0
                
                # Calculate duplicate count
                duplicate_count = total_count - distinct_count
                
                uniqueness_results[column] = {
                    'uniqueness_percentage': round(uniqueness_percentage, 2),
                    'distinct_count': distinct_count,
                    'duplicate_count': duplicate_count,
                    'total_count': total_count
                }
                
            except Exception as e:
                self.logger.error(f"Failed to compute uniqueness for {column}: {str(e)}")
                uniqueness_results[column] = {'error': str(e)}
        
        return uniqueness_results
    
    def _compute_validity_metrics(self, df, table_name: str, 
                                column_classification: Dict[str, Any], 
                                metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Compute validity metrics (outlier detection)"""
        self.logger.info(f"Computing validity metrics for {table_name}")
        
        validity_results = {}
        
        # Get numerical columns for outlier detection
        numerical_columns = column_classification.get('numerical', [])
        categorical_columns = column_classification.get('categorical', [])
        
        # Outlier detection for numerical columns
        for column in numerical_columns:
            try:
                validity_results[column] = self._detect_numerical_outliers(df, column)
            except Exception as e:
                self.logger.error(f"Failed to compute validity for {column}: {str(e)}")
                validity_results[column] = {'error': str(e)}
        
        # Rare category detection for categorical columns
        for column in categorical_columns:
            try:
                validity_results[column] = self._detect_rare_categories(df, column)
            except Exception as e:
                self.logger.error(f"Failed to compute validity for {column}: {str(e)}")
                validity_results[column] = {'error': str(e)}
        
        return validity_results
    
    def _detect_numerical_outliers(self, df, column: str) -> Dict[str, Any]:
        """Detect outliers in numerical columns using percentile method"""
        total_count = df.count()
        
        # Calculate percentiles
        percentile_threshold = self.config.get('percentile_threshold', 0.99)
        stats = df.approxQuantile(column, [percentile_threshold], 0.01)
        
        if not stats or len(stats) == 0:
            return {'error': 'Unable to calculate percentiles'}
        
        percentile_value = stats[0]
        
        # Count outliers (values above percentile)
        outlier_count = df.filter(df[column] > percentile_value).count()
        outlier_percentage = (outlier_count / total_count) * 100 if total_count > 0 else 0
        
        # Calculate additional statistics
        zero_count = df.filter(df[column] == 0).count()
        negative_count = df.filter(df[column] < 0).count()
        
        # Classify validity
        if outlier_percentage <= 1:
            validity_level = "High"
        elif 1 < outlier_percentage <= 5:
            validity_level = "Medium"
        else:
            validity_level = "Low"
        
        return {
            'validity_percentage': round(100 - outlier_percentage, 2),
            'outlier_count': outlier_count,
            'outlier_percentage': round(outlier_percentage, 2),
            'percentile_threshold': percentile_threshold,
            'percentile_value': percentile_value,
            'zero_count': zero_count,
            'negative_count': negative_count,
            'validity_level': validity_level
        }
    
    def _detect_rare_categories(self, df, column: str) -> Dict[str, Any]:
        """Detect rare categories in categorical columns"""
        total_count = df.count()
        
        # Calculate frequency threshold
        frequency_threshold = self.config.get('rare_category_threshold', 0.01)
        
        # Calculate category frequencies
        category_counts = df.groupBy(column).count().withColumn(
            "frequency", F.col("count") / total_count
        )
        
        # Find rare categories
        rare_categories_df = category_counts.filter(F.col("frequency") < frequency_threshold)
        rare_categories = [row[column] for row in rare_categories_df.collect()]
        rare_count = rare_categories_df.count()
        
        # Calculate percentage of records with rare categories
        rare_records_count = df.filter(F.col(column).isin(rare_categories)).count()
        rare_percentage = (rare_records_count / total_count) * 100 if total_count > 0 else 0
        
        # Classify validity
        if rare_percentage < 1:
            validity_level = "High"
        elif 1 <= rare_percentage <= 5:
            validity_level = "Medium"
        else:
            validity_level = "Low"
        
        return {
            'validity_percentage': round(100 - rare_percentage, 2),
            'rare_categories_count': rare_count,
            'rare_categories': rare_categories[:10],  # Limit to first 10
            'rare_records_count': rare_records_count,
            'rare_percentage': round(rare_percentage, 2),
            'frequency_threshold': frequency_threshold,
            'validity_level': validity_level
        }
    
    def _compute_consistency_metrics(self, df, table_name: str, 
                                   column_classification: Dict[str, Any], 
                                   metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Compute consistency metrics (format and pattern validation)"""
        self.logger.info(f"Computing consistency metrics for {table_name}")
        
        consistency_results = {}
        
        # Get columns to analyze
        columns_to_analyze = metrics_config.get('columns', {}).get('consistency', [])
        if not columns_to_analyze:
            # Use text columns by default
            columns_to_analyze = column_classification.get('text', [])
        
        for column in columns_to_analyze:
            try:
                consistency_results[column] = self._analyze_format_consistency(df, column)
            except Exception as e:
                self.logger.error(f"Failed to compute consistency for {column}: {str(e)}")
                consistency_results[column] = {'error': str(e)}
        
        return consistency_results
    
    def _analyze_format_consistency(self, df, column: str) -> Dict[str, Any]:
        """Analyze format consistency for text columns"""
        total_count = df.count()
        
        # Basic format analysis
        length_stats = df.select(F.length(F.col(column)).alias("length")).describe()
        
        # Pattern analysis (simplified)
        non_null_df = df.filter(F.col(column).isNotNull())
        
        # Check for common patterns
        email_pattern_count = non_null_df.filter(F.col(column).rlike(".*@.*\\..*")).count()
        phone_pattern_count = non_null_df.filter(F.col(column).rlike("\\d{3}-\\d{3}-\\d{4}")).count()
        
        # Calculate consistency score (simplified)
        consistency_score = 85.0  # Placeholder - would need more sophisticated analysis
        
        return {
            'consistency_percentage': consistency_score,
            'total_count': total_count,
            'email_pattern_count': email_pattern_count,
            'phone_pattern_count': phone_pattern_count,
            'consistency_level': 'High' if consistency_score > 90 else 'Medium' if consistency_score > 70 else 'Low'
        }
    
    def _compute_accuracy_metrics(self, df, table_name: str, 
                                column_classification: Dict[str, Any], 
                                metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Compute accuracy metrics (referential integrity and range validation)"""
        self.logger.info(f"Computing accuracy metrics for {table_name}")
        
        accuracy_results = {}
        
        # Get columns to analyze
        columns_to_analyze = metrics_config.get('columns', {}).get('accuracy', [])
        
        for column in columns_to_analyze:
            try:
                # Basic accuracy check - this would need to be enhanced with business rules
                accuracy_results[column] = self._check_column_accuracy(df, column)
            except Exception as e:
                self.logger.error(f"Failed to compute accuracy for {column}: {str(e)}")
                accuracy_results[column] = {'error': str(e)}
        
        return accuracy_results
    
    def _check_column_accuracy(self, df, column: str) -> Dict[str, Any]:
        """Check accuracy for a specific column"""
        total_count = df.count()
        
        # Basic accuracy checks
        non_null_count = df.filter(F.col(column).isNotNull()).count()
        
        # Placeholder accuracy calculation
        accuracy_score = (non_null_count / total_count) * 100 if total_count > 0 else 0
        
        return {
            'accuracy_percentage': round(accuracy_score, 2),
            'accurate_count': non_null_count,
            'total_count': total_count,
            'accuracy_level': 'High' if accuracy_score > 95 else 'Medium' if accuracy_score > 85 else 'Low'
        }
    
    def _compute_timeliness_metrics(self, df, table_name: str, 
                                  column_classification: Dict[str, Any], 
                                  metrics_config: Dict[str, Any]) -> Dict[str, Any]:
        """Compute timeliness metrics (data freshness)"""
        self.logger.info(f"Computing timeliness metrics for {table_name}")
        
        timeliness_results = {}
        
        # Get temporal columns
        temporal_columns = column_classification.get('temporal', [])
        
        for column in temporal_columns:
            try:
                timeliness_results[column] = self._analyze_data_freshness(df, column)
            except Exception as e:
                self.logger.error(f"Failed to compute timeliness for {column}: {str(e)}")
                timeliness_results[column] = {'error': str(e)}
        
        return timeliness_results
    
    def _analyze_data_freshness(self, df, column: str) -> Dict[str, Any]:
        """Analyze data freshness for temporal columns"""
        # Get min and max dates
        min_max_result = df.select(
            F.min(column).alias("min_date"),
            F.max(column).alias("max_date")
        ).collect()[0]
        
        min_date = min_max_result["min_date"]
        max_date = min_max_result["max_date"]
        
        # Calculate freshness score (simplified)
        current_date = datetime.now()
        
        if max_date:
            days_since_latest = (current_date - max_date).days if hasattr(max_date, 'date') else 0
            freshness_score = max(0, 100 - days_since_latest)
        else:
            freshness_score = 0
        
        return {
            'timeliness_percentage': round(freshness_score, 2),
            'min_date': str(min_date) if min_date else None,
            'max_date': str(max_date) if max_date else None,
            'days_since_latest': days_since_latest if max_date else None,
            'freshness_level': 'High' if freshness_score > 90 else 'Medium' if freshness_score > 70 else 'Low'
        }
    
    def _generate_adaptive_thresholds(self, table_name: str, metrics_results: Dict[str, Any], 
                                    schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate adaptive thresholds using AI"""
        if not self.config.get('use_adaptive_thresholds', True):
            return {'enabled': False}
        
        try:
            prompt = f"""
            Based on the following data quality metrics for table {table_name}, 
            suggest adaptive thresholds for monitoring:
            
            Metrics Results: {json.dumps(metrics_results, indent=2, default=str)}
            
            Consider:
            1. Current data quality levels
            2. Business criticality of different metrics
            3. Historical patterns and trends
            4. Industry best practices
            
            Provide adaptive thresholds in JSON format with:
            - completeness_threshold: Minimum acceptable completeness percentage
            - uniqueness_threshold: Minimum acceptable uniqueness percentage
            - validity_threshold: Minimum acceptable validity percentage
            - consistency_threshold: Minimum acceptable consistency percentage
            - accuracy_threshold: Minimum acceptable accuracy percentage
            - timeliness_threshold: Minimum acceptable timeliness percentage
            
            Also provide reasoning for each threshold.
            """
            
            adaptive_thresholds = self.llm_service.parse_json_response(prompt)
            return adaptive_thresholds
            
        except Exception as e:
            self.logger.warning(f"Failed to generate adaptive thresholds: {str(e)}")
            return {'enabled': False, 'error': str(e)}
    
    def _generate_quality_insights(self, table_name: str, metrics_results: Dict[str, Any], 
                                 adaptive_thresholds: Dict[str, Any]) -> Dict[str, Any]:
        """Generate quality insights using AI"""
        try:
            prompt = f"""
            Analyze the data quality metrics for table {table_name} and provide insights:
            
            Metrics: {json.dumps(metrics_results, indent=2, default=str)}
            Adaptive Thresholds: {json.dumps(adaptive_thresholds, indent=2, default=str)}
            
            Provide insights about:
            1. Overall data quality assessment
            2. Critical issues requiring attention
            3. Columns with quality problems
            4. Recommended actions
            5. Business impact assessment
            
            Format as descriptive text with actionable recommendations.
            """
            
            insights = self.llm_service.complete(prompt)
            
            return {
                'ai_insights': insights,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to generate quality insights: {str(e)}")
            return {'error': str(e)}