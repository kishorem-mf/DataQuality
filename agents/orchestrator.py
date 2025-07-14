"""
DataQualityOrchestrator Agent
Main coordinator that manages the entire data quality assessment workflow.
"""

import json
import time
import concurrent.futures
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass

from .base_agent import AIEnabledAgent, AgentResult, AgentStatus
from services.llm_service import LLMService

@dataclass
class DataQualityJob:
    """Represents a data quality assessment job"""
    job_id: str
    tables: List[str]
    metrics: Dict[str, Any]
    config: Dict[str, Any]
    status: str = "pending"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    results: Optional[Dict[str, Any]] = None

class DataQualityOrchestrator(AIEnabledAgent):
    """
    Main orchestrator agent that coordinates all data quality assessment activities.
    Manages workflow execution, resource allocation, and result aggregation.
    """
    
    def __init__(self, config: Dict[str, Any], llm_service: LLMService):
        super().__init__("DataQualityOrchestrator", config, llm_service)
        self.active_jobs: Dict[str, DataQualityJob] = {}
        self.agent_registry: Dict[str, Any] = {}
        
    def _execute_logic(self, **kwargs) -> Dict[str, Any]:
        """Main orchestration logic"""
        
        # Get job parameters
        tables_config = kwargs.get('tables_config', {})
        metrics_config = kwargs.get('metrics_config', {})
        job_type = kwargs.get('job_type', 'full_assessment')
        
        # Create job
        job = self._create_job(tables_config, metrics_config, job_type)
        
        try:
            # Execute job based on type
            if job_type == 'full_assessment':
                results = self._execute_full_assessment(job)
            elif job_type == 'nlp_query':
                results = self._execute_nlp_query(job, kwargs.get('query', ''))
            elif job_type == 'single_table':
                results = self._execute_single_table_assessment(job, kwargs.get('table_name', ''))
            else:
                raise ValueError(f"Unknown job type: {job_type}")
            
            # Finalize job
            job.status = "completed"
            job.end_time = datetime.now()
            job.results = results
            
            return {
                'job_id': job.job_id,
                'status': job.status,
                'execution_time': (job.end_time - job.start_time).total_seconds(),
                'results': results,
                'summary': self._generate_summary(results)
            }
            
        except Exception as e:
            job.status = "failed"
            job.end_time = datetime.now()
            raise
    
    def _create_job(self, tables_config: Dict[str, Any], metrics_config: Dict[str, Any], job_type: str) -> DataQualityJob:
        """Create a new data quality job"""
        job_id = f"dq_job_{int(time.time())}"
        
        job = DataQualityJob(
            job_id=job_id,
            tables=tables_config.get('tables', []),
            metrics=metrics_config,
            config={
                'tables_config': tables_config,
                'metrics_config': metrics_config,
                'job_type': job_type
            },
            start_time=datetime.now()
        )
        
        self.active_jobs[job_id] = job
        self.logger.info(f"Created job {job_id} for {len(job.tables)} tables")
        
        return job
    
    def _execute_full_assessment(self, job: DataQualityJob) -> Dict[str, Any]:
        """Execute full data quality assessment for all tables"""
        self.logger.info(f"Starting full assessment for job {job.job_id}")
        
        # Plan execution strategy
        execution_plan = self._create_execution_plan(job)
        
        # Execute table discovery
        discovery_results = self._execute_table_discovery(job.tables)
        
        # Execute metric computation
        metrics_results = self._execute_metric_computation(job.tables, job.metrics, discovery_results)
        
        # Write results
        write_results = self._execute_results_writing(metrics_results)
        
        # Generate insights
        insights = self._generate_ai_insights(metrics_results)
        
        return {
            'execution_plan': execution_plan,
            'discovery_results': discovery_results,
            'metrics_results': metrics_results,
            'write_results': write_results,
            'insights': insights,
            'summary_stats': self._calculate_summary_stats(metrics_results)
        }
    
    def _execute_nlp_query(self, job: DataQualityJob, query: str) -> Dict[str, Any]:
        """Execute natural language query"""
        self.logger.info(f"Processing NLP query for job {job.job_id}: {query}")
        
        # Import here to avoid circular dependencies
        from .nlp_query import NLPQueryAgent
        
        nlp_agent = NLPQueryAgent(self.config.get('nlp_query', {}), self.llm_service)
        
        # Execute query
        nlp_result = nlp_agent.execute(
            query=query,
            available_tables=job.tables,
            metrics_config=job.metrics
        )
        
        return {
            'query': query,
            'nlp_result': nlp_result.data,
            'execution_time': nlp_result.execution_time
        }
    
    def _execute_single_table_assessment(self, job: DataQualityJob, table_name: str) -> Dict[str, Any]:
        """Execute assessment for a single table"""
        self.logger.info(f"Starting single table assessment for {table_name}")
        
        if table_name not in job.tables:
            raise ValueError(f"Table {table_name} not found in job configuration")
        
        # Execute for single table
        discovery_results = self._execute_table_discovery([table_name])
        metrics_results = self._execute_metric_computation([table_name], job.metrics, discovery_results)
        
        return {
            'table_name': table_name,
            'discovery_results': discovery_results,
            'metrics_results': metrics_results,
            'insights': self._generate_ai_insights(metrics_results, focus_table=table_name)
        }
    
    def _create_execution_plan(self, job: DataQualityJob) -> Dict[str, Any]:
        """Create intelligent execution plan based on job requirements"""
        
        # Analyze job complexity
        complexity_prompt = f"""
        Analyze this data quality job and create an execution plan:
        
        Tables: {job.tables}
        Metrics Config: {json.dumps(job.metrics, indent=2)}
        
        Consider:
        1. Table dependencies and relationships
        2. Optimal processing order
        3. Resource allocation strategy
        4. Parallel processing opportunities
        5. Risk factors and mitigation
        
        Provide execution plan in JSON format with:
        - processing_order: List of tables in optimal order
        - parallel_groups: Groups of tables that can be processed in parallel
        - resource_allocation: Memory and compute recommendations
        - estimated_time: Estimated execution time in minutes
        - risk_factors: Potential issues and mitigation strategies
        """
        
        try:
            plan = self.llm_service.parse_json_response(complexity_prompt)
            self.logger.info(f"Generated execution plan with {len(plan.get('parallel_groups', []))} parallel groups")
            return plan
        except Exception as e:
            self.logger.warning(f"Failed to generate AI execution plan: {str(e)}, using default plan")
            return self._create_default_execution_plan(job)
    
    def _create_default_execution_plan(self, job: DataQualityJob) -> Dict[str, Any]:
        """Create default execution plan without AI"""
        return {
            'processing_order': job.tables,
            'parallel_groups': [job.tables[:5], job.tables[5:]] if len(job.tables) > 5 else [job.tables],
            'resource_allocation': {'memory': 'medium', 'cores': 4},
            'estimated_time': len(job.tables) * 2,
            'risk_factors': ['Large table processing', 'Memory constraints']
        }
    
    def _execute_table_discovery(self, tables: List[str]) -> Dict[str, Any]:
        """Execute table discovery for all tables"""
        self.logger.info(f"Starting table discovery for {len(tables)} tables")
        
        # Import here to avoid circular dependencies
        from .table_discovery import TableDiscoveryAgent
        
        discovery_agent = TableDiscoveryAgent(
            self.config.get('table_discovery', {}), 
            self.llm_service
        )
        
        results = {}
        
        # Process tables in parallel if configured
        if self.config.get('parallel_processing', True):
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                future_to_table = {
                    executor.submit(discovery_agent.execute, table_name=table): table 
                    for table in tables
                }
                
                for future in concurrent.futures.as_completed(future_to_table):
                    table = future_to_table[future]
                    try:
                        result = future.result()
                        results[table] = result.data
                    except Exception as e:
                        self.logger.error(f"Table discovery failed for {table}: {str(e)}")
                        results[table] = {'error': str(e)}
        else:
            # Sequential processing
            for table in tables:
                try:
                    result = discovery_agent.execute(table_name=table)
                    results[table] = result.data
                except Exception as e:
                    self.logger.error(f"Table discovery failed for {table}: {str(e)}")
                    results[table] = {'error': str(e)}
        
        return results
    
    def _execute_metric_computation(self, tables: List[str], metrics_config: Dict[str, Any], 
                                  discovery_results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute metric computation for all tables"""
        self.logger.info(f"Starting metric computation for {len(tables)} tables")
        
        # Import here to avoid circular dependencies
        from .metric_computation import MetricComputationAgent
        
        computation_agent = MetricComputationAgent(
            self.config.get('metric_computation', {}), 
            self.llm_service
        )
        
        results = {}
        
        for table in tables:
            if table in discovery_results and 'error' not in discovery_results[table]:
                try:
                    result = computation_agent.execute(
                        table_name=table,
                        schema_info=discovery_results[table],
                        metrics_config=metrics_config.get('table_specific_config', {}).get(table, {})
                    )
                    results[table] = result.data
                except Exception as e:
                    self.logger.error(f"Metric computation failed for {table}: {str(e)}")
                    results[table] = {'error': str(e)}
            else:
                self.logger.warning(f"Skipping metric computation for {table} due to discovery failure")
                results[table] = {'error': 'Table discovery failed'}
        
        return results
    
    def _execute_results_writing(self, metrics_results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute results writing to Delta tables"""
        self.logger.info("Starting results writing")
        
        # Import here to avoid circular dependencies
        from .results_writer import ResultsWriterAgent
        
        writer_agent = ResultsWriterAgent(
            self.config.get('results_writer', {}), 
            self.llm_service
        )
        
        try:
            result = writer_agent.execute(metrics_results=metrics_results)
            return result.data
        except Exception as e:
            self.logger.error(f"Results writing failed: {str(e)}")
            return {'error': str(e)}
    
    def _generate_ai_insights(self, metrics_results: Dict[str, Any], 
                            focus_table: Optional[str] = None) -> Dict[str, Any]:
        """Generate AI-powered insights from metrics results"""
        self.logger.info("Generating AI insights")
        
        try:
            # Prepare data for AI analysis
            if focus_table:
                analysis_data = {focus_table: metrics_results.get(focus_table, {})}
            else:
                analysis_data = metrics_results
            
            insights = self.llm_service.generate_quality_insights(analysis_data)
            
            return {
                'insights': insights,
                'focus_table': focus_table,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"AI insights generation failed: {str(e)}")
            return {
                'insights': 'AI insights generation failed',
                'error': str(e)
            }
    
    def _calculate_summary_stats(self, metrics_results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate summary statistics from metrics results"""
        stats = {
            'total_tables': len(metrics_results),
            'successful_assessments': 0,
            'failed_assessments': 0,
            'total_columns_assessed': 0,
            'average_completeness': 0.0,
            'tables_with_issues': 0
        }
        
        completeness_scores = []
        
        for table, results in metrics_results.items():
            if 'error' not in results:
                stats['successful_assessments'] += 1
                table_metrics = results.get('metrics', {})
                
                if 'completeness' in table_metrics:
                    for column, score in table_metrics['completeness'].items():
                        completeness_scores.append(float(score))
                
                stats['total_columns_assessed'] += len(table_metrics.get('completeness', {}))
                
                # Check for issues
                if any(float(score) < 90 for scores in table_metrics.values() for score in scores.values()):
                    stats['tables_with_issues'] += 1
            else:
                stats['failed_assessments'] += 1
        
        if completeness_scores:
            stats['average_completeness'] = sum(completeness_scores) / len(completeness_scores)
        
        return stats
    
    def _generate_summary(self, results: Dict[str, Any]) -> str:
        """Generate human-readable summary of results"""
        summary_stats = results.get('summary_stats', {})
        
        summary = f"""
        Data Quality Assessment Summary:
        
        ✓ Tables Assessed: {summary_stats.get('successful_assessments', 0)}/{summary_stats.get('total_tables', 0)}
        ✓ Columns Analyzed: {summary_stats.get('total_columns_assessed', 0)}
        ✓ Average Completeness: {summary_stats.get('average_completeness', 0):.1f}%
        ⚠ Tables with Issues: {summary_stats.get('tables_with_issues', 0)}
        
        Execution Time: {results.get('execution_time', 0):.2f} seconds
        """
        
        return summary.strip()
    
    def get_job_status(self, job_id: str) -> Optional[DataQualityJob]:
        """Get status of a specific job"""
        return self.active_jobs.get(job_id)
    
    def list_active_jobs(self) -> List[str]:
        """List all active job IDs"""
        return list(self.active_jobs.keys())
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        if job_id in self.active_jobs:
            job = self.active_jobs[job_id]
            job.status = "cancelled"
            job.end_time = datetime.now()
            self.logger.info(f"Job {job_id} cancelled")
            return True
        return False