"""
LLM Service for Azure OpenAI integration with the Data Quality Agent framework.
Provides centralized AI capabilities with error handling, retry logic, and cost optimization.
"""

import os
import json
import time
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_exponential
import openai
from openai import AzureOpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class AIUsageStats:
    """Track AI usage for cost optimization"""
    total_requests: int = 0
    total_tokens: int = 0
    total_cost: float = 0.0
    failed_requests: int = 0
    average_response_time: float = 0.0

class LLMService:
    """Azure OpenAI service wrapper with enterprise features"""
    
    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.client = self._initialize_client()
        self.usage_stats = AIUsageStats()
        self.logger = self._setup_logger()
        self._response_cache = {}
        
    def _load_config(self, config_path: str = None) -> Dict[str, Any]:
        """Load configuration from file or environment"""
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                file_config = json.load(f)
        else:
            file_config = {}
        
        # Merge with environment variables
        config = {
            'endpoint': os.getenv('AZURE_OPENAI_ENDPOINT', file_config.get('endpoint', '')),
            'api_key': os.getenv('AZURE_OPENAI_API_KEY', file_config.get('api_key', '')),
            'api_version': os.getenv('AZURE_OPENAI_API_VERSION', file_config.get('api_version', '2024-02-01')),
            'deployment_name': os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME', file_config.get('deployment_name', 'gpt-4')),
            'temperature': float(os.getenv('AZURE_OPENAI_TEMPERATURE', file_config.get('temperature', 0.3))),
            'max_tokens': int(os.getenv('AZURE_OPENAI_MAX_TOKENS', file_config.get('max_tokens', 1000))),
            'timeout': int(os.getenv('AZURE_OPENAI_TIMEOUT', file_config.get('timeout', 30))),
            'max_retries': int(os.getenv('AZURE_OPENAI_MAX_RETRIES', file_config.get('max_retries', 3))),
            'backoff_factor': float(os.getenv('AZURE_OPENAI_BACKOFF_FACTOR', file_config.get('backoff_factor', 2))),
            'enable_caching': os.getenv('AZURE_OPENAI_ENABLE_CACHING', 'true').lower() == 'true',
            'log_ai_interactions': os.getenv('LOG_AI_INTERACTIONS', 'false').lower() == 'true'
        }
        
        # Validate required fields
        required_fields = ['endpoint', 'api_key']
        for field in required_fields:
            if not config.get(field):
                raise ValueError(f"Missing required configuration: {field}")
        
        return config
    
    def _initialize_client(self) -> AzureOpenAI:
        """Initialize Azure OpenAI client"""
        try:
            client = AzureOpenAI(
                api_key=self.config['api_key'],
                api_version=self.config['api_version'],
                azure_endpoint=self.config['endpoint'],
                timeout=self.config['timeout']
            )
            return client
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Azure OpenAI client: {str(e)}")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for LLM service"""
        logger = logging.getLogger('LLMService')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _generate_cache_key(self, prompt: str, **kwargs) -> str:
        """Generate cache key for prompt"""
        key_data = {
            'prompt': prompt,
            'temperature': kwargs.get('temperature', self.config['temperature']),
            'max_tokens': kwargs.get('max_tokens', self.config['max_tokens'])
        }
        return str(hash(json.dumps(key_data, sort_keys=True)))
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _make_api_call(self, prompt: str, **kwargs) -> str:
        """Make API call with retry logic"""
        start_time = time.time()
        
        try:
            response = self.client.chat.completions.create(
                model=self.config['deployment_name'],
                messages=[
                    {"role": "system", "content": "You are a helpful assistant for data quality analysis."},
                    {"role": "user", "content": prompt}
                ],
                temperature=kwargs.get('temperature', self.config['temperature']),
                max_tokens=kwargs.get('max_tokens', self.config['max_tokens']),
                timeout=self.config['timeout']
            )
            
            # Update usage stats
            self.usage_stats.total_requests += 1
            if hasattr(response, 'usage') and response.usage:
                self.usage_stats.total_tokens += response.usage.total_tokens
            
            response_time = time.time() - start_time
            self.usage_stats.average_response_time = (
                (self.usage_stats.average_response_time * (self.usage_stats.total_requests - 1) + response_time) 
                / self.usage_stats.total_requests
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            self.usage_stats.failed_requests += 1
            self.logger.error(f"AI API call failed: {str(e)}")
            raise
    
    def complete(self, prompt: str, use_cache: bool = True, **kwargs) -> str:
        """
        Complete a prompt using Azure OpenAI
        
        Args:
            prompt: The prompt to complete
            use_cache: Whether to use response caching
            **kwargs: Additional parameters for the API call
            
        Returns:
            The AI response text
        """
        if self.config.get('log_ai_interactions', False):
            self.logger.debug(f"AI Prompt: {prompt[:200]}...")
        
        # Check cache first
        cache_key = None
        if use_cache and self.config.get('enable_caching', True):
            cache_key = self._generate_cache_key(prompt, **kwargs)
            if cache_key in self._response_cache:
                self.logger.debug("Using cached response")
                return self._response_cache[cache_key]
        
        # Make API call
        try:
            response = self._make_api_call(prompt, **kwargs)
            
            # Cache the response
            if cache_key and self.config.get('enable_caching', True):
                self._response_cache[cache_key] = response
            
            if self.config.get('log_ai_interactions', False):
                self.logger.debug(f"AI Response: {response[:200]}...")
            
            return response
            
        except Exception as e:
            self.logger.error(f"LLM completion failed: {str(e)}")
            raise
    
    def parse_json_response(self, prompt: str, **kwargs) -> Dict[str, Any]:
        """
        Get a JSON response from the AI
        
        Args:
            prompt: The prompt requesting JSON output
            **kwargs: Additional parameters for the API call
            
        Returns:
            Parsed JSON response
        """
        json_prompt = f"{prompt}\n\nPlease respond with valid JSON only, no additional text."
        
        try:
            response = self.complete(json_prompt, **kwargs)
            
            # Clean response to extract JSON
            response = response.strip()
            if response.startswith('```json'):
                response = response[7:]
            if response.endswith('```'):
                response = response[:-3]
            
            return json.loads(response)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {str(e)}")
            self.logger.error(f"Raw response: {response}")
            raise ValueError(f"Invalid JSON response from AI: {str(e)}")
    
    def analyze_schema(self, table_name: str, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze table schema using AI
        
        Args:
            table_name: Name of the table
            columns: List of column information
            
        Returns:
            AI analysis of the schema
        """
        prompt = f"""
        Analyze the database table schema below and provide insights:
        
        Table: {table_name}
        Columns: {json.dumps(columns, indent=2)}
        
        Please provide analysis in JSON format with:
        1. table_purpose: Brief description of what this table likely contains
        2. column_classifications: For each column, classify as 'key', 'categorical', 'numerical', 'temporal', or 'text'
        3. data_quality_concerns: Potential data quality issues to monitor
        4. recommended_metrics: Suggested data quality metrics for each column
        5. business_context: Business relevance and importance
        
        Respond with valid JSON only.
        """
        
        return self.parse_json_response(prompt)
    
    def generate_quality_insights(self, metrics_data: Dict[str, Any]) -> str:
        """
        Generate quality insights from metrics data
        
        Args:
            metrics_data: Dictionary containing quality metrics
            
        Returns:
            Human-readable quality insights
        """
        prompt = f"""
        Based on the following data quality metrics, provide insights and recommendations:
        
        Metrics Data: {json.dumps(metrics_data, indent=2)}
        
        Please provide:
        1. Overall data quality assessment (High/Medium/Low)
        2. Critical issues requiring immediate attention
        3. Trends and patterns in the data
        4. Specific recommendations for improvement
        5. Business impact assessment
        
        Provide a clear, actionable summary.
        """
        
        return self.complete(prompt)
    
    def get_usage_stats(self) -> AIUsageStats:
        """Get current usage statistics"""
        return self.usage_stats
    
    def clear_cache(self) -> None:
        """Clear the response cache"""
        self._response_cache.clear()
        self.logger.info("Response cache cleared")
    
    def health_check(self) -> Dict[str, Any]:
        """Check service health"""
        try:
            test_response = self.complete("Test connection. Respond with 'OK'.", use_cache=False)
            return {
                'status': 'healthy',
                'response_time': self.usage_stats.average_response_time,
                'test_response': test_response[:50]
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }