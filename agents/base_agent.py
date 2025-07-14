"""
Base Agent class for the Data Quality Agent framework.
Provides common functionality for all agents including logging, error handling, and AI integration.
"""

import logging
import time
import uuid
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

class AgentStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"

@dataclass
class AgentResult:
    """Standard result format for all agents"""
    agent_id: str
    agent_name: str
    status: AgentStatus
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None

class BaseAgent(ABC):
    """Base class for all Data Quality agents"""
    
    def __init__(self, agent_name: str, config: Dict[str, Any], llm_service=None):
        self.agent_id = str(uuid.uuid4())
        self.agent_name = agent_name
        self.config = config
        self.llm_service = llm_service
        self.status = AgentStatus.IDLE
        self.logger = self._setup_logger()
        self.start_time = None
        self.end_time = None
        
    def _setup_logger(self) -> logging.Logger:
        """Setup agent-specific logger"""
        logger = logging.getLogger(f"{self.__class__.__name__}_{self.agent_id}")
        logger.setLevel(getattr(logging, self.config.get('log_level', 'INFO')))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def execute(self, **kwargs) -> AgentResult:
        """Execute the agent with error handling and timing"""
        self.start_time = time.time()
        self.status = AgentStatus.RUNNING
        
        try:
            self.logger.info(f"Agent {self.agent_name} starting execution")
            
            # Validate inputs
            self._validate_inputs(**kwargs)
            
            # Execute the main logic
            result_data = self._execute_logic(**kwargs)
            
            self.status = AgentStatus.COMPLETED
            self.end_time = time.time()
            
            result = AgentResult(
                agent_id=self.agent_id,
                agent_name=self.agent_name,
                status=self.status,
                data=result_data,
                execution_time=self.end_time - self.start_time,
                metadata=self._get_metadata()
            )
            
            self.logger.info(f"Agent {self.agent_name} completed successfully in {result.execution_time:.2f}s")
            return result
            
        except Exception as e:
            self.status = AgentStatus.FAILED
            self.end_time = time.time()
            
            error_msg = f"Agent {self.agent_name} failed: {str(e)}"
            self.logger.error(error_msg)
            
            return AgentResult(
                agent_id=self.agent_id,
                agent_name=self.agent_name,
                status=self.status,
                error=error_msg,
                execution_time=self.end_time - self.start_time if self.start_time else None,
                metadata=self._get_metadata()
            )
    
    @abstractmethod
    def _execute_logic(self, **kwargs) -> Dict[str, Any]:
        """Main execution logic - to be implemented by subclasses"""
        pass
    
    def _validate_inputs(self, **kwargs) -> None:
        """Validate input parameters - can be overridden by subclasses"""
        pass
    
    def _get_metadata(self) -> Dict[str, Any]:
        """Get agent metadata for result"""
        return {
            'config': self.config,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'agent_version': '1.0.0'
        }
    
    def get_status(self) -> AgentStatus:
        """Get current agent status"""
        return self.status
    
    def reset(self) -> None:
        """Reset agent to initial state"""
        self.status = AgentStatus.IDLE
        self.start_time = None
        self.end_time = None
        self.logger.info(f"Agent {self.agent_name} reset to idle state")

class AIEnabledAgent(BaseAgent):
    """Base class for agents that use AI capabilities"""
    
    def __init__(self, agent_name: str, config: Dict[str, Any], llm_service):
        if llm_service is None:
            raise ValueError("AIEnabledAgent requires llm_service")
        super().__init__(agent_name, config, llm_service)
    
    def _call_ai(self, prompt: str, **kwargs) -> str:
        """Call AI service with error handling"""
        try:
            if self.config.get('log_ai_interactions', False):
                self.logger.debug(f"AI Prompt: {prompt[:200]}...")
            
            response = self.llm_service.complete(prompt, **kwargs)
            
            if self.config.get('log_ai_interactions', False):
                self.logger.debug(f"AI Response: {response[:200]}...")
            
            return response
            
        except Exception as e:
            self.logger.error(f"AI call failed: {str(e)}")
            raise
    
    def _call_ai_with_retry(self, prompt: str, max_retries: int = 3, **kwargs) -> str:
        """Call AI service with retry logic"""
        for attempt in range(max_retries):
            try:
                return self._call_ai(prompt, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"AI call attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        raise Exception("AI call failed after all retries")