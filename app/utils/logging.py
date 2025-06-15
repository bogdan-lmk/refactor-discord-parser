# app/utils/logging.py
import structlog
import logging.config
from typing import Any, Dict
from ..config import Settings

def setup_logging(settings: Settings):
    """Configure structured logging"""
    
    # Configure standard logging
    logging.config.dictConfig(settings.log_config)
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if not settings.debug else structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    logger = structlog.get_logger()
    logger.info("Logging configured", debug_mode=settings.debug, log_level=settings.log_level)
    
    return logger

# app/utils/rate_limiter.py
import asyncio
import time
from typing import Dict, Optional
from dataclasses import dataclass, field

@dataclass
class RateLimitBucket:
    """Rate limiting bucket for tracking requests"""
    requests: int = 0
    reset_time: float = 0
    window_seconds: float = 60  # Default 1 minute window

class RateLimiter:
    """Advanced rate limiter with multiple strategies"""
    
    def __init__(self, 
                 requests_per_second: Optional[float] = None,
                 requests_per_minute: Optional[int] = None,
                 name: str = "default"):
        self.name = name
        self.requests_per_second = requests_per_second
        self.requests_per_minute = requests_per_minute
        
        # Tracking buckets
        self.buckets: Dict[str, RateLimitBucket] = {}
        self._lock = asyncio.Lock()
        
        # Adaptive rate limiting
        self.error_count = 0
        self.success_count = 0
        self.adaptive_multiplier = 1.0
    
    async def acquire(self, identifier: str = "global") -> bool:
        """Acquire rate limit permission"""
        async with self._lock:
            now = time.time()
            
            # Get or create bucket
            if identifier not in self.buckets:
                self.buckets[identifier] = RateLimitBucket()
            
            bucket = self.buckets[identifier]
            
            # Reset bucket if window expired
            if now >= bucket.reset_time:
                bucket.requests = 0
                bucket.reset_time = now + bucket.window_seconds
            
            # Check per-minute limit
            if self.requests_per_minute:
                if bucket.requests >= self.requests_per_minute * self.adaptive_multiplier:
                    return False
            
            # Check per-second limit (more granular)
            if self.requests_per_second:
                window_1s = now - (now % 1)  # Current second window
                second_bucket_key = f"{identifier}_1s_{window_1s}"
                
                if second_bucket_key not in self.buckets:
                    self.buckets[second_bucket_key] = RateLimitBucket(window_seconds=1)
                
                second_bucket = self.buckets[second_bucket_key]
                if second_bucket.requests >= self.requests_per_second * self.adaptive_multiplier:
                    return False
                
                second_bucket.requests += 1
            
            # Increment main bucket
            bucket.requests += 1
            return True
    
    async def wait_if_needed(self, identifier: str = "global") -> None:
        """Wait until rate limit allows request"""
        while not await self.acquire(identifier):
            await asyncio.sleep(0.1)  # Wait 100ms and retry
    
    def record_success(self):
        """Record successful request for adaptive rate limiting"""
        self.success_count += 1
        
        # Gradually increase rate if many successes
        if self.success_count > 100 and self.error_count < 5:
            self.adaptive_multiplier = min(1.2, self.adaptive_multiplier + 0.01)
            self.success_count = 0
            self.error_count = 0
    
    def record_error(self):
        """Record failed request for adaptive rate limiting"""
        self.error_count += 1
        
        # Decrease rate if many errors
        if self.error_count > 3:
            self.adaptive_multiplier = max(0.5, self.adaptive_multiplier - 0.1)
            self.success_count = 0
            self.error_count = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics"""
        return {
            "name": self.name,
            "requests_per_second": self.requests_per_second,
            "requests_per_minute": self.requests_per_minute,
            "adaptive_multiplier": self.adaptive_multiplier,
            "active_buckets": len(self.buckets),
            "success_count": self.success_count,
            "error_count": self.error_count
        }

# app/utils/__init__.py
from .logging import setup_logging
from .rate_limiter import RateLimiter

__all__ = ["setup_logging", "RateLimiter"]