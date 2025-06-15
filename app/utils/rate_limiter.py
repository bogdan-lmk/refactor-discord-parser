# app/utils/rate_limiter.py
import asyncio
import time
from typing import Dict, Optional, Any
from dataclasses import dataclass

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
    
    # ИСПРАВЛЕНИЕ: Добавляем таймаут для предотвращения бесконечного цикла
    async def wait_if_needed(self, identifier: str = "global", max_wait: float = 60.0) -> bool:
        """Wait until rate limit allows request with timeout protection"""
        start_time = time.time()
        
        while not await self.acquire(identifier):
            # Проверяем таймаут
            if time.time() - start_time > max_wait:
                raise TimeoutError(
                    f"Rate limiter timeout for {identifier} after {max_wait} seconds. "
                    f"Consider adjusting rate limits or check for system issues."
                )
            
            # Wait and retry
            await asyncio.sleep(0.1)
        
        return True
    
    # Добавляем альтернативный метод без исключений для обратной совместимости
    async def wait_if_needed_safe(self, identifier: str = "global", max_wait: float = 60.0) -> bool:
        """Safe version that returns False instead of raising timeout exception"""
        try:
            return await self.wait_if_needed(identifier, max_wait)
        except TimeoutError:
            return False
    
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
    
    def reset_stats(self):
        """Reset statistics (useful for testing or maintenance)"""
        self.error_count = 0
        self.success_count = 0
        self.adaptive_multiplier = 1.0
    
    def clear_old_buckets(self, max_age_seconds: int = 3600):
        """Clear old buckets to prevent memory leaks"""
        now = time.time()
        old_buckets = [
            identifier for identifier, bucket in self.buckets.items()
            if bucket.reset_time < now - max_age_seconds
        ]
        
        for identifier in old_buckets:
            del self.buckets[identifier]
        
        return len(old_buckets)