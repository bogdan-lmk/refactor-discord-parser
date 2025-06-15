# app/config.py
from pydantic import BaseSettings, Field, validator
from typing import List, Dict, Optional
from functools import lru_cache
import os

class Settings(BaseSettings):
    """Application settings with validation for 35-50 channels"""
    
    # Application Settings
    app_name: str = "Discord Telegram Parser MVP"
    app_version: str = "2.0.0"
    debug: bool = Field(default=False, env="DEBUG")
    
    # Discord Configuration
    discord_tokens: List[str] = Field(
        default_factory=list,
        env="DISCORD_AUTH_TOKENS",
        description="Comma-separated Discord auth tokens"
    )
    
    # Telegram Configuration  
    telegram_bot_token: str = Field(..., env="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: int = Field(..., env="TELEGRAM_CHAT_ID")
    
    # Server/Channel Mappings (will be populated dynamically)
    server_channel_mappings: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    
    # Performance Limits for MVP (35-50 channels)
    max_channels_per_server: int = Field(default=10, ge=1, le=20)
    max_total_channels: int = Field(default=50, ge=10, le=100)
    max_servers: int = Field(default=10, ge=1, le=15)
    
    # Rate Limiting
    discord_rate_limit_per_second: float = Field(default=2.0, ge=0.5, le=10.0)
    telegram_rate_limit_per_minute: int = Field(default=20, ge=5, le=100)
    
    # Message Processing
    max_message_length: int = Field(default=4000, ge=1000, le=4096)
    message_batch_size: int = Field(default=10, ge=1, le=50)
    max_history_messages: int = Field(default=100, ge=10, le=500)
    
    # ИСПРАВЛЕНИЕ: Добавляем отсутствующее поле
    message_ttl_seconds: int = Field(
        default=86400,  # 1 день по умолчанию
        ge=3600,        # минимум 1 час
        le=604800,      # максимум 1 неделя
        description="TTL for message deduplication in Redis"
    )
    
    # WebSocket Configuration
    websocket_heartbeat_interval: int = Field(default=41250, ge=30000)
    websocket_reconnect_delay: int = Field(default=30, ge=5, le=300)
    websocket_max_retries: int = Field(default=5, ge=1, le=10)
    
    # Memory Management
    cleanup_interval_minutes: int = Field(default=5, ge=1, le=60)
    max_memory_mb: int = Field(default=2048, ge=512, le=8192)
    
    # Telegram UI Preferences
    use_topics: bool = Field(default=True, env="TELEGRAM_USE_TOPICS")
    show_timestamps: bool = Field(default=True)
    show_server_in_message: bool = Field(default=True)
    
    # Monitoring & Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    enable_metrics: bool = Field(default=True)
    metrics_port: int = Field(default=9090, ge=1024, le=65535)
    
    # Redis Configuration (for caching)
    redis_url: Optional[str] = Field(default=None, env="REDIS_URL")
    cache_ttl_seconds: int = Field(default=300, ge=60, le=3600)
    
    # Health Check Configuration
    health_check_interval: int = Field(default=60, ge=10, le=300)
    
    @validator('discord_tokens', pre=True)
    def parse_discord_tokens(cls, v):
        if isinstance(v, str):
            return [token.strip() for token in v.split(',') if token.strip()]
        return v
    
    @validator('discord_tokens')
    def validate_discord_tokens(cls, v):
        if not v:
            raise ValueError('At least one Discord token is required')
        
        for token in v:
            if len(token) < 50:  # Basic token length validation
                raise ValueError(f'Invalid Discord token format: {token[:10]}...')
        
        return v
    
    @validator('telegram_chat_id')
    def validate_telegram_chat_id(cls, v):
        if v == 0:
            raise ValueError('Telegram chat ID cannot be 0')
        return v
    
    @validator('max_total_channels')
    def validate_channel_limits(cls, v, values):
        max_per_server = values.get('max_channels_per_server', 10)
        max_servers = values.get('max_servers', 10)
        
        theoretical_max = max_per_server * max_servers
        if v > theoretical_max:
            raise ValueError(
                f'max_total_channels ({v}) cannot exceed '
                f'max_channels_per_server * max_servers ({theoretical_max})'
            )
        return v
    
    @property
    def discord_tokens_count(self) -> int:
        """Number of available Discord tokens"""
        return len(self.discord_tokens)
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return not self.debug
    
    @property
    def log_config(self) -> dict:
        """Structured logging configuration"""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {
                    "()": "structlog.stdlib.ProcessorFormatter",
                    "processor": "structlog.dev.ConsoleRenderer" if self.debug else "structlog.processors.JSONRenderer",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "json",
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": f"logs/{self.app_name.lower().replace(' ', '_')}.log",
                    "maxBytes": 10485760,  # 10MB
                    "backupCount": 5,
                    "formatter": "json",
                },
            },
            "loggers": {
                "": {
                    "handlers": ["console", "file"],
                    "level": self.log_level,
                    "propagate": True,
                },
            },
        }
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        # Allow extra fields for dynamic server mappings
        extra = "allow"

# Global settings instance with caching
@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()

# Convenience alias
settings = get_settings()