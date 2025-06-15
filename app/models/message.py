# app/models/message.py
from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Optional, Dict, Any
import re

class DiscordMessage(BaseModel):
    """Typed Discord message model with validation"""
    
    content: str = Field(..., min_length=1, max_length=4000)
    timestamp: datetime
    server_name: str = Field(..., min_length=1, max_length=100)
    channel_name: str = Field(..., min_length=1, max_length=100)
    author: str = Field(..., min_length=1, max_length=50)
    
    # Optional fields
    message_id: Optional[str] = None
    channel_id: Optional[str] = None
    guild_id: Optional[str] = None
    translated_content: Optional[str] = None
    attachments: Optional[list] = Field(default_factory=list)
    embeds: Optional[list] = Field(default_factory=list)
    
    # Processing metadata
    processed_at: Optional[datetime] = None
    telegram_message_id: Optional[int] = None
    
    @validator('content', pre=True)
    def clean_content(cls, v):
        """Clean and sanitize message content"""
        if not v:
            raise ValueError('Message content cannot be empty')
        
        # Remove Discord mentions and clean formatting
        v = re.sub(r'<@!?\d+>', '[User]', v)  # User mentions
        v = re.sub(r'<#\d+>', '[Channel]', v)  # Channel mentions
        v = re.sub(r'<@&\d+>', '[Role]', v)   # Role mentions
        
        # Trim whitespace
        v = v.strip()
        
        if not v:
            raise ValueError('Message content is empty after cleaning')
        
        return v
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is not in the future"""
        if v > datetime.now():
            raise ValueError('Message timestamp cannot be in the future')
        return v
    
    @validator('server_name', 'channel_name', 'author', pre=True)
    def clean_names(cls, v):
        """Clean server, channel, and author names"""
        if not v:
            raise ValueError('Name cannot be empty')
        
        # Remove problematic characters
        v = re.sub(r'[^\w\s\-\.]', '', v)
        v = v.strip()
        
        if not v:
            raise ValueError('Name is empty after cleaning')
        
        return v
    
    def to_telegram_format(self, show_timestamp: bool = True, show_server: bool = True) -> str:
        """Format message for Telegram"""
        parts = []
        
        if show_server:
            parts.append(f"🏰 **{self.server_name}**")
        
        parts.append(f"📢 #{self.channel_name}")
        
        if show_timestamp:
            parts.append(f"📅 {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        parts.append(f"👤 {self.author}")
        parts.append(f"💬 {self.content}")
        
        return "\n".join(parts)
    
    class Config:
        # Allow datetime to be set from various formats
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        
        # Example for JSON schema generation
        schema_extra = {
            "example": {
                "content": "🎉 New feature released!",
                "timestamp": "2024-01-15T12:00:00",
                "server_name": "My Discord Server",
                "channel_name": "announcements",
                "author": "ServerBot",
                "message_id": "1234567890123456789"
            }
        }

# app/models/server.py
from pydantic import BaseModel, Field, validator
from typing import Dict, List, Optional, Set
from enum import Enum

class ServerStatus(str, Enum):
    """Server status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    PENDING = "pending"

class ChannelInfo(BaseModel):
    """Channel information model"""
    
    channel_id: str = Field(..., regex=r'^\d{17,19}$')  # Discord snowflake format
    channel_name: str = Field(..., min_length=1, max_length=100)
    category_id: Optional[str] = Field(None, regex=r'^\d{17,19}$')
    
    # Access tracking
    http_accessible: bool = Field(default=False)
    websocket_accessible: bool = Field(default=False)
    last_checked: Optional[datetime] = None
    
    # Statistics
    message_count: int = Field(default=0, ge=0)
    last_message_time: Optional[datetime] = None
    error_count: int = Field(default=0, ge=0)
    
    @property
    def is_accessible(self) -> bool:
        """Check if channel is accessible via any method"""
        return self.http_accessible or self.websocket_accessible
    
    @property
    def access_method(self) -> str:
        """Get primary access method"""
        if self.http_accessible and self.websocket_accessible:
            return "HTTP+WebSocket"
        elif self.http_accessible:
            return "HTTP only"
        elif self.websocket_accessible:
            return "WebSocket only"
        else:
            return "No access"

class ServerInfo(BaseModel):
    """Discord server information model"""
    
    server_name: str = Field(..., min_length=1, max_length=100)
    guild_id: str = Field(..., regex=r'^\d{17,19}$')
    
    # Channel management
    channels: Dict[str, ChannelInfo] = Field(default_factory=dict)
    max_channels: int = Field(default=10, ge=1, le=20)
    
    # Status tracking
    status: ServerStatus = Field(default=ServerStatus.PENDING)
    last_sync: Optional[datetime] = None
    error_message: Optional[str] = None
    
    # Telegram integration
    telegram_topic_id: Optional[int] = None
    topic_created_at: Optional[datetime] = None
    
    # Statistics
    total_messages: int = Field(default=0, ge=0)
    active_channels: int = Field(default=0, ge=0)
    last_activity: Optional[datetime] = None
    
    @validator('channels')
    def validate_channel_count(cls, v, values):
        """Ensure channel count doesn't exceed limit"""
        max_channels = values.get('max_channels', 10)
        if len(v) > max_channels:
            raise ValueError(f'Cannot have more than {max_channels} channels per server')
        return v
    
    @property
    def accessible_channels(self) -> Dict[str, ChannelInfo]:
        """Get only accessible channels"""
        return {
            channel_id: channel_info 
            for channel_id, channel_info in self.channels.items()
            if channel_info.is_accessible
        }
    
    @property
    def channel_count(self) -> int:
        """Total number of channels"""
        return len(self.channels)
    
    @property
    def accessible_channel_count(self) -> int:
        """Number of accessible channels"""
        return len(self.accessible_channels)
    
    def add_channel(self, channel_info: ChannelInfo) -> bool:
        """Add a new channel if under limit"""
        if len(self.channels) >= self.max_channels:
            return False
        
        self.channels[channel_info.channel_id] = channel_info
        return True
    
    def remove_channel(self, channel_id: str) -> bool:
        """Remove a channel"""
        if channel_id in self.channels:
            del self.channels[channel_id]
            return True
        return False
    
    def update_stats(self):
        """Update server statistics"""
        self.active_channels = self.accessible_channel_count
        self.last_sync = datetime.now()
        
        if self.active_channels > 0:
            self.status = ServerStatus.ACTIVE
        else:
            self.status = ServerStatus.INACTIVE

class SystemStats(BaseModel):
    """System-wide statistics"""
    
    total_servers: int = Field(default=0, ge=0)
    total_channels: int = Field(default=0, ge=0)
    active_servers: int = Field(default=0, ge=0)
    active_channels: int = Field(default=0, ge=0)
    
    # Message statistics
    messages_processed_today: int = Field(default=0, ge=0)
    messages_processed_total: int = Field(default=0, ge=0)
    
    # Performance metrics
    average_response_time_ms: float = Field(default=0.0, ge=0.0)
    memory_usage_mb: float = Field(default=0.0, ge=0.0)
    uptime_seconds: int = Field(default=0, ge=0)
    
    # Rate limiting
    discord_requests_per_hour: int = Field(default=0, ge=0)
    telegram_requests_per_hour: int = Field(default=0, ge=0)
    
    # Error tracking
    errors_last_hour: int = Field(default=0, ge=0)
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None
    
    @property
    def health_score(self) -> float:
        """Calculate overall health score (0-100)"""
        score = 100.0
        
        # Reduce score for errors
        if self.errors_last_hour > 0:
            score -= min(50, self.errors_last_hour * 5)
        
        # Reduce score for high memory usage
        if self.memory_usage_mb > 1500:  # Above 1.5GB
            score -= 20
        
        # Reduce score for low activity
        if self.active_channels == 0:
            score -= 30
        
        return max(0.0, score)
    
    @property
    def status(self) -> str:
        """Get system status based on health score"""
        health = self.health_score
        if health >= 90:
            return "🟢 Excellent"
        elif health >= 70:
            return "🟡 Good"
        elif health >= 50:
            return "🟠 Warning"
        else:
            return "🔴 Critical"

# app/models/__init__.py
from .message import DiscordMessage
from .server import ServerInfo, ChannelInfo, ServerStatus, SystemStats

__all__ = [
    "DiscordMessage",
    "ServerInfo", 
    "ChannelInfo",
    "ServerStatus",
    "SystemStats"
]