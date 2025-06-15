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