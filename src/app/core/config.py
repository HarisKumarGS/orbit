"""
Configuration settings for the application.
"""
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """
    Application settings.
    
    These settings can be overridden with environment variables.
    """
    # AWS S3 settings
    AWS_S3_BUCKET_NAME: str = os.getenv("AWS_S3_BUCKET_NAME", "document-processing-bucket")
    
    # API settings
    API_PREFIX: str = "/api/v1"
    
    class Config:
        env_file = ".env"
        case_sensitive = True

# Create a global settings object
settings = Settings()
