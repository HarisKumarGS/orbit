"""
Models for document processing.
"""
from pydantic import BaseModel
from typing import Optional, Dict, Any, List

class DocumentResponse(BaseModel):
    """Response model for document processing."""
    job_id: str
    status: str
    document_key: str
    results_key: Optional[str] = None
    message: str

class DocumentStatusResponse(BaseModel):
    """Response model for document status."""
    job_id: str
    status: str
    message: Optional[str] = None

class DocumentResultsResponse(BaseModel):
    """Response model for document analysis results."""
    job_id: str
    status: str
    document_metadata: Optional[Dict[str, Any]] = None
    pages: Optional[List[Dict[str, Any]]] = None
    message: Optional[str] = None
