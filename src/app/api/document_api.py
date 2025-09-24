"""
API endpoints for document processing.
"""
import uuid
import io
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

from src.app.core.config import settings
from src.app.service.document_service import DocumentService
from src.app.core.models.document_models import (
    DocumentResponse,
    DocumentStatusResponse,
    DocumentResultsResponse
)

# Create router
router = APIRouter()

# Create document service
document_service = DocumentService(
    bucket_name=settings.AWS_S3_BUCKET_NAME,
    role_arn=settings.AWS_TEXTRACT_ROLE_ARN,
    sns_topic_arn=settings.AWS_SNS_TOPIC_ARN
)

def process_document_background(file_content: bytes, filename: str, s3_key: str):
    """
    Process a document in the background.
    
    Args:
        file_content (bytes): Content of the PDF file
        filename (str): Name of the PDF file
        s3_key (str): S3 key for the document
    """
    # Create a file-like object from the bytes
    file_obj = io.BytesIO(file_content)
    
    # Process the document
    document_service.process_document_fileobj(file_obj, filename, s3_key)

@router.post("/documents/process", response_model=DocumentResponse)
async def process_document(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """
    Process a document using AWS Textract.
    
    Args:
        background_tasks: FastAPI background tasks
        file: PDF file to process
        
    Returns:
        DocumentResponse: Information about the document processing job
    """
    # Validate file type
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    try:
        # Read the file content
        content = await file.read()
        
        # Generate a unique S3 key
        s3_key = f"uploads/{uuid.uuid4()}/{file.filename}"
        
        # Start processing in the background
        background_tasks.add_task(process_document_background, content, file.filename, s3_key)
        
        # Return immediate response
        return DocumentResponse(
            job_id="background_job",
            status="SUBMITTED",
            document_key=s3_key,
            results_key=None,
            message="Document submitted for processing"
        )
    
    except Exception as e:
        # Return error response
        raise HTTPException(status_code=500, detail=f"Error processing document: {str(e)}")

@router.get("/documents/{job_id}/status", response_model=DocumentStatusResponse)
async def get_document_status(job_id: str):
    """
    Get the status of a document processing job.
    
    Args:
        job_id: ID of the document processing job
        
    Returns:
        DocumentStatusResponse: Status information
    """
    try:
        # Check job status
        status = document_service.textract_service.check_job_status(job_id)
        
        return DocumentStatusResponse(
            job_id=job_id,
            status=status
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking job status: {str(e)}")

@router.get("/documents/{job_id}/results", response_model=DocumentResultsResponse)
async def get_document_results(job_id: str):
    """
    Get the results of a document processing job.
    
    Args:
        job_id: ID of the document processing job
        
    Returns:
        DocumentResultsResponse: Document analysis results
    """
    try:
        # Check job status
        status = document_service.textract_service.check_job_status(job_id)
        
        if status != "SUCCEEDED":
            return DocumentResultsResponse(
                job_id=job_id,
                status=status,
                message=f"Job is not completed yet. Current status: {status}"
            )
        
        # Get analysis results
        results = document_service.get_analysis_results(job_id)
        
        return DocumentResultsResponse(
            job_id=job_id,
            status=status,
            document_metadata=results.get('DocumentMetadata'),
            pages=results.get('Pages'),
            message="Document analysis completed successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting document results: {str(e)}")
