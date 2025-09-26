"""
API routes for document operations.
"""
import logging
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, status
from src.app.service.document_service import DocumentService

logger = logging.getLogger(__name__)
router = APIRouter()
document_service = DocumentService()

@router.post("/document", status_code=status.HTTP_200_OK)
async def upload_document(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """
    Upload a document to S3 and process it.
    
    Args:
        background_tasks (BackgroundTasks): FastAPI background tasks.
        file (UploadFile): The file to upload.
        
    Returns:
        dict: The document information.
    """
    try:
        result = await document_service.upload_document(file, background_tasks)
        return result
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error uploading document: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while uploading the document"
        )
