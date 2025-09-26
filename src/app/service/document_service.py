"""
Service for handling document operations.
"""
import logging
import json
import uuid
import tempfile
from fastapi import UploadFile, BackgroundTasks
from src.app.service.s3_service import S3Service
from src.app.service.db_service import DBService
from src.app.core.pdf_processor.extractor import (
    extract_elements_from_pdf,
    chunk_elements_by_title,
    group_narrative_by_title
)

logger = logging.getLogger(__name__)

class DocumentService:
    """
    Service for handling document operations.
    """
    def __init__(self):
        """
        Initialize the document service.
        """
        self.s3_service = S3Service()
        self.db_service = DBService()
    
    async def process_pdf_file(self, file_path):
        """
        Process a PDF file using unstructured library.
        
        Args:
            file_path (str): Path to the PDF file.
        """
        try:
            logger.info(f"Processing PDF file: {file_path}")
            elements = extract_elements_from_pdf(file_path)
            logger.info(f"Sample Element {elements[0]}")
            chunks = chunk_elements_by_title(elements)
            group_narrative_by_title(chunks)
        except Exception as e:
            logger.error(f"Error processing PDF file: {e}")
    
    async def upload_document(self, file: UploadFile, background_tasks: BackgroundTasks):
        """
        Upload a document to S3 and create a database record.
        
        Args:
            file (UploadFile): The uploaded file.
            background_tasks (BackgroundTasks): FastAPI background tasks.
            
        Returns:
            dict: The document information.
            
        Raises:
            ValueError: If the file is not a PDF.
        """
        # Validate file type
        if not file.filename.lower().endswith('.pdf'):
            raise ValueError("Only PDF files are allowed")
        
        try:
            # Generate a unique filename
            file_content = await file.read()
            unique_id = str(uuid.uuid4())
            original_filename = file.filename
            s3_key = f"documents/{unique_id}/{original_filename}"
            
            # Upload to S3
            await self.s3_service.upload_file(
                file_content=file_content,
                file_name=s3_key,
                content_type="application/pdf"
            )
            
            # Create database record
            document = await self.db_service.create_document(
                filename=original_filename,
                s3_key=s3_key
            )
            
            # Process PDF in background
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name
            
            background_tasks.add_task(self.process_pdf_file, temp_file_path)
            
            return {
                "id": str(document.id),
                "filename": document.filename,
                "s3_key": document.s3_key,
                "uploaded_at": document.uploaded_at.isoformat()
            }
        except Exception as e:
            logger.error(f"Error uploading document: {e}")
            raise
