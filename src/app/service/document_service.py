"""
Document Service for handling the complete document processing workflow.
"""
import os
import json
import time
import logging
from pathlib import Path

from src.app.service.s3_service import S3Service
from src.app.service.textract_service import TextractService

logger = logging.getLogger(__name__)

class DocumentService:
    """
    Service for processing documents through the complete workflow:
    1. Upload PDF to S3
    2. Start document analysis with Textract
    3. Monitor job progress
    4. Get final results
    5. Upload results back to S3
    """
    
    def __init__(self, bucket_name, role_arn, sns_topic_arn=None):
        """
        Initialize the Document service.
        
        Args:
            bucket_name (str): The name of the S3 bucket to use
            role_arn (str): ARN of the IAM role with permissions to send to SNS
            sns_topic_arn (str, optional): ARN of the SNS topic for Textract notifications
        """
        self.bucket_name = bucket_name
        self.role_arn = role_arn
        self.sns_topic_arn = sns_topic_arn
        
        # Initialize services
        self.s3_service = S3Service(bucket_name)
        self.textract_service = TextractService()
    
    def upload_pdf_to_s3(self, pdf_path, s3_key=None):
        """
        Upload a PDF file to S3.
        
        Args:
            pdf_path (str): Local path to the PDF file
            s3_key (str, optional): S3 object key. If not provided, uses the filename
            
        Returns:
            str: S3 key of the uploaded file
            
        Raises:
            FileNotFoundError: If the PDF file doesn't exist
            ClientError: If an error occurs during upload
        """
        # Validate file exists and is a PDF
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        if not pdf_path.lower().endswith('.pdf'):
            logger.warning(f"File {pdf_path} may not be a PDF")
        
        # Use filename as key if not provided
        if s3_key is None:
            s3_key = os.path.basename(pdf_path)
        
        # Upload file to S3
        self.s3_service.upload_file(pdf_path, s3_key)
        logger.info(f"PDF uploaded to S3: {self.bucket_name}/{s3_key}")
        
        return s3_key
    
    def upload_pdf_fileobj_to_s3(self, file_obj, filename, s3_key=None):
        """
        Upload a PDF file object to S3.
        
        Args:
            file_obj: File-like object containing PDF data
            filename (str): Original filename of the PDF
            s3_key (str, optional): S3 object key. If not provided, uses the filename
            
        Returns:
            str: S3 key of the uploaded file
            
        Raises:
            ClientError: If an error occurs during upload
        """
        # Use filename as key if not provided
        if s3_key is None:
            s3_key = filename
        
        # Upload file object to S3
        self.s3_service.upload_fileobj(file_obj, s3_key, content_type='application/pdf')
        logger.info(f"PDF file object uploaded to S3: {self.bucket_name}/{s3_key}")
        
        return s3_key
    
    def start_document_analysis(self, s3_key):
        """
        Start document analysis for a PDF in S3.
        
        Args:
            s3_key (str): S3 object key of the PDF
            
        Returns:
            str: Job ID for the started analysis job
            
        Raises:
            ClientError: If an error occurs during job start
        """
        # Start document analysis
        job_id = self.textract_service.start_document_analysis(
            bucket_name=self.bucket_name,
            document_key=s3_key,
            sns_topic_arn=self.sns_topic_arn,
            role_arn=self.role_arn
        )
        
        logger.info(f"Started document analysis job {job_id} for {s3_key}")
        return job_id
    
    def monitor_job_progress(self, job_id, polling_interval=30, max_time=3600):
        """
        Monitor the progress of a document analysis job.
        
        Args:
            job_id (str): The ID of the analysis job
            polling_interval (int, optional): Interval in seconds between status checks
            max_time (int, optional): Maximum time in seconds to monitor
            
        Returns:
            str: Final job status
            
        Raises:
            TimeoutError: If job doesn't complete within max_time
        """
        start_time = time.time()
        elapsed_time = 0
        
        while elapsed_time < max_time:
            # Check job status
            status = self.textract_service.check_job_status(job_id)
            
            # If job completed, return status
            if status in ['SUCCEEDED', 'FAILED', 'PARTIAL_SUCCESS']:
                logger.info(f"Job {job_id} completed with status: {status}")
                return status
            
            # Wait for next check
            time.sleep(polling_interval)
            elapsed_time = time.time() - start_time
        
        raise TimeoutError(f"Job {job_id} monitoring timed out after {max_time} seconds")
    
    def get_analysis_results(self, job_id):
        """
        Get the results of a completed document analysis job.
        
        Args:
            job_id (str): The ID of the analysis job
            
        Returns:
            dict: Document analysis results
            
        Raises:
            ClientError: If an error occurs during result retrieval
        """
        # Get analysis results
        results = self.textract_service.get_document_analysis(job_id)
        
        logger.info(f"Retrieved analysis results for job {job_id}")
        return results
    
    def upload_results_to_s3(self, results, original_s3_key):
        """
        Upload analysis results to S3 in the same path as the original document.
        
        Args:
            results (dict): Document analysis results
            original_s3_key (str): S3 key of the original document
            
        Returns:
            str: S3 key of the uploaded results
            
        Raises:
            ClientError: If an error occurs during upload
        """
        # Create results key in the same path as the original document
        path = Path(original_s3_key)
        results_key = f"{path.parent}/{path.stem}_results.json"
        
        # Upload results to S3
        self.s3_service.upload_json(results, results_key)
        logger.info(f"Results uploaded to S3: {self.bucket_name}/{results_key}")
        
        return results_key
    
    def process_document(self, pdf_path, s3_key=None):
        """
        Process a document through the entire workflow.
        
        Args:
            pdf_path (str): Local path to the PDF file
            s3_key (str, optional): S3 object key. If not provided, uses the filename
            
        Returns:
            dict: {
                'job_id': str,
                'status': str,
                'document_key': str,
                'results_key': str
            }
            
        Raises:
            Various exceptions from the underlying services
        """
        try:
            # Step 1: Upload PDF to S3
            document_key = self.upload_pdf_to_s3(pdf_path, s3_key)
            
            # Step 2: Start document analysis
            job_id = self.start_document_analysis(document_key)
            
            # Step 3: Monitor job progress
            status = self.monitor_job_progress(job_id)
            
            # If job failed, return early
            if status != 'SUCCEEDED':
                return {
                    'job_id': job_id,
                    'status': status,
                    'document_key': document_key,
                    'results_key': None
                }
            
            # Step 4: Get analysis results
            results = self.get_analysis_results(job_id)
            
            # Step 5: Upload results to S3
            results_key = self.upload_results_to_s3(results, document_key)
            
            return {
                'job_id': job_id,
                'status': status,
                'document_key': document_key,
                'results_key': results_key
            }
        
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            raise
    
    def process_document_fileobj(self, file_obj, filename, s3_key=None):
        """
        Process a document file object through the entire workflow.
        
        Args:
            file_obj: File-like object containing PDF data
            filename (str): Original filename of the PDF
            s3_key (str, optional): S3 object key. If not provided, uses the filename
            
        Returns:
            dict: {
                'job_id': str,
                'status': str,
                'document_key': str,
                'results_key': str
            }
            
        Raises:
            Various exceptions from the underlying services
        """
        try:
            # Step 1: Upload PDF file object to S3
            document_key = self.upload_pdf_fileobj_to_s3(file_obj, filename, s3_key)
            
            # Step 2: Start document analysis
            job_id = self.start_document_analysis(document_key)
            
            # Step 3: Monitor job progress
            status = self.monitor_job_progress(job_id)
            
            # If job failed, return early
            if status != 'SUCCEEDED':
                return {
                    'job_id': job_id,
                    'status': status,
                    'document_key': document_key,
                    'results_key': None
                }
            
            # Step 4: Get analysis results
            results = self.get_analysis_results(job_id)
            
            # Step 5: Upload results to S3
            results_key = self.upload_results_to_s3(results, document_key)
            
            return {
                'job_id': job_id,
                'status': status,
                'document_key': document_key,
                'results_key': results_key
            }
        
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            raise
