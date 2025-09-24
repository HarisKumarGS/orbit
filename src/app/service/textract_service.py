"""
Textract Service for handling AWS Textract operations.
"""
import time
import boto3
from botocore.exceptions import ClientError
import logging
from ..utils.aws_utils import assume_role

logger = logging.getLogger(__name__)

class TextractService:
    """
    Service for interacting with AWS Textract for document analysis.
    """
    
    def __init__(self):
        """
        Initialize the Textract service.
        """
        self.textract_client = boto3.client('textract')
    
    def start_document_analysis(self, bucket_name, document_key, sns_topic_arn=None, role_arn=None):
        """
        Start document analysis for layout only and send notifications to SNS.
        
        Args:
            bucket_name (str): The name of the S3 bucket containing the document
            document_key (str): S3 object key for the document
            sns_topic_arn (str, optional): ARN of the SNS topic for Textract notifications
            role_arn (str, optional): ARN of the IAM role with permissions to send to SNS
            
        Returns:
            str: Job ID for the started analysis job
            
        Raises:
            ClientError: If an error occurs during job start
        """
        try:
            # Basic parameters for document analysis
            params = {
                'DocumentLocation': {
                    'S3Object': {
                        'Bucket': bucket_name,
                        'Name': document_key
                    }
                },
                'FeatureTypes': ['LAYOUT'],
                'JobTag': 'DocumentLayoutAnalysis',
            }
            
            # Add notification channel if both SNS topic ARN and role ARN are provided
            if sns_topic_arn and role_arn:
                params['NotificationChannel'] = {
                    'SNSTopicArn': sns_topic_arn,
                    'RoleArn': role_arn
                }
            
            response = self.textract_client.start_document_analysis(**params)
            
            job_id = response['JobId']
            logger.info(f"Started document analysis job {job_id} for {bucket_name}/{document_key}")
            return job_id
        except ClientError as e:
            logger.error(f"Error starting document analysis: {e}")
            raise
    
    def check_job_status(self, job_id):
        """
        Check the status of a document analysis job.
        
        Args:
            job_id (str): The ID of the analysis job
            
        Returns:
            str: Status of the job ('IN_PROGRESS', 'SUCCEEDED', 'FAILED', etc.)
            
        Raises:
            ClientError: If an error occurs during status check
        """
        try:
            response = self.textract_client.get_document_analysis(JobId=job_id)
            status = response['JobStatus']
            logger.info(f"Job {job_id} status: {status}")
            return status
        except ClientError as e:
            logger.error(f"Error checking job status: {e}")
            raise
    
    def get_document_analysis(self, job_id):
        """
        Get document analysis results.
        
        Args:
            job_id (str): The ID of the analysis job
            
        Returns:
            dict: Complete analysis results
            
        Raises:
            ClientError: If an error occurs during result retrieval
        """
        try:
            pages = []
            next_token = None
            
            # Textract paginates results, so we need to get all pages
            while True:
                if next_token:
                    response = self.textract_client.get_document_analysis(
                        JobId=job_id,
                        NextToken=next_token
                    )
                else:
                    response = self.textract_client.get_document_analysis(JobId=job_id)
                
                pages.append(response)
                
                if 'NextToken' in response:
                    next_token = response['NextToken']
                else:
                    break
            
            logger.info(f"Retrieved complete analysis results for job {job_id}")
            return {
                'JobId': job_id,
                'JobStatus': pages[0]['JobStatus'],
                'DocumentMetadata': pages[0]['DocumentMetadata'],
                'Pages': pages
            }
        except ClientError as e:
            logger.error(f"Error getting document analysis: {e}")
            raise
    
    def wait_for_job_completion(self, job_id, max_attempts=60, delay=10):
        """
        Wait for a document analysis job to complete.
        
        Args:
            job_id (str): The ID of the analysis job
            max_attempts (int): Maximum number of status check attempts
            delay (int): Delay in seconds between status checks
            
        Returns:
            str: Final job status ('SUCCEEDED', 'FAILED', etc.)
            
        Raises:
            TimeoutError: If job doesn't complete within max_attempts
            ClientError: If an error occurs during status check
        """
        for attempt in range(max_attempts):
            status = self.check_job_status(job_id)
            
            if status in ['SUCCEEDED', 'FAILED', 'PARTIAL_SUCCESS']:
                logger.info(f"Job {job_id} completed with status: {status}")
                return status
            
            logger.info(f"Job {job_id} still in progress. Waiting {delay} seconds...")
            time.sleep(delay)
        
        raise TimeoutError(f"Job {job_id} did not complete within the allowed time")
