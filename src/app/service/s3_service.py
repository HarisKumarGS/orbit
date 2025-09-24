"""
S3 Service for handling S3 bucket operations.
"""
import json
import boto3
from botocore.exceptions import ClientError
import logging
from ..utils.aws_utils import assume_role

logger = logging.getLogger(__name__)

class S3Service:
    """
    Service for interacting with AWS S3 buckets.
    """
    
    def __init__(self, bucket_name):
        """
        Initialize the S3 service with a bucket name.
        
        Args:
            bucket_name (str): The name of the S3 bucket to use
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
    
    def upload_file(self, file_path, s3_key):
        """
        Upload a file to the S3 bucket.
        
        Args:
            file_path (str): Local path to the file to upload
            s3_key (str): S3 object key (path in the bucket)
            
        Returns:
            bool: True if upload was successful, False otherwise
            
        Raises:
            ClientError: If an error occurs during upload
        """
        try:
            # Add ACL to ensure bucket owner has full control
            extra_args = {
                'ACL': 'bucket-owner-full-control'
            }
            
            self.s3_client.upload_file(
                file_path, 
                self.bucket_name, 
                s3_key,
                ExtraArgs=extra_args
            )
            logger.info(f"Successfully uploaded {file_path} to {self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Error uploading file to S3: {e}")
            raise
    
    def upload_fileobj(self, file_obj, s3_key, content_type=None):
        """
        Upload a file object to the S3 bucket.
        
        Args:
            file_obj: File-like object to upload
            s3_key (str): S3 object key (path in the bucket)
            content_type (str, optional): Content type of the file
            
        Returns:
            bool: True if upload was successful, False otherwise
            
        Raises:
            ClientError: If an error occurs during upload
        """
        try:
            # Add ACL to ensure bucket owner has full control
            extra_args = {
                'ACL': 'bucket-owner-full-control'
            }
            
            if content_type:
                extra_args['ContentType'] = content_type
                
            self.s3_client.upload_fileobj(
                file_obj, 
                self.bucket_name, 
                s3_key,
                ExtraArgs=extra_args
            )
            logger.info(f"Successfully uploaded file object to {self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Error uploading file object to S3: {e}")
            raise
    
    def download_file(self, s3_key, local_path):
        """
        Download a file from the S3 bucket.
        
        Args:
            s3_key (str): S3 object key (path in the bucket)
            local_path (str): Local path where the file will be saved
            
        Returns:
            bool: True if download was successful, False otherwise
            
        Raises:
            ClientError: If an error occurs during download
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Successfully downloaded {self.bucket_name}/{s3_key} to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Error downloading file from S3: {e}")
            raise
    
    def upload_json(self, data, s3_key):
        """
        Upload JSON data to the S3 bucket.
        
        Args:
            data (dict): JSON-serializable data to upload
            s3_key (str): S3 object key (path in the bucket)
            
        Returns:
            bool: True if upload was successful, False otherwise
            
        Raises:
            ClientError: If an error occurs during upload
        """
        try:
            json_data = json.dumps(data)
            self.s3_client.put_object(
                Body=json_data,
                Bucket=self.bucket_name,
                Key=s3_key,
                ContentType='application/json',
                ACL='bucket-owner-full-control'  # Add ACL to ensure bucket owner has full control
            )
            logger.info(f"Successfully uploaded JSON data to {self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Error uploading JSON to S3: {e}")
            raise
    
    def check_if_file_exists(self, s3_key):
        """
        Check if a file exists in the S3 bucket.
        
        Args:
            s3_key (str): S3 object key (path in the bucket)
            
        Returns:
            bool: True if the file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking if file exists in S3: {e}")
                raise
