"""
Service for handling AWS S3 operations.
"""
import logging
import boto3
from botocore.exceptions import ClientError
from src.app.core.config import settings

logger = logging.getLogger(__name__)

class S3Service:
    """
    Service for handling AWS S3 operations.
    """
    def __init__(self):
        """
        Initialize the S3 client.
        """
        self.s3_client = boto3.client('s3')
        self.bucket_name = settings.AWS_S3_BUCKET_NAME

    async def upload_file(self, file_content, file_name, content_type=None):
        """
        Upload a file to S3.

        Args:
            file_content: The content of the file to upload.
            file_name: The name of the file in S3.
            content_type: The content type of the file (optional).

        Returns:
            str: The S3 key of the uploaded file.
            
        Raises:
            Exception: If the upload fails.
        """
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=file_content,
                **extra_args
            )
            
            logger.info(f"File {file_name} uploaded to S3 bucket {self.bucket_name}")
            return file_name
        except ClientError as e:
            logger.error(f"Error uploading file to S3: {e}")
            raise Exception(f"Failed to upload file to S3: {str(e)}")
