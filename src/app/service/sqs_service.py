"""
SQS Service for handling AWS SQS operations.
"""
import json
import datetime
import boto3
from botocore.exceptions import ClientError
import logging
from ..utils.aws_utils import assume_role

logger = logging.getLogger(__name__)

class SQSService:
    """
    Service for interacting with AWS SQS queues.
    """
    
    def __init__(self, queue_url):
        """
        Initialize the SQS service with a queue URL.
        
        Args:
            queue_url (str): The URL of the SQS queue to use
        """
        self.queue_url = queue_url
        self.sqs_client = boto3.client('sqs')
    
    def send_message(self, message_body, message_attributes=None, delay_seconds=0):
        """
        Send a message to the SQS queue.
        
        Args:
            message_body (dict or str): The message body to send
            message_attributes (dict, optional): Message attributes
            delay_seconds (int, optional): Delay in seconds before the message is available
            
        Returns:
            str: Message ID if successful
            
        Raises:
            ClientError: If an error occurs during sending
        """
        try:
            # Convert dict to JSON string if necessary
            if isinstance(message_body, dict):
                message_body = json.dumps(message_body)
            
            params = {
                'QueueUrl': self.queue_url,
                'MessageBody': message_body
            }
            
            if delay_seconds > 0:
                params['DelaySeconds'] = delay_seconds
                
            if message_attributes:
                params['MessageAttributes'] = message_attributes
            
            response = self.sqs_client.send_message(**params)
            message_id = response['MessageId']
            logger.info(f"Successfully sent message {message_id} to {self.queue_url}")
            return message_id
        except ClientError as e:
            logger.error(f"Error sending message to SQS: {e}")
            raise
    
    def send_status_update(self, job_id, status, details=None):
        """
        Send a status update message to the SQS queue.
        
        Args:
            job_id (str): The ID of the job being updated
            status (str): Status of the job ('STARTED', 'IN_PROGRESS', 'COMPLETED', etc.)
            details (dict, optional): Additional details about the status
            
        Returns:
            str: Message ID if successful
            
        Raises:
            ClientError: If an error occurs during sending
        """
        message = {
            'jobId': job_id,
            'status': status,
            'timestamp': str(datetime.datetime.now()),
        }
        
        if details:
            message['details'] = details
            
        return self.send_message(message)
    
    def receive_messages(self, max_messages=10, wait_time=20, visibility_timeout=30):
        """
        Receive messages from the SQS queue.
        
        Args:
            max_messages (int, optional): Maximum number of messages to receive (1-10)
            wait_time (int, optional): Long polling wait time in seconds (0-20)
            visibility_timeout (int, optional): Visibility timeout in seconds
            
        Returns:
            list: List of received messages
            
        Raises:
            ClientError: If an error occurs during receiving
        """
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=visibility_timeout,
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            logger.info(f"Received {len(messages)} messages from {self.queue_url}")
            return messages
        except ClientError as e:
            logger.error(f"Error receiving messages from SQS: {e}")
            raise
    
    def delete_message(self, receipt_handle):
        """
        Delete a message from the SQS queue.
        
        Args:
            receipt_handle (str): The receipt handle of the message to delete
            
        Returns:
            bool: True if deletion was successful
            
        Raises:
            ClientError: If an error occurs during deletion
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info(f"Successfully deleted message from {self.queue_url}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting message from SQS: {e}")
            raise
    
    def process_messages(self, handler_function, max_messages=10, wait_time=20):
        """
        Process messages from the SQS queue using a handler function.
        
        Args:
            handler_function (callable): Function to process each message
            max_messages (int, optional): Maximum number of messages to receive
            wait_time (int, optional): Long polling wait time in seconds
            
        Returns:
            int: Number of messages processed
            
        Raises:
            ClientError: If an error occurs during processing
        """
        messages = self.receive_messages(max_messages, wait_time)
        processed_count = 0
        
        for message in messages:
            try:
                # Parse message body if it's JSON
                try:
                    body = json.loads(message['Body'])
                except (json.JSONDecodeError, TypeError):
                    body = message['Body']
                
                # Process the message
                handler_function(body, message)
                
                # Delete the message after successful processing
                self.delete_message(message['ReceiptHandle'])
                processed_count += 1
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Don't delete the message so it can be retried
        
        return processed_count
