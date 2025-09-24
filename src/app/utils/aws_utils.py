"""
AWS utility functions.
"""
import boto3
import logging

logger = logging.getLogger(__name__)

def assume_role(role_arn, session_name="AssumedRoleSession"):
    """
    Assume an IAM role and return temporary credentials.
    
    Args:
        role_arn (str): The ARN of the role to assume
        session_name (str): A name for the assumed role session
        
    Returns:
        dict: Temporary credentials (access key, secret key, session token)
    """
    if not role_arn:
        logger.info("No role ARN provided, using default credentials")
        return {}
        
    try:
        logger.info(f"Assuming role: {role_arn}")
        sts_client = boto3.client('sts')
        response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name
        )
        
        credentials = response['Credentials']
        return {
            'aws_access_key_id': credentials['AccessKeyId'],
            'aws_secret_access_key': credentials['SecretAccessKey'],
            'aws_session_token': credentials['SessionToken']
        }
    except Exception as e:
        logger.error(f"Error assuming role {role_arn}: {e}")
        # Fall back to default credentials
        return {}
