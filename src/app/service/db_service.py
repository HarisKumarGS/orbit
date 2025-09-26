"""
Service for handling database operations.
"""
import logging
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.app.core.config import settings
from src.app.core.models.document import Base, Document

logger = logging.getLogger(__name__)

class DBService:
    """
    Service for handling database operations.
    """
    def __init__(self):
        """
        Initialize the database connection.
        """
        self.engine = create_engine(settings.DATABASE_URL)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create tables if they don't exist
        Base.metadata.create_all(bind=self.engine)
    
    def get_session(self):
        """
        Get a database session.
        
        Returns:
            Session: A SQLAlchemy session.
        """
        session = self.SessionLocal()
        try:
            return session
        except Exception as e:
            session.close()
            logger.error(f"Error getting database session: {e}")
            raise
    
    async def create_document(self, filename, s3_key):
        """
        Create a new document record in the database.
        
        Args:
            filename (str): The original filename.
            s3_key (str): The S3 key where the file is stored.
            
        Returns:
            Document: The created document.
        """
        session = self.get_session()
        try:
            document_id = uuid.uuid4()
            document = Document(
                id=document_id,
                filename=filename,
                s3_key=s3_key
            )
            session.add(document)
            session.commit()
            session.refresh(document)
            logger.info(f"Document created with ID: {document.id}")
            return document
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating document: {e}")
            raise
        finally:
            session.close()
