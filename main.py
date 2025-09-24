from typing import Union
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.app.api.document_api import router as document_router
from src.app.core.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include routers
app.include_router(document_router, prefix=settings.API_PREFIX)


@app.get("/health")
def health_check():
    return {"status": "healthy"}
