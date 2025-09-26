"""
PDF extraction functionality using unstructured library.
"""
import logging
import json
from unstructured.partition.pdf import partition_pdf
from unstructured.chunking.title import chunk_by_title
from unstructured.staging.base import elements_from_base64_gzipped_json

logger = logging.getLogger(__name__)

def extract_elements_from_pdf(file_path):
    """
    Extract elements from a PDF file using unstructured library with OCR.
    
    Args:
        file_path (str): Path to the PDF file.
        
    Returns:
        list: List of extracted elements.
    """
    try:
        logger.info(f"Extracting elements from PDF: {file_path}")
        elements = partition_pdf(
            filename=file_path,
            ocr_only=True,
            languages=["eng"]
        )
        logger.info(f"Extracted {len(elements)} elements from PDF")
        return elements
    except Exception as e:
        logger.error(f"Error extracting elements from PDF: {e}")
        raise

def chunk_elements_by_title(elements):
    """
    Chunk elements by title using unstructured library.
    
    Args:
        elements (list): List of elements extracted from PDF.
        
    Returns:
        list: List of chunks.
    """
    try:
        logger.info("Chunking elements by title")
        chunks = chunk_by_title(
            elements = elements
        )
        logger.info(f"Created {len(chunks)} chunks")
        return chunks
    except Exception as e:
        logger.error(f"Error chunking elements: {e}")
        raise

def group_narrative_by_title(chunks):
    result = []
    current_titles = []
    current_content = []
    current_page_numbers = set()
    
    def save_current_group():
        if current_titles or current_content:
            title = " | ".join(current_titles) if current_titles else "Untitled"
            content = " ".join(current_content).strip()
            page_numbers = sorted(list(current_page_numbers)) if current_page_numbers else []
            
            if title or content:  # Only add if there's actual content
                group = {
                    "title": title, 
                    "content": content,
                    "page_numbers": page_numbers
                }

                              # Print the group being saved
                print("="*60)
                print("SAVING GROUP:")
                print(f"Title: {title}")
                print(f"Content: {content}")
                print(f"Page Numbers: {page_numbers}")
                print(f"Content Length: {len(content)} characters")
                print("="*60)

                result.append(group)
    
    for chunk in chunks:
        metadata = chunk.metadata.to_dict()
        page_number = metadata.get("page_number")
        chunk_text = chunk.text.strip()
        
        if not chunk_text:
            continue
            
        # Add page number for this chunk
        if page_number is not None:
            current_page_numbers.add(page_number)
        
        orig_elements = elements_from_base64_gzipped_json(metadata["orig_elements"])
        
        # Check if this chunk contains any titles
        chunk_titles = [elem.text.strip() for elem in orig_elements 
                       if elem.category == "Title" and elem.text.strip()]
        
        if chunk_titles:
            if current_content:
                # Save current group when we have content
                save_current_group()
                current_titles = chunk_titles
                current_content = [chunk_text]
                current_page_numbers = {page_number} if page_number is not None else set()
            else:
                # Accumulate consecutive titles
                current_titles.extend(chunk_titles)
                current_content = [chunk_text]
        else:
            # No titles in this chunk, add text to current content
            current_content.append(chunk_text)
    
    # Save final group
    save_current_group()
    return result

