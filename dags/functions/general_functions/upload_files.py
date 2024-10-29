from dags.functions.general_functions.vector_store import VectorStoreManager  # Ensure this is correctly imported
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get configuration from environment
API_KEY = os.getenv('OPENAI_API_KEY')
VECTOR_STORE_ID = os.getenv('VECTOR_STORE_ID')
PREPROCESSED_FOLDER = '/opt/airflow/dags/files/preprocessed'

def upload_preprocessed_files_to_vector_store():
    """Upload preprocessed files to the OpenAI vector store."""
    try:
        # Initialize the manager
        manager = VectorStoreManager(api_key=API_KEY)
        
        # List files
        preprocessed_files = manager.list_documents(root_folder=PREPROCESSED_FOLDER)
        logger.info(f"Found {len(preprocessed_files)} files to process")
        
        if not preprocessed_files:
            logger.warning("No preprocessed files found in the folder")
            return
            
        # Upload files
        result = manager.add_files_to_vector_store(
            vector_store_id=VECTOR_STORE_ID,
            local_paths=preprocessed_files
        )
        
        # Clean up successfully uploaded files
        for file in preprocessed_files:
            os.remove(file)
            logger.info(f"Deleted processed file: {file}")
            
        logger.info("File upload and cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"Error in upload process: {str(e)}")
        raise