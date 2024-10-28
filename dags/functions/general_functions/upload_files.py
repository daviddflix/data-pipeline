from dags.functions.general_functions.vector_store import VectorStoreManager  # Ensure this is correctly imported
from dotenv import load_dotenv
import os

# Load the environment variables from the .env file
load_dotenv('/opt/airflow/dags/.env')

# Retrieve the OpenAI API key and Vector Store ID from environment variables
API_KEY = os.getenv('OPENAI_API_KEY')
VECTOR_STORE_ID = os.getenv('VECTOR_STORE_ID')

# Initialize the VectorStoreManager using the API key
manager = VectorStoreManager(api_key=API_KEY)
print(API_KEY)
# Define the folder where preprocessed files are located
PREPROCESSED_FOLDER = '/opt/airflow/dags/files/preprocessed'  # Adjust this path based on where your preprocessed files are stored

def upload_preprocessed_files_to_vector_store():
    """
    Upload preprocessed files from the specified folder to the OpenAI vector store.

    This function lists all preprocessed files from the `PREPROCESSED_FOLDER`,
    uploads them to the OpenAI vector store using the VectorStoreManager, and 
    then deletes the local files after a successful upload.

    Raises:
        ValueError: If no files are found in the preprocessed folder.
        Exception: If an error occurs during the file upload process.
    """
    try:
        # List all preprocessed files in the specified folder
        preprocessed_files = manager.list_documents(root_folder=PREPROCESSED_FOLDER)
        print(preprocessed_files)
        
        # Raise an error if no files are found in the folder
        if not preprocessed_files:
            raise ValueError("No preprocessed files found in the folder.")

        # Upload the preprocessed files to the vector store
        result = manager.add_files_to_vector_store(
            vector_store_id=VECTOR_STORE_ID,
            local_paths=preprocessed_files
        )
        
        # Log the result of the upload
        print(f"Files successfully added to the vector store: {result}")
        
        # Remove the processed files from the local folder
        for file in preprocessed_files:
            os.remove(file)
            print(f"File {file} deleted after saving in the vector store.")

    except Exception as e:
        # Log and raise any errors that occur during the upload process
        print(f"Error uploading files to the vector store: {str(e)}")
        raise