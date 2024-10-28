import os
from openai import OpenAI
from typing import List, Dict, Any, Optional
import logging
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()


class VectorStoreManager:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)
        self.logger = logging.getLogger(__name__)
        self.extensions = ['.pdf', '.txt', '.json']

    def create_vector_store(self, name: str, 
                            file_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Create a new vector store with the specified parameters.
        """
        try:
            vector_store = self.client.beta.vector_stores.create(
                name=name,
                file_ids=file_ids or []
            )
            return vector_store
        except Exception as e:
            self.logger.error(f"Error creating vector store: {str(e)}")
            raise

    def list_vector_stores(self, limit: int = 20, order: str = "desc", 
                           after: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all vector stores, up to the specified limit.
        """
        try:
            vector_stores = self.client.beta.vector_stores.list(
                limit=limit,
                order=order,
                after=after
            )
            return [vs.model_dump() for vs in vector_stores.data]
        except Exception as e:
            self.logger.error(f"Error listing vector stores: {str(e)}")
            raise

    def delete_vector_store(self, vector_store_id: str) -> Dict[str, Any]:
        """
        Delete a vector store by its ID.
        """
        try:
            deleted_vector_store = self.client.beta.vector_stores.delete(vector_store_id)
            return deleted_vector_store.model_dump()
        except Exception as e:
            self.logger.error(f"Error deleting vector store: {str(e)}")
            raise

    def update_vector_store(self, vector_store_id: str, name: Optional[str] = None, 
                            description: Optional[str] = None) -> Dict[str, Any]:
        """
        Update a vector store with the provided parameters.
        """
        try:
            updated_vector_store = self.client.beta.vector_stores.update(
                vector_store_id,
                name=name,
            )
            return updated_vector_store.model_dump()
        except Exception as e:
            self.logger.error(f"Error updating vector store: {str(e)}")
            raise

    def add_files_to_vector_store(self, vector_store_id: str, 
                            name: Optional[str] = None, 
                            description: Optional[str] = None,
                            local_paths: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Update a vector store with new metadata and/or add new files.

        Args:
            vector_store_id (str): The ID of the vector store to update.
            name (Optional[str]): New name for the vector store.
            description (Optional[str]): New description for the vector store.
            local_paths (Optional[List[str]]): A list of local file paths to add to the vector store.

        Returns:
            Dict[str, Any]: A dictionary containing the update status and details.
        """
        try:
            update_result = {}

            # Update metadata if provided
            if name is not None or description is not None:
                updated_vector_store = self.client.beta.vector_stores.update(
                    vector_store_id,
                    name=name,
                    description=description
                )
                update_result["metadata_update"] = updated_vector_store

            # Add new files if provided
            if local_paths:
                file_ids = []
                for path in local_paths:
                    with open(path, 'rb') as file:
                        new_file = self.client.files.create(
                            file=file,
                            purpose='assistants'
                        )
                        if new_file.id:
                            update_response = self.client.beta.vector_stores.files.create_and_poll(
                                vector_store_id=vector_store_id,
                                file_id=new_file.id
                            )
                            file_ids.append(new_file.id)
                            self.logger.info(f"File added to vector store: {os.path.basename(path)}")

                update_result["files_added"] = {
                    "file_ids": file_ids,
                    "count": len(file_ids)
                }

            return update_result

        except Exception as e:
            self.logger.error(f"Error updating vector store: {str(e)}")
            raise

    def list_vector_store_files(self, vector_store_id: str, limit: int = 20, 
                                order: str = "desc", after: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List files in a vector store.
        """
        try:
            files = self.client.beta.vector_stores.files.list(
                vector_store_id=vector_store_id,
                limit=limit,
                order=order,
                after=after
            )
            return [file.model_dump() for file in files.data]
        except Exception as e:
            self.logger.error(f"Error listing vector store files: {str(e)}")
            raise
    
    def list_documents(self, root_folder: str, extensions: Optional[List[str]]=None):
        """
        Recursively lists absolute paths to documents (files) within a root folder, filtered by extensions.

        Args:
            root_folder (str): The root folder to start the search from.
            extensions (list): List of file extensions to filter. Example: ['.pdf', '.txt']. If None, uses self.extensions.

        Returns:
            file_paths (list): List of absolute paths to documents found matching the extensions.
        """
        file_paths = []

        if extensions is None:
            extensions = self.extensions

        for root, _, files in os.walk(root_folder):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                _, file_ext = os.path.splitext(file_name)
                if file_ext.lower() in extensions:
                    file_paths.append(file_path)

        return file_paths
    
    def add_files_to_vector_store_v2(self, vector_store_id: str, 
                            name: Optional[str] = None, 
                            batch_size: int = 200,
                            description: Optional[str] = None,
                            local_paths: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Update a vector store with new metadata and/or add new files in batches.

        Args:
            vector_store_id (str): The ID of the vector store to update.
            name (Optional[str]): New name for the vector store.
            description (Optional[str]): New description for the vector store.
            local_paths (Optional[List[str]]): A list of local file paths to add to the vector store.

        Returns:
            Dict[str, Any]: A dictionary containing the update status and details.
        """
        try:
            update_result = {}

            # Update metadata if provided
            if name is not None or description is not None:
                updated_vector_store = self.client.beta.vector_stores.update(
                    vector_store_id,
                    name=name,
                )
                update_result["metadata_update"] = updated_vector_store.model_dump()

            # Add new files if provided
            if local_paths:
                batch_size = batch_size
                total_files_added = 0
                batch_results = []

                for i in range(0, len(local_paths), batch_size):
                    batch_paths = local_paths[i:i + batch_size]
                    file_streams = []

                    try:
                        for path in batch_paths:
                            if os.path.exists(path):
                                file_streams.append(open(path, "rb"))
                            else:
                                self.logger.warning(f"File not found: {path}")

                        if file_streams:
                            file_batch = self.client.beta.vector_stores.file_batches.upload_and_poll(
                                vector_store_id=vector_store_id,
                                files=file_streams
                            )
                            batch_result = {
                                "batch_number": i // batch_size + 1,
                                "status": file_batch.status,
                                "file_counts": file_batch.file_counts
                            }
                            batch_results.append(batch_result)
                            total_files_added += file_batch.file_counts.completed
                        else:
                            self.logger.info(f"Batch {i // batch_size + 1}: No valid files to upload.")

                    except Exception as e:
                        self.logger.error(f"Error in batch {i // batch_size + 1}: {str(e)}")
                        batch_results.append({
                            "batch_number": i // batch_size + 1,
                            "error": str(e)
                        })

                    finally:
                        for file in file_streams:
                            file.close()

                update_result["files_added"] = {
                    "total_files_added": total_files_added,
                    "batch_results": batch_results
                }

            return update_result

        except Exception as e:
            self.logger.error(f"Error updating vector store: {str(e)}")
            raise

# Example usage

# Retrieve API key from environment variable
#api_key = os.getenv("OPENAI_API_KEY")
# if not api_key:
#     raise ValueError("OPENAI_API_KEY environment variable not set")

# manager = VectorStoreManager(api_key)

# # Create a vector store
# new_vector_store = manager.create_vector_store(
#     name="protocols",
# )
# print("\nCreated Vector Store:", new_vector_store)

# # List vector stores
# vector_stores = manager.list_vector_stores()
# print("\nList of Vector Stores:", vector_stores)

# # Update a vector store
# updated_vector_store = manager.update_vector_store(
#     new_vector_store.id,
#     description="An updated description for the financial statements vector store."
# )
# print("\nUpdated Vector Store:", updated_vector_store)

# # Add files to the vector store
# file_paths = manager.list_documents(root_folder='penelope_database')
# added_files = manager.add_files_to_vector_store(local_paths=file_paths, vector_store_id=new_vector_store.id)
# print("Added Files:", added_files)

# # List files in the vector store
# vector_store_files = manager.list_vector_store_files(new_vector_store.id)
# print("\nVector Store Files:", vector_store_files)

# # Delete a vector store
# deleted_vector_store = manager.delete_vector_store('vs_JrnFQpY13BFw5sNjBKnRqTWF')
# print("\nDeleted Vector Store:", deleted_vector_store)
