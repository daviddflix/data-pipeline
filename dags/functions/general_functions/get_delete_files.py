import os

# Function to get all files with a specific extension in a directory
def get_all_files(directory, file_extension='.txt'):
    """
    Retrieve all files with the specified extension from the given directory.

    Args:
        directory (str): The path to the directory to search for the files.
        file_extension (str): The file extension to filter by (default is '.txt').

    Returns:
        list: A list of full paths of the files found, or an empty list if no files are found.
    """
    return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(file_extension)]


# Function to delete files from a source directory
def delete_files(source_directory, file_extension='.txt'):
    """
    Delete all files with a specified extension from the source directory.

    Args:
        source_directory (str): Directory to delete files from.
        file_extension (str): The file extension to filter by (default is '.txt').
    """
    files_to_delete = get_all_files(source_directory, file_extension)
    if not files_to_delete:
        print(f"No {file_extension} files found to delete from {source_directory}.")
        return

    for file in files_to_delete:
        os.remove(file)
        print(f"Deleted file {file}")