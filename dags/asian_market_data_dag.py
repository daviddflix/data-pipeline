from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
# Import necessary functions from external scripts
from functions.financial_market_tracker.webscarper import save_market_data_to_file
from dags.functions.general_functions.upload_files import upload_preprocessed_files_to_vector_store
from dags.functions.financial_market_tracker.slack_bot import post_file_content_to_slack

# Define default arguments for the DAG (Directed Acyclic Graph)
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Tasks don't depend on previous task runs
    'start_date': datetime(2023, 10, 1),  # Start date of the DAG
    'email_on_failure': False,  # Disable email notifications on failure
    'email_on_retry': False,  # Disable email notifications on retries
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

# Define the DAG for retrieving Asian market closing data
with DAG(
    'asian_market_closing_data_retrieval',  # Name of the DAG
    default_args=default_args,  # Default arguments used for all tasks in this DAG
    description='DAG to extract data for the Asian market',  # Brief description of the DAG
    schedule_interval='30 6 * * 1-5',  # Schedule: runs at 6:30 AM UTC (Monday to Friday)
    catchup=False  # Do not run any previous missed tasks
) as dag:

    # Define paths for saving the market data (preprocessed and local)
    output_path = "/opt/airflow/dags/files/preprocessed/asian_market_closing_data.txt"
    #local_path = "/opt/airflow/files/asian_market_closing_data.txt"

    # Define the function to retrieve and save Asian market closing data
    def run_asian_market_data():
        """
        Retrieves and saves the closing data for the Asian market.

        This function saves the data in two locations:
        - 'output_path' for preprocessed data
        """
        save_market_data_to_file(output_path)  # Save data to the preprocessed path

    # Create a PythonOperator task to extract the Asian market closing data
    extract_asian_market_closing_data = PythonOperator(
        task_id='extract_asian_market_closing_data',  # Unique identifier for the task
        python_callable=run_asian_market_data,  # Function to be executed
        dag=dag  # Link the task to the DAG
    )

    # Define the function to post the extracted data to Slack
    def post_content_to_slack():
        """
        Posts the content of the Asian market closing data to a Slack channel.

        This function uses 'post_file_content_to_slack()' to post the file's content to Slack.
        """
        post_file_content_to_slack(output_path)

    # Create a PythonOperator task to post the market data to Slack
    post_the_file_content_to_slack = PythonOperator(
        task_id='post_file_content_to_slack',  # Unique identifier for the task
        python_callable=post_content_to_slack,  # Function to be executed
        dag=dag  # Link the task to the DAG
    )

    # Define the function to upload preprocessed files to the vector store
    def upload_preprocessed_files():
        """
        Uploads preprocessed files (CSV format) to the vector store for further processing.

        This function assumes the files are already saved in a predefined directory
        and uses 'upload_preprocessed_files_to_vector_store()' for the upload process.
        """
        upload_preprocessed_files_to_vector_store()  # Call the function to upload the files

    # Create a PythonOperator task to upload the files to the vector store
    upload_files_task = PythonOperator(
        task_id='upload_preprocessed_files',  # Unique identifier for the task
        python_callable=upload_preprocessed_files,  # Function to be executed
    )

    # Define the task dependencies: extract data -> post to Slack -> upload files
    extract_asian_market_closing_data >> post_the_file_content_to_slack >> upload_files_task
