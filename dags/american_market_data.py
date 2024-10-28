from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
# Import necessary functions from other scripts
from functions.financial_market_tracker.webscarper import save_market_data_to_file
from dags.functions.general_functions.upload_files import upload_preprocessed_files_to_vector_store
from functions.financial_market_tracker.slack_bot import post_file_content_to_slack

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Each task run is independent of past runs
    'start_date': datetime(2023, 10, 1),  # Start date of the DAG
    'email_on_failure': False,  # Do not send emails on task failure
    'email_on_retry': False,  # Do not send emails on task retry
    'retries': 1,  # Number of retries allowed if the task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

# Define the DAG that schedules the data retrieval and upload process for the American market
with DAG(
    'american_market_closing_data_retrieval',
    default_args=default_args,
    description='DAG to extract data for the American market',  # Short description of the DAG
    schedule_interval='30 21 * * 1-5',  # Runs at 9:30 PM UTC (Monday to Friday)
    catchup=False  # Do not run missed task instances from the past
) as dag: 

    # Define the output path where the market data will be saved
    output_path = "/opt/airflow/dags/files/preprocessed/american_market_closing_data.txt"
    local_path = "/opt/airflow/files/american_market_closing_data.txt"

    # Define the task function that retrieves and saves the market data
    def run_american_market_data():
        """
        Retrieve and save the American market closing data to two file paths: 
        the preprocessed directory and a local copy.
        
        This function calls 'save_market_data_to_file()' to perform the data retrieval and saving.
        """
        save_market_data_to_file(output_path)  # Save to preprocessed directory
    
    # Create the PythonOperator task to extract American market data
    extract_american_closing_market_data = PythonOperator(
        task_id='extract_american_closing_market_data',  # Unique task identifier
        python_callable=run_american_market_data,  # The Python function to be executed
        dag=dag
    )
    
    # Define the task function to post the market data to Slack
    def post_content_to_slack():
        """
        Posts the content of the American market closing data to a Slack channel.
        
        This function calls 'post_file_content_to_slack()' and sends the content of 
        the specified file to the Slack channel.
        """
        post_file_content_to_slack(output_path)  # Post data to Slack
    
    # Create the PythonOperator task to post the market data to Slack
    post_the_file_content_to_slack = PythonOperator(
        task_id='post_file_content_to_slack',  # Unique task identifier
        python_callable=post_content_to_slack,  # The Python function to be executed
        dag=dag
    )

    # Define the task function to upload preprocessed files to a vector store
    def upload_preprocessed_files():
        """
        Uploads preprocessed files (e.g., CSV files) to a vector store for further processing.
        
        This function uses 'upload_preprocessed_files_to_vector_store()' to handle the 
        upload operation, assuming the files are already saved in the predefined directory.
        """
        upload_preprocessed_files_to_vector_store()  # Call the function to upload files

    # Create the PythonOperator task to upload files to the vector store
    upload_files_task = PythonOperator(
        task_id='upload_preprocessed_files',  # Unique task identifier
        python_callable=upload_preprocessed_files,  # The Python function to be executed
    )
    
    # Define the task execution order: first extract market data, then post to Slack, and finally upload files
    extract_american_closing_market_data >> post_the_file_content_to_slack >> upload_files_task
