from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
# Import necessary functions from external scripts
from functions.financial_market_tracker.webscarper import save_market_data_to_file
from dags.functions.general_functions.upload_files import upload_preprocessed_files_to_vector_store
from functions.financial_market_tracker.slack_bot import post_file_content_to_slack

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Task runs are independent of past runs
    'start_date': datetime(2024, 10, 29),  # The start date for the DAG
    'email_on_failure': False,  # No emails on failure
    'email_on_retry': False,  # No emails on retries
    'retries': 1,  # Number of retry attempts if the task fails
    'retry_delay': timedelta(minutes=5),  # Delay between retry attempts
}

# Define the DAG for retrieving European market closing data
with DAG(
    'european_market_closing_data_retrieval',  # Name of the DAG
    default_args=default_args,  # Use the default arguments defined above
    description='DAG to extract data for the European market',  # Short description of the DAG
    schedule_interval='0 16 * * 1-5',  # Schedule to run at 4:00 PM UTC, Monday to Friday
    catchup=False  # No backfilling for missed tasks
) as dag:

    # Define the file paths where the market data will be saved
    output_path = "/opt/airflow/dags/files/preprocessed/european_market_closing_data.txt"
    #local_path = "/opt/airflow/files/european_market_closing_data.txt"

    # Task to extract and save European market data
    def run_european_market_data():
        """
        Retrieves and saves the European market closing data.

        This function uses 'save_market_data_to_file()' to save the closing data
        to two file paths:
        - 'output_path': for the preprocessed data directory
        """
        save_market_data_to_file(output_path)  # Save data to preprocessed directory

    # Create the PythonOperator task to extract European market closing data
    extract_european_market_closing_data = PythonOperator(
        task_id='extract_european_market_closing_data',  # Task identifier
        python_callable=run_european_market_data,  # Function to execute for this task
        dag=dag  # Link task to the DAG
    )
    
    # Task to post the extracted market data to Slack
    def post_content_to_slack():
        """
        Posts the content of the European market closing data to a Slack channel.

        This function sends the market data content to a pre-configured Slack channel.
        The 'post_file_content_to_slack()' function can be used to achieve this.
        """
        # Uncomment the following line to enable posting to Slack
        post_file_content_to_slack(output_path)

    # Create a PythonOperator task to post the market data to Slack
    post_the_file_content_to_slack = PythonOperator(
        task_id='post_file_content_to_slack',  # Task identifier
        python_callable=post_content_to_slack,  # Function to execute for this task
        dag=dag  # Link task to the DAG
    )

    # Task to upload preprocessed market data files to the vector store
    def upload_preprocessed_files():
        """
        Uploads preprocessed files (in CSV format) to the vector store.

        This function uploads the preprocessed market data files to a vector store
        using 'upload_preprocessed_files_to_vector_store()' function.
        """
        upload_preprocessed_files_to_vector_store()  # Perform the upload to the vector store

    # Create the PythonOperator task to upload the preprocessed files
    upload_files_task = PythonOperator(
        task_id='upload_preprocessed_files',  # Task identifier
        python_callable=upload_preprocessed_files,  # Function to execute for this task
    )

    # Define the task dependencies: 
    # First, extract market data -> then post data to Slack -> then upload files to the vector store
    extract_european_market_closing_data >> post_the_file_content_to_slack >> upload_files_task
