from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv
load_dotenv() 
import os
# Import necessary functions from external scripts
from functions.nv_coins_automation.monday_client import get_formatted_board_items
from functions.nv_coins_automation.monday_client import update_coin_prices

MONDAY_API_KEY_NOVATIDE = os.getenv('MONDAY_API_KEY_NOVATIDE')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Task runs are independent of past runs
    'start_date': datetime(2024, 11, 19),  # The start date for the DAG
    'email_on_failure': False,  # No emails on failure
    'email_on_retry': False,  # No emails on retries
    'retries': 1,  # Number of retry attempts if the task fails
    'retry_delay': timedelta(minutes=5),  # Delay between retry attempts
}

# Define the DAG for retrieving European market closing data
with DAG(
    'nv_coins_automation',  # Name of the DAG
    default_args=default_args,  # Use the default arguments defined above
    description='DAG for nv coins automation',  # Short description of the DAG
    schedule_interval='0 16 * * 1-5',  # Schedule to run at 4:00 PM UTC, Monday to Friday
    catchup=False  # No backfilling for missed tasks
) as dag:

    # Task to extract and save European market data
    def nv_coins_automation_py():
        print(MONDAY_API_KEY_NOVATIDE)
        search_param = "Master sheet"
        formatted_json = get_formatted_board_items(search_param)
        print("The results have been saved in 'board_items.json'")
    
        with open('all_boards_data.json', 'r') as f:
            json_data = json.load(f)
    
        # Update prices and Monday.com
        updated_json = update_coin_prices(json_data, MONDAY_API_KEY_NOVATIDE)
    
        # Clear the JSON to make it ready for the next use
        with open('all_boards_data.json', 'w') as f:
            json.dump({"boards": []}, f, indent=2)  # Save an empty object
    
        print("The JSON has been cleared and is ready for the next use.")
        #print(f"Number of calls made to Coingecko: {get_coin_prices.coingecko_calls}")  # Informative print
    
    # Create the PythonOperator task to extract European market closing data
    nv_coins_automation = PythonOperator(
        task_id='nv_coins_automation',  # Task identifier
        python_callable=nv_coins_automation_py,  # Function to execute for this task
        dag=dag  # Link task to the DAG
    )

    nv_coins_automation
