from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
from pytz import timezone

# Import necessary functions from external scripts for handling Monday.com board operations
from functions.nv_coins_automation.monday_client import get_formatted_board_items
from functions.nv_coins_automation.monday_client import update_coin_prices

# Define the U.K. time zone for consistent scheduling
uk_tz = timezone('Europe/London')

# Define default arguments for the DAG with standard Airflow configurations
default_args = {
    'owner': 'airflow',  # Specifies who owns/maintains this DAG
    'depends_on_past': False,  # Each run is independent of previous runs
    'email_on_failure': False,  # Disables email notifications on task failure
    'email_on_retry': False,  # Disables email notifications on task retry
    'retries': 1,  # Attempts to retry failed task once
    'retry_delay': timedelta(minutes=5),  # Waits 5 minutes before retry attempt
}

# Define the DAG for retrieving and updating coin prices
with DAG(
    'nv_coins_automation',  # Unique identifier for this DAG
    default_args=default_args,  # Apply the default configurations
    description='DAG for automating coin price updates in Monday.com boards',  
    schedule_interval='0 12,0 * * *',  # Executes at 12:00 PM and 12:00 AM UTC daily
    start_date=datetime(2024, 11, 20, tzinfo=uk_tz),  # Initial execution date
    catchup=False  # Prevents backfilling of missed runs
) as dag:

    def nv_coins_automation_py():
        """
        Automates the process of updating coin prices in Monday.com boards.
        
        This function performs the following operations:
        1. Retrieves board items from Monday.com boards containing "Master" in their names
        2. Saves the retrieved data to a JSON file for processing
        3. Updates coin prices using external price sources
        4. Updates the Monday.com boards with new price data
        5. Clears the temporary JSON storage for the next run
        
        The function runs twice daily to ensure price data stays current across all boards.
        """
        search_param = "Master"
        formatted_json = get_formatted_board_items(search_param)
        print("The results have been saved in 'board_items.json'")

        with open('all_boards_data.json', 'r') as f:
            json_data = json.load(f)

        # Update prices and Monday.com
        updated_json = update_coin_prices(json_data)

        # Clear the JSON to make it ready for the next use
        with open('all_boards_data.json', 'w') as f:
            json.dump({"boards": []}, f, indent=2)  # Save an empty object

        print("The JSON has been cleared and is ready for the next use.")
        
     # Create the task that will execute our coin price update function
    nv_coins_automation = PythonOperator(
        task_id='nv_coins_automation',  # Unique identifier for this task
        python_callable=nv_coins_automation_py,  # Points to our processing function
        dag=dag  # Associates this task with our DAG
    )

    # Define task flow (single task, so no dependencies needed)
    nv_coins_automation