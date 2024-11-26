from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone

# Import necessary functions from external scripts
from functions.top_20_boards.top_20_boards import get_board_items
from functions.top_20_boards.top_20_boards import save_board_items_to_json
from functions.top_20_boards.top_20_boards import load_board_data
from functions.top_20_boards.top_20_boards import save_to_json
from functions.top_20_boards.top_20_boards import update_monday_boards

# Define the U.K. time zone
uk_tz = timezone('Europe/London')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Task runs are independent of past runs
    'email_on_failure': False,  # No emails on failure
    'email_on_retry': False,  # No emails on retries
    'retries': 1,  # Number of retry attempts if the task fails
    'retry_delay': timedelta(minutes=5),  # Delay between retry attempts
}

# Define the DAG for retrieving European market closing data
with DAG(
    'top_20_boards',  # Name of the DAG
    default_args=default_args,  # Use the default arguments defined above
    description='DAG for nv coins automation',  # Short description of the DAG
    schedule_interval='0 * * *',  # Runs at 12:00 PM and 12:00 AM UTC (adjusted for UK time)
    start_date=datetime(2024, 11, 20, tzinfo=uk_tz),
    catchup=False  # No backfilling for missed tasks
) as dag:

    # Task to extract and save European market data
    def top_20_boards_py():
        CEX_MASTER = 1652251054
        DEX_MASTER = 1678221568

        # Retrieve and process data from Monday.com
        board_items = get_board_items(board_ids=[DEX_MASTER, CEX_MASTER])
        if board_items:
            save_board_items_to_json(board_items)
        else:
            print("Could not retrieve data from the board.")
            return

        # Load data and process for filtering
        board_data = load_board_data()

        # Filter for best and worst coins
        bestcoins = [
            coin for coin in board_data 
            if coin.get('ROI', 0) > 50 and coin.get('Valuation Price', None) is not None
        ]

        worstcoins = [
            coin for coin in board_data 
            if coin.get('ROI', 0) < -90 and coin.get('Valuation Price', None) is not None
        ]

        # Save results to respective JSON files
        save_to_json(bestcoins, "best.json")
        save_to_json(worstcoins, "worst.json")
        print("Results saved in best.json and worst.json")

        # Update Monday.com boards
        update_monday_boards()
    
    # Create the PythonOperator task to extract European market closing data
    top_20_boards = PythonOperator(
        task_id='top_20_boards',  # Task identifier
        python_callable=top_20_boards_py,  # Function to execute for this task
        dag=dag  # Link task to the DAG
    )

    top_20_boards