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

# Define the DAG for retrieving and processing top 20 coins data
with DAG(
    'top_20_boards',  # Name of the DAG
    default_args=default_args,  # Use the default arguments defined above
    description='DAG for processing and updating top 20 performing coins',  # Short description of the DAG
    schedule_interval='0 10 * * 5',  # Runs at 10:00 UK time every Friday
    start_date=datetime(2024, 11, 29, tzinfo=uk_tz),  # Start on the next Friday
    catchup=False  # No backfilling for missed tasks
) as dag:

    def top_20_boards_py():
        """
        Main function to process and update top 20 performing coins boards.
        
        This function performs the following steps:
        1. Retrieves data from specified Monday.com boards (CEX and DEX)
        2. Processes and filters coins based on ROI performance
        3. Saves filtered results to JSON files
        4. Updates Monday.com boards with the processed data
        
        Board IDs:
        - CEX_MASTER: 1652251054 (Centralized Exchange Master Board)
        - DEX_MASTER: 1678221568 (Decentralized Exchange Master Board)
        """
        # Define board IDs for CEX and DEX master boards
        CEX_MASTER = 1652251054
        DEX_MASTER = 1678221568

        # Step 1: Retrieve and process data from Monday.com
        board_items = get_board_items(board_ids=[DEX_MASTER, CEX_MASTER])
        if board_items:
            save_board_items_to_json(board_items)
        else:
            print("Could not retrieve data from the board.")
            return

        # Step 2: Load processed data for filtering
        board_data = load_board_data()

        # Step 3: Filter coins based on performance criteria
        # Get best performing coins (ROI > 50%)
        bestcoins = [
            coin for coin in board_data 
            if coin.get('ROI', 0) > 50 and coin.get('Valuation Price', None) is not None
        ]

        # Get worst performing coins (ROI < -90%)
        worstcoins = [
            coin for coin in board_data 
            if coin.get('ROI', 0) < -90 and coin.get('Valuation Price', None) is not None
        ]

        # Step 4: Save filtered results to JSON files
        save_to_json(bestcoins, "best.json")
        save_to_json(worstcoins, "worst.json")
        print("Results saved in best.json and worst.json")

        # Step 5: Update Monday.com boards with new data
        update_monday_boards()
    
    # Create the PythonOperator task for top 20 boards processing
    top_20_boards = PythonOperator(
        task_id='top_20_boards',  # Task identifier
        python_callable=top_20_boards_py,  # Function to execute for this task
        dag=dag  # Link task to the DAG
    )

    # Set task dependencies (single task, no dependencies needed)
    top_20_boards