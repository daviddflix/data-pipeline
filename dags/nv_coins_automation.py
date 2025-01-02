from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
import time 
from pytz import timezone

# Import necessary functions from external scripts for handling Monday.com board operations
from functions.nv_coins_automation.monday_client import get_formatted_board_items
from functions.nv_coins_automation.monday_client import update_coin_prices
from functions.nv_coins_automation.monday_client import get_specific_wallets_data
from functions.nv_coins_automation.monday_client import get_coin_prices
from functions.nv_coins_automation.monday_client import change_column_value
from functions.nv_coins_automation.monday_client import update_sentx_prices
 
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
    schedule_interval="0 13,20 * * *",  # Executes at 1:00 PM and 8:00 PM UK time
    start_date=datetime(2024, 11, 20, tzinfo=uk_tz),  # Initial execution date
    catchup=False  # Prevents backfilling of missed runs
) as dag:

    def nv_coins_automation_py():
        """
        Main function for automating cryptocurrency price updates and wallet data synchronization in Monday.com boards.
        
        The function performs the following steps:
        1. Retrieves Master Board data from Monday.com
        2. Updates cryptocurrency prices for the Master Board
        3. Fetches and processes wallet data
        4. Updates individual wallet prices in Monday.com using the CoinGecko API
        5. Updates SENTX-specific prices
        6. Performs cleanup of temporary files
        
        The function includes error handling and logging at each step.
        
        Raises:
            Exception: If there are errors in retrieving wallet data or other critical operations
            
        Note:
            - Uses CoinGecko API for price data
            - Implements rate limiting (0.5s delay between API calls)
            - Handles price formatting to 7 decimal places
            - Operates on CEX MASTER board (ID: 1652251054)
        """
        try:
            print("\n=== Starting Price Update Process ===")
            
            # 1. Get Master Board data
            print("\n1. Getting Master Board data...")
            search_param = "Master"
            formatted_json = get_formatted_board_items(search_param)
            print(" Master Board data saved to 'board_items.json'")

            # 2. Load and update prices
            print("\n2. Updating prices for Master Board...")
            with open('all_boards_data.json', 'r') as f:
                master_board_data = json.load(f)
            updated_master = update_coin_prices(master_board_data)
            print("✓ Master Board prices updated")

            # 3. Get wallet data
            print("\n1. Getting wallet data...")
            wallets_data = get_specific_wallets_data()
            if not wallets_data['success']:
                raise Exception(f"Failed to get wallet data: {wallets_data['error']}")
            print("✓ Wallet data retrieved")

            # 4. Update prices directly using change_column_value
            print("\n2. Updating prices in Monday.com...")
            board_id = 1652251054  # CEX MASTER BOARD ID
            
            for group_name, items in wallets_data['data'].items():
                print(f"\nProcessing group: {group_name}")
                for item in items:
                    try:
                        item_id = item['id']
                        code = item['columns']['Code']['value']
                        valuation_column_id = item['columns']['Valuation Price']['id']
                        
                        if code:
                            print(f"\nProcessing wallet item: {item['name']} with code: {code}")
                            # Use CoinGecko API to get current prices
                            prices = get_coin_prices("CG-4uzPgs2oyq4aL8vqJEoB2zfD", [
                                {
                                    "coin_symbol": code,
                                    "coin_name": item['name']
                                }
                            ])
                            
                            # Get price using lowercase code
                            price = prices.get(code.lower(), {}).get('usd', 0)
                            print(f"Found price for {code}: ${price}")
                            
                            # Format price to 7 decimal places
                            try:
                                price = float(price)
                                formatted_price = f"{price:.7f}".rstrip('0').rstrip('.')
                            except ValueError:
                                formatted_price = "0"
                            
                            result = change_column_value(
                                item_id=int(item_id),
                                board_id=board_id,
                                column_id=valuation_column_id,
                                value=formatted_price
                            )
                            print(f"{'✓' if result else '⚠️'} Updated {code}: {item['name']} with price {formatted_price}")
                        
                        time.sleep(0.5)  # Rate limiting
                        
                    except Exception as e:
                        print(f"⚠️ Error updating {item['name']}: {str(e)}")
                        continue
            
            # 5. Update SENTX prices   
            print("\n4. Updating SENTX prices...")
            update_sentx_prices()
            
            # 6. Clean up temporary files
            print("\n5. Cleaning up...")
            # Clean all_boards_data.json
            with open('all_boards_data.json', 'w') as f:
                json.dump({"boards": []}, f, indent=2)
            # Clean all_items_minimal.json
            with open('all_items_minimal.json', 'w') as f:
                json.dump({"success": True, "data": {}, "error": None}, f, indent=2)
            print("✓ Temporary files cleaned")

            print("\n=== Process Completed Successfully ===")
        except Exception as e:
            print(f"\n⚠️ Error in main process: {str(e)}")
            print("Process terminated with errors")
            
   # Create the task that will execute our coin price update function
    nv_coins_automation = PythonOperator(
       task_id='nv_coins_automation',  # Unique identifier for this task
       python_callable=nv_coins_automation_py,  # Points to our processing function
       dag=dag  # Associates this task with our DAG
    )

    # Define task flow (single task, so no dependencies needed)
    nv_coins_automation