import os
import requests
from typing import List
from datetime import timedelta, datetime
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Load environment variables from the .env file
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
BASE_URL = os.getenv('BASE_URL')

headers = {
    "Content-Type": "application/json",
    "x-cg-pro-api-key": COINGECKO_API_KEY,
}

# Get today's date
current_date = datetime.now()
# Calculate the date one year ago
one_year_ago = current_date - timedelta(days=365)
# Format the date as "dd-mm-yyyy"
formatted_date = one_year_ago.strftime('%d-%m-%Y')
print("Date one year ago:", formatted_date)

def get_token_data(coin):
    try:
        formatted_coin = str(coin).casefold().strip()
        response = requests.get(f'{BASE_URL}/coins/{formatted_coin}', headers=headers)
        historical_response = requests.get(f'{BASE_URL}/coins/{formatted_coin}/history?date={formatted_date}', headers=headers)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        if response.status_code == 200 and historical_response.status_code == 200:
            response = response.json()
            historical_response = historical_response.json()
            id = response.get('id')
            symbol = response.get('symbol')
            market_cap_usd = response['market_data']['market_cap']['usd'] \
                if 'market_data' in response and 'market_cap' in response['market_data'] and 'usd' in response['market_data']['market_cap'] else None
            total_volume = response['market_data']['total_volume']['usd'] \
                if 'market_data' in response and 'total_volume' in response['market_data'] and 'usd' in response['market_data']['total_volume'] else None
            circulating_supply = response['market_data'].get('circulating_supply')
            percentage_circulating_supply = (float(circulating_supply) / float(response['market_data'].get('total_supply'))) * 100 \
                if circulating_supply and response['market_data'].get('total_supply') else None
            current_price = response['market_data']['current_price']['usd'] \
                if 'market_data' in response and 'current_price' in response['market_data'] and 'usd' in response['market_data']['current_price'] else None
            ath = response['market_data']['ath']['usd'] \
                if 'market_data' in response and 'ath' in response['market_data'] and 'usd' in response['market_data']['ath'] else None
            price_a_year_ago = historical_response['market_data']['current_price']['usd'] \
                if 'market_data' in historical_response and 'current_price' in historical_response['market_data'] and 'usd' in historical_response['market_data']['current_price'] else None

            # Return the formatted data as a string
            return f"Coin: {formatted_coin}\n" + \
                   f"id: {id}\n" + \
                   f"symbol: {symbol}\n" + \
                   f"market_cap_usd: {market_cap_usd}\n" + \
                   f"total_volume: {total_volume}\n" + \
                   f"circulating_supply: {circulating_supply}\n" + \
                   f"percentage_circulating_supply: {percentage_circulating_supply}\n" + \
                   f"current_price: {current_price}\n" + \
                   f"ath: {ath}\n" + \
                   f"price_a_year_ago: {price_a_year_ago}\n" + \
                   f"current_date: {timestamp}\n\n"
        else:
            return f"Error retrieving data for {formatted_coin}: {response.content.decode('utf-8')}\n"
    except Exception as e:
        return f"Error processing {coin}: {str(e)}\n"


# Function to process multiple coins and save to a single file
def process_multiple_coins(coin_list: List[str], file_directory: str):
    # Asegúrate de que file_directory sea un directorio y no un archivo
    if not os.path.exists(file_directory):
        os.makedirs(file_directory)
    
    # Define el archivo dentro del directorio
    file_path = os.path.join(file_directory, 'all_coins_data.txt')
    
    with open(file_path, 'w') as file:
        for coin in coin_list:
            print(f"Processing coin: {coin}")
            result = get_token_data(coin)
            print(result)
            file.write(result)
    print(f"All coins data saved in: {file_path}")
    local_path = os.path.join('/opt/airflow/files/', 'all_coins_coingecko_data.txt')
    with open(local_path, 'w') as file:
        for coin in coin_list:
            print(f"Processing coin: {coin}")
            result = get_token_data(coin)
            print(result)
            file.write(result)
    print(f"All coins data saved in: {file_path}")
            
            
# List of coins to process
#coins = [
#    "bitcoin", "ethereum", "hack", "Lido-dao", "rocket-pool", "frax-share", "cosmos", "polkadot",
#    "quant-network", "cardano", "solana", "avalanche-2", "near", "fantom", "kaspa", "matic-network",
#    "arbitrum", "optimism", "chainlink", "api3", "band-protocol", "stellar", "algorand", "ripple",
#    "dydx", "velodrome-finance", "gmx", "uniswap", "sushi", "pancakeswap-token", "aave", "pendle",
#    "1inch", "ocean-protocol", "Fetch-ai", "Render-token"
#]

# File path to save the results
#file_path = '/home/lourdes22/data-pipeline/'

# Process each coin in the list
#process_multiple_coins(coins, file_path)
