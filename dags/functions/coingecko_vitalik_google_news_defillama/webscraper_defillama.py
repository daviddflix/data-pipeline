import requests
import json

# Function to format large numbers to a short format
def format_number_short(number):
    try:
        formatted_number = float(number)
    except (TypeError, ValueError):
        return "Invalid input"
    if formatted_number < 0:
        formatted_number = abs(formatted_number)
        negative_flag = True
    else:
        negative_flag = False
    suffixes = ['', 'k', 'M', 'B', 'T', 'P', 'E', 'Z', 'Y']
    suffix_index = 0
    while formatted_number >= 1000 and suffix_index < len(suffixes) - 1:
        formatted_number /= 1000.0
        suffix_index += 1
    formatted_string = '{:.3f}{}'.format(formatted_number, suffixes[suffix_index])
    if negative_flag:
        formatted_string = '-' + formatted_string
    return formatted_string

# Lists of chains and protocols of interest
coins_chains = [
    "bitcoin", "ethereum", "cosmos", "polkadot", "cardano", "solana", "avalanche-2", "near", "fantom", "kaspa",
    "matic-network", "arbitrum", "optimism", "band-protocol", "stellar", "algorand", "ripple", "dydx", "aave"
]
coins_protocol = [
    "lido", "rocket-pool", "frax-swap", "quantumx-network", "linkswap", "api3", "velodrome", "gmx-v1",
    "uniswap", "sushiswap", "pancakeswap", "pendle", "1inch-network", "ocean-one"
]

# Function to get chains from Defillama
def get_llama_chains():
    url = 'https://api.llama.fi/chains'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            filtered_data = []

            # Filter the chains data by the coins of interest
            for chain in data:
                if chain['gecko_id'] in coins_chains or chain['name'].lower().replace(" ", "-") in [coin.lower() for coin in coins_chains]:
                    formatted_tvl = format_number_short(chain['tvl']) if 'tvl' in chain else '0'
                    filtered_data.append({
                        'coin': chain['name'],
                        'tvl': formatted_tvl,
                        'type': 'chain'
                    })

            return filtered_data
        else:
            print(f"Failed to fetch chains data: {response.content}")
            return []
    except Exception as e:
        print(f"An error occurred while fetching chains: {e}")
        return []

# Function to get the TVL of a specific protocol
def get_protocol_tvl(token_id):
    formatted_id = str(token_id).casefold()
    url = f"https://api.llama.fi/tvl/{formatted_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200 and response.content:  # Check for valid and non-empty response
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"Invalid JSON response for {token_id}. Skipping...")
                return {'coin': token_id, 'tvl': 'N/A'}

            # If 'data' is a dictionary, get TVL value
            tvl_value = data.get("tvl", 0) if isinstance(data, dict) else data
            formatted_tvl = format_number_short(tvl_value)
            return {'coin': token_id, 'tvl': formatted_tvl}
        else:
            return {'coin': token_id, 'tvl': 'N/A'}
    except Exception as e:
        print(f"An error occurred while fetching protocol TVL for {token_id}: {e}")
        return {'coin': token_id, 'tvl': 'N/A'}

# Function to save combined data to a specified file path
def save_combined_data_to_file(file_path):
    # Get chains data
    chains_data = get_llama_chains()

    # Get protocols data
    protocols_data = [get_protocol_tvl(coin) for coin in coins_protocol]

    # Combine both chains and protocols data
    combined_data = chains_data + protocols_data

    # Save to a specified text file
    with open(file_path, "w") as txt_file:
        for entry in combined_data:
            txt_file.write(f"Coin: {entry['coin']}, TVL: {entry['tvl']}\n")
    
    print(f"Combined chains and protocols saved successfully to '{file_path}'.")

# Call the function and pass the desired file path
#save_combined_data_to_file("\home\lourdes22\data-pipeline\all_coins_defillama.txt")