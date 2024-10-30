import yfinance as yf
import requests
import time
from datetime import datetime, timedelta
from typing import Optional

def get_stock_price(symbol, stock):
    # URLs for different time ranges
    url_day = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1m'
    url_week = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=5d&interval=1d'
    url_month = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1mo&interval=1d'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Function to fetch price data for a given range
    def fetch_price_data(url):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("Rate limit exceeded, retrying in 60 seconds.")
            time.sleep(60)
            return fetch_price_data(url)
        else:
            return None

    # Fetch day, week, and month data
    data_day = fetch_price_data(url_day)
    data_week = fetch_price_data(url_week)
    data_month = fetch_price_data(url_month)

    if data_day and data_week and data_month:
        # Day change calculation
        meta_day = data_day['chart']['result'][0]['meta']
        current_price = meta_day.get('regularMarketPrice', None)
        previous_close = meta_day.get('chartPreviousClose', None)
        
        if current_price and previous_close:
            day_change = ((current_price - previous_close) / previous_close) * 100
            day_change_str = f"{day_change:.2f}%"
        else:
            day_change_str = "Unavailable"

        # Week change calculation (comparing current price with the first price of the last 5 days)
        week_timestamps = data_week['chart']['result'][0]['timestamp']
        week_prices = data_week['chart']['result'][0]['indicators']['quote'][0]['close']
        week_price_start = week_prices[0]  # Price at the start of the week
        week_change = ((current_price - week_price_start) / week_price_start) * 100 if week_price_start else None
        week_change_str = f"{week_change:.2f}%" if week_change else "Unavailable"

        # Month change calculation (comparing current price with the first price of the last month)
        month_timestamps = data_month['chart']['result'][0]['timestamp']
        month_prices = data_month['chart']['result'][0]['indicators']['quote'][0]['close']
        month_price_start = month_prices[0]  # Price at the start of the month
        month_change = ((current_price - month_price_start) / month_price_start) * 100 if month_price_start else None
        month_change_str = f"{month_change:.2f}%" if month_change else "Unavailable"
        if symbol in american_market_indices.values():
            price_prefix = "$"  # Añadir el símbolo de dólar
        else:
            price_prefix = ""  # Sin símbolo de dólar
        # Construct the message
        response = (
        f"💰 **Current Price of {stock}** 💰\n"
        f"💲 **Price:** **{price_prefix}{current_price:.2f}**\n"
        f"📊 **Day Change:** {day_change_str}\n"
        f"📅 **Weekly Change:** {week_change_str}\n"
        f"🗓️ **Monthly Change:** {month_change_str}\n"
        f"Stay up-to-date with market trends! 💼📈"
        )
        return response
    else:
        return f"❌ Failed to fetch data for **{stock}**."
    
# Function to get stock price and changes over day, week, and month from Yahoo Finance
def get_commodity_price(ticker, commodity):
    stock = yf.Ticker(ticker)

    # Get historical data for the past month
    today = datetime.today().strftime('%Y-%m-%d')
    month_ago = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    week_ago = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')

    hist_data_month = stock.history(start=month_ago, end=today)
    hist_data_week = stock.history(start=week_ago, end=today)

    # Get current price, previous day close, week ago close, and month ago close
    current_price = hist_data_month['Close'].iloc[-1] if not hist_data_month.empty else None
    previous_close = hist_data_month['Close'].iloc[-2] if len(hist_data_month) > 1 else None
    week_close = hist_data_week['Close'].iloc[0] if not hist_data_week.empty else None
    month_close = hist_data_month['Close'].iloc[0] if not hist_data_month.empty else None
    
    # Calculate changes (daily, weekly, monthly)
    if current_price and previous_close:
        day_change = ((current_price - previous_close) / previous_close) * 100
        day_change_str = f"{day_change:.2f}%"
    else:
        day_change_str = "Unavailable"

    if current_price and week_close:
        week_change = ((current_price - week_close) / week_close) * 100
        week_change_str = f"{week_change:.2f}%"
    else:
        week_change_str = "Unavailable"
    
    if current_price and month_close:
        month_change = ((current_price - month_close) / month_close) * 100
        month_change_str = f"{month_change:.2f}%"
    else:
        month_change_str = "Unavailable" 

    # Generate the response
    if current_price:
        response = (
            f"💵 **Current Price of {commodity}** 💵\n"
            f"💲 **Price:** **${current_price:.2f}**\n"
            f"📊 **Day Change:** {day_change_str}\n"
            f"📅 **Weekly Change:** {week_change_str}\n"
            f"🗓️ **Monthly Change:** {month_change_str}\n"
            f"Keep an eye on the market! 🔍📝"
        )
    else:
        response = f"❌ Could not retrieve the price for ticker **{commodity}**."

    return response

BINANCE_API_URL = "https://api3.binance.com/api"

def get_ohlc_binance_data(symbol: str, vs_currency: str, interval: str, precision: Optional[int] = None):
    """
    Fetch OHLC data from Binance for a specified trading pair and time interval.
    """
    pair = f"{symbol.upper().strip()}{vs_currency.upper().strip()}"
    endpoint = f"{BINANCE_API_URL}/v3/klines"

    params = {
        'symbol': pair,
        'interval': interval.casefold(),
        'limit': 180  # You can adjust this to the number of data points needed
    }
    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        if data:
            precision_int = int(precision) if precision is not None else None
            ohlc_data = [
                [
                    int(candle[0]),
                    round(float(candle[1]), precision_int) if precision_int is not None else float(candle[1]),  # Open
                    round(float(candle[2]), precision_int) if precision_int is not None else float(candle[2]),  # High
                    round(float(candle[3]), precision_int) if precision_int is not None else float(candle[3]),  # Low
                    round(float(candle[4]), precision_int) if precision_int is not None else float(candle[4])   # Close
                ] for candle in data
            ]
            return ohlc_data, 200
        return None, 404
    except requests.exceptions.RequestException as e:
        return None, 500


def get_binance_price_data(symbol: str, crypto_name):
    # Get current price data
    ohlc_data, status = get_ohlc_binance_data(symbol, "USDT", "1d", 2)
    if status == 200 and ohlc_data:
        current_price = ohlc_data[-1][4]  # Latest closing price
        day_change = (ohlc_data[-1][4] - ohlc_data[-2][4]) / ohlc_data[-2][4] * 100  # Change from previous day
    else:
        return f"Error fetching data for {symbol.upper()}. Status code: {status}"

    # Get weekly and monthly changes (assuming the data is available)
    week_change = (ohlc_data[-1][4] - ohlc_data[-7][4]) / ohlc_data[-7][4] * 100 if len(ohlc_data) >= 7 else None
    month_change = (ohlc_data[-1][4] - ohlc_data[-30][4]) / ohlc_data[-30][4] * 100 if len(ohlc_data) >= 30 else None

    # Formatting the changes
    day_change_str = f"{day_change:.2f}%" if day_change is not None else "N/A"
    week_change_str = f"{week_change:.2f}%" if week_change is not None else "N/A"
    month_change_str = f"{month_change:.2f}%" if month_change is not None else "N/A"

    # Build the response string
    response = (
        f"🪙 **Current Price of {crypto_name} ** 🪙\n"
        f"💲 **Price:** **${current_price:,.2f}**\n"
        f"📊 **Day Change:** {day_change_str}\n"
        f"📅 **Weekly Change:** {week_change_str}\n"
        f"🗓️ **Monthly Change:** {month_change_str}\n"
        f"Stay updated on your crypto assets! 📉📈"
    )
    
    return response

# Mapping stock market indices to their Yahoo Finance symbols
asian_market_indices = {
    'Nikkei 225 (Japan)': '^N225',
    'Hang Seng Index (Hong Kong)': '^HSI',
    'Shanghai Composite (China)': '000001.SS',
}

european_market_indices = {
    'FTSE 100 (UK)': '^FTSE',
    'DAX (Germany)': '^GDAXI',
}

american_market_indices = {
    'S&P 500 (USA)': '^GSPC',
    'NASDAQ Composite (USA)': '^IXIC',
    'US Dollar Index': 'DX-Y.NYB'
}

cryptocurrencies = {
    'Bitcoin (USDT)': 'BTC',
    'Ethereum (USDT)': 'ETH'
}

commodities = {
    'Gold': 'GC=F',
    'Oil (Brent Crude)': 'BZ=F'
}

# Function to detect the market type based on the file name
def detect_market(filename):
    """
    Detect the market type based on the given filename.
    
    Parameters:
    filename (str): Name of the file to analyze.
    
    Returns:
    str: The market type (Asian, American, or European).
    """
    if 'asian' in filename.lower():
        return 'Asian'
    elif 'european' in filename.lower():
        return 'European'
    elif 'american' in filename.lower():
        return 'American'
    else:
        return 'Global'

# Function to save market data to a file based on detected market
def save_market_data_to_file(filepath):
    """
    Extracts stock and cryptocurrency data based on the market type and saves it to a file.
    
    Parameters:
    filepath (str): The path to the file where the data will be saved.
    """
    market = detect_market(filepath)
    
    # Selecting the correct indices based on market type
    if market == 'Asian':
        indices = asian_market_indices
    elif market == 'European':
        indices = european_market_indices
    elif market == 'American':
        indices = american_market_indices
    else:
        indices = {**asian_market_indices, **european_market_indices, **american_market_indices}  # Global market

    with open(filepath, 'w', encoding='utf-8') as f:
        # Writing stock indices data
        f.write(f"{market} Market Closing Prices\n")
        f.write("=" * 40 + "\n")
        f.write(f"Market: {market} Stock Indices Report\n")
        f.write("=" * 40 + "\n")
        for market_name, symbol in indices.items():
            stock_info = get_stock_price(symbol,market_name)
            f.write(f"{stock_info}\n\n")

        # Writing cryptocurrency data
        f.write("=" * 40 + "\n")
        f.write("Cryptocurrency Prices\n")
        f.write("=" * 40 + "\n")
        for crypto_name, crypto_id in cryptocurrencies.items():
            crypto_price = get_binance_price_data(crypto_id, crypto_name)
            f.write(f"{crypto_price}\n\n")
        # Writing commodities data
        f.write("=" * 40 + "\n")        
        f.write("Commodities prices\n")
        f.write("=" * 40 + "\n")
        for commodity, symbol in commodities.items():
            commodity_info = get_commodity_price(symbol,commodity)
            f.write(f"{commodity_info}\n\n")

        f.write("=" * 40 + "\n")