import requests
import mysql.connector
from datetime import datetime

# Alpha Vantage API key
API_KEY = 'EPA8BRZITE8OYAK0'

# Function to fetch stock data from Alpha Vantage API (Time Series Daily)
def fetch_stock_data(symbol):
    url = f'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': 'EPA8BRZITE8OYAK0'
    }
    response = requests.get(url, params=params)
    data = response.json()

    # Check if the response contains the daily time series data
    if 'Time Series (Daily)' in data:
        return data['Time Series (Daily)']
    else:
        print(f"Error fetching data for {symbol}")
        return None

# Function to create a table for each stock symbol if it doesn't already exist
def create_table_for_symbol(symbol, cursor):
    table_name = symbol.lower()  # Use lowercase table name for consistency
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        symbol VARCHAR(50),
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INT,
        PRIMARY KEY (symbol, date)
    )
    """
    cursor.execute(query)

# Function to store stock data in MySQL database for a particular stock table
def store_stock_data(symbol, data):
    # Connect to MySQL database
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Kamble@123',
        database='stock_market'
    )
    cursor = connection.cursor()

    # Ensure the table for the symbol exists
    create_table_for_symbol(symbol, cursor)

    # Prepare and execute the insert query for each record
    table_name = symbol.lower()  # Convert symbol to lowercase for the table name
    for date, values in data.items():
        open_price = float(values['1. open'])
        high_price = float(values['2. high'])
        low_price = float(values['3. low'])
        close_price = float(values['4. close'])
        volume = int(values['5. volume'])

        # Insert data into the table corresponding to the stock symbol
        query = f"""
            INSERT INTO {table_name} (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (symbol, date, open_price, high_price, low_price, close_price, volume))

    # Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()

# Main function to fetch and store data for multiple stocks
def main():
    symbols = ['GOOG', 'AMZN', 'AAPL']  # List of stock symbols
    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        stock_data = fetch_stock_data(symbol)

        if stock_data:
            store_stock_data(symbol, stock_data)
            print(f"Stock data for {symbol} has been successfully stored in the database in the {symbol.lower()} table.")
        else:
            print(f"Error fetching data for {symbol}.")

if __name__ == "__main__":
    main()

