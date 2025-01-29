import yfinance as yf
import mysql.connector
from datetime import datetime

# Function to fetch historical stock data from Yahoo Finance (from 2011 to 2024)
def fetch_historical_stock_data(symbol, start_date="2011-01-01", end_date="2024-09-02"):
    # Download the historical data using yfinance with specific start and end dates
    stock = yf.Ticker(symbol)
    data = stock.history(start="2011-01-01", end="2024-09-02")  # Specified date range
    if data.empty:
        print(f"Error fetching data for {symbol}")
        return None
    return data

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

# Function to store historical stock data in MySQL database for a particular stock table
def store_historical_stock_data(symbol, data):
    # Connect to MySQL database (new database: stock_market_data)
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Kamble@123',
        database='historical_data'  # New database name
    )
    cursor = connection.cursor()

    # Ensure the table for the symbol exists
    create_table_for_symbol(symbol, cursor)

    # Prepare and execute the insert query for each record
    table_name = symbol.lower()  # Convert symbol to lowercase for the table name
    for date, row in data.iterrows():
        open_price = float(row['Open'])   # Explicitly convert to Python float
        high_price = float(row['High'])  # Explicitly convert to Python float
        low_price = float(row['Low'])    # Explicitly convert to Python float
        close_price = float(row['Close'])# Explicitly convert to Python float
        volume = int(row['Volume'])

        # Insert data into the table corresponding to the stock symbol
        query = f"""
            INSERT INTO {table_name} (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (symbol, date.date(), open_price, high_price, low_price, close_price, volume))

    # Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()

# Main function to fetch and store historical data for multiple stocks
def main():
    symbols = ['AMZN', 'GOOG', 'AAPL']  # List of stock symbols
    for symbol in symbols:
        print(f"Fetching historical data for {symbol}...")
        historical_data = fetch_historical_stock_data(symbol, start_date="2011-01-01", end_date="2024-09-02")  

        if historical_data is not None:
            store_historical_stock_data(symbol, historical_data)
            print(f"Historical data for {symbol} has been successfully stored in the database in the {symbol.lower()} table.")
        else:
            print(f"Error fetching data for {symbol}.")

if __name__ == "__main__":
    main()
    
    
