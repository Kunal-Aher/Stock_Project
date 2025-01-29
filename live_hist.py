import pandas as pd
from sqlalchemy import create_engine

# Function to fetch live data from the live data database
def fetch_live_data_from_live_db(symbol, live_db_engine):
    query = f"SELECT * FROM {symbol.lower()} WHERE symbol = '{symbol}'"
    live_data_df = pd.read_sql(query, live_db_engine)
    return live_data_df

# Function to fetch historical data from the historical data database
def fetch_historical_data_from_historical_db(symbol, historical_db_engine):
    query = f"SELECT * FROM {symbol.lower()} WHERE symbol = '{symbol}'"
    historical_data_df = pd.read_sql(query, historical_db_engine)
    return historical_data_df

# Function to create a new table in the new database
def create_table_for_symbol_in_new_db(symbol, cursor):
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

# Function to store combined data into the new database
def store_combined_data_in_new_db(symbol, combined_data, new_db_engine):
    with new_db_engine.connect() as connection:
        table_name = symbol.lower()
        # Ensure the table for the symbol exists in the new database
        create_table_for_symbol_in_new_db(symbol, connection)

        # Insert the combined data
        combined_data.to_sql(table_name, con=connection, if_exists='replace', index=False)

# Main function to combine live and historical data and store it in the new database
def combine_and_store_data(symbol):
    # SQLAlchemy engine connections to the live, historical, and new databases
    live_db_engine = create_engine('mysql+mysqlconnector://root:Kamble%40123@localhost/stock_market')
    historical_db_engine = create_engine('mysql+mysqlconnector://root:Kamble%40123@localhost/historical_data')
    new_db_engine = create_engine('mysql+mysqlconnector://root:Kamble%40123@localhost/live_hist')

    # Fetch live and historical data for the symbol
    live_data = fetch_live_data_from_live_db(symbol, live_db_engine)
    historical_data = fetch_historical_data_from_historical_db(symbol, historical_db_engine)

    # Combine the two datasets (live data and historical data) without duplicates
    combined_data = pd.concat([historical_data, live_data]).drop_duplicates(subset=['symbol', 'date'], keep='last')

    # Store the combined data in the new database
    store_combined_data_in_new_db(symbol, combined_data, new_db_engine)

# Main function to process a list of symbols
def main():
    symbols = ['AMZN', 'GOOG', 'AAPL']  # List of stock symbols
    
    # Loop through each stock symbol and combine/store data
    for symbol in symbols:
        print(f"Processing data for {symbol}...")
        combine_and_store_data(symbol)
        print(f"Data for {symbol} has been successfully stored in the combined database.")

if __name__ == "__main__":
    main()


