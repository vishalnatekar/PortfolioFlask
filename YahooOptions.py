import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine


def get_option_data(ticker_symbol,dte):
    # Create a ticker object
    ticker = yf.Ticker(ticker_symbol)

    current_date = datetime.now()

    # Calculate the date 45 days from now
    target_date = current_date + timedelta(days=dte)

    # Convert the date to string in 'YYYY-MM-DD' format
    target_date_str = target_date.strftime('%Y-%m-%d')

    # Get the list of all available expiry dates for the options
    expiry_dates = ticker.options

    # Find the closest expiry date that is at least 45 days away
    target_expiry_date = min(date for date in expiry_dates if date >= target_date_str)

    # Get the options data for the chosen expiry date
    option_chain = ticker.option_chain(target_expiry_date)

    # Create dataframes for the calls and puts
    df_calls = pd.DataFrame(option_chain.calls)
    df_puts = pd.DataFrame(option_chain.puts)

    return df_calls, df_puts

def save_to_db(df, table_name, engine):
    df.to_sql(table_name, engine, if_exists='replace', index=False)

def main():
    # Database connection parameters
    db_user = 'postgres'
    db_password = '1234'
    db_name = 'postgres'
    db_host = 'localhost'
    db_port = 5432

    # Create a SQLAlchemy engine
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    ticker_symbol = "AAPL"
    days_until_expiry = 45
    df_calls, df_puts = get_option_data(ticker_symbol, days_until_expiry)

    if df_calls is not None and df_puts is not None:
        print("Calls data:")
        print(df_calls.head())
        save_to_db(df_calls, 'calls_data', engine)
        print("\n")

        print("Puts data:")
        print(df_puts.head())
        save_to_db(df_puts, 'puts_data', engine)
    else:
        print("No options data available.")


if __name__ == "__main__":
    main()
