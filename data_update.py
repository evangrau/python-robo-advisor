import json
import pandas as pd
from loguru import logger as log
from concurrent.futures import ThreadPoolExecutor
from supabase_methods import (
    create_records_in_table, delete_records_in_table, update_records_in_table, get_all_symbols_from_db
)
from alpaca_methods import get_all_tradable_assets, fetch_stock_data

def update_assets_in_db():
    """
    Fetch all tradable stock symbols from Alpaca and update the database.
    """
    assets = get_all_tradable_assets()
    if not assets:
        log.error("No symbols found. Exiting.")
        return

    # Fetch existing symbols in ONE query (Much faster)
    existing_symbols = set(get_all_symbols_from_db())

    # Filter only new assets
    new_assets = [
        {'symbol': asset.symbol, 'name': asset.name, 'exchange': asset.exchange, 'asset_class': asset.asset_class}
        for asset in assets if asset.symbol not in existing_symbols
    ]

    # Batch insert new assets (if any)
    if new_assets:
        create_records_in_table('stock_assets', new_assets)
        log.info(f"Inserted {len(new_assets)} new assets into the database")
    else:
        log.info("No new assets to insert")

def update_stock_data_in_db():
    """
    Fetch stock data for all symbols and store them in a JSONB format in the database.
    """
    symbols = get_all_symbols_from_db()
    if not symbols:
        log.error("No symbols found in the database. Exiting.")
        return

    all_records = []
    for symbol in symbols:
        df = fetch_stock_data(symbol, period="1mo")

        if df is None or df.empty:
            log.warning(f"Skipping {symbol}: No valid stock data available.")
            continue

        # Drop the Ticker level, keeping only column names ('Close', 'High', etc.)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Rename columns to lowercase
        df = df.rename(columns=lambda x: x.lower().strip())

        # Select only the required columns (Fix missing stock price issue)
        required_columns = {"open", "high", "low", "close", "volume"}
        available_columns = set(df.columns)
        selected_columns = list(required_columns.intersection(available_columns))

        if not selected_columns:
            log.warning(f"Skipping {symbol}: No valid price columns found.")
            continue

        df = df[selected_columns].copy()

        # Ensure Date is a column (Fix KeyError)
        df = df.reset_index(names="date")

        # Convert DataFrame to JSON serializable format
        stock_data = {
            str(row["date"]): {
                key: float(value) if pd.api.types.is_number(value) and not pd.isna(value) else None
                for key, value in row.items() if key != "date"
            }
            for _, row in df.iterrows()
        }

        record = {
            "symbol": symbol,
            "data": json.dumps(stock_data) # store as JSON
        }

        all_records.append(record)

    if all_records:
        update_records_in_table("stock_data", all_records)  # Bulk insert JSONB data
        log.info(f"Inserted stock data for {len(symbols)} symbols.")
    else:
        log.info("No new stock data to insert.")

def cleanup_db():
    """
    Remove old records from the database to save space.
    """
    log.info("Cleaning up the database...")

    # Get current tradable symbols
    tradable_assets = get_all_tradable_assets()
    tradable_symbols = {asset.symbol for asset in tradable_assets}

    # Get symbols stored in the database
    db_symbols = set(get_all_symbols_from_db())

    # Find symbols to delete
    delete_symbols = list(db_symbols - tradable_symbols)

    if delete_symbols:
        delete_records_in_table('stock_assets', delete_symbols)
        delete_records_in_table('stock_data', delete_symbols)
        log.info(f"Deleted {len(delete_symbols)} outdated symbols from the database")
    else:
        log.info("No outdated symbols to delete")

def main():
    update_assets_in_db()
    update_stock_data_in_db()
    cleanup_db()
    log.info("Data update completed")

if __name__ == '__main__':
    main()
