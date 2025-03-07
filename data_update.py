from loguru import logger as log
from concurrent.futures import ThreadPoolExecutor
from supabase_methods import (
    create_records_in_table, delete_records_in_table, update_records_in_table, 
    get_all_symbols_from_db, exists_in_table
)
from alpaca_methods import get_all_tradable_assets, fetch_stock_data

def update_assets_in_db() -> None:
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

def fetch_and_store_stock_data(symbol: str):
    """
    Fetch stock data for a given symbol and update the database.
    """
    df = fetch_stock_data(symbol, period="1mo")
    if df is None:
        return None

    records = [
        {
            'symbol': symbol,
            'date': index.strftime("%Y-%m-%d"),
            'open': row['Open'],
            'high': row['High'],
            'low': row['Low'],
            'close': row['Close'],
            'volume': row['Volume']
        }
        for index, row in df.iterrows()
    ]

    return records if records else None

def update_stock_data_in_db() -> None:
    """
    Fetch stock data for all symbols in the database and update the database using threading.
    """
    symbols = get_all_symbols_from_db()
    if not symbols:
        log.error("No symbols found in the database. Exiting.")
        return

    all_records = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_and_store_stock_data, symbols)

    for records in results:
        if records:
            all_records.extend(records)

    if all_records:
        update_records_in_table('stock_data', all_records)
        log.info(f"Inserted/Updated stock data for {len(symbols)} symbols")
    else:
        log.info("No new stock data to update")

def cleanup_db() -> None:
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

if __name__ == '__main__':
    main()
