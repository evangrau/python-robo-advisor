import os
from dotenv import load_dotenv
from loguru import logger as log
from alpaca_methods import generate_trading_signal, execute_order, determine_order_quantity, filter_best_symbols, get_best_symbols_from_db
from supabase_methods import get_all_records_in_table, get_number_of_records_in_table, get_all_symbols_from_db, get_record_from_table

# Load environment variables from .env file
load_dotenv()

# Trading parameters
SHORT_WINDOW: int = int(os.getenv('SHORT_WINDOW'))  # Days for short moving average
LONG_WINDOW: int = int(os.getenv('LONG_WINDOW'))   # Days for long moving average
RISK_PERCENT: int = int(os.getenv('RISK_PERCENT'))  # Percentage of equity to risk on a trade
STOP_LOSS: int = int(os.getenv('STOP_LOSS'))  # Dollar amount for stop loss

log.add("logs/robo_advisor.log", rotation="10 MB", retention="7 days", compression="zip", level="INFO")

def main():
    log.info("Starting the trading bot...")
    log.info("Getting the best symbols from the database...")
    
    best_symbols = get_best_symbols_from_db()

    if not best_symbols:
        log.error("No best symbols found in the database. Exiting.")
        return

    for symbol in best_symbols:

        data = symbol['data']
        if data is None:
            log.warning(f"No data found for {symbol}. Skipping.")
            continue
        
        signal = generate_trading_signal(data, SHORT_WINDOW, LONG_WINDOW)
        if signal == "HOLD":
            log.info(f"No trading signal for {symbol}. Holding position.")
            continue
        order_quantity = determine_order_quantity(RISK_PERCENT, STOP_LOSS)

        if order_quantity > 0:
            log.info(f"Trading {symbol}: {signal} {order_quantity} shares")
            # execute_order(symbol, signal, order_quantity)
        else:
            log.info(f"Not enough equity to trade {symbol}")

if __name__ == '__main__':
    main()
