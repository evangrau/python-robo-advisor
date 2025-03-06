import os
from dotenv import load_dotenv
from loguru import logger as log
from utils.methods import fetch_stock_data, generate_trading_signal, execute_order, determine_order_quantity, get_all_tradable_symbols, filter_best_symbols

# Load environment variables from .env file
load_dotenv()

# Trading parameters
SHORT_WINDOW = os.getenv('SHORT_WINDOW')  # Days for short moving average
LONG_WINDOW = os.getenv('LONG_WINDOW')   # Days for long moving average
RISK_PERCENT = os.getenv('RISK_PERCENT')  # Percentage of equity to risk on a trade
STOP_LOSS = os.getenv('STOP_LOSS')  # Dollar amount for stop loss

log.add(
            "logs/robo_advisor.log", 
            rotation="10 MB",  # Create a new log file when the file reaches 10 MB
            retention="7 days",  # Keep logs for 7 days
            compression="zip",  # Compress old log files to save space
            level="INFO" # Log only INFO and higher messages
        )

def main():
    symbols = get_all_tradable_symbols()
    best_symbols = filter_best_symbols(symbols)

    for symbol in best_symbols:
        df = fetch_stock_data(symbol)
        if df.empty:
            continue
        
        signal = generate_trading_signal(df, SHORT_WINDOW, LONG_WINDOW)
        order_quantity = determine_order_quantity(RISK_PERCENT, STOP_LOSS)

        if order_quantity > 0:
            log.info(f"Trading {symbol}: {signal} {order_quantity} shares")
            # execute_order(symbol, signal, order_quantity)
        else:
            log.info(f"Not enough equity to trade {symbol}")

if __name__ == '__main__':
    main()
