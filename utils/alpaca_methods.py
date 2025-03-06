import yfinance as yf
import pandas as pd
from loguru import logger as log
from utils.api import get_apca_api_connection
from utils.supabase_methods import create_record_in_table
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from concurrent.futures import ThreadPoolExecutor

APCA_API = get_apca_api_connection()

def get_all_tradable_symbols():
    """
    Fetch all tradable stock symbols from Alpaca.
    """
    try:
        request_params = GetAssetsRequest(
            asset_class=AssetClass.US_EQUITY,
            status=AssetStatus.ACTIVE
        )
        assets = APCA_API.get_all_assets(request_params)
        symbols = [asset.symbol for asset in assets if asset.tradable]
        log.info(f"Fetched {len(symbols)} tradable symbols from Alpaca")
        return symbols
    except Exception as e:
        log.error(f"Error fetching tradable symbols: {e}")
        return []

def fetch_stock_data(symbol, period='max'):
    """
    Fetch stock data and handle errors properly.
    """
    try:
        log.info(f"Fetching data for {symbol}...")
        data = yf.download(symbol, period=period, auto_adjust=True)

        if data is None or data.empty:
            log.warning(f"No data found for {symbol}. Skipping.")
            return None

        log.info(f"Fetched {len(data)} records for {symbol}")
        return data

    except Exception as e:
        log.error(f"Error fetching data for {symbol}: {e}")
        return None
    
def fetch_and_process_symbol(symbol):
    """
    Fetch stock data and determine if the symbol meets trading criteria.
    """
    df = fetch_stock_data(symbol, period="1mo")

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        log.warning(f"Skipping {symbol}: DataFrame is empty or invalid.")
        return None

    # Check DataFrame structure
    log.info(f"{symbol} DataFrame columns: {df.columns}")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)  # Flatten MultiIndex

    if 'Close' not in df.columns or 'Volume' not in df.columns:
        log.warning(f"Skipping {symbol}: Missing required columns (Close, Volume).")
        return None

    # Ensure numeric conversion
    try:
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')

        avg_volume = df['Volume'].dropna().mean()
        close_mean = df['Close'].dropna().mean()
        price_range = df['Close'].max() - df['Close'].min()

        # Apply filtering criteria
        if pd.notna(avg_volume) and avg_volume > 1_000_000:
            if pd.notna(close_mean) and price_range / close_mean > 0.05:
                return symbol  # Valid symbol

    except Exception as e:
        log.error(f"Error processing {symbol}: {e}")

    return None  # Symbol does not meet criteria

    
def filter_best_symbols(symbols):
    """
    Parallelized fetching and filtering of stock symbols.
    Processes all symbols dynamically.
    """
    best_symbols = []

    # Use ThreadPoolExecutor to fetch and process symbols in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(fetch_and_process_symbol, symbols))

    # Collect valid results
    best_symbols = [symbol for symbol in results if symbol is not None]

    log.info(f"Selected {len(best_symbols)} best symbols for trading.")
    return best_symbols

def generate_trading_signal(df, short_window, long_window):
    """
    Compute short and long moving averages and generate a trading signal.
    Returns 'BUY' if the short MA is above the long MA, otherwise returns 'SELL'.
    """
    # Ensure 'Close' is a Series, not a DataFrame
    if isinstance(df['Close'], pd.DataFrame):
        df['Close'] = df['Close'].iloc[:, 0]  # Select the first column if multi-column

    # Compute moving averages as standalone variables (no need to modify df)
    short_ma = df['Close'].rolling(window=short_window).mean()
    long_ma = df['Close'].rolling(window=long_window).mean()

    # Handle NaN values before making a decision
    if pd.isna(short_ma.iloc[-1]) or pd.isna(long_ma.iloc[-1]):
        log.warning("Moving averages contain NaN values. Not enough data for a signal.")
        return "HOLD"

    # Generate the trading signal
    return 'BUY' if short_ma.iloc[-1] > long_ma.iloc[-1] else 'SELL'

def get_current_position(symbol):
    """
    Return the current position (quantity of shares) for the given symbol.
    If no position exists, return 0.
    """
    try:
        position = APCA_API.get_open_position(symbol)
        return float(position.qty)
    except Exception as e:
        if 'position does not exist' in str(e):
            log.info(f"No position found for {symbol}")
        else:
            log.error(f"Error fetching position for {symbol}: {e}")
        return 0.0
    
def get_account_equity():
    """
    Retrieve and return the account's equity.
    Equity is the net value of your account (assets minus liabilities).
    
    Returns:
        float: The account equity.
    """
    try:
        account = APCA_API.get_account()
        equity = float(account.equity)
        log.info(f"Account equity: ${equity:.2f}")
        return equity
    except Exception as e:
        log.error(f"Error fetching account equity: {e}")
        return None
    
def determine_order_quantity(risk_percent, stop_loss_distance):
    """
    Determine the number of shares to trade based on risk management.
    
    Parameters:
    - account_equity: Total available capital.
    - risk_percent: Percentage of capital you're willing to risk per trade.
    - stop_loss_distance: Dollar difference between entry price and stop loss.
    
    Returns:
    - Quantity of shares to trade.
    """
    account_equity = get_account_equity()
    if account_equity is None:
        return 0
    risk_amount = account_equity * (risk_percent / 100.0)
    quantity = risk_amount / stop_loss_distance
    return int(quantity)  # Returning an integer number of share

def execute_order(symbol, signal, quantity):
    """
    Depending on the generated signal and current holdings,
    submit a market order using Alpaca.
    - If the signal is BUY and no shares are held, a buy order is submitted.
    - If the signal is SELL and shares are held, a sell order is submitted.
    """
    current_qty = get_current_position(symbol)
    log.info(f"Current position for {symbol}: {current_qty} shares")
    
    if signal == 'BUY' and current_qty <= 0:
        log.info(f"Placing a BUY order for {quantity} shares of {symbol}")
        market_order_data = MarketOrderRequest(
                        symbol=symbol,
                        qty=quantity,
                        side=OrderSide.BUY,
                        time_in_force=TimeInForce.DAY
                    )
        log.info(f"Buy order data: {market_order_data}")
        # order = APCA_API.submit_order(order_data=market_order_data)
        # log.info(f"Buy order submitted: {order}")
        create_record_in_table("orders", {
            "symbol": symbol,
            "order_type": "BUY",
            "quantity": quantity
        })
    elif signal == 'SELL' and current_qty > 0:
        log.info(f"Placing a SELL order for {quantity} shares of {symbol}")
        market_order_data = MarketOrderRequest(
                        symbol=symbol,
                        qty=quantity,
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY
                    )
        log.info(f"Sell order data: {market_order_data}")
        # order = APCA_API.submit_order(order_data=market_order_data)
        # log.info(f"Sell order submitted: {order}")
        create_record_in_table("orders", {
            "symbol": symbol,
            "order_type": "SELL",
            "quantity": quantity
        })
    else:
        log.info("No order executed. Either already in desired position or signal unchanged.")
