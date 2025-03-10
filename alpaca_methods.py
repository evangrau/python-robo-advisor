import yfinance as yf
import pandas as pd
import json
from loguru import logger as log
from api import get_apca_api_connection
from supabase_methods import create_record_in_table, get_all_records_in_table, get_record_from_table
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from concurrent.futures import ThreadPoolExecutor

APCA_API: TradingClient = get_apca_api_connection()

def get_all_tradable_assets() -> list:
    """
    Fetch all tradable stock assets from Alpaca.
    """
    try:
        request_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
        assets = APCA_API.get_all_assets(request_params)
        tradable_assets = [asset for asset in assets if asset.tradable]
        log.info(f"Fetched {len(tradable_assets)} tradable assets from Alpaca")
        return tradable_assets
    except Exception as e:
        log.error(f"Error fetching tradable assets: {e}")
        return []

def fetch_stock_data(symbol: str, period: str) -> pd.DataFrame:
    """
    Fetch stock data and handle errors properly.
    """
    try:
        log.info(f"Fetching data for {symbol}...")
        data = yf.download(symbol, period=period, auto_adjust=True)

        if data is None or data.empty:
            log.warning(f"No data found for {symbol}. Skipping.")
            return pd.DataFrame()

        log.info(f"Fetched {len(data)} records for {symbol}")
        return data

    except Exception as e:
        log.error(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()
    
def get_best_symbols_from_db() -> list:
    """
    Get best symbols from the database.
    """
    try:
        best_symbols = []

        records = get_all_records_in_table('best_assets')
        if not records:
            log.warning("No records found in 'best_assets' table.")
            return best_symbols
        
        return records

    except Exception as e:
        log.error(f"Error fetching best symbols: {e}")
        return []

def generate_trading_signal(data_dict: dict, short_window: int, long_window: int) -> str:
    """
    Accepts a Python dictionary containing {date: {close, volume, ...}}.
    """
    if not data_dict:
        log.warning("Empty dictionary. Cannot generate signal.")
        return "HOLD"
    
    data_dict = json.loads(data_dict['data'])  # Convert JSON string to dictionary if needed

    try:
        # 1) Convert dictionary to DataFrame
        df = pd.DataFrame.from_dict(data_dict, orient="index")

        # 2) Convert index to datetime objects & sort
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)

        # 3) Ensure 'close' exists
        df.columns = df.columns.str.lower()
        if 'close' not in df.columns:
            log.warning("No 'close' column found in data. Cannot generate signal.")
            return "HOLD"

        # 4) Compute short & long MAs
        df = df.reset_index(drop=True) # Removes datetime index
        short_ma = df['close'].rolling(window=short_window).mean()
        long_ma = df['close'].rolling(window=long_window).mean()

        # 5) Check for NaN (insufficient data)
        if pd.isna(short_ma.iloc[-1]) or pd.isna(long_ma.iloc[-1]):
            log.info("Not enough data for moving averages.")
            return "HOLD"

        # 6) Generate signal
        return 'BUY' if short_ma.iloc[-1] > long_ma.iloc[-1] else 'SELL'

    except Exception as e:
        log.error(f"Error generating trading signal: {e}")
        return "HOLD"


def get_current_position(symbol: str) -> float:
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
    
def get_account_equity() -> float:
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
    
def determine_order_quantity(risk_percent: int, stop_loss_distance: int) -> int:
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

def execute_order(symbol: str, signal: str, quantity: float):
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
