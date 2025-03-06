import os
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient

# Load environment variables from .env file
load_dotenv()

APCA_API_KEY_ID = os.getenv('APCA_API_KEY_ID')
APCA_API_SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')
APCA_API_BASE_URL = os.getenv('APCA_API_BASE_URL')

def get_api_connection():
    """
    Create a connection to the Alpaca API and return the client object."
    """
    return TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=(True if 'paper' in APCA_API_BASE_URL else False))
