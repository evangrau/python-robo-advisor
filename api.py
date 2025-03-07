import os
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

APCA_API_KEY_ID = os.getenv('APCA_API_KEY_ID')
APCA_API_SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')
APCA_API_BASE_URL = os.getenv('APCA_API_BASE_URL')
SUPABASE_API_KEY = os.getenv('SUPABASE_API_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')

def get_apca_api_connection() -> TradingClient:
    """
    Create a connection to the Alpaca API and return the client object."
    """
    return TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=(True if 'paper' in APCA_API_BASE_URL else False))

def get_supabase_api_connection() -> Client:
    """
    Create a connection to the Supabase API and return the client object."
    """
    return create_client(SUPABASE_URL, SUPABASE_API_KEY)
