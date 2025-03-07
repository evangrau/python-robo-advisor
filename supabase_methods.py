from loguru import logger as log
from api import get_supabase_api_connection
from supabase import Client

DB: Client = get_supabase_api_connection()

def get_all_records_in_table(table_name: str) -> list:
    """
    Fetch all records from a table in the database.
    """
    try:
        response = DB.table(table_name).select('*').execute()
        return response.data
    except Exception as e:
        log.error(f"Error fetching records from {table_name}: {e}")
        return []
    
def get_number_of_records_in_table(table_name: str) -> int:
    """
    Fetch the number of records in a table.
    """
    try:
        if (table_name != 'orders'):
            response = DB.table(table_name).select('symbol').execute()
        else:
            response = DB.table(table_name).select('id').execute()
        return len(response.data)
    except Exception as e:
        log.error(f"Error fetching number of records from {table_name}: {e}")
        return 0
    
def get_all_symbols_from_db() -> list:
    """
    Fetch all symbols from the database.
    """
    records = get_all_records_in_table('stock_assets')
    symbols = [record['symbol'] for record in records]
    return symbols

def create_record_in_table(table_name: str, data: dict) -> None:
    """
    Create a new record in a table.
    """
    try:
        response = DB.table(table_name).insert(data).execute()
        log.info(f"Inserted record into {table_name}: {response}")
    except Exception as e:
        log.error(f"Error creating record in {table_name}: {e}")

def create_records_in_table(table_name: str, data: list) -> None:
    """
    Create multiple records in a table.
    """
    try:
        response = DB.table(table_name).insert(data).execute()
        log.info(f"Inserted records into {table_name}: {response}")
    except Exception as e:
        log.error(f"Error creating records in {table_name}: {e}")
    
def update_record_in_table(table_name: str, record_id: str, data: dict) -> None:
    """
    Update an existing record in a table.
    """
    try:
        response = DB.table(table_name).update(data).eq('symbol', record_id).execute()
        log.info(f"Updated record in {table_name}: {response}")
    except Exception as e:
        log.error(f"Error updating record in {table_name}: {e}")

def update_records_in_table(table_name: str, data: list) -> None:
    """
    Update multiple records in a table.
    """
    try:
        response = DB.table(table_name).upsert(data).execute()
        log.info(f"Updated records in {table_name}: {response}")
    except Exception as e:
        log.error(f"Error updating records in {table_name}: {e}")
    
def exists_in_table(table_name: str, record_id: str) -> bool:
    """
    Check if a record exists in a table.
    """
    try:
        response = DB.table(table_name).select('symbol').eq('symbol', record_id).execute()
        return len(response.data) > 0
    except Exception as e:
        log.error(f"Error checking record in {table_name}: {e}")
        return False
    
def table_is_empty(table_name: str) -> bool:
    """
    Check if a table is empty.
    """
    return get_number_of_records_in_table(table_name) == 0

def delete_record_in_table(table_name: str, record_id: str) -> None:
    """
    Delete a record from a table.
    """
    try:
        response = DB.table(table_name).delete().eq('symbol', record_id).execute()
        log.info(f"Deleted record from {table_name}: {response}")
    except Exception as e:
        log.error(f"Error deleting record in {table_name}: {e}")

def delete_records_in_table(table_name: str, data: list) -> None:
    """
    Delete multiple records from a table.
    """
    try:
        response = DB.table(table_name).delete().in_('symbol', data).execute()
        log.info(f"Deleted records from {table_name}: {response}")
    except Exception as e:
        log.error(f"Error deleting records in {table_name}: {e}")

