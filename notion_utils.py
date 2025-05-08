import os
from dotenv import load_dotenv
from notion_client import Client, APIResponseError
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
ACCOUNT_STATUS_DB_ID = os.getenv("NOTION_ACCOUNT_STATUS_DB_ID")
TRADE_LOG_DB_ID = os.getenv("NOTION_TRADE_LOG_DB_ID") # Trade Log DB ID 로드

def init_notion_client():
    """Initializes and returns a Notion client."""
    if not NOTION_API_TOKEN:
        raise ValueError("NOTION_API_TOKEN not found in environment variables.")
    return Client(auth=NOTION_API_TOKEN)

def get_or_create_account_status_page(notion_client, ticker_symbol):
    """
    Retrieves an existing page for the ticker_symbol from the Account Status DB
    or creates a new one if it doesn't exist.
    The 'Position ID' (Title) property will be set to the ticker_symbol.
    Returns the page_id.
    """
    if not ACCOUNT_STATUS_DB_ID:
        raise ValueError("ACCOUNT_STATUS_DB_ID not found in environment variables.")

    try:
        # Query database for an existing page with the given ticker_symbol in 'Position ID'
        response = notion_client.databases.query(
            database_id=ACCOUNT_STATUS_DB_ID,
            filter={
                "property": "Position ID", # This MUST match your Title property name in Notion
                "title": {
                    "equals": ticker_symbol
                }
            }
        )

        if response["results"]:
            page_id = response["results"][0]["id"]
            print(f"Found existing page for {ticker_symbol}: {page_id}")
            return page_id
        else:
            print(f"No page found for {ticker_symbol}. Creating a new one...")
            new_page_props = {
                "Position ID": {
                    "title": [
                        {
                            "text": {
                                "content": ticker_symbol
                            }
                        }
                    ]
                },
                "Ticker": { # Assuming a 'Select' property named 'Ticker'
                    "select": { "name": ticker_symbol } if ticker_symbol != "KRW" else None
                }
            }
            if ticker_symbol == "KRW":
                if "Ticker" in new_page_props:
                    del new_page_props["Ticker"]

            created_page = notion_client.pages.create(
                parent={"database_id": ACCOUNT_STATUS_DB_ID},
                properties=new_page_props
            )
            print(f"Created new page for {ticker_symbol}: {created_page['id']}")
            return created_page["id"]

    except APIResponseError as e:
        print(f"Notion API Error in get_or_create_account_status_page for {ticker_symbol}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error in get_or_create_account_status_page for {ticker_symbol}: {e}")
        raise

def update_account_status_page(notion_client, page_id, data):
    """
    Updates a specific page in the Account Status database.
    'data' is a dictionary of the properties to update.
    Property names in 'data' MUST match the Notion database property names.
    """
    properties_to_update = {}
    if "Ticker" in data and data["Ticker"]:
        properties_to_update["Ticker"] = {"select": {"name": data["Ticker"]}}
    if "Amount" in data:
        properties_to_update["Amount"] = {"number": data["Amount"]}
    if "Average_Price" in data:
        properties_to_update["Average_Price"] = {"number": data["Average_Price"]}
    if "Current_Price" in data:
        properties_to_update["Current_Price"] = {"number": data["Current_Price"]}
    if "Total_Value" in data:
        properties_to_update["Total_Value"] = {"number": data["Total_Value"]}
    if "Last_update" in data:
        properties_to_update["Last_update"] = {"date": {"start": data["Last_update"]}}

    if not properties_to_update:
        print(f"No data provided to update for page_id: {page_id}")
        return None

    try:
        updated_page = notion_client.pages.update(
            page_id=page_id,
            properties=properties_to_update
        )
        print(f"Successfully updated page {page_id} for Ticker: {data.get('Ticker', 'N/A')}")
        return updated_page
    except APIResponseError as e:
        print(f"Notion API Error in update_account_status_page for page_id {page_id}: {e}")
        if "could not be found" in str(e).lower() and "select option" in str(e).lower():
            print("Hint: A 'select' option might be missing in your Notion database for a Ticker.")
        raise
    except Exception as e:
        print(f"Unexpected error in update_account_status_page for page_id {page_id}: {e}")
        raise

def add_trade_log_entry(notion_client, trade_data):
    """
    Adds a new entry to the Trade Log database.
    'trade_data' is a dictionary where keys match Trade Log DB property names.
    Example trade_data:
    {
        "Trade_ID": "20231027_BTC_BUY_001", (Title property)
        "Timestamp": "2023-10-27T10:30:00Z", (Date property)
        "Ticker": "BTC", (Select property)
        "Strategy_Signal": "Donchian Breakout L", (Text property)
        "Event_Type": "Order Filled", (Select property)
        "Side": "Buy", (Select property)
        "Order_Type": "Limit", (Select property)
        "Requested_Price": 60500000, (Number property)
        "Filled_Price": 60510000, (Number property)
        "Requested_Quantity": 0.01, (Number property)
        "Filled_Quantity": 0.01, (Number property)
        "Remaining_Quantity": 0, (Number property)
        "Total_Filled_Value": 605100, (Number property)
        "Fee": 302.55, (Number property)
        "Order_Status": "Filled", (Text property)
        "PnL": None, (Number property, or actual PnL if a closing trade)
        "Notes": "Initial entry for testing." (Text property)
    }
    """
    if not TRADE_LOG_DB_ID:
        raise ValueError("TRADE_LOG_DB_ID not found in environment variables.")
    if not notion_client:
        raise ValueError("Notion client is not initialized.")
    if not trade_data.get("Trade_ID"): # Title property is mandatory
        raise ValueError("Trade_ID (Title property) is missing in trade_data.")

    properties = {}

    # Map trade_data to Notion property structure
    # Title Property
    properties["Trade_ID"] = {"title": [{"text": {"content": str(trade_data["Trade_ID"])}}]}

    # Date Property
    if "Timestamp" in trade_data:
        properties["Timestamp"] = {"date": {"start": trade_data["Timestamp"]}}

    # Select Properties
    for prop_name in ["Ticker", "Event_Type", "Side", "Order_Type"]:
        if prop_name in trade_data and trade_data[prop_name]:
            properties[prop_name] = {"select": {"name": str(trade_data[prop_name])}}
    
    # Text Properties
    for prop_name in ["Strategy_Signal", "Order_Status", "Notes"]:
        if prop_name in trade_data and trade_data[prop_name] is not None: # Allow empty string but not None
             properties[prop_name] = {"rich_text": [{"text": {"content": str(trade_data[prop_name])}}]}


    # Number Properties
    for prop_name in ["Requested_Price", "Filled_Price", "Requested_Quantity", 
                      "Filled_Quantity", "Remaining_Quantity", "Total_Filled_Value", "Fee", "PnL"]:
        if prop_name in trade_data and trade_data[prop_name] is not None: # Allow 0 but not None
            properties[prop_name] = {"number": trade_data[prop_name]}
        elif prop_name == "PnL" and trade_data.get(prop_name) is None : # explicitly allow PnL to be None (cleared)
             properties[prop_name] = {"number": None}


    try:
        created_page = notion_client.pages.create(
            parent={"database_id": TRADE_LOG_DB_ID},
            properties=properties
        )
        print(f"Successfully added trade log entry: {created_page['id']} for Trade_ID: {trade_data['Trade_ID']}")
        return created_page
    except APIResponseError as e:
        print(f"Notion API Error in add_trade_log_entry for Trade_ID {trade_data.get('Trade_ID', 'N/A')}: {e}")
        print(f"Data attempted: {trade_data}")
        print(f"Properties to create: {properties}")
        if "could not be found" in str(e).lower() and "select option" in str(e).lower():
            error_prop = ""
            if "select option" in e.body:
                try:
                    # Try to parse the property name from the error message if possible
                    # This is a bit fragile as it depends on Notion's error message format
                    body_json = e.json()
                    if body_json and "message" in body_json:
                        msg = body_json["message"]
                        # Example: "Property Ticker's option BTC could not be found."
                        parts = msg.split("'")
                        if len(parts) >= 3:
                            error_prop = parts[1] # e.g. "Ticker"
                except:
                    pass # Ignore parsing errors
            print(f"Hint: A 'select' option might be missing in your Notion Trade Log database for property: '{error_prop}'. Check Ticker, Event_Type, Side, Order_Type.")
        raise
    except Exception as e:
        print(f"Unexpected error in add_trade_log_entry for Trade_ID {trade_data.get('Trade_ID', 'N/A')}: {e}")
        raise

if __name__ == '__main__':
    print("Testing notion_utils.py...")
    client = init_notion_client()

    if not client:
        print("Failed to initialize Notion client. Exiting test.")
    else:
        print("Notion client initialized.")
        
        # --- Test Account Status Update ---
        krw_ticker = "KRW"
        krw_page_id = None
        try:
            krw_page_id = get_or_create_account_status_page(client, krw_ticker)
            if krw_page_id:
                krw_data_to_update = {
                    "Amount": 2000000.00,
                    "Average_Price": 1,
                    "Current_Price": 1,
                    "Total_Value": 2000000.00,
                    "Last_update": datetime.now().isoformat()
                }
                update_account_status_page(client, krw_page_id, krw_data_to_update)
        except Exception as e:
            print(f"Error during KRW test: {e}")

        print("-" * 20)

        btc_ticker = "BTC" 
        btc_page_id = None
        try:
            btc_page_id = get_or_create_account_status_page(client, btc_ticker)
            if btc_page_id:
                btc_data_to_update = {
                    "Ticker": btc_ticker,
                    "Amount": 0.15,
                    "Average_Price": 61000000,
                    "Current_Price": 62000000,
                    "Total_Value": 9300000, # 0.15 * 62000000
                    "Last_update": datetime.now().isoformat()
                }
                update_account_status_page(client, btc_page_id, btc_data_to_update)
        except APIResponseError as e_api:
            print(f"API Error during {btc_ticker} test: {e_api.code} - {e_api.body}")
        except Exception as e:
            print(f"Error during {btc_ticker} test: {e}")

        print("-" * 20)
        
        # --- Test Trade Log Entry ---
        print("Testing Trade Log entry...")
        if TRADE_LOG_DB_ID: # Only run if ID is set
            try:
                # Ensure your Trade Log DB has these select options:
                # Ticker: BTC
                # Event_Type: Order Filled
                # Side: Buy
                # Order_Type: Limit
                example_trade_data = {
                    "Trade_ID": f"TestTrade_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    "Timestamp": datetime.now().isoformat(),
                    "Ticker": "BTC", 
                    "Strategy_Signal": "Manual Test Entry",
                    "Event_Type": "Order Filled", 
                    "Side": "Buy", 
                    "Order_Type": "Limit", 
                    "Requested_Price": 61500000,
                    "Filled_Price": 61550000,
                    "Requested_Quantity": 0.001,
                    "Filled_Quantity": 0.001,
                    "Remaining_Quantity": 0,
                    "Total_Filled_Value": 61550, # 0.001 * 61550000
                    "Fee": 30.775,
                    "Order_Status": "filled", # Assuming 'Order_Status' is a Text property
                    "PnL": None, # No PnL for an entry trade
                    "Notes": "This is a test entry from notion_utils.py"
                }
                add_trade_log_entry(client, example_trade_data)
            except APIResponseError as e_api:
                 print(f"API Error during Trade Log test: {e_api.code} - {e_api.body}") # More detailed error
            except Exception as e:
                print(f"Error during Trade Log test: {e}")
        else:
            print("TRADE_LOG_DB_ID not set in .env. Skipping Trade Log test.")
        
        print("\nTest finished. Check your Notion databases for updates.")