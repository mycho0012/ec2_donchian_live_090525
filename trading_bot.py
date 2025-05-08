import pyupbit
import pandas as pd
import numpy as np
import schedule
import time
import os
import sys
import json
import csv
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- Notion & Slack Integration ---
from notion_utils import (
    init_notion_client,
    get_or_create_account_status_page,
    update_account_status_page,
    add_trade_log_entry 
)
from slack_utils import (
    init_slack_client,
    send_slack_message,
    send_trade_alert,
    send_status_update,
    send_error_alert
)

# --- Configuration ---
load_dotenv()
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY")
TICKER = "KRW-BTC"
BUY_AMOUNT_KRW = 1000000  # Amount in KRW to buy BTC
LOG_FILE = "trade_log.csv"  # 기존 CSV 로그 파일 (유지)
PARAMS_FILE = "optimal_params.json"  # File to load optimal params from
BACKTEST_RESULTS_FILE = None  # Will be set dynamically
RUN_IMMEDIATELY = True  # Run the first check immediately upon start?
STATUS_UPDATE_INTERVAL = 6  # Hours between status updates

# --- Default Parameters (used if PARAMS_FILE not found or invalid) ---
DEFAULT_INTERVAL = "minute240"
DEFAULT_DONCHIAN_LOOKBACK = 24

# --- Global Variables ---
BACKTEST_RESULTS = None
BACKTEST_RESULTS_FILE = None

# --- Logging Setup ---
def log_message(message_type, details):
    """Appends a timestamped message to the CSV log file and prints to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {'timestamp': timestamp, 'type': message_type, 'details': details}
    
    try:
        file_exists = os.path.isfile(LOG_FILE)
        with open(LOG_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'type', 'details']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(log_entry)
        print(f"{timestamp} - [{message_type}] {details}")
        sys.stdout.flush()
    except Exception as e:
        print(f"Error writing to log file {LOG_FILE}: {e}")
        sys.stdout.flush()

# --- Initialize Log File ---
def initialize_log_file():
    if not os.path.isfile(LOG_FILE):
        pass  # log_message handles file creation
initialize_log_file()

# --- Global Client Variables ---
upbit = None
notion_client = None
slack_client = None

# --- Function to Load Optimal Parameters ---
def load_optimal_params(params_file, default_interval, default_lookback):
    """Loads optimal interval and lookback from a JSON file."""
    global BACKTEST_RESULTS
    
    try:
        log_message("INFO", f"Attempting to load parameters from {params_file}...")
        
        if not os.path.exists(params_file):
            log_message("WARNING", f"{params_file} not found. Using default parameters: Interval={default_interval}, Lookback={default_lookback}.")
            return default_interval, default_lookback
            
        with open(params_file, 'r', encoding='utf-8') as f:
            params = json.load(f)
            
        # 파일 내용 로그
        log_message("DEBUG", f"Loaded JSON content: {json.dumps(params, indent=2)}")
            
        interval = params.get('interval')
        donchian_lookback = params.get('donchian_lookback')
        backtest_results = params.get('backtest_results')

        # 파라미터 유효성 검사
        if not isinstance(interval, str) or not interval:
            log_message("WARNING", f"Invalid interval in {params_file}: {interval}. Using default: {default_interval}")
            interval = default_interval
            
        if not isinstance(donchian_lookback, int) or donchian_lookback <= 0:
            log_message("WARNING", f"Invalid donchian_lookback in {params_file}: {donchian_lookback}. Using default: {default_lookback}")
            donchian_lookback = default_lookback

        log_message("INFO", f"Loaded parameters: Interval={interval}, Lookback={donchian_lookback}")
        
        # 백테스트 결과 로드
        if backtest_results and isinstance(backtest_results, dict):
            BACKTEST_RESULTS = backtest_results
            log_message("INFO", f"Loaded backtest results from {params_file}")
        else:
            log_message("INFO", "No backtest results found in parameters file.")
            # 백테스트 결과가 없으면 CSV에서 로드 시도하는 기존 코드 유지
            try:
                csv_files = [f for f in os.listdir() if f.startswith("comprehensive_opt_summary_") and f.endswith(".csv")]
                if csv_files:
                    csv_files.sort(reverse=True)
                    global BACKTEST_RESULTS_FILE
                    BACKTEST_RESULTS_FILE = csv_files[0]
                    log_message("INFO", f"Found backtest results file: {BACKTEST_RESULTS_FILE}")
            except Exception as e:
                log_message("WARNING", f"Could not find backtest results file: {e}")
        
        return interval, donchian_lookback
                
    except json.JSONDecodeError as e:
        log_message("ERROR", f"Invalid JSON in {params_file}: {e}. Using default parameters.")
        return default_interval, default_lookback
    except Exception as e:
        log_message("ERROR", f"Error loading parameters from {params_file}: {e}. Using defaults.")
        import traceback
        log_message("ERROR", traceback.format_exc())
        return default_interval, default_lookback

# --- Load Parameters for this Run ---
INTERVAL, DONCHIAN_LOOKBACK = load_optimal_params(PARAMS_FILE, DEFAULT_INTERVAL, DEFAULT_DONCHIAN_LOOKBACK)

# --- Load Backtest Results ---
def load_backtest_results():
    """Loads backtest results for the current parameter set from CSV file."""
    global BACKTEST_RESULTS
    
    # 이미 optimal_params.json에서 백테스트 결과를 로드했으면 그대로 반환
    if BACKTEST_RESULTS:
        return BACKTEST_RESULTS
        
    # 아직 백테스트 결과가 없고 CSV 파일이 있으면 파일에서 로드
    if not BACKTEST_RESULTS_FILE:
        return None
    
    try:
        df = pd.read_csv(BACKTEST_RESULTS_FILE)
        # Find the row matching our parameters
        result_row = df[(df['Interval'] == INTERVAL) & 
                         (df['DonchianLKBK'] == DONCHIAN_LOOKBACK) & 
                         ((df['IndicatorLKBK'] == 'Baseline') | df['IndicatorLKBK'].isna())].iloc[0]
        
        results = {
            'PF': result_row['PF'],
            'CumRet': result_row['CumRet'],
            'MDD': result_row['MDD'],
            'Sortino': result_row['Sortino'],
            'WinRate': result_row['WinRate'],
            'AvgTrade': result_row['AvgTrade'],
            'Trades': result_row['Trades']
        }
        return results
    except Exception as e:
        log_message("WARNING", f"Could not load backtest results from CSV: {e}")
        return None

# --- Initialize or Load Backtest Results ---
if not BACKTEST_RESULTS:
    BACKTEST_RESULTS = load_backtest_results()

# --- Upbit, Notion, and Slack Client Initialization ---
def initialize_all_clients():
    """Initializes Upbit, Notion, and Slack clients."""
    global upbit, notion_client, slack_client

    # Initialize Upbit client
    try:
        if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
            print("Error: Upbit API keys not found in .env file. Exiting.")
            log_message("CRITICAL", "Upbit API keys not found in .env file.")
            return False
        upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        print("Upbit authentication successful.")
        initial_balances = upbit.get_balances()  # Check initial connection
        print("Upbit initial balance check successful.")
        log_message("INFO", "Upbit client initialized successfully.")
    except Exception as e:
        print(f"Error during Upbit authentication or initial balance check: {e}")
        log_message("CRITICAL", f"Upbit authentication failed: {e}")
        return False

    # Initialize Notion client
    try:
        print("Initializing Notion client...")
        notion_client = init_notion_client()
        if notion_client:
            print("Notion client initialized successfully.")
            log_message("INFO", "Notion client initialized successfully.")
        else:
            print("Failed to initialize Notion client.")
            log_message("ERROR", "Failed to initialize Notion client.")
            return False
    except Exception as e:
        print(f"Error initializing Notion client: {e}")
        log_message("ERROR", f"Failed to initialize Notion client: {e}")
        return False
    
    # Initialize Slack client
    try:
        print("Initializing Slack client...")
        slack_client = init_slack_client()
        if slack_client:
            print("Slack client initialized successfully.")
            log_message("INFO", "Slack client initialized successfully.")
            
            # Send startup notification to Slack with enhanced format
            startup_message = (f"🟢 *Donchian Trading Bot Started*\n"
                              f"• Ticker: {TICKER}\n"
                              f"• Interval: {INTERVAL}\n"
                              f"• Donchian Lookback: {DONCHIAN_LOOKBACK}")
            
            # Add backtest results if available
            if BACKTEST_RESULTS:
                backtest_message = (f"\n\n📊 *Backtest Performance*\n"
                                   f"• *Profit Factor:* {BACKTEST_RESULTS.get('PF')}\n"
                                   f"• *Cumulative Return:* {BACKTEST_RESULTS.get('CumRet', 'N/A')}\n"
                                   f"• *Maximum Drawdown:* {BACKTEST_RESULTS.get('MDD', 'N/A')}\n"
                                   f"• *Sortino Ratio:* {BACKTEST_RESULTS.get('Sortino', 'N/A')}\n"
                                   f"• *Win Rate:* {BACKTEST_RESULTS.get('WinRate', 'N/A')}\n"
                                   f"• *Average Trade Return:* {BACKTEST_RESULTS.get('AvgTrade', 'N/A')}\n"
                                   f"• *Total Trades:* {BACKTEST_RESULTS.get('Trades', 'N/A')}")
                startup_message += backtest_message
            else:
                startup_message += "\n\n⚠️ *Backtest Results Not Available*\nUsing default parameters. To get optimized results, run donchian.py."
            
            send_slack_message(slack_client, startup_message)
        else:
            print("Failed to initialize Slack client. Continuing without Slack notifications.")
            log_message("WARNING", "Failed to initialize Slack client. Continuing without Slack notifications.")
    except Exception as e:
        print(f"Error initializing Slack client: {e}")
        log_message("WARNING", f"Failed to initialize Slack client: {e}. Continuing without Slack notifications.")
    
    return True

# --- Notion: Sync Account Balances ---
def sync_account_balances_to_notion():
    """UPBIT Account Balances to Notion's Account Status Database."""
    if not notion_client:
        log_message("WARNING", "Notion client is not initialized. Cannot sync balances to Notion.")
        return None
    if not upbit:
        log_message("WARNING", "Upbit client is not initialized. Cannot fetch balances.")
        return None

    log_message("INFO", "Starting to sync account balances to Notion...")
    balances_data = {}  # Will store balance info for Slack updates
    
    try:
        balances = upbit.get_balances()
        if not balances:
            log_message("INFO", "No balances found in Upbit account or failed to fetch for Notion sync.")
            return None
            
        log_message("DEBUG", f"Fetched {len(balances)} asset(s) from Upbit for Notion sync.")
        balances_data = {"balances": {}}

        for balance_info in balances:
            ticker_currency = balance_info['currency']  # e.g., "BTC", "KRW"
            
            amount = float(balance_info['balance'])
            avg_buy_price_str = balance_info.get('avg_buy_price', '0')
            avg_buy_price = float(avg_buy_price_str)
            
            current_price = 0
            market_to_query_price = None

            if ticker_currency == "KRW":
                current_price = 1
                avg_buy_price = 1
            else:
                market_to_query_price = f"KRW-{ticker_currency}"
                try:
                    # pyupbit.get_current_price보다 get_orderbook이 더 안정적일 수 있음
                    orderbook = pyupbit.get_orderbook(ticker=market_to_query_price)
                    time.sleep(0.05)  # API 호출 간격
                    if orderbook and orderbook['orderbook_units']:
                        current_price = orderbook['orderbook_units'][0]["ask_price"]  # 매도 1호가
                    else:
                        log_message("WARNING", f"Could not fetch orderbook for {market_to_query_price} for Notion. Using avg_buy_price.")
                        current_price = avg_buy_price
                except Exception as e_price:
                    log_message("ERROR", f"Error fetching current price for {market_to_query_price} for Notion: {e_price}")
                    current_price = avg_buy_price
            
            if current_price is None: current_price = 0

            total_value = amount * current_price
            log_message("DEBUG", f"Notion Sync - Asset: {ticker_currency}, Amt: {amount}, AvgP: {avg_buy_price}, CurP: {current_price}, Val: {total_value}")

            # Store for Slack updates
            balances_data["balances"][ticker_currency] = {
                "amount": amount,
                "avg_price": avg_buy_price,
                "current_price": current_price,
                "value_krw": total_value
            }

            page_id = get_or_create_account_status_page(notion_client, ticker_currency)
            if page_id:
                data_to_update = {
                    "Ticker": ticker_currency,
                    "Amount": amount,
                    "Average_Price": avg_buy_price,
                    "Current_Price": current_price,
                    "Total_Value": total_value,
                    "Last_update": datetime.now().isoformat()
                }
                update_account_status_page(notion_client, page_id, data_to_update)
        
        log_message("INFO", "Account balances synced to Notion successfully.")
        return balances_data

    except pyupbit.errors.UpbitError as ue:
        log_message("ERROR", f"Upbit API Error during Notion sync: {ue}")
        if slack_client:
            send_error_alert(slack_client, "Upbit API Error during balance sync", str(ue))
        return None
    except Exception as e:
        log_message("ERROR", f"Unexpected error during Notion sync: {e}")
        if slack_client:
            send_error_alert(slack_client, "Error during balance sync", str(e))
        return None

# --- Notion: Add Trade Log Entry ---
def notion_log_trade(trade_details):
    """Helper function to log trade events to Notion and Slack."""
    if not notion_client:
        log_message("WARNING", "Notion client not initialized. Cannot log trade to Notion.")
        return
    try:
        # Add to Notion
        add_trade_log_entry(notion_client, trade_details)
        log_message("INFO", f"Trade event logged to Notion: {trade_details.get('Trade_ID', 'N/A')}")
        
        # Also send to Slack if available
        if slack_client:
            send_trade_alert(slack_client, trade_details)
            log_message("INFO", f"Trade alert sent to Slack: {trade_details.get('Trade_ID', 'N/A')}")
    except Exception as e:
        log_message("ERROR", f"Failed to log trade to Notion/Slack for {trade_details.get('Trade_ID', 'N/A')}: {e}")


# --- Constants ---
MIN_BTC_HOLDING = 0.00005
MIN_KRW_ORDER = 100000  # Minimum KRW balance requirement (100,000 KRW)

# --- Core Logic Implementation ---
def calculate_donchian_signal(df: pd.DataFrame, lookback: int) -> int | None:
    if df is None or len(df) < lookback:
        log_message("ERROR", f"Insufficient data for Donchian calculation. Need {lookback}, got {len(df) if df is not None else 0}.")
        return None
    try:
        upper_band = df['high'].rolling(window=lookback - 1, min_periods=lookback - 1).max().shift(1).iloc[-1]
        lower_band = df['low'].rolling(window=lookback - 1, min_periods=lookback - 1).min().shift(1).iloc[-1]
        latest_close = df['close'].iloc[-1]

        if pd.isna(upper_band) or pd.isna(lower_band):
            log_message("INFO", "Donchian bands not ready yet (NaN).")
            return None
        log_message("DEBUG", f"Latest Close: {latest_close}, Upper Band: {upper_band}, Lower Band: {lower_band}")
        if latest_close > upper_band: return 1
        elif latest_close < lower_band: return 0
        else: return None
    except Exception as e:
        log_message("ERROR", f"Error calculating Donchian signal: {e}")
        return None

def get_current_balances_from_upbit() -> dict | None:
    """Fetches current KRW and BTC balances directly from Upbit for trade decisions."""
    try:
        krw_balance = upbit.get_balance("KRW")
        coin_ticker_symbol = TICKER.split('-')[1] 
        btc_balance_str = upbit.get_balance(coin_ticker_symbol)
        btc_balance = float(btc_balance_str) if btc_balance_str is not None else 0.0
        
        if krw_balance is None: krw_balance = 0.0
        
        log_message("DEBUG", f"Fetched balances from Upbit - KRW: {krw_balance}, {coin_ticker_symbol}: {btc_balance}")
        return {'KRW': krw_balance, coin_ticker_symbol: btc_balance}
    except Exception as e:
        log_message("ERROR", f"Failed to fetch balances from Upbit: {e}")
        return None

def send_status_to_slack():
    """Send current status update to Slack including account balances and strategy parameters."""
    if not slack_client:
        return
    
    try:
        # Get current balances
        balances_data = sync_account_balances_to_notion()
        
        # Prepare strategy data
        strategy_data = {
            "strategy": {
                "ticker": TICKER,
                "interval": INTERVAL,
                "donchian_lookback": DONCHIAN_LOOKBACK
            }
        }
        
        # Combine data
        status_data = balances_data if balances_data else {"balances": {}}
        status_data.update(strategy_data)
        
        # Construct status message directly (instead of using send_status_update)
        balances_message = ""
        total_value_krw = 0
        
        if "balances" in status_data:
            for currency, data in status_data["balances"].items():
                if currency == "KRW":
                    balances_message += f"• *{currency}:* ₩{data['amount']:,.0f}\n"
                else:
                    balances_message += f"• *{currency}:* {data['amount']:.8f} (₩{data.get('value_krw', 0):,.0f})\n"
                total_value_krw += data.get('value_krw', 0)
        
        # Create status message with Korean headers
        status_message = (
            f"🔄 *Trading Bot Status Update*\n\n"
            f"*Strategy Parameters:*\n"
            f"• Ticker: {TICKER}\n"
            f"• Interval: {INTERVAL}\n"
            f"• Donchian Lookback: {DONCHIAN_LOOKBACK}\n\n"
            f"*Account Balances:*\n{balances_message}\n"
            f"*Total Asset Value:* ₩{total_value_krw:,.0f}\n"
        )
        
        # Add backtest results if available
        if BACKTEST_RESULTS:
            backtest_message = (
                f"\n*Backtest Performance:*\n"
                f"• Profit Factor: {BACKTEST_RESULTS.get('PF')}\n"
                f"• Cumulative Return: {BACKTEST_RESULTS.get('CumRet', 'N/A')}\n"
                f"• Maximum Drawdown: {BACKTEST_RESULTS.get('MDD', 'N/A')}\n"
                f"• Sortino Ratio: {BACKTEST_RESULTS.get('Sortino', 'N/A')}\n"
                f"• Win Rate: {BACKTEST_RESULTS.get('WinRate', 'N/A')}\n"
                f"• Average Trade Return: {BACKTEST_RESULTS.get('AvgTrade', 'N/A')}\n"
                f"• Total Trades: {BACKTEST_RESULTS.get('Trades', 'N/A')}"
            )
            status_message += backtest_message
        
        # Add timestamp
        status_message += f"\n\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Send the message
        send_slack_message(slack_client, status_message)
        log_message("INFO", "Status update sent to Slack.")
    except Exception as e:
        log_message("ERROR", f"Failed to send status update to Slack: {e}")

def check_and_trade():
    """Main function to check signals and execute trades."""
    log_message("INFO", f"Running trade check for {TICKER} interval {INTERVAL} (Lookback: {DONCHIAN_LOOKBACK})...")
    
    # --- 0. Sync Balances to Notion ---
    sync_account_balances_to_notion()

    # --- 1. Fetch Data ---
    df = None
    try:
        fetch_count = DONCHIAN_LOOKBACK + 5 
        df = pyupbit.get_ohlcv(ticker=TICKER, interval=INTERVAL, count=fetch_count, period=0.1) 
        time.sleep(0.2)
        if df is None or df.empty:
            error_msg = "Failed to fetch OHLCV data."
            log_message("ERROR", error_msg)
            if slack_client:
                send_error_alert(slack_client, error_msg)
            return
        log_message("DEBUG", f"Fetched {len(df)} candles ending at {df.index[-1]}")
    except Exception as e:
        error_msg = f"Error fetching OHLCV data: {e}"
        log_message("ERROR", error_msg)
        if slack_client:
            send_error_alert(slack_client, error_msg, str(e))
        return

    # --- 2. Calculate Signal ---
    latest_signal = calculate_donchian_signal(df, DONCHIAN_LOOKBACK)
    signal_text = "HOLD"
    if latest_signal == 1: signal_text = "BUY"
    elif latest_signal == 0: signal_text = "SELL"

    if latest_signal is None:
        log_message("INFO", "No trading signal generated or error occurred.")
    else:
         log_message("INFO", f"Calculated Signal: {signal_text}")

    # --- 3. Get Balances ---
    balances = get_current_balances_from_upbit()
    if balances is None:
        error_msg = "Could not retrieve balances from Upbit. Skipping trade execution check."
        log_message("ERROR", error_msg)
        if slack_client:
            send_error_alert(slack_client, error_msg)
        return
    
    coin_ticker_symbol = TICKER.split('-')[1]
    krw_available = balances['KRW']
    coin_balance = balances.get(coin_ticker_symbol, 0.0)

    # --- 4. Check Holding Status ---
    is_holding_coin = coin_balance > MIN_BTC_HOLDING
    log_message("INFO", f"Current State - Holding {coin_ticker_symbol}: {is_holding_coin} (Balance: {coin_balance:.8f}), KRW Available: {krw_available:,.0f}")

    # --- 5. Execute Trade Logic ---
    trade_id_prefix = datetime.now().strftime('%Y%m%d%H%M%S')
    
    try:
        # BUY Logic - Proceed regardless of holding status if BUY signal is received
        if latest_signal == 1:
            # Holding status log
            if is_holding_coin:
                log_message("INFO", f"Already holding {coin_balance:.8f} {coin_ticker_symbol}, but BUY signal detected. Proceeding with 50% KRW buy.")
            
            log_message("ACTION", f"BUY Signal for {coin_ticker_symbol} detected. Checking KRW balance...")
            
            # KRW balance check and minimum order value (100,000 KRW) check
            if krw_available < MIN_KRW_ORDER:
                insufficient_msg = f"Insufficient KRW balance ({krw_available:,.0f}) for minimum order value ({MIN_KRW_ORDER:,.0f}). Skipping BUY."
                log_message("INFO", insufficient_msg)
                
                # Notion order skip log
                skip_log = {
                    "Trade_ID": f"{trade_id_prefix}_{coin_ticker_symbol}_BUY_SKIP",
                    "Timestamp": datetime.now().isoformat(),
                    "Ticker": coin_ticker_symbol,
                    "Strategy_Signal": f"Donchian({DONCHIAN_LOOKBACK}) - {signal_text}",
                    "Event_Type": "Order Skip",
                    "Side": "Buy",
                    "Order_Type": "Limit",
                    "Notes": insufficient_msg
                }
                notion_log_trade(skip_log)
                return
            
            # Set 50% of available KRW balance as buy amount
            buy_amount = krw_available * 0.5
            log_message("INFO", f"Using 50% of available KRW balance for BUY: {buy_amount:,.0f} KRW")
            
            # Current price check and limit price calculation
            current_price = pyupbit.get_current_price(TICKER)
            if current_price is None:
                log_message("ERROR", f"Could not get current price for {TICKER}. Skipping buy order.")
                return
            
            # Limit price calculation (99.8% of current price)
            limit_price = round(current_price * 0.998)
            quantity = round(buy_amount / limit_price, 8)  # Quantity calculation
            
            log_message("ACTION", f"Attempting to BUY {quantity:.8f} {coin_ticker_symbol} at limit price ₩{limit_price:,}")
            
            # Notion order attempt log
            buy_attempt_log = {
                "Trade_ID": f"{trade_id_prefix}_{coin_ticker_symbol}_BUY_ATTEMPT",
                "Timestamp": datetime.now().isoformat(),
                "Ticker": coin_ticker_symbol,
                "Strategy_Signal": f"Donchian({DONCHIAN_LOOKBACK}) - {signal_text}",
                "Event_Type": "New Order Attempt",
                "Side": "Buy",
                "Order_Type": "Limit",
                "Requested_Price": limit_price,
                "Requested_Quantity": quantity,
                "Notes": f"Attempting limit buy (50% of KRW balance) at ₩{limit_price:,} for {quantity:.8f} {coin_ticker_symbol}"
            }
            notion_log_trade(buy_attempt_log)

            # Execute limit buy order
            buy_result = upbit.buy_limit_order(TICKER, limit_price, quantity)
            time.sleep(1) 
            
            if buy_result and 'uuid' in buy_result:
                log_message("SUCCESS", f"BUY limit order placed. Order ID: {buy_result['uuid']}, Details: {buy_result}")
                
                # Notion order success log
                buy_success_log = {
                    "Trade_ID": buy_result['uuid'], 
                    "Timestamp": datetime.now().isoformat(),
                    "Ticker": coin_ticker_symbol, 
                    "Strategy_Signal": f"Donchian({DONCHIAN_LOOKBACK}) - {signal_text}",
                    "Event_Type": "Order Placed", 
                    "Side": "Buy", 
                    "Order_Type": "Limit",
                    "Requested_Price": limit_price,
                    "Requested_Quantity": quantity,
                    "Order_Status": "open",
                    "Notes": f"Limit buy order placed successfully. Order will be filled when price reaches ₩{limit_price:,}."
                }
                notion_log_trade(buy_success_log)
            else:
                error_detail = buy_result.get('error', {}).get('message', 'No details provided') if isinstance(buy_result, dict) else str(buy_result)
                log_message("FAILURE", f"BUY order failed. Response: {error_detail}")
                
                # Notion order failure log
                buy_fail_log = {
                    "Trade_ID": f"{trade_id_prefix}_{coin_ticker_symbol}_BUY_FAIL",
                    "Timestamp": datetime.now().isoformat(), 
                    "Ticker": coin_ticker_symbol,
                    "Strategy_Signal": f"Donchian({DONCHIAN_LOOKBACK}) - {signal_text}",
                    "Event_Type": "Order Error", 
                    "Side": "Buy", 
                    "Order_Type": "Limit",
                    "Notes": f"Limit buy order failed. Reason: {error_detail}"
                }
                notion_log_trade(buy_fail_log)
        
        # SELL Logic - Sell all held coins
        elif latest_signal == 0 and is_holding_coin:
            # Current price check and limit price calculation
            current_price = pyupbit.get_current_price(TICKER)
            if current_price is None:
                log_message("ERROR", f"Could not get current price for {TICKER}. Skipping sell order.")
                return
            
            # Limit price calculation (99.8% of current price - for faster execution)
            limit_price = round(current_price * 0.998)
            
            log_message("ACTION", f"SELL Signal for {coin_ticker_symbol}. Attempting to SELL ALL {coin_balance:.8f} at limit price ₩{limit_price:,}")
            
            min_value_check_passed = False
            estimated_value = coin_balance * current_price
            if estimated_value >= MIN_KRW_ORDER:
                min_value_check_passed = True
            else:
                log_message("INFO", f"{coin_ticker_symbol} balance {coin_balance:.8f} is below min sell value {MIN_KRW_ORDER:,.0f} KRW (Est: {estimated_value:,.0f} KRW). Holding.")

            if min_value_check_passed:
                # Notion order attempt log
                sell_attempt_log = {
                    "Trade_ID": f"{trade_id_prefix}_{coin_ticker_symbol}_SELL_ATTEMPT",
                    "Timestamp": datetime.now().isoformat(), 
                    "Ticker": coin_ticker_symbol,
                    "Strategy_Signal": f"Donchian({DONCHIAN_LOOKBACK}) - {signal_text}",
                    "Event_Type": "New Order Attempt", 
                    "Side": "Sell", 
                    "Order_Type": "Limit",
                    "Requested_Price": limit_price,
                    "Requested_Quantity": coin_balance,
                    "Notes": f"Attempting limit sell for ALL {coin_balance:.8f} {coin_ticker_symbol} at ₩{limit_price:,}"
                }
                notion_log_trade(sell_attempt_log)

                # Sell all coins
                sell_result = upbit.sell_limit_order(TICKER, limit_price, coin_balance)
                time.sleep(1)
                
                if sell_result and 'uuid' in sell_result:
                    log_message("SUCCESS", f"SELL ALL limit order placed. Order ID: {sell_result['uuid']}, Details: {sell_result}")
                    
                    # Notion order success log
                    sell_success_log = {
                        "Trade_ID": sell_result['uuid'], 
                        "Timestamp": datetime.now().isoformat(),
                        "Ticker": coin_ticker_symbol, 
                        "Strategy_Signal": f"Donchian({DONCHIAN_LOOKBACK}) - {signal_text}",
                        "Event_Type": "Order Placed", 
                        "Side": "Sell", 
                        "Order_Type": "Limit",
                        "Requested_Price": limit_price,
                        "Requested_Quantity": coin_balance,
                        "Order_Status": "open",
                        "Notes": f"Limit sell ALL order placed successfully. Order will be filled when price reaches ₩{limit_price:,}."
                    }
                    notion_log_trade(sell_success_log)
                else:
                    error_detail = sell_result.get('error', {}).get('message', 'No details provided') if isinstance(sell_result, dict) else str(sell_result)
                    log_message("FAILURE", f"SELL order failed. Response: {error_detail}")
                    
                    # Notion order failure log
                    sell_fail_log = {
                        "Trade_ID": f"{trade_id_prefix}_{coin_ticker_symbol}_SELL_FAIL",
                        "Timestamp": datetime.now().isoformat(), 
                        "Ticker": coin_ticker_symbol,
                        "Strategy_Signal": f"Donchian({DONCHIAN_LOOKBACK}) - {signal_text}",
                        "Event_Type": "Order Error", 
                        "Side": "Sell", 
                        "Order_Type": "Limit",
                        "Notes": f"Limit sell ALL order failed. Reason: {error_detail}"
                    }
                    notion_log_trade(sell_fail_log)
        
        # HOLD Logic
        else:
            current_action_msg = "Holding position."
            if latest_signal == 0 and not is_holding_coin: 
                current_action_msg = "SELL Signal, but not holding. Holding."
            elif latest_signal is None: 
                current_action_msg = "No BUY/SELL signal. Holding."
            log_message("INFO", current_action_msg)

    except Exception as e:
        error_msg = f"Exception during trade execution logic: {e}"
        log_message("ERROR", error_msg)
        
        if slack_client:
            send_error_alert(slack_client, "Trade Execution Error", str(e))
        
        # Notion order error log
        exec_error_log = {
            "Trade_ID": f"{trade_id_prefix}_{coin_ticker_symbol}_EXEC_ERROR",
            "Timestamp": datetime.now().isoformat(), 
            "Ticker": coin_ticker_symbol,
            "Event_Type": "Bot Error",
            "Notes": f"Exception in trade_execution: {str(e)}"
        }
        notion_log_trade(exec_error_log)

    log_message("INFO", f"Trade check cycle complete for {TICKER}.")
    
    # --- Check open orders status ---
    try:
        open_orders = upbit.get_order(TICKER)
        if open_orders and len(open_orders) > 0:
            log_message("INFO", f"Found {len(open_orders)} open orders for {TICKER}. Will monitor these in future cycles.")
            for order in open_orders:
                order_type = "buy" if order.get("side") == "bid" else "sell"
                log_message("DEBUG", f"Open {order_type} order: ID={order.get('uuid')}, Price={order.get('price')}, Qty={order.get('remaining_volume')}")
    except Exception as e:
        log_message("ERROR", f"Failed to check open orders: {e}")

# --- Check Orders Status ---
def check_order_status():
    """Check status of open orders and update Notion/Slack."""
    if not upbit:
        return
    
    try:
        open_orders = upbit.get_order(TICKER)
        if not open_orders:
            return
        
        log_message("INFO", f"Checking status of {len(open_orders)} open orders...")
        
        for order in open_orders:
            order_id = order.get("uuid")
            order_type = "Buy" if order.get("side") == "bid" else "Sell"
            price = float(order.get("price"))
            orig_volume = float(order.get("volume"))
            remaining = float(order.get("remaining_volume"))
            
            # If order is partially filled
            if remaining < orig_volume and remaining > 0:
                filled_amount = orig_volume - remaining
                
                partial_fill_log = {
                    "Trade_ID": f"{order_id}_PARTIAL",
                    "Timestamp": datetime.now().isoformat(),
                    "Ticker": TICKER.split('-')[1],
                    "Event_Type": "Order Partially Filled",
                    "Side": order_type,
                    "Order_Type": "Limit",
                    "Filled_Price": price,
                    "Filled_Quantity": filled_amount,
                    "Remaining_Quantity": remaining,
                    "Order_Status": "partially_filled",
                    "Notes": f"Order {order_id} partially filled. {filled_amount:.8f} filled at ₩{price:,}, {remaining:.8f} remaining."
                }
                notion_log_trade(partial_fill_log)
    
    except Exception as e:
        log_message("ERROR", f"Error checking order status: {e}")

# --- Scheduling ---
def run_scheduler():
    print(f"Scheduling trade check for {TICKER} every {INTERVAL} (Lookback: {DONCHIAN_LOOKBACK})...")
    log_message("INFO", f"Bot started. Scheduling checks for {TICKER} on {INTERVAL} interval with Donchian Lookback {DONCHIAN_LOOKBACK}.")

    # Schedule status updates to Slack
    status_times = [f"{h:02d}:00" for h in range(0, 24, STATUS_UPDATE_INTERVAL)]
    for status_time in status_times:
        schedule.every().day.at(status_time).do(send_status_to_slack)
    
    log_message("INFO", f"Scheduled status updates for {', '.join(status_times)} (System Time).")
    
    # Schedule order status checks every 10 minutes
    schedule.every(10).minutes.do(check_order_status)
    log_message("INFO", "Scheduled order status checks every 10 minutes.")

    # Schedule trading checks based on interval
    if INTERVAL == "minute240":  # 4-hour candle
        schedule.every().day.at("01:02").do(check_and_trade)
        schedule.every().day.at("05:02").do(check_and_trade)
        schedule.every().day.at("09:02").do(check_and_trade)
        schedule.every().day.at("13:02").do(check_and_trade)
        schedule.every().day.at("17:02").do(check_and_trade)
        schedule.every().day.at("21:02").do(check_and_trade)
        log_message("INFO", "Scheduled for 01:02, 05:02, 09:02, 13:02, 17:02, 21:02 (System Time).")
    else:
        log_message("WARNING", f"Scheduler for interval '{INTERVAL}' not explicitly defined, using default test schedule (every day at 01:02).")
        schedule.every().day.at("01:02").do(check_and_trade)

    print("Scheduler started. Waiting for scheduled jobs...")
    
    # Print next scheduled runs
    next_runs = get_next_runs()
    if next_runs:
        log_message("INFO", f"Next scheduled runs: {next_runs}")
        if slack_client is not None:
            try:
                send_slack_message(slack_client, f"📅 *Next Scheduled Runs*\n{next_runs}")
            except Exception as e:
                log_message("WARNING", f"Failed to send schedule to Slack: {e}")
    
    if RUN_IMMEDIATELY:
        print("Running initial check immediately...")
        log_message("INFO", "Executing initial trade check immediately as RUN_IMMEDIATELY is True.")
        check_and_trade()

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute if a job is due

def get_next_runs():
    """Get list of next scheduled runs."""
    try:
        now = datetime.now()
        next_runs = []
        
        for job in schedule.jobs:
            next_run = job.next_run
            if next_run:
                time_diff = next_run - now
                hours, remainder = divmod(time_diff.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                
                # Only include if it's within the next 24 hours
                if time_diff.days == 0:
                    # Get job name
                    job_name = "Unknown"
                    if job.job_func.__name__ == "check_and_trade":
                        job_name = "Trade Check"
                    elif job.job_func.__name__ == "send_status_to_slack":
                        job_name = "Status Update"
                    elif job.job_func.__name__ == "check_order_status":
                        job_name = "Order Status Check"
                        
                    next_runs.append(f"• {job_name}: {next_run.strftime('%H:%M')} (in {hours}h {minutes}m)")
        
        return "\n".join(next_runs)
    except Exception as e:
        log_message("ERROR", f"Error getting next scheduled runs: {e}")
        return None

if __name__ == '__main__':
    import subprocess
    import sys
    
    print("=== Donchian Trading System Started ===")
    
    # 1. Check existing JSON file
    should_run_backtest = False
    
    if os.path.exists(PARAMS_FILE):
        try:
            # Read and check file content
            with open(PARAMS_FILE, 'r', encoding='utf-8') as f:
                params_content = json.load(f)
            
            # Validate required fields
            if ('interval' in params_content and 
                'donchian_lookback' in params_content):
                print(f"\n✅ Using existing parameter file:")
                print(f"  - Interval: {params_content['interval']}")
                print(f"  - Donchian Lookback: {params_content['donchian_lookback']}")
                print("  - Backtest Results: " + ("Included" if 'backtest_results' in params_content else "Not included"))
                
                # Skip backtesting
                should_run_backtest = False
            else:
                print(f"\n⚠️ Existing {PARAMS_FILE} file found but missing required fields.")
                should_run_backtest = True
        except Exception as e:
            print(f"\n⚠️ Error reading parameter file: {e}")
            should_run_backtest = True
    else:
        print(f"\n❓ {PARAMS_FILE} file not found. Running backtest.")
        should_run_backtest = True
    
    # 2. Run backtest only if needed
    if should_run_backtest:
        print("\nRunning backtest to find optimal parameters...")
        
        try:
            # Run donchian.py
            print("Executing donchian.py... (This may take a few minutes)")
            cmd = [sys.executable, 'donchian.py', '--mode', '5']
            print(f"Command: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # Check result
            if result.returncode == 0:
                print("\n✅ Backtest completed!")
                
                # Check if file was created
                if os.path.exists(PARAMS_FILE):
                    try:
                        with open(PARAMS_FILE, 'r', encoding='utf-8') as f:
                            params = json.load(f)
                        print(f"Parameter file created: Interval={params.get('interval')}, Lookback={params.get('donchian_lookback')}")
                    except Exception as e:
                        print(f"Error checking parameter file: {e}")
                else:
                    print("⚠️ Backtest successful but parameter file was not created.")
                    print("Using default parameters.")
            else:
                print(f"\n⚠️ Error during backtest (exit code: {result.returncode})")
                print("Using default parameters.")
                
                # Create file with default parameters
                default_params = {
                    'interval': DEFAULT_INTERVAL,
                    'donchian_lookback': DEFAULT_DONCHIAN_LOOKBACK,
                    'backtest_results': {
                        'PF': 0, 'CumRet': 0, 'MDD': 0, 'Sortino': 0,
                        'WinRate': 0, 'AvgTrade': 0, 'Trades': 0
                    },
                    'note': 'Default parameters - backtest failed'
                }
                
                with open(PARAMS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(default_params, f, indent=4)
        except Exception as e:
            print(f"\n⚠️ Exception during backtest: {e}")
            print("Using default parameters.")
            
            # Create file with default parameters
            default_params = {
                'interval': DEFAULT_INTERVAL,
                'donchian_lookback': DEFAULT_DONCHIAN_LOOKBACK,
                'backtest_results': {
                    'PF': 0, 'CumRet': 0, 'MDD': 0, 'Sortino': 0,
                    'WinRate': 0, 'AvgTrade': 0, 'Trades': 0
                },
                'note': 'Default parameters - exception occurred'
            }
            
            with open(PARAMS_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_params, f, indent=4)
    else:
        print("\nSkipping backtest and using existing parameters.")
    
    # 3. Reload parameters (to reflect backtest results)
    INTERVAL, DONCHIAN_LOOKBACK = load_optimal_params(PARAMS_FILE, DEFAULT_INTERVAL, DEFAULT_DONCHIAN_LOOKBACK)
    
    # 4. Initialize clients and start trading
    print("\nPreparing to start trading...")
    print(f"Using parameters: Interval={INTERVAL}, Lookback={DONCHIAN_LOOKBACK}")
    
    if initialize_all_clients():
        print("\n✅ Clients initialized. Starting trading.")
        run_scheduler()
    else:
        log_message("CRITICAL", "Client initialization failed. Exiting.")
        print("\n❌ Client initialization failed. Exiting.") 