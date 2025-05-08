import os
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID") or os.getenv("SLACK_CHANNEL")

def init_slack_client():
    """Initializes and returns a Slack WebClient."""
    try:
        if not SLACK_BOT_TOKEN:
            print("SLACK_BOT_TOKEN not found in environment variables. Slack notifications will be disabled.")
            return None
        if not SLACK_CHANNEL_ID:
            print("Neither SLACK_CHANNEL_ID nor SLACK_CHANNEL found in environment variables. Slack notifications will be disabled.")
            return None
        return WebClient(token=SLACK_BOT_TOKEN)
    except Exception as e:
        print(f"Error initializing Slack client: {e}")
        return None

def send_slack_message(client, message, channel_id=None):
    """Sends a simple text message to Slack channel."""
    if client is None:
        return None
        
    if not channel_id:
        channel_id = SLACK_CHANNEL_ID
    
    if not channel_id:
        print("SLACK_CHANNEL_ID not provided and not found in environment variables.")
        return None
    
    try:
        # 채널이 # 으로 시작하는 경우 (이름으로 간주) 또는 직접 채널 ID를 사용
        response = client.chat_postMessage(
            channel=channel_id,
            text=message
        )
        print(f"Message sent to Slack channel: {channel_id}")
        return response
    except SlackApiError as e:
        print(f"Slack API Error: {e.response['error']}")
        return None
    except Exception as e:
        print(f"Error sending Slack message: {e}")
        return None

def send_trade_alert(client, trade_data, channel_id=None):
    """
    Sends a trade alert with rich formatting to Slack.
    
    Parameters:
    - client: Slack WebClient
    - trade_data: Dictionary with trade details
    - channel_id: Optional channel override
    """
    if client is None:
        return None
        
    if not channel_id:
        channel_id = SLACK_CHANNEL_ID
    
    if not channel_id:
        print("SLACK_CHANNEL_ID not provided and not found in environment variables.")
        return None
    
    try:
        # Extract key information from trade_data
        trade_id = trade_data.get("Trade_ID", "Unknown")
        ticker = trade_data.get("Ticker", "Unknown")
        event_type = trade_data.get("Event_Type", "Unknown")
        side = trade_data.get("Side", "")
        order_type = trade_data.get("Order_Type", "")
        
        # Determine color based on event type and side
        color = "#DDDDDD"  # Default gray
        if event_type == "Order Filled":
            if side == "Buy":
                color = "#36a64f"  # Green for buys
            elif side == "Sell":
                color = "#e01e5a"  # Red for sells
        elif "Error" in event_type:
            color = "#ff0000"  # Bright red for errors
        
        # Construct formatted message blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{ticker} Trade Alert: {event_type}"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Trade ID:*\n{trade_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Type:*\n{side} {order_type}" if side and order_type else "*Type:*\n-"
                    }
                ]
            }
        ]
        
        # Add relevant financial details if available
        financial_fields = []
        
        if trade_data.get("Filled_Price") is not None:
            financial_fields.append({
                "type": "mrkdwn",
                "text": f"*Price:*\n₩{float(trade_data['Filled_Price']):,.0f}"
            })
        
        if trade_data.get("Filled_Quantity") is not None:
            financial_fields.append({
                "type": "mrkdwn",
                "text": f"*Quantity:*\n{float(trade_data['Filled_Quantity']):.8f}"
            })
        
        if trade_data.get("Total_Filled_Value") is not None:
            financial_fields.append({
                "type": "mrkdwn",
                "text": f"*Value:*\n₩{float(trade_data['Total_Filled_Value']):,.0f}"
            })
        
        if financial_fields:
            blocks.append({
                "type": "section",
                "fields": financial_fields[:5]  # Limit to 5 fields per section
            })
        
        # Add notes if available
        if trade_data.get("Notes"):
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Notes:*\n{trade_data['Notes']}"
                }
            })
        
        # Add timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                }
            ]
        })
        
        # Send message with blocks
        response = client.chat_postMessage(
            channel=channel_id,
            blocks=blocks,
            text=f"{ticker} {event_type}: {side} {order_type}"  # Fallback text
        )
        return response
    except SlackApiError as e:
        print(f"Slack API Error: {e.response['error']}")
        return None
    except Exception as e:
        print(f"Error sending trade alert: {e}")
        return None

def send_status_update(client, status_data, backtest_results=None, channel_id=None):
    """
    Sends a status update to Slack with account balances and strategy parameters.
    
    Parameters:
    - client: Slack WebClient
    - status_data: Dictionary with account balances and positions
    - backtest_results: Optional dictionary with backtest performance metrics
    - channel_id: Optional channel override
    """
    if client is None:
        return None
        
    if not channel_id:
        channel_id = SLACK_CHANNEL_ID
    
    if not channel_id:
        print("SLACK_CHANNEL_ID not provided and not found in environment variables.")
        return None
    
    try:
        # Create blocks for status message
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Trading Bot Status Update"
                }
            }
        ]
        
        # Add account balances
        if status_data and "balances" in status_data:
            balance_fields = []
            for currency, data in status_data["balances"].items():
                balance_fields.append({
                    "type": "mrkdwn",
                    "text": f"*{currency}:*\n{data['amount']:.8f}" if currency != "KRW" else f"*{currency}:*\n₩{data['amount']:,.0f}"
                })
                
                if data.get("value_krw"):
                    balance_fields.append({
                        "type": "mrkdwn",
                        "text": f"*{currency} Value:*\n₩{data['value_krw']:,.0f}"
                    })
            
            # Split into multiple sections if needed
            for i in range(0, len(balance_fields), 5):
                blocks.append({
                    "type": "section",
                    "fields": balance_fields[i:i+5]
                })
        
        # Add strategy parameters
        if status_data and "strategy" in status_data:
            strategy_fields = [
                {
                    "type": "mrkdwn",
                    "text": f"*Ticker:*\n{status_data['strategy'].get('ticker', 'Unknown')}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Interval:*\n{status_data['strategy'].get('interval', 'Unknown')}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Donchian Lookback:*\n{status_data['strategy'].get('donchian_lookback', 'Unknown')}"
                }
            ]
            
            blocks.append({
                "type": "section",
                "fields": strategy_fields
            })
        
        # Add backtest results if available
        if backtest_results:
            backtest_blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "Backtest Performance"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Profit Factor:*\n{backtest_results.get('PF', 'N/A')}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Cumulative Return:*\n{backtest_results.get('CumRet', 'N/A')}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Max Drawdown:*\n{backtest_results.get('MDD', 'N/A')}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Sortino:*\n{backtest_results.get('Sortino', 'N/A')}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Win Rate:*\n{backtest_results.get('WinRate', 'N/A')}"
                        }
                    ]
                }
            ]
            blocks.extend(backtest_blocks)
        
        # Add timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                }
            ]
        })
        
        # Send message with blocks
        response = client.chat_postMessage(
            channel=channel_id,
            blocks=blocks,
            text="Trading Bot Status Update"  # Fallback text
        )
        return response
    except SlackApiError as e:
        print(f"Slack API Error: {e.response['error']}")
        return None
    except Exception as e:
        print(f"Error sending status update: {e}")
        return None

def send_error_alert(client, error_message, error_details=None, channel_id=None):
    """
    Sends an error alert to Slack.
    
    Parameters:
    - client: Slack WebClient
    - error_message: Main error message
    - error_details: Optional details about the error
    - channel_id: Optional channel override
    """
    if client is None:
        return None
        
    if not channel_id:
        channel_id = SLACK_CHANNEL_ID
    
    if not channel_id:
        print("SLACK_CHANNEL_ID not provided and not found in environment variables.")
        return None
    
    try:
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "⚠️ Trading Bot Error Alert ⚠️"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:* {error_message}"
                }
            }
        ]
        
        if error_details:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Details:*\n```{error_details}```"
                }
            })
        
        # Add timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                }
            ]
        })
        
        response = client.chat_postMessage(
            channel=channel_id,
            blocks=blocks,
            text=f"⚠️ ERROR: {error_message}"  # Fallback text
        )
        return response
    except SlackApiError as e:
        print(f"Slack API Error: {e.response['error']}")
        return None
    except Exception as e:
        print(f"Error sending error alert: {e}")
        return None

if __name__ == "__main__":
    print("Testing slack_utils.py...")
    
    try:
        client = init_slack_client()
        print("Slack client initialized.")
        
        # Test simple message
        response = send_slack_message(client, "Test message from slack_utils.py")
        print(f"Simple message sent. Response: {response['ts']}")
        
        # Test trade alert
        test_trade = {
            "Trade_ID": f"TEST_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "Ticker": "BTC",
            "Event_Type": "Order Filled",
            "Side": "Buy",
            "Order_Type": "Limit",
            "Filled_Price": 61000000,
            "Filled_Quantity": 0.001,
            "Total_Filled_Value": 61000,
            "Notes": "This is a test trade alert from slack_utils.py"
        }
        response = send_trade_alert(client, test_trade)
        print(f"Trade alert sent. Response: {response['ts']}")
        
        # Test status update
        test_status = {
            "balances": {
                "KRW": {"amount": 1500000, "value_krw": 1500000},
                "BTC": {"amount": 0.05, "value_krw": 3050000}
            },
            "strategy": {
                "ticker": "KRW-BTC",
                "interval": "minute240",
                "donchian_lookback": 90
            }
        }
        test_backtest = {
            "PF": 1.75,
            "CumRet": 0.21,
            "MDD": 0.15,
            "Sortino": 0.08,
            "WinRate": 0.55
        }
        response = send_status_update(client, test_status, test_backtest)
        print(f"Status update sent. Response: {response['ts']}")
        
        # Test error alert
        response = send_error_alert(client, "Test Error", "This is a test error from slack_utils.py")
        print(f"Error alert sent. Response: {response['ts']}")
        
        print("All tests completed successfully.")
    except Exception as e:
        print(f"Test failed: {e}") 