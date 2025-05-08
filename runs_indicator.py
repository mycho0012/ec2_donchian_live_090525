import pandas as pd
import numpy as np
import scipy
import pandas_ta as ta
from runs_test import runs_test
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import pyupbit
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# 환경 변수에서 API 키 가져오기
upbit_access_key = os.getenv("UPBIT_ACCESS_KEY")
upbit_secret_key = os.getenv("UPBIT_SECRET_KEY")

# pyupbit 객체 생성 (API 키가 있는 경우)
upbit = None
if upbit_access_key and upbit_secret_key:
    try:
        upbit = pyupbit.Upbit(upbit_access_key, upbit_secret_key)
        # 인증 성공/실패 메시지는 각 스크립트 실행 시 한 번만 출력되도록 주석 처리 가능 (필요시)
        # print("Successfully authenticated with Upbit API in runs_indicator.") 
    except Exception as e:
        # print(f"Failed to authenticate with Upbit API in runs_indicator: {e}")
        pass # 인증 실패 시에도 공개 데이터 조회는 계속 진행
# else:
    # print("Upbit API keys not found in .env for runs_indicator. Proceeding with public data.")

# Didnt include in video, but I've seen talk online about
# using the runs test as an indicator
# Here I compute the runs test z-score using the signs of returns
# in a rolling window. I have not researched this much, idk if its any good.
# But its a well normalized indicator for yoru collection, have fun.  

def runs_trend_indicator(close: pd.Series, lookback: int):
    if lookback <= 1 or len(close) < lookback:
        # Return NaNs of the same shape if lookback is too small or not enough data
        return pd.Series([np.nan] * len(close), index=close.index)
        
    change_sign = np.sign(close.diff().to_numpy()) # Convert to numpy array
    ind = np.full(len(close), np.nan) # Initialize with NaNs

    for i in range(lookback, len(close)):
        segment = change_sign[i - lookback + 1 : i + 1] # +1 for diff, +1 for inclusive upper bound means original slice was fine
        
        # Ensure the segment is valid for runs_test
        # 1. Filter out NaNs (especially the first one from .diff())
        segment_no_nans = segment[~np.isnan(segment)]
        
        # 2. Check if there are enough data points and at least two different signs
        if len(segment_no_nans) >= 2: # runs_test expects at least 2 points
            # Check if all elements are the same (e.g., all 1s or all -1s)
            # runs_test might return NaN or error if all signs are the same (no runs to test)
            # The modified runs_test should handle this by returning NaN.
            ind[i] = runs_test(segment_no_nans)
        # else, ind[i] remains np.nan as initialized

    return pd.Series(ind, index=close.index) # Return as Series with original index

if __name__ == '__main__':
    try:
        ticker = "KRW-BTC"
        interval = "minute60"
        count = 4000 # Increased data points
        print(f"Fetching {count} recent {interval} data for {ticker} from Upbit (runs_indicator.py)...")
        
        df_upbit = pyupbit.get_ohlcv(ticker=ticker, interval=interval, count=count, period=0.1)

        if df_upbit is None or df_upbit.empty:
            raise ValueError(f"Failed to fetch data for {ticker} from Upbit or data is empty.")

        print(f"Successfully fetched {len(df_upbit)} data points.")

        df_upbit.index.name = 'date'
        data = df_upbit[['open', 'high', 'low', 'close', 'volume']].copy()
        data = data.dropna()

        if data.empty:
            raise ValueError("Data is empty after dropping NA values.")
        
        print(f"Using {len(data)} data points for analysis after cleaning.")

        data['runs_ind'] = runs_trend_indicator(data['close'], 24)

        # Create subplots
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1,
                            subplot_titles=('Close Price', f'Runs Trend Indicator (Z-Score) - LKBK 24, {len(data)} points'))

        # Plot 1: Close Price
        fig.add_trace(
            go.Scatter(x=data.index, y=data['close'], name='Close Price', line=dict(color='white')),
            row=1, col=1
        )

        # Plot 2: Runs Trend Indicator
        if not data['runs_ind'].isna().all(): # Check if there is any valid indicator data to plot
            fig.add_trace(
                go.Scatter(x=data.index, y=data['runs_ind'], name='Runs Indicator (Z-Score)', line=dict(color='yellow')),
                row=2, col=1
            )
        else:
            print("No valid data to plot for Runs Trend Indicator.")
        
        # Update layout
        fig.update_layout(
            height=800,
            width=1000,
            showlegend=True,
            template='plotly_dark',
            title_text=f"Runs Trend Indicator Analysis (Live Data, {len(data)} points)",
            title_x=0.5
        )

        # Update axes labels
        fig.update_yaxes(title_text="Price", row=1, col=1)
        fig.update_yaxes(title_text="Z-Score", row=2, col=1)
        fig.update_xaxes(title_text="Date", row=2, col=1)

        fig.show()

    except ValueError as e:
        print(f"ValueError: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        print(traceback.format_exc())


    



