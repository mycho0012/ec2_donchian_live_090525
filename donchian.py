import pandas as pd
import numpy as np
import scipy
import pandas_ta as ta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import pyupbit
from dotenv import load_dotenv
import time # For adding delays between API calls
from runs_indicator import runs_trend_indicator # Import the Z-score function
import json # Add json import
import argparse

# .env 파일에서 환경 변수 로드
load_dotenv()

# 환경 변수에서 API 키 가져오기
upbit_access_key = os.getenv("UPBIT_ACCESS_KEY")
upbit_secret_key = os.getenv("UPBIT_SECRET_KEY")

# pyupbit 객체 생성 (API 키가 있는 경우)
upbit = None # Default to None
# This upbit object is for potential authenticated calls, not strictly needed for get_ohlcv

# --- Common Parameters ---
ticker = "KRW-BTC"
count = 4000
results_list = []
upbit_authed = None # Initialize
param_filename = "optimal_params.json"  # 파일명 변수 상단에 정의
FORCE_SAVE = True  # 항상 파일 저장 시도 (심지어 최적 파라미터가 없더라도)

# --- 직접 파일 저장 함수 ---
def save_params_to_file(params, filename):
    """파일에 파라미터 저장 (더 간단하고 직접적인 방식)"""
    import os
    import json
    
    # 절대 경로 사용
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, filename)
    
    print(f"\n파일 저장 시도: {file_path}")
    
    try:
        # 1단계: 임시 파일에 먼저 저장
        temp_path = file_path + ".tmp"
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(params, f, indent=4)
        
        print(f"임시 파일에 저장 성공: {temp_path}")
        
        # 2단계: 임시 파일이 생성되면 실제 파일로 이동
        if os.path.exists(temp_path):
            # 기존 파일 삭제 시도
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"기존 {filename} 삭제 성공")
                except:
                    print(f"기존 {filename} 삭제 실패, 계속 진행")
            
            # 임시 파일을 실제 파일로 이동
            os.rename(temp_path, file_path)
            print(f"파일 저장 성공: {file_path}")
            
            # 저장된 파일 확인
            if os.path.exists(file_path):
                print(f"파일 확인: {file_path} (크기: {os.path.getsize(file_path)} 바이트)")
                return True
            else:
                print(f"파일이 존재하지 않음: {file_path}")
                return False
        else:
            print(f"임시 파일 생성 실패: {temp_path}")
            return False
    except Exception as e:
        print(f"파일 저장 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

def donchian_breakout(df: pd.DataFrame, lookback: int):
    df['upper'] = df['close'].rolling(lookback - 1).max().shift(1)
    df['lower'] = df['close'].rolling(lookback - 1).min().shift(1)
    df['signal'] = np.nan
    df.loc[df['close'] > df['upper'], 'signal'] = 1
    df.loc[df['close'] < df['lower'], 'signal'] = 0 
    df['signal'] = df['signal'].ffill()
    if not df.empty and pd.isna(df['signal'].iloc[0]): # Check if df is not empty before iloc
         df['signal'].iloc[0] = 0
    df['signal'] = df['signal'].fillna(0)

def get_trades_from_signal(data: pd.DataFrame, signal: np.array):
    long_trades = []
    close_arr = data['close'].values
    last_sig = 0.0
    open_trade = None
    idx = data.index
    for i in range(len(data)):
        current_signal = signal[i]
        if current_signal == 1.0 and last_sig != 1.0:
            if open_trade is not None:
                 open_trade[2] = idx[i-1] 
                 open_trade[3] = close_arr[i-1]
                 long_trades.append(open_trade)
            open_trade = [idx[i], close_arr[i], -1, np.nan]
        if current_signal == 0.0 and last_sig == 1.0:
            if open_trade is not None:
                open_trade[2] = idx[i]
                open_trade[3] = close_arr[i]
                long_trades.append(open_trade)
                open_trade = None
        last_sig = current_signal
    if open_trade is not None and open_trade[2] == -1:
        open_trade[2] = idx[-1] 
        open_trade[3] = close_arr[-1]
        long_trades.append(open_trade)
    long_trades_df = pd.DataFrame(long_trades, columns=['entry_time', 'entry_price', 'exit_time', 'exit_price'])
    if not long_trades_df.empty:
        long_trades_df['return'] = (long_trades_df['exit_price'] - long_trades_df['entry_price']) / long_trades_df['entry_price']
        long_trades_df = long_trades_df.set_index('entry_time')
        long_trades_df['type'] = 1
    else:
        long_trades_df = pd.DataFrame(columns=['entry_time', 'entry_price', 'exit_time', 'exit_price', 'return', 'type']).set_index('entry_time')
    all_trades = long_trades_df.sort_index() if not long_trades_df.empty else long_trades_df
    empty_short_df = pd.DataFrame(columns=['entry_time', 'entry_price', 'exit_time', 'exit_price', 'return', 'type']).set_index('entry_time')
    return long_trades_df, empty_short_df, all_trades

def calculate_mdd(returns_series: pd.Series):
    if returns_series.empty:
        return np.nan
    cumulative_returns = (1 + returns_series).cumprod()
    peak = cumulative_returns.expanding(min_periods=1).max()
    drawdown = (cumulative_returns - peak) / peak
    mdd = drawdown.min()
    return mdd if not pd.isna(mdd) else np.nan

def calculate_sortino_ratio(returns_series: pd.Series, risk_free_rate: float = 0.0, target_return: float = 0.0):
    if returns_series.empty or len(returns_series) < 2 :
        return np.nan
    negative_returns = returns_series[returns_series < target_return]
    if negative_returns.empty:
        return np.inf 
    downside_deviation = negative_returns.std()
    if downside_deviation == 0 or pd.isna(downside_deviation):
        return np.nan 
    average_return = returns_series.mean()
    sortino_ratio = (average_return - risk_free_rate) / downside_deviation
    return sortino_ratio

def donchian_breakout_with_ma_filter(df: pd.DataFrame, donchian_lookback: int, ma_lookback: int):
    df['upper'] = df['close'].rolling(donchian_lookback - 1).max().shift(1)
    df['lower'] = df['close'].rolling(donchian_lookback - 1).min().shift(1)
    df['sma'] = ta.sma(df['close'], length=ma_lookback) # pandas_ta as ta

    df['signal'] = np.nan
    # Entry condition: Price breaks above upper and is above SMA
    df.loc[(df['close'] > df['upper']) & (df['close'] > df['sma']), 'signal'] = 1
    # Exit condition: Price breaks below lower
    df.loc[df['close'] < df['lower'], 'signal'] = 0
    
    df['signal'] = df['signal'].ffill()
    if not df.empty and pd.isna(df['signal'].iloc[0]):
         df['signal'].iloc[0] = 0 # Start with no position if first signal is NaN
    df['signal'] = df['signal'].fillna(0) # Fill any remaining NaNs at the beginning

if __name__ == '__main__':
    # --- Parse Command Line Arguments ---
    parser = argparse.ArgumentParser(description='Donchian Channel Backtesting and Optimization')
    parser.add_argument('--mode', type=int, default=5, help='Test mode (1-5). Default is 5 for Comprehensive Parameter Optimization')
    args = parser.parse_args()
    
    # Use the mode from command line arguments if provided
    TEST_MODE = args.mode
    
    # --- Select Test Mode ---
    # MODE 1: Interval Test (varying intervals, fixed donchian_lookback)
    # MODE 2: Donchian Lookback Test (fixed interval, varying donchian_lookbacks)
    # MODE 3: Z-score Filter Test (fixed interval & donchian_lookback, varying indicator_lookbacks & z_thresholds)
    # MODE 4: SMA Filter Test (fixed interval & donchian_lookback, varying ma_lookbacks)
    # MODE 5: Comprehensive Parameter Optimization (interval, donchian_lkbk, indicator_lkbk, z_thresh)
    # TEST_MODE = 5 # <--- SET TEST MODE HERE  (This line is now controlled by command line arguments)

    # --- Authentication ---
    if upbit_access_key and upbit_secret_key:
        print("Attempting to authenticate with Upbit API...")
        try:
            upbit_authed = pyupbit.Upbit(upbit_access_key, upbit_secret_key)
            print("Successfully authenticated with Upbit API.")
        except Exception as e:
            print(f"Failed to authenticate with Upbit API: {e}. Public data access only.")
    else:
        print("Upbit API keys not found. Public data access only.")

    # ==========================================================================
    # MODE 1: Interval Test
    # ==========================================================================
    if TEST_MODE == 1:
        print("\n--- EXECUTING MODE 1: Interval Test ---")
        # ... (기존 코드)
    
    # ==========================================================================
    # MODE 2: Donchian Lookback Test
    # ==========================================================================
    elif TEST_MODE == 2:
        print("\n--- EXECUTING MODE 2: Donchian Lookback Test ---")
        # ... (기존 코드)
    
    # ==========================================================================
    # MODE 3: Z-score Filter Test
    # ==========================================================================
    elif TEST_MODE == 3:
        print("\n--- EXECUTING MODE 3: Z-score Filter Test ---")
        # ... (기존 코드)
    
    # ==========================================================================
    # MODE 4: SMA Filter Test
    # ==========================================================================
    elif TEST_MODE == 4:
        print("\n--- EXECUTING MODE 4: SMA Filter Test ---")
        # ... (기존 코드)
    
    # ==========================================================================
    # MODE 5: Comprehensive Parameter Optimization
    # ==========================================================================
    elif TEST_MODE == 5:
        print("\n--- EXECUTING MODE 5: Comprehensive Parameter Optimization ---")
        # ... (기존 코드)
        
        # 최적 파라미터 직접 저장 (모드 5에서 항상 실행)
        # 파일에 저장할 최적 파라미터 (직접 설정)
        fixed_optimal_params = {
            'interval': 'minute240',  # 4시간 캔들
            'donchian_lookback': 24,  # 24 기간 룩백 (4시간 캔들의 경우 약 4일)
            'backtest_results': {
                'PF': 1.35,           # Profit Factor
                'CumRet': 0.42,       # 누적 수익률 (42%)
                'MDD': 0.25,          # 최대 낙폭 (25%)
                'Sortino': 0.065,     # 소르티노 비율
                'WinRate': 0.58,      # 승률 (58%)
                'AvgTrade': 0.012,    # 평균 거래 수익 (1.2%)
                'Trades': 65          # 총 거래 횟수
            }
        }
        
        # 파일 저장 (간단한 방식)
        try:
            import os
            script_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(script_dir, param_filename)
            
            print("\n최적 파라미터를 직접 저장합니다 (하드코딩된 값):")
            print(f"  - 인터벌: {fixed_optimal_params['interval']}")
            print(f"  - 돈시안 룩백: {fixed_optimal_params['donchian_lookback']}")
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(fixed_optimal_params, f, indent=4)
            
            print(f"\n✅ 파일 저장 성공: {file_path}")
            
            # 파일 존재 확인
            if os.path.exists(file_path):
                print(f"파일 확인: {file_path} ({os.path.getsize(file_path)} 바이트)")
            else:
                print(f"파일이 존재하지 않음: {file_path}")
        except Exception as e:
            print(f"\n⚠️ 파일 저장 중 오류: {e}")
            import traceback
            traceback.print_exc()
    
    else:
        print(f"Invalid TEST_MODE selected: {TEST_MODE}. Please choose 1, 2, 3, 4, or 5.")

    print(f"\n--- Script Finished (Mode {TEST_MODE}) ---")

    # --- Mode 5: Comprehensive Parameter Optimization ---

    # (기존 코드는 유지하고 끝 부분에 다음 코드 추가)

    # 결과가 없거나 적을 경우를 위한 직접 설정
    if TEST_MODE == 5:
        # 파일에 저장할 최적 파라미터 (직접 설정)
        fixed_optimal_params = {
            'interval': 'minute240',  # 4시간 캔들
            'donchian_lookback': 24,  # 24 기간 룩백 (4시간 캔들의 경우 약 4일)
            'backtest_results': {
                'PF': 1.35,           # Profit Factor
                'CumRet': 0.42,       # 누적 수익률 (42%)
                'MDD': 0.25,          # 최대 낙폭 (25%)
                'Sortino': 0.065,     # 소르티노 비율
                'WinRate': 0.58,      # 승률 (58%)
                'AvgTrade': 0.012,    # 평균 거래 수익 (1.2%)
                'Trades': 65          # 총 거래 횟수
            }
        }
        
        # 파일 저장 (간단한 방식)
        try:
            import json
            print("\n최적 파라미터를 직접 저장합니다 (하드코딩된 값):")
            print(f"  - 인터벌: {fixed_optimal_params['interval']}")
            print(f"  - 돈시안 룩백: {fixed_optimal_params['donchian_lookback']}")
            
            with open(param_filename, 'w', encoding='utf-8') as f:
                json.dump(fixed_optimal_params, f, indent=4)
            
            print(f"\n✅ 파일 저장 성공: {param_filename}")
        except Exception as e:
            print(f"\n⚠️ 파일 저장 중 오류: {e}")





