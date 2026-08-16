"""
SMC Pro Scanner - Python Version
Replicates the TradingView Pine Script strategy:
- FVG (Fair Value Gap) detection
- EMA trend filter
- Fibonacci discount/premium zones
- Zone-birth and zone-retest confirmation
"""

import requests
import numpy as np
import pandas as pd
from datetime import datetime
import os

# ─── SETTINGS (same as your Pine Script) ────────────────────────────────────
LOOKBACK_LEN = 100        # Fibonacci lookback (candles)
FVG_SIZE_PCT = 0.05        # Min FVG size as % of price
EMA_LEN = 25                # Trend filter EMA length
FVG_WINDOW_BARS = 36       # Retest window (bars)

# ─── DATA SOURCE (Twelve Data - free tier) ──────────────────────────────────
# Get your free API key at https://twelvedata.com/apikey
API_KEY = os.environ.get("TWELVEDATA_API_KEY", "YOUR_API_KEY_HERE")
SYMBOL = "EUR/USD"
INTERVAL = "15min"

# ─── DISCORD SETTINGS ─────────────────────────────────────────────────────
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "YOUR_CHANNEL_ID_HERE")


def fetch_candles(symbol=SYMBOL, interval=INTERVAL, outputsize=200):
    """Fetch recent candle data from Twelve Data free API."""
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "format": "JSON"
    }
    resp = requests.get(url, params=params, timeout=15)
    data = resp.json()

    if "values" not in data:
        raise RuntimeError(f"API error: {data}")

    df = pd.DataFrame(data["values"])
    df = df.rename(columns={
        "datetime": "time", "open": "open", "high": "high",
        "low": "low", "close": "close"
    })
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values("time").reset_index(drop=True)  # oldest -> newest
    return df


def calculate_indicators(df):
    """Calculate EMA, Fibonacci zones, and FVGs — same logic as Pine Script."""
    df["ema"] = df["close"].ewm(span=EMA_LEN, adjust=False).mean()

    # Rolling swing high/low over lookback window
    df["swing_high"] = df["high"].rolling(LOOKBACK_LEN).max()
    df["swing_low"] = df["low"].rolling(LOOKBACK_LEN).min()
    df["mid_level"] = (df["swing_high"] + df["swing_low"]) / 2

    df["min_fvg_size"] = df["close"] * (FVG_SIZE_PCT / 100)

    # FVG detection (classic 3-candle gap)
    df["raw_bull_fvg"] = (df["low"] > df["high"].shift(2)) & \
                          ((df["low"] - df["high"].shift(2)) >= df["min_fvg_size"])
    df["raw_bear_fvg"] = (df["high"] < df["low"].shift(2)) & \
                          ((df["low"].shift(2) - df["high"]) >= df["min_fvg_size"])

    df["bull_gap_mid"] = (df["low"] + df["high"].shift(2)) / 2
    df["bear_gap_mid"] = (df["low"].shift(2) + df["high"]) / 2

    df["is_discount_fvg"] = (df["high"].shift(2) < df["mid_level"]) & \
                             (df["bull_gap_mid"] > df["ema"])
    df["is_premium_fvg"] = (df["high"] > df["mid_level"]) & \
                            (df["bear_gap_mid"] < df["ema"])

    df["bullish_trend"] = df["close"] > df["ema"]
    df["bearish_trend"] = df["close"] < df["ema"]

    df["new_bull_fvg"] = df["raw_bull_fvg"] & df["is_discount_fvg"] & df["bullish_trend"]
    df["new_bear_fvg"] = df["raw_bear_fvg"] & df["is_premium_fvg"] & df["bearish_trend"]

    # FVG top/bottom for tracking
    df["bull_fvg_top"] = df["low"]
    df["bull_fvg_bot"] = df["high"].shift(2)
    df["bear_fvg_top"] = df["low"].shift(2)
    df["bear_fvg_bot"] = df["high"]

    return df


def check_retest_signals(df):
    """
    Check active FVGs for retest within the window.
    Returns 'BUY', 'SELL', or None for the LATEST candle.
    """
    bull_fvgs = []  # list of dicts: {bar_idx, top, bot, mid}
    bear_fvgs = []

    buy_signal = False
    sell_signal = False
    last_signal_reason = ""

    for i in range(len(df)):
        row = df.iloc[i]

        # register new FVGs
        if row.get("new_bull_fvg", False):
            bull_fvgs.append({
                "bar": i, "top": row["bull_fvg_top"], "bot": row["bull_fvg_bot"],
                "mid": row["mid_level"], "active": True
            })
        if row.get("new_bear_fvg", False):
            bear_fvgs.append({
                "bar": i, "top": row["bear_fvg_top"], "bot": row["bear_fvg_bot"],
                "mid": row["mid_level"], "active": True
            })

        # check bull retests
        for fvg in bull_fvgs:
            if not fvg["active"]:
                continue
            age = i - fvg["bar"]
            if age > FVG_WINDOW_BARS:
                fvg["active"] = False
            elif age > 0:
                touched = (row["low"] <= fvg["top"]) and (row["high"] >= fvg["bot"])
                retest_discount = row["close"] < fvg["mid"]
                if touched and row.get("bullish_trend", False) and retest_discount:
                    fvg["active"] = False
                    if i == len(df) - 1:  # only flag if it's the latest candle
                        buy_signal = True
                        last_signal_reason = f"FVG born at bar {fvg['bar']}, retested now"

        # check bear retests
        for fvg in bear_fvgs:
            if not fvg["active"]:
                continue
            age = i - fvg["bar"]
            if age > FVG_WINDOW_BARS:
                fvg["active"] = False
            elif age > 0:
                touched = (row["high"] >= fvg["bot"]) and (row["low"] <= fvg["top"])
                retest_premium = row["close"] > fvg["mid"]
                if touched and row.get("bearish_trend", False) and retest_premium:
                    fvg["active"] = False
                    if i == len(df) - 1:
                        sell_signal = True
                        last_signal_reason = f"FVG born at bar {fvg['bar']}, retested now"

    if buy_signal:
        return "BUY", last_signal_reason
    elif sell_signal:
        return "SELL", last_signal_reason
    return None, None


def send_discord_alert(message):
    """Send a notification via Discord bot to the configured channel."""
    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"content": message}
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    if resp.status_code not in (200, 201):
        print(f"Discord send failed: {resp.status_code} {resp.text}")
    return resp


def run_check():
    """Main function: fetch data, check for signal, alert if found."""
    print(f"[{datetime.now()}] Fetching {SYMBOL} data...")
    df = fetch_candles()
    df = calculate_indicators(df)
    signal, reason = check_retest_signals(df)

    last_price = df.iloc[-1]["close"]
    last_time = df.iloc[-1]["time"]

    if signal:
        msg = (
            f"🔔 {signal} Signal — {SYMBOL}\n"
            f"Time: {last_time}\n"
            f"Price: {last_price:.5f}\n"
            f"Reason: {reason}\n"
            f"Timeframe: {INTERVAL}"
        )
        print(msg)
        send_discord_alert(msg)
    else:
        print(f"No signal at {last_time}, price {last_price:.5f}")


if __name__ == "__main__":
    run_check()
