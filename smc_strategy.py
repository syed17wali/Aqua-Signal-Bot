"""
SMC Pro Scanner - Python Version
Replicates the TradingView Pine Script strategy:
- FVG (Fair Value Gap) detection
- EMA trend filter
- Fibonacci discount/premium zones
- Zone-birth and zone-retest confirmation
"""

import csv
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import os

# ─── TIMEZONE (Pakistan Standard Time = UTC+5) ──────────────────────────────
PKT = timezone(timedelta(hours=5))


def to_pkt(dt):
    """Convert any datetime (naive or UTC-aware) to Pakistan Time for display."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PKT)

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


def fetch_candles(symbol=SYMBOL, interval=INTERVAL, outputsize=200, max_retries=3):
    """Fetch recent candle data from Twelve Data free API.

    Instead of asking Twelve Data directly for 15-min candles (which on the
    free REST tier can lag slightly behind live/WebSocket prices), this
    fetches finer-grained 1-min candles and builds the 15-min bars locally
    by resampling. This gets each 15-min bar's OHLC closer to what a
    live chart (e.g. TradingView) would show, reducing feed staleness —
    though it does NOT eliminate broker-to-broker methodology differences
    (Twelve Data aggregates 60+ liquidity providers; a single broker like
    OANDA will still differ slightly — that's inherent, not a bug).

    Only fully-closed 15-min bars (i.e. bins with all 15 one-minute candles
    present) are kept; the still-forming bar is dropped.
    """
    import time as _time

    url = "https://api.twelvedata.com/time_series"
    # Fetch enough 1-min candles to cover the lookback window with buffer.
    # outputsize=2500 minutes (~41.6 hours) comfortably covers
    # LOOKBACK_LEN=100 15-min bars (=1500 minutes) plus retest window buffer.
    one_min_outputsize = 2500
    params = {
        "symbol": symbol,
        "interval": "1min",
        "outputsize": one_min_outputsize,
        "apikey": API_KEY,
        "format": "JSON",
        "timezone": "UTC"  # Twelve Data defaults to exchange-local time otherwise,
                            # which was being wrongly treated as UTC downstream
                            # (caused candle timestamps to show ~10h off in PKT)
    }

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()
            break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            print(f"[Attempt {attempt}/{max_retries}] Twelve Data request failed: {e}")
            if attempt < max_retries:
                _time.sleep(5 * attempt)  # 5s, then 10s backoff
    else:
        raise RuntimeError(f"Twelve Data API unreachable after {max_retries} attempts: {last_error}")

    if "values" not in data:
        raise RuntimeError(f"API error: {data}")

    df1m = pd.DataFrame(data["values"])
    df1m = df1m.rename(columns={
        "datetime": "time", "open": "open", "high": "high",
        "low": "low", "close": "close"
    })
    df1m[["open", "high", "low", "close"]] = df1m[["open", "high", "low", "close"]].astype(float)
    df1m["time"] = pd.to_datetime(df1m["time"])
    df1m = df1m.sort_values("time").reset_index(drop=True)  # oldest -> newest

    # ── Resample 1-min candles into 15-min bars ─────────────────────────
    df1m = df1m.set_index("time")
    ohlc = df1m.resample("15min", label="left", closed="left").agg({
        "open": "first", "high": "max", "low": "min", "close": "last"
    })
    # Only keep bins that actually have all 15 one-minute candles present —
    # this drops the currently-forming (incomplete) bar and any gappy bins.
    counts = df1m["close"].resample("15min", label="left", closed="left").count()
    ohlc = ohlc[counts == 15]
    ohlc = ohlc.dropna().reset_index()

    if len(ohlc) == 0:
        raise RuntimeError("Resampling 1-min data produced no complete 15-min bars — check data availability.")

    return ohlc


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


def log_to_csv(time, price, signal, reason):
    """Append every check result to a CSV file (creates it if not present)."""
    file_path = "signal_history.csv"
    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Time", "Price", "Signal", "Reason"])
        writer.writerow([time, price, signal if signal else "No Signal", reason if reason else ""])


def run_check():
    """Main function: fetch data, check for signal, alert if found, log every check."""
    try:
        print(f"[{datetime.now()}] Fetching {SYMBOL} data...")
        df = fetch_candles()
        df = calculate_indicators(df)
        signal, reason = check_retest_signals(df)

        last_price = df.iloc[-1]["close"]
        last_time_utc = df.iloc[-1]["time"]
        last_time_pkt = to_pkt(last_time_utc.to_pydatetime())
        last_time_str = last_time_pkt.strftime("%Y-%m-%d %I:%M %p PKT")

        log_to_csv(last_time_str, last_price, signal, reason)

        if signal:
            msg = (
                f"🔔 {signal} Signal — {SYMBOL}\n"
                f"Time: {last_time_str}\n"
                f"Price: {last_price:.5f}\n"
                f"Reason: {reason}\n"
                f"Timeframe: {INTERVAL}"
            )
            print(msg)
            send_discord_alert(msg)
        else:
            print(f"No signal at {last_time_str}, price {last_price:.5f}")

    except Exception as e:
        error_msg = f"⚠️ Bot Error — check failed:\n{str(e)}"
        print(error_msg)
        try:
            send_discord_alert(error_msg)
        except Exception:
            print("Could not even send the error alert to Discord.")
        raise  # re-raise so GitHub Actions also marks this run as failed


def send_daily_summary():
    """Reads today's entries from signal_history.csv and sends a summary
    of only BUY/SELL signals (no 'No Signal' spam) to Discord."""
    file_path = "signal_history.csv"
    if not os.path.isfile(file_path):
        send_discord_alert("📊 Daily Summary: No history file found yet.")
        return

    df = pd.read_csv(file_path)

    # Time column should be "YYYY-MM-DD HH:MM AM/PM PKT", but older/stray
    # rows may be in a different format (e.g. 24-hour, no "PKT" suffix).
    # format="mixed" + errors="coerce" handles both instead of crashing;
    # any row that still can't be parsed is dropped and reported.
    cleaned_time = df["Time"].astype(str).str.replace(" PKT", "", regex=False)
    df["Time_parsed"] = pd.to_datetime(cleaned_time, format="mixed", errors="coerce")

    bad_rows = df["Time_parsed"].isna().sum()
    if bad_rows:
        print(f"Warning: {bad_rows} row(s) in signal_history.csv had unparseable dates and were skipped.")
    df = df.dropna(subset=["Time_parsed"])

    today_pkt = datetime.now(PKT).date()
    today_df = df[df["Time_parsed"].dt.date == today_pkt]

    signals_df = today_df[today_df["Signal"] != "No Signal"]
    buy_count = (signals_df["Signal"] == "BUY").sum()
    sell_count = (signals_df["Signal"] == "SELL").sum()

    if len(signals_df) == 0:
        msg = f"📊 Daily Summary ({today_pkt}): No BUY/SELL signals today."
    else:
        lines = [f"📊 Daily Summary ({today_pkt})", f"Total signals: {len(signals_df)} (BUY: {buy_count}, SELL: {sell_count})", ""]
        for _, row in signals_df.iterrows():
            lines.append(f"• {row['Signal']} at {row['Time']} — price {row['Price']}")
        msg = "\n".join(lines)

    send_discord_alert(msg)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "summary":
        send_daily_summary()
    else:
        run_check()