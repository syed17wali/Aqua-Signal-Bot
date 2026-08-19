"""
SMC Pro Scanner - REAL-TIME version (for Render)
=================================================
Replaces the "wait for candle to close, check every 15 min" GitHub Actions
script with a persistent, always-on service that:

  1. Opens a WebSocket to Deriv and subscribes to live EUR/USD candles
     (the forming candle updates on every tick, not just on close).
  2. On every update, recomputes the same FVG / EMA / Fibonacci-zone logic
     from SMC_STRATEGY.py and checks the CURRENT (still forming) candle
     for a retest — so a signal can fire mid-candle, not just at close.
  3. Sends a Discord alert the instant a retest is confirmed.
  4. Logs every check to signal_history.csv locally, and (optionally)
     mirrors that file to a GitHub repo so history survives restarts.
  5. Runs a tiny HTTP server so a free uptime pinger (UptimeRobot etc.)
     can keep the free Render instance awake 24/7.

IMPORTANT TRADE-OFFS (read before relying on this):
  - Render's free tier sleeps after 15 min with no HTTP traffic. You MUST
    point an external pinger (UptimeRobot, cron-job.org, etc.) at this
    service's URL every 5-10 min, or it will go to sleep and miss ticks.
  - Render's free disk is EPHEMERAL. signal_history.csv is wiped on every
    restart/redeploy unless you set GITHUB_TOKEN + GITHUB_REPO so it gets
    mirrored to GitHub after every signal.
  - On every restart, the bot reloads the last ~200 candles from Deriv to
    rebuild "which FVG zones are currently active", so it doesn't need the
    old CSV to function correctly — only the CSV *history/summary* is lost
    on restart if you don't configure GitHub mirroring.
"""

import asyncio
import csv
import json
import os
import base64
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
import websockets
from aiohttp import web

# ─── TIMEZONE ────────────────────────────────────────────────────────────
PKT = timezone(timedelta(hours=5))


def to_pkt(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PKT)


# ─── STRATEGY SETTINGS (same as SMC_STRATEGY.py) ────────────────────────
LOOKBACK_LEN = 100
FVG_SIZE_PCT = 0.05
EMA_LEN = 25
FVG_WINDOW_BARS = 36
GRANULARITY_SECONDS = 900          # 15 min candles
HISTORY_CANDLES = 200              # how many candles to keep in memory

# ─── DERIV (market data — free, no key needed for quotes) ───────────────
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")   # public demo app_id
DERIV_SYMBOL = os.environ.get("DERIV_SYMBOL", "frxEURUSD")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"

# ─── DISCORD ─────────────────────────────────────────────────────────────
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")

# ─── OPTIONAL: mirror signal_history.csv to a GitHub repo ───────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")          # e.g. "user/repo"
GITHUB_CSV_PATH = os.environ.get("GITHUB_CSV_PATH", "signal_history.csv")

CSV_PATH = "signal_history.csv"

# ─── STATE (all in-memory, rebuilt on startup) ───────────────────────────
candles = {}          # epoch(open time) -> {open, high, low, close}
alerted_ids = set()   # fvg ids already alerted on, to avoid duplicate spam
last_summary_date = None


# ─── INDICATOR / SIGNAL LOGIC (same math as SMC_STRATEGY.py) ────────────
def calculate_indicators(df):
    df["ema"] = df["close"].ewm(span=EMA_LEN, adjust=False).mean()
    df["swing_high"] = df["high"].rolling(LOOKBACK_LEN).max()
    df["swing_low"] = df["low"].rolling(LOOKBACK_LEN).min()
    df["mid_level"] = (df["swing_high"] + df["swing_low"]) / 2
    df["min_fvg_size"] = df["close"] * (FVG_SIZE_PCT / 100)

    df["raw_bull_fvg"] = (df["low"] > df["high"].shift(2)) & \
                          ((df["low"] - df["high"].shift(2)) >= df["min_fvg_size"])
    df["raw_bear_fvg"] = (df["high"] < df["low"].shift(2)) & \
                          ((df["low"].shift(2) - df["high"]) >= df["min_fvg_size"])

    df["bull_gap_mid"] = (df["low"] + df["high"].shift(2)) / 2
    df["bear_gap_mid"] = (df["low"].shift(2) + df["high"]) / 2

    df["is_discount_fvg"] = (df["high"].shift(2) < df["mid_level"]) & (df["bull_gap_mid"] > df["ema"])
    df["is_premium_fvg"] = (df["high"] > df["mid_level"]) & (df["bear_gap_mid"] < df["ema"])

    df["bullish_trend"] = df["close"] > df["ema"]
    df["bearish_trend"] = df["close"] < df["ema"]

    df["new_bull_fvg"] = df["raw_bull_fvg"] & df["is_discount_fvg"] & df["bullish_trend"]
    df["new_bear_fvg"] = df["raw_bear_fvg"] & df["is_premium_fvg"] & df["bearish_trend"]

    df["bull_fvg_top"] = df["low"]
    df["bull_fvg_bot"] = df["high"].shift(2)
    df["bear_fvg_top"] = df["low"].shift(2)
    df["bear_fvg_bot"] = df["high"]
    return df


def check_retest_signals(df):
    """Same retest logic as SMC_STRATEGY.py, but:
    - evaluates the LAST row (the currently forming candle) every time it's called
    - skips any fvg id already in `alerted_ids` so a mid-candle re-check
      doesn't spam the same retest repeatedly
    - can fire for MORE THAN ONE fresh event per call (rare, but possible)
    """
    bull_fvgs, bear_fvgs = [], []
    events = []  # list of (direction, reason, fvg_id)

    for i in range(len(df)):
        row = df.iloc[i]

        if row.get("new_bull_fvg", False):
            bull_fvgs.append({"bar": i, "top": row["bull_fvg_top"], "bot": row["bull_fvg_bot"],
                               "mid": row["mid_level"], "active": True})
        if row.get("new_bear_fvg", False):
            bear_fvgs.append({"bar": i, "top": row["bear_fvg_top"], "bot": row["bear_fvg_bot"],
                               "mid": row["mid_level"], "active": True})

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
                    if i == len(df) - 1:
                        fid = ("BUY", fvg["bar"])
                        if fid not in alerted_ids:
                            events.append(("BUY", f"FVG born at bar {fvg['bar']}, retested now", fid))

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
                        fid = ("SELL", fvg["bar"])
                        if fid not in alerted_ids:
                            events.append(("SELL", f"FVG born at bar {fvg['bar']}, retested now", fid))

    return events


# ─── DISCORD ──────────────────────────────────────────────────────────────
def send_discord_alert(message):
    if not DISCORD_BOT_TOKEN or not DISCORD_CHANNEL_ID:
        print("[discord] not configured, skipping send:", message)
        return
    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}", "Content-Type": "application/json"}
    try:
        resp = requests.post(url, headers=headers, json={"content": message}, timeout=15)
        if resp.status_code not in (200, 201):
            print(f"[discord] send failed: {resp.status_code} {resp.text}")
    except Exception as e:
        print("[discord] send error:", e)


# ─── CSV LOGGING (+ optional GitHub mirror) ───────────────────────────────
def log_to_csv(time_str, price, signal, reason):
    file_exists = os.path.isfile(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Time", "Price", "Signal", "Reason"])
        writer.writerow([time_str, price, signal if signal else "No Signal", reason or ""])


def mirror_csv_to_github():
    """Push the local signal_history.csv to a GitHub repo so it survives
    Render restarts. Only called after a real BUY/SELL signal (not every
    tick) to stay well under GitHub API rate limits."""
    if not (GITHUB_TOKEN and GITHUB_REPO):
        return
    try:
        with open(CSV_PATH, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode()

        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_CSV_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}

        sha = None
        r = requests.get(api_url, headers=headers, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")

        payload = {"message": "Update signal history (auto)", "content": content_b64, "branch": "main"}
        if sha:
            payload["sha"] = sha

        r2 = requests.put(api_url, headers=headers, json=payload, timeout=15)
        if r2.status_code not in (200, 201):
            print(f"[github] mirror failed: {r2.status_code} {r2.text}")
    except Exception as e:
        print("[github] mirror error:", e)


def send_daily_summary():
    if not os.path.isfile(CSV_PATH):
        send_discord_alert("📊 Daily Summary: No history file found yet.")
        return
    df = pd.read_csv(CSV_PATH)
    df["Time_parsed"] = pd.to_datetime(df["Time"].str.replace(" PKT", "", regex=False), format="%Y-%m-%d %I:%M %p")
    today_pkt = datetime.now(PKT).date()
    today_df = df[df["Time_parsed"].dt.date == today_pkt]
    signals_df = today_df[today_df["Signal"] != "No Signal"]
    buy_count = (signals_df["Signal"] == "BUY").sum()
    sell_count = (signals_df["Signal"] == "SELL").sum()

    if len(signals_df) == 0:
        msg = f"📊 Daily Summary ({today_pkt}): No BUY/SELL signals today."
    else:
        lines = [f"📊 Daily Summary ({today_pkt})",
                 f"Total signals: {len(signals_df)} (BUY: {buy_count}, SELL: {sell_count})", ""]
        for _, row in signals_df.iterrows():
            lines.append(f"• {row['Signal']} at {row['Time']} — price {row['Price']}")
        msg = "\n".join(lines)
    send_discord_alert(msg)


# ─── CANDLE STATE → DATAFRAME ─────────────────────────────────────────────
def candles_to_df():
    rows = sorted(candles.items(), key=lambda kv: kv[0])
    rows = rows[-HISTORY_CANDLES:]
    df = pd.DataFrame([
        {"time": pd.to_datetime(epoch, unit="s", utc=True), "open": v["open"],
         "high": v["high"], "low": v["low"], "close": v["close"]}
        for epoch, v in rows
    ])
    return df


async def on_candle_update():
    """Called every time the current candle changes (i.e. on every tick)."""
    global last_summary_date

    df = candles_to_df()
    if len(df) < LOOKBACK_LEN + 5:
        return  # not enough history yet to compute swing high/low

    df = calculate_indicators(df)
    events = check_retest_signals(df)

    last_price = df.iloc[-1]["close"]
    last_time_pkt = to_pkt(df.iloc[-1]["time"].to_pydatetime())
    last_time_str = last_time_pkt.strftime("%Y-%m-%d %I:%M %p PKT")

    if events:
        for direction, reason, fid in events:
            alerted_ids.add(fid)
            log_to_csv(last_time_str, last_price, direction, reason)
            msg = (f"🔔 {direction} Signal — {DERIV_SYMBOL}\n"
                   f"Time: {last_time_str}\nPrice: {last_price:.5f}\n"
                   f"Reason: {reason}\nTimeframe: 15min (real-time)")
            print(msg)
            send_discord_alert(msg)
            mirror_csv_to_github()
    # NOTE: we deliberately do NOT log "No Signal" on every tick here —
    # that would create a huge CSV very fast. We log a heartbeat row once
    # per minute instead, from the background loop below.

    # daily summary at ~00:00 PKT, once per day
    now_pkt = datetime.now(PKT)
    if now_pkt.hour == 0 and now_pkt.minute < 2 and last_summary_date != now_pkt.date():
        last_summary_date = now_pkt.date()
        send_daily_summary()
        mirror_csv_to_github()

    # trim old fvg ids so the set doesn't grow forever
    if len(alerted_ids) > 5000:
        alerted_ids.clear()


async def heartbeat_logger():
    """Log one 'No Signal' row per minute so signal_history.csv still shows
    the bot is alive, without flooding it on every tick."""
    while True:
        await asyncio.sleep(60)
        if candles:
            epoch = max(candles.keys())
            price = candles[epoch]["close"]
            t = to_pkt(datetime.fromtimestamp(epoch, tz=timezone.utc)).strftime("%Y-%m-%d %I:%M %p PKT")
            log_to_csv(t, price, None, None)


# ─── DERIV WEBSOCKET LOOP ──────────────────────────────────────────────────
async def deriv_stream():
    subscribe_req = {
        "ticks_history": DERIV_SYMBOL,
        "adjust_start_time": 1,
        "count": HISTORY_CANDLES,
        "end": "latest",
        "granularity": GRANULARITY_SECONDS,
        "style": "candles",
        "subscribe": 1,
    }

    backoff = 5
    while True:
        try:
            print(f"[deriv] connecting to {DERIV_WS_URL} ...")
            async with websockets.connect(DERIV_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(subscribe_req))
                backoff = 5  # reset backoff after a successful connect
                async for raw in ws:
                    msg = json.loads(raw)

                    if msg.get("error"):
                        print("[deriv] API error:", msg["error"])
                        continue

                    if msg.get("msg_type") == "candles":
                        for c in msg["candles"]:
                            candles[int(c["epoch"])] = {
                                "open": float(c["open"]), "high": float(c["high"]),
                                "low": float(c["low"]), "close": float(c["close"]),
                            }
                        print(f"[deriv] loaded {len(msg['candles'])} history candles")
                        await on_candle_update()

                    elif msg.get("msg_type") == "ohlc":
                        o = msg["ohlc"]
                        epoch = int(o.get("open_time", o.get("epoch")))
                        candles[epoch] = {
                            "open": float(o["open"]), "high": float(o["high"]),
                            "low": float(o["low"]), "close": float(o["close"]),
                        }
                        # keep memory bounded
                        if len(candles) > HISTORY_CANDLES + 20:
                            for k in sorted(candles.keys())[:-HISTORY_CANDLES]:
                                candles.pop(k, None)
                        await on_candle_update()

        except Exception as e:
            print(f"[deriv] connection error: {e} — reconnecting in {backoff}s")
            try:
                send_discord_alert(f"⚠️ Bot reconnecting after error: {e}")
            except Exception:
                pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)


# ─── TINY HTTP SERVER (for uptime pinger + Render port binding) ──────────
async def handle_root(request):
    n = len(candles)
    return web.Response(text=f"SMC bot alive. Candles in memory: {n}\n")


async def run_http_server():
    app = web.Application()
    app.router.add_get("/", handle_root)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[http] keep-alive server listening on port {port}")


async def main():
    await run_http_server()
    await asyncio.gather(deriv_stream(), heartbeat_logger())


if __name__ == "__main__":
    asyncio.run(main())