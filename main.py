"""
SMC Real-Time Signal Bot — Render version
Persistent process: live Deriv tick stream + intra-candle FVG retest detection.
"""

import asyncio
import functools
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone, timedelta

# ─── FORCE UNBUFFERED STDOUT ─────────────────────────────────────────────
# Render (and most container hosts) capture stdout via a pipe, not a
# terminal. Python's default is to block-buffer output to pipes, so
# print() lines can sit in an internal buffer for a long time before they
# actually show up in the log viewer — it *looks* like the code isn't
# running even though it is. Forcing every print() to flush immediately
# fixes that.
print = functools.partial(print, flush=True)
try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:
    pass  # older Python without reconfigure(); the print() patch above still covers us

import pandas as pd
import websockets
from aiohttp import web
import aiohttp

# ─── SETTINGS ────────────────────────────────────────────────────────────
LOOKBACK_LEN = 100
FVG_SIZE_PCT = 0.05
EMA_LEN = 25
FVG_WINDOW_BARS = 36
GRANULARITY = 900          # 15 min in seconds
SYMBOL = "frxEURUSD"

DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")
PORT = int(os.environ.get("PORT", 10000))

PKT = timezone(timedelta(hours=5))

# ─── SHARED STATE ────────────────────────────────────────────────────────
state = {"active_bull": [], "active_bear": [], "ema": None, "mid_level": None}
state_lock = asyncio.Lock()

daily_counts = {"BUY": 0, "SELL": 0}
daily_counts_lock = asyncio.Lock()


# ─── DERIV DATA FETCH ────────────────────────────────────────────────────
async def deriv_request(payload, timeout=20):
    uri = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    async with websockets.connect(uri, open_timeout=timeout) as ws:
        await ws.send(json.dumps(payload))
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        return json.loads(raw)


async def fetch_closed_candles(count=300, max_retries=3):
    request = {
        "ticks_history": SYMBOL,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "start": 1,
        "style": "candles",
        "granularity": GRANULARITY,
    }
    last_error = None
    response = None
    for attempt in range(1, max_retries + 1):
        try:
            response = await deriv_request(request)
            if "error" in response:
                raise RuntimeError(f"Deriv API error: {response['error']}")
            if not response.get("candles"):
                raise RuntimeError(f"No candles in response: {response}")
            break
        except Exception as e:
            last_error = e
            print(f"[Attempt {attempt}/{max_retries}] fetch_closed_candles failed: {e}")
            if attempt < max_retries:
                await asyncio.sleep(5 * attempt)
    else:
        raise RuntimeError(f"Deriv API unreachable after {max_retries} attempts: {last_error}")

    candles = response["candles"]
    now_epoch = int(time.time())
    if candles[-1]["epoch"] + GRANULARITY > now_epoch:
        candles = candles[:-1]
    if not candles:
        raise RuntimeError("No fully-closed candles after filtering forming bar.")

    df = pd.DataFrame(candles)
    df = df.rename(columns={"epoch": "time"})
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
    df = df.sort_values("time").reset_index(drop=True)
    return df


# ─── INDICATORS (same logic as Pine Script) ─────────────────────────────
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


def rebuild_active_fvgs(df):
    bull_fvgs, bear_fvgs = [], []
    n = len(df)
    for i in range(n):
        row = df.iloc[i]
        if row.get("new_bull_fvg", False):
            bull_fvgs.append({"bar": i, "top": row["bull_fvg_top"], "bot": row["bull_fvg_bot"],
                               "mid": row["mid_level"], "retired": False})
        if row.get("new_bear_fvg", False):
            bear_fvgs.append({"bar": i, "top": row["bear_fvg_top"], "bot": row["bear_fvg_bot"],
                               "mid": row["mid_level"], "retired": False})

        for fvg in bull_fvgs:
            if fvg["retired"]:
                continue
            age = i - fvg["bar"]
            if age > FVG_WINDOW_BARS:
                fvg["retired"] = True
            elif age > 0:
                touched = (row["low"] <= fvg["top"]) and (row["high"] >= fvg["bot"])
                if touched and row.get("bullish_trend", False) and row["close"] < fvg["mid"]:
                    fvg["retired"] = True

        for fvg in bear_fvgs:
            if fvg["retired"]:
                continue
            age = i - fvg["bar"]
            if age > FVG_WINDOW_BARS:
                fvg["retired"] = True
            elif age > 0:
                touched = (row["high"] >= fvg["bot"]) and (row["low"] <= fvg["top"])
                if touched and row.get("bearish_trend", False) and row["close"] > fvg["mid"]:
                    fvg["retired"] = True

    active_bull = [f for f in bull_fvgs if not f["retired"]]
    active_bear = [f for f in bear_fvgs if not f["retired"]]
    latest = df.iloc[-1]
    context = {"ema": float(latest["ema"]), "mid_level": float(latest["mid_level"])}
    return active_bull, active_bear, context


# ─── DISCORD ─────────────────────────────────────────────────────────────
async def send_discord_alert(session, message):
    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}", "Content-Type": "application/json"}
    try:
        async with session.post(url, headers=headers, json={"content": message},
                                 timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                print(f"Discord send failed: {resp.status} {text}")
    except Exception as e:
        print(f"Discord send exception: {e}")


# ─── CANDLE REFRESH LOOP (rebuilds active FVG list every 15 min) ────────
candle_refresh_failures = 0


async def refresh_candles(session):
    global candle_refresh_failures
    try:
        df = await fetch_closed_candles(count=300)
        df = calculate_indicators(df)
        active_bull, active_bear, context = rebuild_active_fvgs(df)
        async with state_lock:
            state["active_bull"] = active_bull
            state["active_bear"] = active_bear
            state["ema"] = context["ema"]
            state["mid_level"] = context["mid_level"]
        print(f"[{datetime.now(PKT)}] Refreshed. Active FVGs: {len(active_bull)} bull, {len(active_bear)} bear.")

        if candle_refresh_failures >= 2:
            await send_discord_alert(session, "✅ Candle refresh recovered — back to normal.")
        candle_refresh_failures = 0
        return True
    except Exception as e:
        candle_refresh_failures += 1
        err = f"⚠️ Candle refresh failed ({candle_refresh_failures}x): {e}"
        print(err)
        await send_discord_alert(session, err)
        return False


async def refresh_candles_loop(session):
    ok = await refresh_candles(session)
    if ok:
        await send_discord_alert(
            session,
            f"✅ Bot fully initialized — {len(state['active_bull'])} active bull / "
            f"{len(state['active_bear'])} active bear FVGs. Live monitoring is running."
        )
    while True:
        now = time.time()
        wait_seconds = GRANULARITY - (now % GRANULARITY) + 10
        await asyncio.sleep(wait_seconds)
        await refresh_candles(session)


# ─── LIVE TICK LISTENER (real-time intra-candle detection) ─────────────
async def check_tick(session, price):
    triggered = []
    async with state_lock:
        ema, mid = state["ema"], state["mid_level"]
        if ema is None or mid is None:
            return
        bullish_trend = price > ema
        bearish_trend = price < ema

        for fvg in list(state["active_bull"]):
            if fvg["bot"] <= price <= fvg["top"] and bullish_trend and price < fvg["mid"]:
                triggered.append(("BUY", fvg))
                state["active_bull"].remove(fvg)

        for fvg in list(state["active_bear"]):
            if fvg["bot"] <= price <= fvg["top"] and bearish_trend and price > fvg["mid"]:
                triggered.append(("SELL", fvg))
                state["active_bear"].remove(fvg)

    for signal, fvg in triggered:
        now_str = datetime.now(PKT).strftime("%Y-%m-%d %I:%M:%S %p PKT")
        msg = (f"⚡ {signal} Signal (LIVE) — {SYMBOL}\n"
               f"Time: {now_str}\nPrice: {price:.5f}\n"
               f"FVG zone: [{fvg['bot']:.5f} - {fvg['top']:.5f}]")
        print(msg)
        await send_discord_alert(session, msg)
        async with daily_counts_lock:
            daily_counts[signal] += 1


async def tick_listener_loop(session):
    uri = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    consecutive_failures = 0
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                # NOTE: a plain {"ticks": SYMBOL, "subscribe": 1} request was being
                # rejected by Deriv with "Symbol frxEURUSD is invalid", even though
                # the exact same symbol works fine for "ticks_history" (used below
                # for candles). Using "ticks_history" with subscribe:1 instead gets
                # us onto the same request path that's already proven to work, and
                # it still pushes live "tick" messages after the initial snapshot.
                await ws.send(json.dumps({
                    "ticks_history": SYMBOL,
                    "adjust_start_time": 1,
                    "count": 1,
                    "end": "latest",
                    "style": "ticks",
                    "subscribe": 1,
                }))
                print(f"[{datetime.now(PKT)}] Subscribed to live ticks for {SYMBOL}")
                if consecutive_failures >= 3:
                    await send_discord_alert(session, "✅ Live tick stream reconnected — bot is back to normal.")
                consecutive_failures = 0
                async for raw in ws:
                    data = json.loads(raw)
                    if "error" in data:
                        print(f"Tick stream error: {data['error']}")
                        continue
                    tick = data.get("tick")
                    if tick:
                        await check_tick(session, float(tick["quote"]))
        except Exception as e:
            consecutive_failures += 1
            print(f"⚠️ Tick stream disconnected ({consecutive_failures}x): {e}. Reconnecting in 10s...")
            # Only alert on the first drop and then every 3rd repeated failure,
            # so a single brief network blip doesn't spam Discord.
            if consecutive_failures == 1 or consecutive_failures % 3 == 0:
                await send_discord_alert(
                    session,
                    f"⚠️ Live tick stream disconnected (attempt {consecutive_failures}): {e}\nRetrying..."
                )
            await asyncio.sleep(10)


# ─── DAILY SUMMARY (midnight PKT) ───────────────────────────────────────
async def daily_summary_loop(session):
    while True:
        now_pkt = datetime.now(PKT)
        next_midnight = (now_pkt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_midnight - now_pkt).total_seconds())

        async with daily_counts_lock:
            buy_count, sell_count = daily_counts["BUY"], daily_counts["SELL"]
            daily_counts["BUY"] = 0
            daily_counts["SELL"] = 0

        today_str = datetime.now(PKT).strftime("%Y-%m-%d")
        total = buy_count + sell_count
        if total == 0:
            msg = f"📊 Daily Summary ({today_str}): No BUY/SELL signals today."
        else:
            msg = f"📊 Daily Summary ({today_str})\nTotal signals: {total} (BUY: {buy_count}, SELL: {sell_count})"
        print(msg)
        await send_discord_alert(session, msg)


# ─── HEALTH ENDPOINT (keeps Render web service alive) ──────────────────
async def health(request):
    return web.Response(text="OK")


async def start_web_app():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Health server running on port {PORT}")


async def main():
    loop = asyncio.get_running_loop()
    async with aiohttp.ClientSession() as session:

        async def handle_shutdown():
            await send_discord_alert(
                session,
                "🔄 This instance is restarting (new deploy, or Render's own maintenance). "
                "A fresh instance will confirm itself shortly."
            )
            await asyncio.sleep(1)  # give the HTTP request a moment to actually go out
            os._exit(0)

        def on_sigterm():
            asyncio.create_task(handle_shutdown())

        try:
            loop.add_signal_handler(signal.SIGTERM, on_sigterm)
        except NotImplementedError:
            pass  # signal handlers aren't supported on some platforms — safe to skip

        await send_discord_alert(session, "🚀 SMC Real-Time Bot started.")
        await start_web_app()
        await asyncio.gather(
            refresh_candles_loop(session),
            tick_listener_loop(session),
            daily_summary_loop(session),
        )


def send_discord_alert_sync(message):
    """Synchronous fallback used only when the async session is already gone
    (i.e. the whole program is crashing) — used to send one last alert."""
    import urllib.request
    try:
        url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
        req = urllib.request.Request(
            url,
            data=json.dumps({"content": message}).encode("utf-8"),
            headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}", "Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"Fatal-crash Discord alert also failed: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        crash_msg = f"🔴 FATAL: Bot crashed and is shutting down:\n{e}\nRender should auto-restart it shortly."
        print(crash_msg)
        send_discord_alert_sync(crash_msg)
        raise