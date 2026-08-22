"""
SMC Real-Time Signal Bot — Render version
Persistent process: live Deriv tick stream + intra-candle FVG retest detection.
"""

import asyncio
import functools
import gc
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

DERIV_APP_ID = (os.environ.get("DERIV_APP_ID") or "1089").strip()
# NOTE: DERIV_API_TOKEN / real Deriv account no longer needed. The bot only
# ever reads public market data (candles), which Deriv serves anonymously —
# it never authorizes as a specific account. This avoids Deriv's ongoing
# token/app migration entirely (see fetch_current_forming_candle below).
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "").strip()
PORT = int(os.environ.get("PORT", 10000))

PKT = timezone(timedelta(hours=5))

# ─── SHARED STATE ────────────────────────────────────────────────────────
state = {
    "active_bull": [], "active_bear": [], "ema": None, "mid_level": None,
    "last_successful_poll": None,
}
state_lock = asyncio.Lock()

daily_counts = {"BUY": 0, "SELL": 0}
daily_counts_lock = asyncio.Lock()

# Health check considers the bot "unhealthy" if no successful price poll has
# happened within this many seconds. POLL_INTERVAL is 12s normally, and the
# retry backoff on failure can go up to 120s — 150s gives a safety buffer so
# a normal retry cycle doesn't falsely trip this.
HEALTH_STALE_THRESHOLD = 150


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

        # Free the DataFrame explicitly and force a GC pass. pandas/numpy can
        # leave allocator-level memory unreturned to the OS even after Python
        # itself has no more references — over many hours this shows up as
        # slowly climbing RSS on a tightly-limited (512MB) free instance.
        # This won't eliminate it entirely, but reduces how fast it grows.
        del df
        gc.collect()

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
async def check_price_update(session, price, candle_high, candle_low):
    triggered = []
    async with state_lock:
        # candle_high/candle_low come directly from Deriv's live-updating
        # candle stream for the currently-forming bar — this mirrors Pine's
        # "touched = (low <= top) and (high >= bot)" check, which looks at
        # the WHOLE candle's range, not just one exact price point.
        ema, mid = state["ema"], state["mid_level"]
        if ema is None or mid is None:
            return
        bullish_trend = price > ema
        bearish_trend = price < ema

        for fvg in list(state["active_bull"]):
            touched = (candle_low <= fvg["top"]) and (candle_high >= fvg["bot"])
            if touched and bullish_trend and price < fvg["mid"]:
                triggered.append(("BUY", fvg))
                state["active_bull"].remove(fvg)

        for fvg in list(state["active_bear"]):
            touched = (candle_high >= fvg["bot"]) and (candle_low <= fvg["top"])
            if touched and bearish_trend and price > fvg["mid"]:
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


async def fetch_current_forming_candle():
    """Same anonymous call as fetch_closed_candles, but keeps the last
    (possibly still-forming) candle instead of dropping it — this is how
    we get near-real-time OHLC without needing any authorize/subscribe at
    all, sidestepping Deriv's token/app_id auth entirely for this bot's
    read-only needs."""
    request = {
        "ticks_history": SYMBOL,
        "adjust_start_time": 1,
        "count": 2,
        "end": "latest",
        "start": 1,
        "style": "candles",
        "granularity": GRANULARITY,
    }
    response = await deriv_request(request, timeout=15)
    if "error" in response:
        raise RuntimeError(f"Deriv API error: {response['error']}")
    candles = response.get("candles")
    if not candles:
        raise RuntimeError(f"No candles in response: {response}")
    latest = candles[-1]
    return float(latest["close"]), float(latest["high"]), float(latest["low"])


POLL_INTERVAL = 12  # seconds — frequent enough to catch intra-candle FVG
                     # retests promptly, without hammering Deriv's API.


async def tick_listener_loop(session):
    """Polls the current forming candle every POLL_INTERVAL seconds instead
    of using a live websocket subscription. This is deliberately simpler
    and more robust than the previous authorize+subscribe approach: it
    reuses the exact same anonymous, no-auth request that candle refresh
    has used reliably throughout, so it isn't affected by Deriv's ongoing
    API/token migration (new "pat_" tokens aren't accepted by the legacy
    websocket "authorize" call, and registering a new-style app gives an
    app_id that the legacy websocket endpoint itself rejects with HTTP
    401 — both dead ends for this use case, and neither is needed here)."""
    consecutive_failures = 0
    update_count = 0
    last_heartbeat = time.time()

    while True:
        try:
            close, high, low = await fetch_current_forming_candle()
            update_count += 1
            await check_price_update(session, close, high, low)
            state["last_successful_poll"] = time.time()
            if consecutive_failures >= 3:
                await send_discord_alert(session, "✅ Live price polling recovered — bot is back to normal.")
            consecutive_failures = 0
            if time.time() - last_heartbeat >= 300:
                print(f"[{datetime.now(PKT)}] Heartbeat: {update_count} price polls in last ~5 min.")
                update_count = 0
                last_heartbeat = time.time()
            await asyncio.sleep(POLL_INTERVAL)
        except Exception as e:
            consecutive_failures += 1
            retry_delay = min(10 * consecutive_failures, 120)
            print(f"⚠️ Price poll failed ({consecutive_failures}x): {e}. Retrying in {retry_delay}s...")
            should_alert = consecutive_failures <= 3 or consecutive_failures % 10 == 0
            if should_alert:
                await send_discord_alert(
                    session,
                    f"⚠️ Live price polling failed (attempt {consecutive_failures}): {e}\nRetrying..."
                )
            await asyncio.sleep(retry_delay)


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
    last = state.get("last_successful_poll")
    if last is None:
        # Still starting up — hasn't had a chance to poll yet, don't fail this.
        return web.Response(text="OK (starting up)")
    age = time.time() - last
    if age > HEALTH_STALE_THRESHOLD:
        return web.Response(
            status=500,
            text=f"UNHEALTHY: last successful price poll was {age:.0f}s ago (threshold {HEALTH_STALE_THRESHOLD}s)"
        )
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