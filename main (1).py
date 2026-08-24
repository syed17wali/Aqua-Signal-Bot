"""
SMC Real-Time Signal Bot — Render version
Persistent process: live Deriv price monitoring + intra-candle FVG retest detection.

═══════════════════════════════════════════════════════════════════════════
CHANGELOG (most recent first) — kept so this file is self-explanatory if
shared with another AI/developer without the original chat history.
═══════════════════════════════════════════════════════════════════════════

- Added log_fvg_diagnostics(): prints exact indicator values (close, ema,
  swing_high/low, mid_level) and each FVG-birth boolean (raw_fvg,
  discount/premium filter, trend filter) for the last 3 candles, once per
  15-min refresh. Added because "Active FVGs: 0 bull, 0 bear" was
  persisting for hours (Aug 24, ~04:35-06:30 PKT logs) while TradingView's
  own Deriv-fed chart showed a clear BUY signal at 05:25 — meaning the
  bot's FVG-birth conditions are never true, but the existing logs gave no
  way to see WHICH of the three AND'd conditions is the blocker. This is
  diagnostic only — it does NOT fix the missing-signal issue by itself;
  the actual fix depends on what this logging reveals next refresh cycle.
- Added crash-loop protection: tracks restarts across process lifetimes via
  a small /tmp file (best-effort — may not survive a full container
  recreation, only a simple process restart). If 3+ restarts happen within
  a 10-min rolling window, the watchdog stops forcing further self-restarts
  for the rest of that run (switches to alert-only/"degraded mode") so we
  don't trip Render's own crash-loop protection and get the service killed
  entirely. Only gates the watchdog's own voluntary restarts — the nightly
  scheduled restart and Render-initiated deploy/SIGTERM shutdowns are left
  alone since those are controlled, legitimate reasons to restart, not
  symptoms of an unresolved bug.

- Added a watchdog: an independent loop (checks every 60s) that verifies
  the price-polling and candle-refresh loops have each actually succeeded
  recently (tracked via state["last_successful_poll"] /
  state["last_successful_refresh"]). If either has gone stale beyond a
  generous threshold, it sends a distinct "🐛 WATCHDOG" Discord alert and
  force-restarts the process — this catches failure modes that wouldn't
  raise a catchable exception inside the affected loop itself (e.g. a
  genuine deadlock, or an exception type like asyncio.CancelledError that
  a plain `except Exception` won't catch).

- Added Deriv rate-limit-specific handling: a dedicated DerivRateLimitError
  is raised when Deriv's response indicates a rate limit (vs. a generic
  error), and the poll loop backs off a fixed 60s in that case instead of
  the normal faster-escalating generic retry delay — avoids hammering the
  API with quick retries into the same limit window.

- Added nightly scheduled restart (~3:00 AM PKT) timed to the forex market's
  daily rollover gap (~10-15 min low/no-liquidity window each night), so the
  bot proactively refreshes memory during a quiet period instead of waiting
  for Render to force-restart it mid-session when the 512MB free-tier memory
  limit is hit unpredictably.

- Made /health endpoint "smart": it now checks how long ago the last
  successful price poll happened (state["last_successful_poll"]) and
  returns HTTP 500 if stale (> HEALTH_STALE_THRESHOLD seconds), instead of
  always returning 200 OK regardless of whether monitoring is actually
  working. This lets UptimeRobot's own down-alert (email) fire on a real
  stall, not just a fully-dead process.

- Added gc.collect() + explicit `del df` after each candle refresh, to
  reduce (not eliminate) the gradual RSS growth typical of long-running
  pandas/numpy processes, given Render's free tier only has 512MB RAM.

- Switched real-time monitoring from a raw Deriv WebSocket "ticks"
  subscription to POLLING the currently-forming candle's high/low/close
  every ~12s via a one-shot (non-subscribed) ticks_history request. This
  was necessary because BOTH raw "ticks" subscribe and "candles"
  subscribe (subscribe:1) were rejected by Deriv with "InvalidSymbol" for
  frxEURUSD — live subscriptions on forex (frx*) symbols appear to
  require an authorized session, which added complexity; polling
  one-shot historical/current-candle requests works fine anonymously
  (same as the public candle-history fetch already used for indicators).
  This also fixed a deeper accuracy issue: checking one exact tick price
  against the FVG zone could miss a real touch if price moved through the
  zone between polls — using the candle's own high/low (which Deriv
  tracks continuously server-side regardless of poll timing) mirrors the
  Pine Script's "touched = (low <= top) and (high >= bot)" logic much
  more closely.

- Fixed a UTC/PKT timezone bug in earlier iterations of this bot's candle
  fetching (now moot since Deriv's own timestamps are used consistently).

- Original version: Twelve Data REST API (60+ liquidity-provider
  aggregate) on a 15-min GitHub Actions cron — replaced entirely by this
  Deriv-based persistent-process design for (a) a single-broker feed that
  matches the TradingView chart's own Deriv feed, and (b) real-time
  intra-candle signal detection instead of waiting for each 15-min candle
  to close.
═══════════════════════════════════════════════════════════════════════════
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

# ─── CRASH-LOOP PROTECTION ───────────────────────────────────────────────
# Tracks restarts across process lifetimes using a small local file. If the
# process has restarted too many times in quick succession (regardless of
# WHY — watchdog, a fatal crash, anything), that means restarting isn't
# actually fixing the problem, and continuing to self-restart risks
# tripping Render's own crash-loop protection (which could stop the service
# entirely). In that case we deliberately STOP self-restarting and switch
# to alert-only mode instead — better to stay up and noisy than get killed.
#
# CAVEAT: this file lives in /tmp, which normally survives a simple process
# restart within the same container, but is NOT guaranteed to survive
# Render fully recreating the container (e.g. a redeploy). It's a
# best-effort safeguard, not a hard guarantee — but it costs nothing to
# have and helps in the most common case (a process crashing/restarting
# repeatedly within the same running instance).
RESTART_TRACKER_FILE = "/tmp/restart_tracker.json"
RAPID_RESTART_WINDOW = 600     # seconds — restarts within this window of
                                # each other count as "rapid succession"
CRASH_LOOP_THRESHOLD = 3       # this many rapid restarts in a row trips it


def check_and_record_restart():
    """Call once at startup. Returns True if a crash-loop is detected
    (i.e. self-restarting should be disabled for this process's lifetime)."""
    now = time.time()
    try:
        with open(RESTART_TRACKER_FILE, "r") as f:
            data = json.load(f)
        last_restart = data.get("last_restart_time", 0)
        rapid_count = data.get("rapid_restart_count", 0)
    except Exception:
        last_restart = 0
        rapid_count = 0

    if last_restart and (now - last_restart) < RAPID_RESTART_WINDOW:
        rapid_count += 1
    else:
        rapid_count = 1  # not rapid-following-another — reset the counter

    try:
        with open(RESTART_TRACKER_FILE, "w") as f:
            json.dump({"last_restart_time": now, "rapid_restart_count": rapid_count}, f)
    except Exception as e:
        print(f"Warning: couldn't write restart tracker file: {e}")

    print(f"[{datetime.now(PKT)}] Startup #{rapid_count} within the last {RAPID_RESTART_WINDOW}s window.")
    return rapid_count >= CRASH_LOOP_THRESHOLD


# Set once at import time; read by the watchdog and the shutdown handlers
# to decide whether self-restarting is currently allowed.
CRASH_LOOP_DETECTED = check_and_record_restart()

# ─── SHARED STATE ────────────────────────────────────────────────────────
state = {
    "active_bull": [], "active_bear": [], "ema": None, "mid_level": None,
    "last_successful_poll": None,       # updated by tick_listener_loop
    "last_successful_refresh": None,    # updated by refresh_candles — used by
                                         # the watchdog to detect a stuck/dead
                                         # refresh loop (see WATCHDOG section)
}
state_lock = asyncio.Lock()

daily_counts = {"BUY": 0, "SELL": 0}
daily_counts_lock = asyncio.Lock()

# Health check considers the bot "unhealthy" if no successful price poll has
# happened within this many seconds. POLL_INTERVAL is 12s normally, and the
# retry backoff on failure can go up to 120s — 150s gives a safety buffer so
# a normal retry cycle doesn't falsely trip this.
HEALTH_STALE_THRESHOLD = 150

# ─── WATCHDOG THRESHOLDS ─────────────────────────────────────────────────
# The watchdog is a separate, independent check that doesn't rely on a loop
# correctly catching its own errors — it just asks "has this loop actually
# succeeded recently?" and force-restarts if not. This catches failure modes
# the normal try/except handling inside each loop might miss entirely (e.g.
# a silent deadlock, or an exception type that isn't a subclass of Exception
# such as asyncio.CancelledError, which `except Exception` won't catch).
WATCHDOG_CHECK_INTERVAL = 60          # how often the watchdog itself checks, in seconds
WATCHDOG_POLL_STALE_THRESHOLD = 300   # price polling should never go 5 min without success
WATCHDOG_REFRESH_STALE_THRESHOLD = 2700  # candle refresh should never go 45 min without success
                                          # (normal cadence is ~15 min; 3x buffer for retries)

# Nightly proactive restart time (PKT). Timed to land inside forex's daily
# rollover gap (~10-15 min of low/no liquidity each night, roughly when
# it's 5 PM New York time — currently ~2:00-2:15 AM PKT during US Daylight
# Saving, shifting to ~3:00-3:15 AM PKT when US DST ends in Nov). 3:00 AM
# PKT is a safe middle-ground pick; adjust here if the actual gap timing
# is observed to differ.
NIGHTLY_RESTART_HOUR_PKT = 3
NIGHTLY_RESTART_MINUTE_PKT = 0


# ─── DERIV DATA FETCH ────────────────────────────────────────────────────
class DerivRateLimitError(RuntimeError):
    """Raised specifically when Deriv responds with a rate-limit error, so
    callers can back off longer/differently than for a generic API error."""
    pass


def _raise_for_deriv_error(response):
    """Centralized error check for Deriv responses. Distinguishes a
    rate-limit response (Deriv error code containing 'RateLimit', or a
    message mentioning it) from other API errors, since a rate limit
    should be handled with a longer, fixed backoff — retrying quickly
    would just keep tripping the same limit."""
    if "error" in response:
        err = response["error"]
        code = str(err.get("code", "")).lower()
        message = str(err.get("message", "")).lower()
        if "ratelimit" in code or "rate limit" in message or "too many requests" in message:
            raise DerivRateLimitError(f"Deriv rate limit: {err}")
        raise RuntimeError(f"Deriv API error: {err}")


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
            _raise_for_deriv_error(response)
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


def log_fvg_diagnostics(df):
    """Prints the exact indicator values and each FVG-birth boolean for the
    last few candles, once per refresh. This exists because 'Active FVGs: 0
    bull, 0 bear' for hours at a time (while TradingView's own chart shows
    clear BUY/SELL signals) gives no clue WHICH of the three AND'd
    conditions (raw_fvg / discount-premium filter / trend filter) is the
    one always failing — this makes that visible directly in the logs
    instead of guessing blind."""
    last = df.tail(3)
    print(f"[{datetime.now(PKT)}] --- FVG diagnostic (last 3 candles) ---")
    for _, row in last.iterrows():
        print(
            f"  {row['time']} close={row['close']:.5f} ema={row['ema']:.5f} "
            f"swing_hi={row['swing_high']:.5f} swing_lo={row['swing_low']:.5f} "
            f"mid={row['mid_level']:.5f}"
        )
        print(
            f"    BULL: raw={bool(row['raw_bull_fvg'])} "
            f"discount={bool(row['is_discount_fvg'])} "
            f"trend_up={bool(row['bullish_trend'])} "
            f"=> new_bull_fvg={bool(row['new_bull_fvg'])}"
        )
        print(
            f"    BEAR: raw={bool(row['raw_bear_fvg'])} "
            f"premium={bool(row['is_premium_fvg'])} "
            f"trend_dn={bool(row['bearish_trend'])} "
            f"=> new_bear_fvg={bool(row['new_bear_fvg'])}"
        )


async def refresh_candles(session):
    global candle_refresh_failures
    try:
        df = await fetch_closed_candles(count=300)
        df = calculate_indicators(df)
        log_fvg_diagnostics(df)
        active_bull, active_bear, context = rebuild_active_fvgs(df)
        async with state_lock:
            state["active_bull"] = active_bull
            state["active_bear"] = active_bear
            state["ema"] = context["ema"]
            state["mid_level"] = context["mid_level"]
            state["last_successful_refresh"] = time.time()
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
    _raise_for_deriv_error(response)
    candles = response.get("candles")
    if not candles:
        raise RuntimeError(f"No candles in response: {response}")
    latest = candles[-1]
    return float(latest["close"]), float(latest["high"]), float(latest["low"])


POLL_INTERVAL = 12  # seconds — frequent enough to catch intra-candle FVG
                     # retests promptly, without hammering Deriv's API.

RATE_LIMIT_BACKOFF = 60  # seconds — fixed wait specifically for rate-limit
                          # errors (Deriv's limit windows are typically
                          # rolling per-minute, so a 60s pause gives the
                          # window time to reset instead of retrying into
                          # the same limit repeatedly with the normal
                          # escalating-but-faster generic backoff).


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
    rate_limit_hits = 0
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
            rate_limit_hits = 0
            if time.time() - last_heartbeat >= 300:
                print(f"[{datetime.now(PKT)}] Heartbeat: {update_count} price polls in last ~5 min.")
                update_count = 0
                last_heartbeat = time.time()
            await asyncio.sleep(POLL_INTERVAL)
        except DerivRateLimitError as e:
            # Handled separately from generic failures: always wait the
            # full fixed RATE_LIMIT_BACKOFF, regardless of how many times
            # this has happened, since retrying sooner would likely just
            # hit the same limit again.
            rate_limit_hits += 1
            consecutive_failures += 1
            print(f"⏳ Deriv rate limit hit ({rate_limit_hits}x): {e}. Backing off {RATE_LIMIT_BACKOFF}s...")
            should_alert = rate_limit_hits <= 2 or rate_limit_hits % 10 == 0
            if should_alert:
                await send_discord_alert(
                    session,
                    f"⏳ Deriv rate limit hit (attempt {rate_limit_hits}) — backing off {RATE_LIMIT_BACKOFF}s before retrying."
                )
            await asyncio.sleep(RATE_LIMIT_BACKOFF)
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


# ─── NIGHTLY PROACTIVE RESTART ──────────────────────────────────────────
# Instead of waiting for Render to force-restart this instance whenever the
# 512MB free-tier memory limit gets hit (which can happen at any random
# time, including active market hours), we restart ourselves once a day at
# a fixed, predictable time that lands inside forex's own daily low-activity
# rollover gap — so any brief ~20-25s startup gap coincides with a period
# when the market itself is quiet anyway, minimizing the chance a real
# signal is missed because of it.
async def nightly_restart_loop(session):
    while True:
        now_pkt = datetime.now(PKT)
        target = now_pkt.replace(
            hour=NIGHTLY_RESTART_HOUR_PKT, minute=NIGHTLY_RESTART_MINUTE_PKT,
            second=0, microsecond=0
        )
        if now_pkt >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now_pkt).total_seconds())

        await send_discord_alert(
            session,
            f"🌙 Scheduled nightly restart ({NIGHTLY_RESTART_HOUR_PKT:02d}:{NIGHTLY_RESTART_MINUTE_PKT:02d} PKT) — "
            "routine memory refresh, timed to forex's low-activity rollover window. Back up shortly."
        )
        await asyncio.sleep(1)  # let the alert actually go out before exiting
        os._exit(0)  # Render restarts the service automatically after this


# ─── WATCHDOG ────────────────────────────────────────────────────────────
# Independent supervisor that doesn't trust the other loops' own error
# handling to be perfect. It just checks "has each loop actually succeeded
# recently?" using the timestamps they record in `state`, and force-restarts
# the whole process (with a distinct Discord alert first) if either looks
# stuck. This is the safety net for failure modes that wouldn't otherwise
# raise a catchable exception — e.g. a genuine deadlock, or an event type
# like asyncio.CancelledError slipping past a loop's own `except Exception`.
async def watchdog_loop(session):
    start_time = time.time()
    degraded_mode_announced = False
    last_degraded_alert = 0

    while True:
        await asyncio.sleep(WATCHDOG_CHECK_INTERVAL)
        now = time.time()

        last_poll = state.get("last_successful_poll") or start_time
        poll_age = now - last_poll
        last_refresh = state.get("last_successful_refresh") or start_time
        refresh_age = now - last_refresh

        stale_reason = None
        if poll_age > WATCHDOG_POLL_STALE_THRESHOLD:
            stale_reason = (
                f"Live price polling hasn't succeeded in {poll_age:.0f}s "
                f"(should happen every ~{POLL_INTERVAL}s)."
            )
        elif refresh_age > WATCHDOG_REFRESH_STALE_THRESHOLD:
            stale_reason = (
                f"Candle refresh hasn't succeeded in {refresh_age / 60:.0f} min "
                f"(should happen every ~15 min)."
            )

        if stale_reason is None:
            continue

        if CRASH_LOOP_DETECTED:
            # Restarting repeatedly hasn't fixed this — stop self-restarting
            # so we don't trip Render's own crash-loop protection. Alert
            # instead, throttled so it doesn't spam every single check.
            if not degraded_mode_announced:
                await send_discord_alert(
                    session,
                    "🚨 CRASH-LOOP PROTECTION ACTIVE: This process has restarted too many "
                    "times in quick succession, and the problem is still happening "
                    f"({stale_reason}). To avoid Render shutting the service down entirely, "
                    "auto-restart is now DISABLED for this run — the bot will keep alerting "
                    "but won't restart itself anymore. Please check Render's Logs tab and "
                    "fix the underlying issue, then manually redeploy."
                )
                degraded_mode_announced = True
                last_degraded_alert = now
            elif now - last_degraded_alert > 300:  # remind every 5 min, not more often
                await send_discord_alert(
                    session,
                    f"🚨 Still degraded (crash-loop protection active): {stale_reason} "
                    "Manual fix + redeploy needed."
                )
                last_degraded_alert = now
            continue  # do NOT restart

        msg = f"🐛 WATCHDOG: {stale_reason} The loop appears stuck without triggering its own error handling. Forcing a restart."
        print(msg)
        await send_discord_alert(session, msg)
        await asyncio.sleep(1)
        os._exit(1)  # non-zero exit code — distinguishes a watchdog-forced
                      # restart from the graceful nightly one (exit 0) if
                      # ever inspected in Render's process logs


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
            nightly_restart_loop(session),
            watchdog_loop(session),
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