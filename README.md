# SMC Real-Time Bot — Render Deployment

Yeh `render_bot.py` ek **persistent (hamesha chalne wala) service** hai — GitHub
Actions wale periodic script (`SMC_STRATEGY.py`) ki jagah nahi leta, balke
usse **replace** karta hai real-time monitoring ke liye. Data source bhi badla
gaya hai: Twelve Data (jo sirf closed candles deta hai) ki jagah **Deriv** ka
live WebSocket feed use ho raha hai, jo forming candle ko tick-by-tick update
karta hai — isliye retest candle close hone se pehle hi pakda ja sakta hai.

## 1. Files
- `render_bot.py` — main service (isi ko Render pe run karna hai)
- `requirements.txt` — dependencies

## 2. Render pe deploy karna

1. Ek naya **private GitHub repo** banayein aur yeh 2 files usme push karein.
2. [render.com](https://render.com) pe jaake **New → Web Service** choose karein,
   apna repo connect karein.
3. Settings:
   - **Environment:** Python 3
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python render_bot.py`
   - **Instance Type:** Free (ya paid, agar 24/7 bina sleep chahiye)
4. Environment Variables (Render dashboard → Environment):
   - `DISCORD_BOT_TOKEN` — apka Discord bot token
   - `DISCORD_CHANNEL_ID` — jis channel mein alerts bhejne hain
   - `DERIV_APP_ID` — optional, default `1089` (public demo id) chalega
   - `DERIV_SYMBOL` — optional, default `frxEURUSD` (EUR/USD)
   - `GITHUB_TOKEN` — **optional**, agar signal history ko GitHub pe
     mirror karwana hai taake restart pe data na khoye
   - `GITHUB_REPO` — optional, e.g. `yourname/smc-history` (GitHub token
     ke sath zaroori hai)
5. Deploy karein. Logs mein `[deriv] connecting...` aur phir
   `[deriv] loaded 200 history candles` dikhna chahiye.

## 3. Free tier ko sona (sleep) hone se rokna — ZAROORI

Render ka free web service **15 minute bina HTTP traffic ke sula deta hai**.
Isay 24/7 zinda rakhne ke liye:

1. [UptimeRobot](https://uptimerobot.com) (free) pe account banayein.
2. Naya **HTTP(s) Monitor** add karein, URL = apki Render service ka URL
   (jo Render dashboard pe milta hai, e.g. `https://your-app.onrender.com`).
3. Interval = **5 minutes**.

Bas — ab UptimeRobot har 5 min pe ping karega, Render kabhi sula nahi
paayega.

## 4. Trade-offs jo samajhna zaroori hai

| Cheez | Behaviour |
|---|---|
| **Restart / redeploy** | Bot khud ba khud Deriv se last 200 candles reload kar leta hai, isliye FVG zones sahi se dobara ban jate hain — koi manual step nahi chahiye. |
| **signal_history.csv** | Render ka disk *ephemeral* hai — restart pe file khali ho jati hai, **agar** `GITHUB_TOKEN` + `GITHUB_REPO` set na ho. Agar set ho, to har signal ke baad file GitHub pe mirror ho jati hai. |
| **Duplicate alerts** | Ek hi FVG retest ke liye sirf ek dafa alert jaata hai (`alerted_ids` set track karta hai) — mid-candle baar baar check hone se spam nahi hoga. |
| **Daily summary** | Roz ~00:00 PKT pe automatically bhejta hai, based on `signal_history.csv` (isi run ke andar collected data). |
| **Free tier cost** | Bilkul free, bas UptimeRobot ping zaroori hai warna sona hai. |

## 5. Symbol check (agar EUR/USD na mile)

Agar logs mein `[deriv] API error` aaye symbol na milne ki wajah se, Deriv ke
`active_symbols` endpoint se sahi symbol code confirm kar lein (forex symbols
generally `frxEURUSD`, `frxGBPUSD` format mein hote hain).

## 6. Purana GitHub Actions setup ka kya karein?

Ab zaroorat nahi — is bot ke chalte hue `CHECK_SIGNAL.yml` ko disable ya
delete kar dein, warna dono ek sath alerts bhejenge (duplicate).