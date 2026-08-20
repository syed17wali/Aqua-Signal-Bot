# SMC Real-Time Signal Bot — Complete Setup Guide (Deriv + Render version)

Yeh bot 24/7 chalega (laptop ki zaroorat nahi) aur **real-time** BUY/SELL
signals Discord par bhejega — 15-min candle close hone ka wait nahi karega,
jaise hi price FVG retest zone touch kare (trend confirm ke saath), turant
alert aa jayega. Data source **Deriv** (real forex feed, `frxEURUSD`) hai.

---

## STEP 1: Discord Bot Banayein (agar pehle se nahi hai)

1. Jao: https://discord.com/developers/applications
2. **"New Application"** → koi bhi naam do (jaise "SMC Alerts")
3. Left sidebar mein **"Bot"** tab → **"Reset Token"** (ya "Add Bot")
4. Jo token dikhega, save kar lo — **yeh DISCORD_BOT_TOKEN hai**
5. **"OAuth2" → "URL Generator"** → Scopes mein `bot` check karo
6. Bot Permissions mein `Send Messages` check karo
7. Generated URL browser mein kholo, apna Discord server select karke bot invite kar lo
8. Jis channel mein alerts chahiye, uska naam right-click → **"Copy Channel ID"**
   (Developer Mode on karna pade to: User Settings → Advanced → Developer Mode)
   **Yeh number save kar lo — DISCORD_CHANNEL_ID hai**

---

## STEP 2: Deriv App ID

Kuch karne ki zaroorat nahi — bot **App ID 1089** use karta hai, jo Deriv ka
public testing ID hai. Yeh bina signup/login ke free market data padhne ke
liye kaam karta hai. Bas yaad rakho: **DERIV_APP_ID = 1089**

---

## STEP 3: GitHub Repo Mein Sirf Yeh 2 Files Rakhein

Repo (`Aqua-Signal-Bot`) mein sirf yeh files honi chahiye:

- `main.py`
- `requirements.txt`

Baqi purani files (`smc_strategy.py`, `signal_history.csv`,
`.github/workflows/*.yml`, purana `SETUP_GUIDE.md`) delete kar dena —
naya bot inka kaam khud karta hai, aur dono system saath chalne se
Discord par **duplicate alerts** aayenge.

---

## STEP 4: Render.com Par Deploy Karein

1. **render.com** par jao → **"Get Started"** → GitHub se login karo
2. Dashboard mein **"New +" → "Web Service"**
3. Apna `Aqua-Signal-Bot` repo connect karo
4. Settings:
   - **Environment:** Python 3
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python main.py`
   - **Instance Type:** Free
5. **Environment Variables** add karo:

   | Key | Value |
   |---|---|
   | `DERIV_APP_ID` | `1089` |
   | `DISCORD_BOT_TOKEN` | (Step 1 wala token) |
   | `DISCORD_CHANNEL_ID` | (Step 1 wali channel ID) |

6. **"Create Web Service"** dabao — 2-3 min mein deploy ho jayega

---

## STEP 5: Confirm Karein Ke Bot Zinda Hai

1. Render dashboard mein **"Logs"** tab kholo
2. Yeh lines dikhni chahiyein:
   - `Health server running on port ...`
   - `Subscribed to live ticks for frxEURUSD`
3. Discord channel mein **"🚀 SMC Real-Time Bot started."** message aana chahiye

---

## STEP 6: 24/7 Zinda Rakhne Ke Liye UptimeRobot (Zaroori)

Render ka **free tier 15 min inactivity ke baad bot ko "sleep"** kar deta hai.
Isay hamesha jagaye rakhne ke liye:

1. **uptimerobot.com** par free account banao
2. **"New Monitor"** → Type: **HTTP(s)**
3. URL: apne Render service ka public URL (jaise `https://your-app.onrender.com`)
4. Monitoring Interval: **5 minutes**
5. Save karo

Yeh har 5 min bot ke health-check endpoint ko "ping" karega, jisse Render
usay kabhi sone nahi dega.

---

## Bot Kya Karta Hai (Summary)

- Har 15 min pe khud candles refresh kar ke EMA, swing high/low, aur
  active FVG zones dobara calculate karta hai
- Live price stream (Deriv se, har second) continuously monitor karta hai
- Jaise hi price kisi active FVG zone ko retest kare (trend ke sath match),
  **turant** Discord par alert bhejta hai — candle close ka wait nahi karta
- Agar connection kabhi disconnect ho, khud 10 sec mein dobara jud jata hai
- Raat 12 baje PKT us din ka BUY/SELL total count Discord par bhejta hai

---

## Important Notes

- **Completely free** — Deriv free, Render free tier, UptimeRobot free
- Is version mein `signal_history.csv` jaisi permanent file nahi hai —
  daily count sirf memory mein rehta hai (agar Render kabhi restart ho,
  usi din ka partial count reset ho sakta hai — summary agle din se
  theek chalta rahega)
- Kuch masla ho to Render ke **"Logs"** tab mein error dikh jayega