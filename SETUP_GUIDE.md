# SMC Signal Bot — Complete Setup Guide (Discord version)

Yeh bot aapke laptop ke bina, 24/7, free mein chalega aur EURUSD signals
**Discord** channel par bhejega. Timing cron-job.org se control hoti hai
(GitHub ka apna schedule use nahi ho raha — is se signal_history.csv mein
duplicate entries nahi banengi).

---

## STEP 1: Discord Bot Banayein (5 minute)

1. Jao: https://discord.com/developers/applications
2. **"New Application"** click karo, koi bhi naam do (jaise "SMC Alerts")
3. Left sidebar mein **"Bot"** tab par jao
4. **"Reset Token"** (ya "Add Bot" agar pehli baar hai) click karo, confirm karo
5. Jo token dikhega usay copy kar lo — kuch aisa dikhega:
   `MTA1234567890.GxxxYY.abcDEFghiJKLmnoPQRstuVWXyz`
   **Yeh save kar lo — isko DISCORD_BOT_TOKEN kehte hain**
   (Ek dafa page se hat gaye to dobara "Reset Token" karna padega)

6. Left sidebar mein **"OAuth2" → "URL Generator"** par jao
7. **Scopes** mein `bot` check karo
8. **Bot Permissions** mein `Send Messages` aur `Read Message History` check karo
9. Neeche generated URL copy karo, browser mein kholo, apna Discord server
   select karke bot ko **invite/add** kar lo apne server mein

10. Apne Discord server mein wo channel kholo jahan alerts aane chahiye
    (ya naya channel bana lo, jaise `#smc-signals`)
11. Channel ka naam par right-click karo → **"Copy Channel ID"**
    (agar yeh option nahi dikhta: Discord app mein **User Settings → Advanced
    → Developer Mode** on karo, phir right-click se ID copy hoga)
    **Yeh number save kar lo — yeh DISCORD_CHANNEL_ID hai**

---

## STEP 2: Free Data API Key Lein (2 minute)

1. Jao: https://twelvedata.com/apikey
2. Free account banao (email se sign up)
3. Dashboard mein apni **API Key** milegi — save kar lo
   **Yeh TWELVEDATA_API_KEY hai**

Free tier mein 800 calls/day milte hain — bot din mein ~96 baar
check karega (har 15 min), yeh limit ke andar hai.

---

## STEP 3: GitHub Account Banayein (agar nahi hai)

1. Jao: https://github.com/signup
2. Free account banao

---

## STEP 4: Code GitHub Par Upload Karein

1. GitHub par login karke, top-right "+" > "New repository" click karo
2. Naam do: `smc-signal-bot` (private rakhna better hai)
3. "Create repository" click karo

4. Is folder ke saare files (`smc_strategy.py`, `.github/workflows/check_signal.yml`)
   apne naye repository mein upload karo:
   - "Add file" > "Upload files" click karo
   - Files drag-drop karo
   - "Commit changes" click karo

   **Important:** `.github/workflows/check_signal.yml` file ka path
   bilkul waisa hi rehna chahiye (`.github/workflows/` folder ke andar)

---

## STEP 5: Secrets Add Karein (Yeh Zaroori Hai)

1. Apne repository mein jao > **Settings** tab
2. Left side mein: **Secrets and variables** > **Actions**
3. "New repository secret" click karo, teen secrets add karo ek ek karke:

   | Name | Value |
   |------|-------|
   | `TWELVEDATA_API_KEY` | (Step 2 wali key) |
   | `DISCORD_BOT_TOKEN` | (Step 1 wala bot token) |
   | `DISCORD_CHANNEL_ID` | (Step 1 wali channel ID) |

---

## STEP 6: Timing Setup — cron-job.org (Yeh GitHub ke schedule ki jagah hai)

Is version mein workflow **khud se** schedule pe nahi chalta
(`workflow_dispatch` hi trigger hai) — cron-job.org (https://console.cron-job.org)
se do jobs banao:

**Job 1 — Regular signal check (har 15 min):**
- URL: `https://api.github.com/repos/<username>/<repo>/actions/workflows/check_signal.yml/dispatches`
- Method: POST
- Headers: `Authorization: token <YOUR_GITHUB_TOKEN>`, `Accept: application/vnd.github+json`, `Content-Type: application/json`
- Schedule: Every 15 minutes
- Body: `{"ref":"main"}`

**Job 2 — Midnight PKT daily summary (din mein sirf 1 baar):**
- Same URL, method, headers
- Schedule: Daily, 12:00 AM (Asia/Karachi timezone select karna, taake PKT midnight ho)
- Body: `{"ref":"main","inputs":{"mode":"summary"}}`

Job 2 hi wo hai jo din bhar ke saare BUY/SELL signals gin ke ek summary
Discord par bhejega. Job 1 sirf regular per-15-min check karta hai.

---

## Test Karne Ke Liye

Workflow ko turant manually chalane ke liye:
1. GitHub repo mein "Actions" tab > "SMC Signal Checker" > "Run workflow" button click karo
   (isme bhi "mode" input dikhega — "check" ya "summary" choose kar sakte ho)
2. Ya cron-job.org mein us job ke "Test Run" button se
3. 1-2 minute mein result Discord par ya GitHub "Actions" log mein dikhega

---

## Important Notes

- Yeh **completely free** hai — GitHub Actions free tier mein 2000
  minutes/month milte hain, script sirf kuch second leta hai har run mein
- Yeh TradingView se bilkul independent hai — koi violation nahi
- Timing **cron-job.org** control karta hai, GitHub ka apna schedule
  jaan-boojh kar off rakha gaya hai (duplicate history entries se bachne ke liye)
- Agar signal miss ho jaye kisi wajah se, GitHub "Actions" tab check karo —
  har run ka log wahan save rehta hai, aur cron-job.org ki "History" mein
  bhi request/response dikhta hai