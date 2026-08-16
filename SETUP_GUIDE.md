# SMC Signal Bot — Complete Setup Guide

Yeh bot aapke laptop ke bina, 24/7, free mein chalega aur EURUSD signals
Telegram par bhejega. Neeche diye steps follow karein.

---

## STEP 1: Telegram Bot Banayein (5 minute)

1. Telegram app kholein, search karein: **@BotFather**
2. BotFather ko message karein: `/newbot`
3. Bot ka naam do (jo bhi chaho, jaise "Wali SMC Alerts")
4. Username do (unique hona chahiye, jaise `wali_smc_bot`)
5. BotFather aapko ek **TOKEN** dega — kuch aisa dikhega:
   `123456789:ABCdefGhIjKlmNoPQRsTUVwxyZ`
   **Yeh save kar lo — isko TELEGRAM_BOT_TOKEN kehte hain**

6. Ab apne naye bot ko dhundo Telegram search mein (jo username diya tha)
   aur usay ek message bhejo (jaise "hi") — bot ko activate karne ke liye

7. Apna **Chat ID** nikalne ke liye, browser mein yeh URL kholo
   (apna token daal ke):
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`

   Isme "chat":{"id": 123456789} dikhega — yeh number save kar lo,
   **yeh TELEGRAM_CHAT_ID hai**

---

## STEP 2: Free Data API Key Lein (2 minute)

1. Jao: https://twelvedata.com/apikey
2. Free account banao (email se sign up)
3. Dashboard mein apni **API Key** milegi — save kar lo
   **Yeh TWELVEDATA_API_KEY hai**

Free tier mein 800 calls/day milte hain — hamara bot din mein ~96 baar
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
   | `TELEGRAM_BOT_TOKEN` | (Step 1 wala token) |
   | `TELEGRAM_CHAT_ID` | (Step 1 wala chat ID) |

---

## STEP 6: Bot Ko Activate Karein

1. Apne repository mein "Actions" tab par jao
2. "SMC Signal Checker" workflow dikhega
3. Agar disabled ho, "Enable workflow" click karo
4. Ab yeh automatically **har 15 minute** mein chalega, hamesha,
   bina kisi laptop ke — jab bhi signal aayega, Telegram par
   message aa jayega

---

## Test Karne Ke Liye

Workflow ko turant manually chalane ke liye:
1. "Actions" tab > "SMC Signal Checker" > "Run workflow" button click karo
2. 1-2 minute mein result Telegram par ya "Actions" log mein dikhega

---

## Important Notes

- Yeh **completely free** hai — GitHub Actions free tier mein 2000
  minutes/month milte hain, hamara script sirf kuch second leta hai
  har run mein, kaafi zyada hai
- Yeh TradingView se bilkul independent hai — koi violation nahi
- Agar signal miss ho jaye kisi wajah se, GitHub Actions ka "Actions"
  tab check kar sakte ho — har run ka log wahan save rehta hai