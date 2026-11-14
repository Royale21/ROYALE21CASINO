# ROYALE21CASINO

A luxury casino that uses virtual credits only. The credits have no cash value and cannot be redeemed.

Games: Slots, Baccarat, Roulette, Crash, Blackjack (with dealer, or with friends) and Texas Hold'em Poker (with dealer, or with friends).

## The 5 files

| File | What it is |
|---|---|
| `server.js` | The whole backend, bundled into one file. It needs no other packages. |
| `site.bundle` | The whole website (pages, fonts, logo, favicon), packed into one file. |
| `package.json` | Tells Render how to start the server. |
| `render.yaml` | Optional Render settings. |
| `README.md` | This guide. |

All 5 files go in the top folder of the GitHub repo. No folders are needed.

## Hosting (free)

### 1. Create the database (Neon, free)
1. Go to **neon.tech** and sign up. No card is needed.
2. Create a project. Name it `royale21casino` and pick the Singapore region.
3. Copy the **connection string**. It looks like `postgresql://user:password@ep-xxx.ap-southeast-1.aws.neon.tech/neondb?sslmode=require`

The site creates all of its tables on first start.

### 2. Upload to GitHub
1. Create a new repository, for example `royale21casino`.
2. Click **Add file → Upload files** and drag in the 5 files.
3. Check that the names are exactly `server.js`, `site.bundle`, `package.json`, `render.yaml` and `README.md`. Downloads sometimes arrive as `server-2.js` or similar, so rename any that changed.
4. Commit.

### 3. Deploy on Render
1. Go to Render, then **New → Web Service**, and connect the repo.
2. Use these settings:
   - Region: **Singapore**
   - Build command: `npm install`
   - Start command: `node server.js`
   - Instance type: **Free**
3. Under **Environment**, add:

| Key | Value |
|---|---|
| `DATABASE_URL` | the Neon connection string from step 1 |
| `ADMIN_EMAIL` | the email you will register with. That account becomes the admin. |
| `PUBLIC_URL` | optional, e.g. `https://royale21casino.onrender.com` (used in reset-email links) |
| `RESEND_API_KEY` | optional, for real password-reset emails (resend.com, free) |
| `MAIL_FROM` | optional, e.g. `ROYALE21CASINO <noreply@yourdomain.com>` |

4. Click **Create Web Service**. When the log shows `ROYALE21CASINO running on port ...`, the site is live.
5. Open the site and register with your `ADMIN_EMAIL`. The **Admin** page appears in the sidebar.

Note: Render's free plan allows only one always-on service. If the old royale21-vip service is still running, delete it or reuse that service for this repo. Its Blackjack-with-friends feature is now built in here as **Blackjack → Dealer + Friends**.

### 4. Keep it awake (UptimeRobot, free)
Add an HTTP monitor for `https://YOUR-SITE.onrender.com/health` that runs every 5 minutes.

### 5. Google Search Console
The verification file `google743a355452be7de8.html` is built in. You can verify the new domain with the same file.

## Without an email service
Password resets still work. An admin can open **Admin → Players → Manage → Password reset link** and send the link to the player.

## Settings you can change
- Welcome bonus: `WELCOME_BONUS` (default 10000)
- Daily bonus: `DAILY_BONUS` (default 1000)
- Bet limits for each game, or switching a game on or off: **Admin → Games**

## How the ledger stays accurate
- Every credit movement is one permanent row with: Transaction ID, exact time, game, type, opening balance, wagered, won, net and closing balance.
- The database itself enforces **closing = opening + net**, and blocks any edit or delete of a ledger row.
- Each wager and its result are written in the same database transaction as the wallet change, so there are never half-finished rounds.
- **My Ledger** shows a "Verified" badge after it checks that the whole chain reconciles with the wallet. Admins can run the same check for every player under **Admin → System**.
- Table buy-ins are saved after every hand. If the server restarts, every seated player's chips are returned to their wallet automatically.
