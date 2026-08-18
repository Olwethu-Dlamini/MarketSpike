# Deploying MarketSpike to Render

> **Already deployed:** https://marketspike.onrender.com
> Health check: `https://marketspike.onrender.com/api/v1/health` · Interactive API: `https://marketspike.onrender.com/docs`

Written for someone who has never deployed anything. Read section 1 even if you want to skip ahead — it explains what you're actually doing.

---

## 1. What hosting actually is

Right now, when you run `python -m marketspike.main`, the program runs **on your laptop**. It listens on port 8000, and only your laptop can answer `http://localhost:8000`. Close the lid and it's gone.

**Hosting means Render runs that exact same program on one of their computers**, permanently, and gives it a public web address like `https://marketspike.onrender.com`. Anyone can reach it: your teammates, a judge, a phone on mobile data.

Render does this in four steps, every time you push to GitHub:

1. **Pull** your code from the repo.
2. **Build** — run `pip install -r requirements.txt` to install dependencies.
3. **Start** — run your start command to launch the app.
4. **Route** — put it behind a public HTTPS URL and send traffic to it.

You never copy files to a server. **You push to GitHub, and Render notices and redeploys.** That's the whole workflow.

### The one thing that trips everyone up: the port

On your laptop you chose port 8000. On Render you don't get to choose — Render picks a port and tells your program via an environment variable called `PORT`. Your app **must** read it and listen there.

If it ignores `PORT` and stubbornly binds 8000, Render's health check gets no answer, and after a few minutes it declares the deploy failed. This is the single most common first-deploy failure.

**This is already fixed** in `marketspike/config.py` — it reads `PORT` and falls back to 8000 locally. Verified: setting `PORT=9137` makes it bind 9137.

---

## 2. Before you deploy: what will and won't work

Be clear-eyed about this. Render's **free** tier has two properties that matter for this app.

### The filesystem is wiped on every restart

Your app records ticks into a SQLite file. On Render's free tier that file **does not survive** a redeploy or restart — there's no persistent disk.

**What this means:** the hosted service is great for *serving* — sizing, regime, latency, the API your frontend calls. It is **useless for accumulating training data.**

**What to do:** keep recording on your laptop, train there, and commit `model.json`. The hosted service then serves your trained model. That file is deliberately committed to the repo for exactly this reason.

```bash
# on your laptop, over hours
MS_DB_PATH=$HOME/marketspike-live.db python -m marketspike.main

# then
python -m marketspike.ml.train --db $HOME/marketspike-live.db --symbols BTCUSDT --out model.json
git add model.json && git commit -m "chore: retrain model" && git push
```

Render redeploys, and `/api/v1/model/card` reports `source: "trained"`.

**A useful wrinkle, measured on the live instance.** Render's Frankfurt region is far
closer to Binance than a laptop in South Africa, so it sees roughly **8x the tick rate** —
about 127 ticks/sec against 16 locally. In its first ten minutes the deployed service
recorded 78,144 ticks; the same wall-clock locally yields around 10,000.

So between deploys, Render is the *better* recorder. Two caveats before relying on it:

- The database is wiped on every redeploy, so pull the data out before you push again.
- A free instance sleeps after ~15 minutes without inbound HTTP, and the recorder stops
  with it. Keeping a browser tab open on `/api/v1/health` (or any uptime pinger) keeps it
  awake and recording.

To retrieve what it captured, run the capture script against the live service rather than
a local file — or simply train locally, where the data is yours to keep.

### Free instances go to sleep

After about 15 minutes with no incoming web requests, Render spins a free service down. The next visitor's request wakes it, but that first request takes **30–60 seconds** while it boots.

**Why this matters on demo day:** a sleeping service means a cold start *and* a cold engine. The volatility horizon needs roughly 150 seconds after boot before `v_ratio` is trustworthy.

**What to do:** open the URL 5 minutes before you present and leave a tab on it. Or upgrade to a paid instance, which never sleeps.

---

## 3. Deploying — step by step

### Step 1 — push the deployment config

`render.yaml` in the repo root tells Render how to build and run the app. Make sure it's pushed:

```bash
git push origin main
```

### Step 2 — create the service

1. Sign in at [dashboard.render.com](https://dashboard.render.com) with your GitHub account.
2. **New → Blueprint**.
3. Choose the `MarketSpike` repository. Render reads `render.yaml` and fills everything in.
4. Click **Apply**.

If Blueprint gives you trouble, do it manually — **New → Web Service** — and enter:

| Field | Value |
|---|---|
| Runtime | Python 3 |
| Build command | `pip install -r requirements.txt && pip install -e .` |
| Start command | `uvicorn marketspike.main:app --host 0.0.0.0 --port $PORT` |
| Health check path | `/api/v1/health` |

### Step 3 — watch the build log

Render shows the log live. You want to see, in order:

```
==> Installing dependencies
==> Build successful
==> Starting service
INFO:marketspike.main:started with symbols=['BTCUSDT']
INFO:     Uvicorn running on http://0.0.0.0:10000
INFO:marketspike.main:seeded BTCUSDT slow variance at 8.826e-10
==> Your service is live 🎉
```

The port will be Render's number, not 8000. That's correct — it means `PORT` is being honoured.

### Step 4 — check it

Replace the hostname with your actual URL:

```bash
curl -s https://marketspike.onrender.com/api/v1/health
```

Or just open **`https://marketspike.onrender.com/docs`** in a browser — the interactive API page, with a "Try it out" button on every endpoint. Easiest way to confirm it's alive.

---

## 4. Connecting your frontend

Your teammates change one thing: the base URL.

```js
// before
const API = "http://localhost:8000";
const WS  = "ws://localhost:8000/ws/v1/stream";

// after
const API = "https://marketspike.onrender.com";
const WS  = "wss://marketspike.onrender.com/ws/v1/stream";
```

Note **`wss://`**, not `ws://`. Render serves HTTPS, and a secure page cannot open an insecure WebSocket — browsers block it. Getting this wrong produces a silent connection failure with a console error, which is a horrible thing to debug at midnight.

### CORS — the other thing that will bite you

A browser refuses to let a page on one domain call an API on another, unless the API explicitly allows it. That allow-list is the `MS_CORS_ORIGINS` environment variable.

Once your frontend has its own URL, add it:

1. Render dashboard → your service → **Environment**
2. Edit `MS_CORS_ORIGINS`, comma-separated, no spaces:
   ```
   http://localhost:5173,https://marketspike-ui.onrender.com
   ```
3. Save. Render restarts automatically.

Symptom if you forget: the frontend loads but every API call fails, and the browser console says something about "blocked by CORS policy". The backend is fine — it's the allow-list.

---

## 5. Adding EURUSD

EURUSD needs OANDA credentials. **Never put them in `render.yaml`** — that file is in your public repo. `render.yaml` marks them `sync: false`, meaning "I'll set this in the dashboard".

Render dashboard → **Environment** → add:

| Key | Value |
|---|---|
| `MS_OANDA_TOKEN` | your practice token |
| `MS_OANDA_ACCOUNT_ID` | your practice account id |
| `MS_SYMBOLS` | `BTCUSDT,EURUSD` |

Remember forex is closed Fri 21:00 → Sun 21:00 UTC, so EURUSD will report `MARKET_CLOSED` over a weekend. That's normal, not a bug.

---

## 6. Demo day: hosted or local?

Honestly — **have both ready.**

| | Hosted (Render) | Local (your laptop) |
|---|---|---|
| Judges can open it themselves | ✅ | ❌ |
| Works if venue wifi is bad | ✅ (it's not on your machine) | ❌ |
| Survives your laptop sleeping | ✅ | ❌ |
| Cold start delay | 30–60 s if asleep | none |
| Full control if something breaks | ❌ | ✅ |
| Accumulates training data | ❌ (wiped) | ✅ |

**Recommended:** demo from the hosted URL, because a judge being able to open it themselves is worth a lot. Keep the local one running as a fallback — if Render is slow or the wifi dies, switch to `localhost` mid-sentence and keep going.

---

## 7. When it goes wrong

| Symptom | Cause | Fix |
|---|---|---|
| Build fails on `pip install` | Dependency won't build on Render's Python | Check `PYTHON_VERSION` is `3.11.9` in the environment |
| Deploy times out, "no open ports" | App not listening on `$PORT` | Start command must end `--port $PORT` |
| Health check fails | Wrong path | Must be `/api/v1/health` |
| Frontend calls all fail, CORS error | Frontend URL not allow-listed | Add it to `MS_CORS_ORIGINS`, save, wait for restart |
| WebSocket won't connect from a live site | Using `ws://` on an HTTPS page | Use `wss://` |
| First request takes a minute | Free instance was asleep | Expected. Open it early on demo day |
| `model_source: fallback_coefficients` | `model.json` not committed | Train locally, commit the file, push |
| `/health` shows 0 ticks after a redeploy | Ephemeral filesystem wiped the database | Expected on free tier. Record locally |

**Reading logs:** Render dashboard → your service → **Logs**. It's the same output you see in your terminal locally. If something breaks, the answer is almost always in there.
