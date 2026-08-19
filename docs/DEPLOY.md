# Deploying MarketSpike to Render

> **Already deployed:** https://marketspike.onrender.com — the instrument panel is at the root; the backend serves it.
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
  with it. The keep-alive workflow below handles that, but it only keeps the instance
  *awake* — it cannot make the database survive the next deploy.

To retrieve what it captured, run the capture script against the live service rather than
a local file — or simply train locally, where the data is yours to keep.

### Free instances go to sleep — unless something keeps pinging them

After about 15 minutes with no incoming web requests, Render spins a free service down. The next visitor's request wakes it, but that first request takes **30–60 seconds** while it boots.

**Why this matters:** a sleeping service means a cold start *and* a cold engine. The volatility horizon needs roughly 150 seconds after boot before `v_ratio` is trustworthy, and the recorder loses everything it had collected. A judge who opens the URL and waits 40 seconds for a blank page has already formed an opinion.

**What is in place:** [`.github/workflows/keepalive.yml`](../.github/workflows/keepalive.yml) pings `/api/v1/health` every 10 minutes from GitHub Actions, so the idle timer never reaches 15. Nothing to configure — it runs from the repo. Check it under the repo's **Actions** tab; each run prints the health payload it got back.

Three honest caveats:

- **GitHub's cron is best-effort.** Scheduled runs are queued at low priority and can fire several minutes late. 10 minutes was chosen to absorb that; it is not a guarantee.
- **GitHub disables scheduled workflows after 60 days of repo inactivity.** It emails you first. Re-enable from the Actions tab.
- **The free tier gives 750 instance-hours a month.** Awake around the clock is about 730, so this fits only while MarketSpike is the *only* free service on the account. A second one and both start getting suspended near month end.

If the service genuinely must not sleep — a graded demo, anything with a deadline — pay for the Starter instance for that month. It never spins down, has a persistent disk, and removes this entire section's worth of caveats. Keep the ping job anyway; it costs nothing and doubles as an uptime alarm, since a failing run means the service stopped answering.

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

## 4. The frontend

**The backend serves it.** The instrument panel is [`index.html`](../index.html) at the repo root, and `marketspike/main.py` serves it at `/`:

```python
REPO_ROOT = Path(__file__).resolve().parent.parent
INDEX_HTML = REPO_ROOT / "index.html"

@app.get("/", include_in_schema=False)
async def index(request: Request):
    page = INDEX_HTML.read_text(encoding="utf-8")
    page = page.replace(PANEL_DEFAULT_API_BASE, _request_origin(request))
    return HTMLResponse(page, headers={"Cache-Control": "no-store"})
```

So there is no second service and no second URL:

| URL | What it is |
|---|---|
| `https://marketspike.onrender.com/` | the instrument panel |
| `https://marketspike.onrender.com/api/v1/...` | the API the panel calls |
| `https://marketspike.onrender.com/docs` | interactive API reference |

There is deliberately **no `StaticFiles` mount.** The page is self-contained — inline CSS, inline script, fonts from Google's CDN — so it has no sibling assets to serve, and the one-line version (`StaticFiles(directory=REPO_ROOT)`) would publish the whole repository over HTTP, `.git/` included. Add a mount against a dedicated assets directory if the page ever grows one.

### Pointing the page at a backend — nothing to do

`index.html` hardcodes its API base as `http://localhost:8000`, in the `api` input's `value` attribute and again as the script's initial `state.apiBase`, and it connects on load. That default is right locally and wrong everywhere else: served from Render it aims the visitor's browser at the visitor's *own* machine, fails, and drops into preview mode — which draws the placeholder figures baked into the file rather than live ones.

So the backend **rewrites that default to the origin the request arrived on** as it serves the page. Open `http://localhost:8000/` and the page gets `http://localhost:8000`; open `https://marketspike.onrender.com/` and it gets `https://marketspike.onrender.com`. Same origin either way, which is also why CORS never engages. Nothing to type, nothing per-visitor, and the same behaviour on a custom domain or a preview deploy without anyone editing a URL.

Two details that matter if you touch this:

- **The scheme comes from `X-Forwarded-Proto`, not from `request.base_url`.** Render terminates TLS and forwards over plain HTTP, and uvicorn only trusts forwarded headers from `--forwarded-allow-ips` (default `127.0.0.1`), which Render's proxy is not. `base_url` therefore reports `http` on an HTTPS deployment — and an `http://` base on an `https://` page is blocked as mixed content, with `ws://` instead of `wss://` blocked too.
- **The rewrite is a literal string substitution**, so it would fail *silently* if the frontend's default ever changed. `tests/test_frontend.py` asserts the marker still appears in `index.html` exactly twice, so that turns into a failing test naming `PANEL_DEFAULT_API_BASE` instead of a panel quietly serving placeholder numbers.

`index.html` itself is not modified — on disk it stays exactly as the frontend author wrote it, and only the copy going out over the wire differs, by those two strings. The `api` box still accepts a hand-typed address for pointing a local page at the hosted backend.

**What the failure looks like**, if it ever regresses: a panel showing numbers with `estimated` / `simulated` badges is not connected. The status chip in the top right reads `preview mode` rather than `live`.

### CORS — why it no longer bites

A browser refuses to let a page on one domain call an API on another unless the API allows it. Because the page is served by this service and defaults to the origin it was served from, page and API are always the **same origin**, so that rule never engages: no preflight, no allow-list, nothing to forget.

`MS_CORS_ORIGINS` still exists and still matters in exactly one case — a page served from somewhere *other* than this service (a Vercel deploy, a Vite dev server, a teammate's static host) calling this API. Then add that page's origin:

1. Render dashboard → your service → **Environment**
2. Edit `MS_CORS_ORIGINS`, comma-separated, no spaces:
   ```
   http://localhost:5173,https://marketspike-ui.vercel.app
   ```
3. Save. Render restarts automatically.

Symptom if you forget: the page loads but every API call fails, and the console says "blocked by CORS policy". The backend is fine — it's the allow-list.

### If you do host the page separately

The two things that catch people: use `https://` for the API, and **`wss://`**, not `ws://`, for the stream. Render serves HTTPS, and a secure page cannot open an insecure WebSocket — browsers block it silently apart from a console error, which is a horrible thing to debug at midnight.

```js
const API = "https://marketspike.onrender.com";
const WS  = "wss://marketspike.onrender.com/ws/v1/stream";
```

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
| First request takes a minute | Free instance was asleep | Check the keep-alive workflow is enabled under the repo's Actions tab |
| `/` returns 404 | Deployed before the `/` route landed, or `index.html` missing from the repo | Confirm `index.html` is committed at the repo root, then redeploy |
| Panel loads but shows "preview mode" | The page could not reach `/api/v1/health` on its own origin | Check the `api` box: if it says `localhost:8000` on the deployed URL the rewrite broke — run `pytest tests/test_frontend.py`. Otherwise open `<url>/api/v1/health` directly; a slow answer means the instance was asleep, so reload |
| `model_source: fallback_coefficients` | `model.json` not committed | Train locally, commit the file, push |
| `/health` shows 0 ticks after a redeploy | Ephemeral filesystem wiped the database | Expected on free tier. Record locally |

**Reading logs:** Render dashboard → your service → **Logs**. It's the same output you see in your terminal locally. If something breaks, the answer is almost always in there.
