// ============================================================
// MarketSpike frontend — vanilla JS, no build step
// ============================================================

const $ = (sel) => document.querySelector(sel);
const apiBase = () => $("#apiBase").value.replace(/\/$/, "");
const symbol  = () => $("#symbolPicker").value;

// ---------- Instruments picker ----------
async function loadInstruments() {
  try {
    const r = await fetch(`${apiBase()}/instruments`);
    const data = await r.json();
    const sel = $("#symbolPicker");
    sel.innerHTML = "";
    (data.instruments || data || []).forEach((inst) => {
      const sym = typeof inst === "string" ? inst : inst.symbol;
      const opt = document.createElement("option");
      opt.value = sym; opt.textContent = sym;
      sel.appendChild(opt);
    });
    if (!sel.value) sel.value = "BTCUSDT";
    refreshAll();
  } catch (e) {
    $("#symbolPicker").innerHTML = `<option>error loading</option>`;
    console.error(e);
  }
}

// ---------- Position sizing ----------
$("#sizeForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const payload = {
    symbol: symbol(),
    account_balance_minor: Number(fd.get("account_balance_minor")),
    free_margin_minor:     Number(fd.get("free_margin_minor")),
    risk_pct:              Number(fd.get("risk_pct")),
    stop_distance_price:   Number(fd.get("stop_distance_price")),
  };
  const box = $("#sizeResult");
  box.classList.remove("hidden");
  box.innerHTML = `<p class="muted">Calculating…</p>`;

  try {
    const r = await fetch(`${apiBase()}/api/v1/size`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    renderSize(d);
  } catch (err) {
    box.innerHTML = `<p style="color:var(--danger)">Error: ${err.message}</p>`;
  }
});

function renderSize(d) {
  const box = $("#sizeResult");
  const modelSrc = d.model_source || "unknown";
  const badge = modelSrc === "trained"
    ? `<span class="source-badge source-trained">model: trained</span>`
    : `<span class="source-badge source-fallback">model: ${modelSrc}</span>`;

  const overexposure = d.overexposure_pct ?? 0;
  const stale = d.stale_quote ? `<span class="source-badge source-fallback">stale quote</span>` : "";

  box.innerHTML = `
    <div style="display:flex;align-items:center;gap:10px">
      <strong>Result</strong>${badge}${stale}
      ${d.fx_assumed ? `<span class="source-badge source-fallback">fx assumed</span>` : ""}
    </div>
    <div class="result-grid">
      <div class="metric">
        <div class="label">Recommended lots</div>
        <div class="value">${d.lots}</div>
      </div>
      <div class="metric">
        <div class="label">Adverse price (incl. slippage)</div>
        <div class="value">${Number(d.adverse_price).toFixed(4)}</div>
      </div>
      <div class="metric">
        <div class="label">Actual risk (minor)</div>
        <div class="value">${d.actual_risk_amount_minor}</div>
      </div>
      <div class="metric highlight">
        <div class="label">Conventional calculator overexposure</div>
        <div class="value">${overexposure.toFixed(2)}%</div>
      </div>
    </div>
    <details style="margin-top:14px"><summary class="muted" style="cursor:pointer">Raw JSON</summary>
      <pre class="json">${JSON.stringify(d, null, 2)}</pre>
    </details>
  `;
}

// ---------- Regime ----------
async function loadRegime() {
  try {
    const r = await fetch(`${apiBase()}/api/v1/regime?symbol=${symbol()}`);
    const d = await r.json();
    const el = $("#regimeState");
    el.textContent = d.state || "—";
    el.className = "regime-state " + (d.state || "");
    $("#regimeScore").textContent   = (d.score ?? 0).toFixed(2);
    $("#regimeV").textContent       = (d.v_ratio ?? 0).toFixed(3);
    $("#regimeZ").textContent       = (d.spread_z ?? 0).toFixed(2);
    $("#regimeTrigger").textContent = d.trigger || "—";
  } catch (e) { console.error(e); }
}

// ---------- Latency ----------
async function loadLatency() {
  try {
    const r = await fetch(`${apiBase()}/api/v1/latency/summary?symbol=${symbol()}`);
    const d = await r.json();
    const tbody = $("#latencyTable tbody");
    tbody.innerHTML = "";
    for (const [hop, m] of Object.entries(d)) {
      if (typeof m !== "object") continue;
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${hop.replace(/_/g, " ")}</td>
        <td class="num">${m.p50 ?? "—"}</td>
        <td class="num" style="color:var(--warn)">${m.p95 ?? "—"}</td>
        <td class="num">${m.p99 ?? "—"}</td>
        <td><span class="source-badge source-${m.source === 'measured' ? 'trained' : 'fallback'}">${m.source || "?"}</span></td>
      `;
      tbody.appendChild(row);
    }
  } catch (e) { console.error(e); }
}

// ---------- Health + Model card ----------
async function loadHealth() {
  try {
    const [h, m] = await Promise.all([
      fetch(`${apiBase()}/health`).then((r) => r.json()),
      fetch(`${apiBase()}/api/v1/model/card`).then((r) => r.json()),
    ]);
    $("#healthJson").textContent = JSON.stringify(h, null, 2);
    $("#modelJson").textContent  = JSON.stringify(m, null, 2);
  } catch (e) { console.error(e); }
}

// ---------- Refresh all ----------
async function refreshAll() {
  await Promise.all([loadRegime(), loadLatency(), loadHealth()]);
}

// ---------- WebSocket live stream ----------
let ws = null;
function connectWs() {
  const wsUrl = apiBase().replace(/^http/, "ws") + "/ws/v1/stream";
  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    const badge = $("#wsStatus");
    badge.textContent = "ws: live";
    badge.className = "badge badge-online";
    // Subscribe to channels we care about
    ws.send(JSON.stringify({ action: "subscribe", channels: ["tick", "regime", "latency", "event", "market"] }));
  };

  ws.onmessage = (ev) => {
    let frame;
    try { frame = JSON.parse(ev.data); } catch { return; }
    handleFrame(frame);
  };

  ws.onclose = () => {
    const badge = $("#wsStatus");
    badge.textContent = "ws: offline";
    badge.className = "badge badge-offline";
    setTimeout(connectWs, 3000); // reconnect
  };

  ws.onerror = () => ws.close();
}

function handleFrame(f) {
  switch (f.type) {
    case "tick": {
      const log = $("#tickLog");
      const div = document.createElement("div");
      div.className = "tick";
      const ts = new Date(Number(f.server_ts_ns || 0) / 1e6).toISOString().slice(11, 23);
      div.innerHTML = `<span class="ts">${ts}</span> ${f.symbol} bid=<span class="bid">${f.bid}</span> ask=<span class="ask">${f.ask}</span> <span class="muted">[${f.source || "?"}]</span>`;
      log.prepend(div);
      while (log.children.length > 50) log.removeChild(log.lastChild);
      break;
    }
    case "regime_change": {
      // Live-update the regime box without polling
      const el = $("#regimeState");
      el.textContent = f.state;
      el.className = "regime-state " + f.state;
      if (f.score != null) $("#regimeScore").textContent = f.score.toFixed(2);
      break;
    }
    case "event_alert": {
      // Could render a toast; for now log it
      console.log("event_alert", f);
      break;
    }
  }
}

// ---------- Symbol change ----------
$("#symbolPicker").addEventListener("change", refreshAll);
$("#apiBase").addEventListener("change", () => { loadInstruments(); connectWs(); });

// ---------- Boot ----------
loadInstruments();
connectWs();
setInterval(refreshAll, 10000); // REST poll as a safety net