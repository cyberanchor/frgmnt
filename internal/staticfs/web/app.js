/* ============================================================================
 * fragment_checker — web panel client
 * Vanilla ES2020. No framework. All state held in a few maps + the log
 * EventSource; tiles are polled. The code is intentionally flat and short so
 * it is easy to audit/extend.
 * ========================================================================== */
"use strict";

/* -------------------------------------------------------------------------- */
/*  Tiny helpers                                                              */
/* -------------------------------------------------------------------------- */

const $  = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));

const STATUSES = ["free", "reserved", "auction", "available", "taken", "sold", "unknown", "error"];
const TILE_STAT_KEYS = ["free", "reserved", "auction", "available", "taken", "sold", "unknown", "errors"];
const STAT_LABEL = {
    free: "Free", reserved: "Reserved", auction: "Auction", available: "Avail",
    taken: "Taken", sold: "Sold", unknown: "Unknown", errors: "Errors",
};
// "free" only means that Fragment has no page for the name; a regular
// account may hold it (@telegram comes back free).
const FREE_HINT = "Not on Fragment: no page, not a collectible, not for sale. " +
    "A regular Telegram account may still hold the name.";

function escapeHTML(s) {
    return String(s ?? "")
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function fmtTime(t) {
    try {
        const d = new Date(t);
        return d.toLocaleTimeString([], { hour12: false }) +
               "." + String(d.getMilliseconds()).padStart(3, "0");
    } catch { return ""; }
}

function pct(n, total) {
    if (!total || total <= 0) return 0;
    return Math.max(0, Math.min(100, (n / total) * 100));
}

async function api(method, url, body) {
    const opts = { method, headers: {} };
    if (body !== undefined) {
        if (body instanceof FormData) {
            opts.body = body;
        } else {
            opts.headers["Content-Type"] = "application/json";
            opts.body = JSON.stringify(body);
        }
    }
    const res = await fetch(url, opts);
    if (res.status === 401) {
        // No valid access token (expired, or the key was rotated): log in again.
        location.href = "/login";
        throw new Error("access token required");
    }
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = text; }
    if (!res.ok) {
        const msg = data?.error || res.statusText || `HTTP ${res.status}`;
        throw new Error(msg);
    }
    return data;
}

function toast(msg, kind = "ok", ttl = 3500) {
    const box = $("#toasts");
    const el = document.createElement("div");
    el.className = `toast ${kind}`;
    el.textContent = msg;
    box.appendChild(el);
    setTimeout(() => {
        el.style.opacity = "0";
        el.style.transform = "translateY(8px)";
        setTimeout(() => el.remove(), 200);
    }, ttl);
}

/* -------------------------------------------------------------------------- */
/*  Global state                                                              */
/* -------------------------------------------------------------------------- */

const state = {
    workers: new Map(),       // id -> worker view
    detailsResults: new Map(),// id -> last seen result id (for details modal)
    detailsCounts: new Map(), // id -> {status: count}
    detailsFilter: "all",     // active status chip in the details modal
    settings: null,
    logsTopic: "",
    logsPaused: false,
    notifyOnComplete: true,   // window.Notification("completed")
    titleBase: document.title,
};

/* -------------------------------------------------------------------------- */
/*  Theme                                                                     */
/* -------------------------------------------------------------------------- */

// Theme rules:
//   - On boot we trust localStorage ("fc.theme") so the page paints with the
//     user's choice immediately, before /api/settings is fetched.
//   - The first /api/settings response only seeds the theme if there is no
//     local override yet. After that, the user is authoritative: pressing
//     the toggle stores the new value locally and the server is updated
//     when they press "Save" on the Settings tab.
// localStorage throws when the browser blocks site storage (private mode,
// blocked cookies). The theme is only a convenience, so treat that as "no
// saved theme": the toggle used to throw and loadSettings showed an error
// instead of applying the server theme.
function storageGet(key) {
    try { return localStorage.getItem(key); } catch { return null; }
}
function storageSet(key, value) {
    try { localStorage.setItem(key, value); } catch {}
}

function applyTheme(theme, source = "user") {
    const t = theme === "light" ? "light" : "dark";
    document.documentElement.dataset.theme = t;
    if (source === "user") storageSet("fc.theme", t);
}

$("#logout").addEventListener("click", async () => {
    try { await fetch("/logout", { method: "POST" }); } catch {}
    location.href = "/login";
});

$("#theme-toggle").addEventListener("click", () => {
    const cur = document.documentElement.dataset.theme;
    applyTheme(cur === "dark" ? "light" : "dark", "user");
});

// A theme saved in this browser wins. Without one the page starts dark and
// loadSettings() applies the theme from settings.json. Boot used to store
// "dark" in localStorage right away, so the server theme never applied.
const savedTheme = storageGet("fc.theme");
applyTheme(savedTheme || "dark", savedTheme ? "user" : "server");

/* -------------------------------------------------------------------------- */
/*  Tabs                                                                      */
/* -------------------------------------------------------------------------- */

$$(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
        $$(".tab").forEach((b) => b.classList.toggle("active", b === btn));
        const tab = btn.dataset.tab;
        $$(".panel").forEach((p) => p.classList.toggle("active", p.id === "tab-" + tab));
    });
});

/* -------------------------------------------------------------------------- */
/*  Health poll                                                               */
/* -------------------------------------------------------------------------- */

async function pollHealth() {
    try {
        await api("GET", "/api/health");
        $("#health-dot").classList.remove("bad");
        $("#health-dot").classList.add("ok");
    } catch {
        $("#health-dot").classList.remove("ok");
        $("#health-dot").classList.add("bad");
    }
}
pollHealth();
setInterval(pollHealth, 5000);

/* -------------------------------------------------------------------------- */
/*  Workers                                                                   */
/* -------------------------------------------------------------------------- */

// loadWorkers fetches every worker and redraws the tiles. pollWorkers()
// runs it once a second; quiet skips the error toast for that refresh.
async function loadWorkers(quiet = false) {
    let workers;
    try {
        workers = await api("GET", "/api/workers") || [];
    } catch (e) {
        if (!quiet) toast("Failed to load workers: " + e.message, "error");
        return;
    }
    const prevStatus = new Map();
    for (const [id, w] of state.workers) prevStatus.set(id, w.status);
    const listChanged = workers.length !== state.workers.size ||
        workers.some((w) => state.workers.get(w.id)?.name !== w.name);
    state.workers.clear();
    for (const w of workers) state.workers.set(w.id, w);
    renderWorkers();
    if (listChanged) refreshLogsTopicFilter();
    refreshTitle();
    // Notification on transition: running -> done / error.
    for (const w of workers) {
        const before = prevStatus.get(w.id);
        if (before === "running" && (w.status === "done" || w.status === "error")) {
            notifyComplete(w);
        }
    }
}

function renderWorkers() {
    const grid = $("#workers-grid");
    const empty = $("#workers-empty");
    if (state.workers.size === 0) {
        grid.innerHTML = "";
        empty.classList.remove("hidden");
        return;
    }
    empty.classList.add("hidden");

    const ids = Array.from(state.workers.keys());
    // Remove tiles whose worker is gone.
    $$(".tile", grid).forEach((t) => {
        if (!ids.includes(t.dataset.id)) t.remove();
    });
    // Upsert each known worker.
    for (const id of ids) {
        const existing = grid.querySelector(`.tile[data-id="${id}"]`);
        if (existing) updateTile(existing, state.workers.get(id));
        else grid.appendChild(buildTile(state.workers.get(id)));
    }
}

function buildTile(w) {
    const el = document.createElement("article");
    el.className = "tile";
    el.dataset.id = w.id;
    el.innerHTML = `
        <header class="tile-head">
            <div>
                <div class="tile-title"></div>
                <div class="tile-id"></div>
            </div>
            <span class="tile-status"></span>
        </header>
        <div class="tile-progress"><span></span></div>
        <div class="tile-meta">
            <span class="meta-progress"></span>
            <span class="meta-config"></span>
        </div>
        <div class="tile-stats">
            ${TILE_STAT_KEYS.map(k => `
                <div class="stat ${k}"${k === "free" ? ` title="${FREE_HINT}"` : ""}>
                    <div class="stat-n" data-k="${k}">0</div>
                    <div class="stat-l">${STAT_LABEL[k]}</div>
                </div>
            `).join("")}
        </div>
        <div class="tile-actions">
            <button class="btn primary act-start" title="Run from scratch — clears previous results">Start</button>
            <button class="btn act-resume" title="Continue from the first unchecked name; keeps previous results">Resume</button>
            <button class="btn act-stop">Stop</button>
            <button class="btn act-details">Input / Results</button>
            <button class="btn act-edit">Edit</button>
            <button class="btn danger act-delete">Delete</button>
        </div>
    `;
    wireTileActions(el);
    updateTile(el, w);
    return el;
}

// resultCount returns how many results the worker has stored.
async function resultCount(id) {
    const data = await api("GET", `/api/workers/${id}/results?limit=1`);
    return Object.values((data && data.counts) || {}).reduce((a, b) => a + b, 0);
}

function wireTileActions(el) {
    const id = el.dataset.id;
    el.querySelector(".act-start").addEventListener("click", async () => {
        try {
            // A fresh run deletes the worker's results first; Start sits next
            // to Resume, so ask before a click wipes them.
            const n = await resultCount(id);
            const name = state.workers.get(id)?.name || id;
            if (n > 0 && !confirm(`Start "${name}" from scratch?\n\nThis deletes its ${n} results. ` +
                "Press Cancel and use Resume to continue instead.")) return;
            await api("POST", `/api/workers/${id}/start`);
            toast("Worker started (fresh run)", "ok");
        } catch (e) { toast(e.message, "error"); }
    });
    el.querySelector(".act-resume").addEventListener("click", async () => {
        try {
            await api("POST", `/api/workers/${id}/start?resume=true`);
            toast("Worker resumed", "ok");
        } catch (e) { toast(e.message, "error"); }
    });
    el.querySelector(".act-stop").addEventListener("click", async () => {
        try {
            await api("POST", `/api/workers/${id}/stop`);
            toast("Worker stopped", "warn");
        } catch (e) { toast(e.message, "error"); }
    });
    el.querySelector(".act-details").addEventListener("click", () => openDetails(id));
    el.querySelector(".act-edit").addEventListener("click", () => openWorkerModal(state.workers.get(id)));
    el.querySelector(".act-delete").addEventListener("click", async () => {
        if (!confirm(`Delete worker "${state.workers.get(id)?.name || id}"? This removes its results.`)) return;
        try {
            await api("DELETE", `/api/workers/${id}`);
            toast("Worker deleted", "ok");
            await loadWorkers();
        } catch (e) { toast(e.message, "error"); }
    });
}

function updateTile(el, w) {
    el.querySelector(".tile-title").textContent = w.name;
    el.querySelector(".tile-id").textContent = "#" + w.id.slice(0, 8);
    const status = el.querySelector(".tile-status");
    status.textContent = w.status;
    status.dataset.s = w.status;

    const checked = (w.stats && w.stats.checked) || 0;
    const total = w.total || (w.stats && w.stats.total) || 0;
    el.querySelector(".tile-progress > span").style.width = pct(checked, total) + "%";
    el.querySelector(".meta-progress").textContent =
        `${checked}/${total}` + (total > 0 ? ` · ${pct(checked, total).toFixed(0)}%` : "");
    const cfg = w.config || {};
    el.querySelector(".meta-config").textContent =
        `w=${cfg.workers ?? "-"} · c=${cfg.max_conns ?? "-"} · rps=${cfg.rate_rps ?? 0}`;

    const stats = w.stats || {};
    for (const k of TILE_STAT_KEYS) {
        el.querySelector(`.stat-n[data-k="${k}"]`).textContent = stats[k] ?? 0;
    }

    // Button states by lifecycle.
    const running = w.status === "running";
    el.querySelector(".act-start").disabled = running;
    el.querySelector(".act-resume").disabled = running;
    el.querySelector(".act-stop").disabled = !running;
    el.querySelector(".act-delete").disabled = running;
    el.querySelector(".act-edit").disabled = running;
}

/* -------------------------------------------------------------------------- */
/*  Bulk actions                                                              */
/* -------------------------------------------------------------------------- */

$("#bulk-start").addEventListener("click", async () => {
    try {
        const res = await api("POST", "/api/workers-bulk/start");
        toast(`Started ${res.started}, skipped ${res.skipped}`, res.started > 0 ? "ok" : "warn");
        await loadWorkers();
    } catch (e) { toast(e.message, "error"); }
});

$("#bulk-stop").addEventListener("click", async () => {
    try {
        const res = await api("POST", "/api/workers-bulk/stop");
        toast(`Stopped ${res.stopped} worker(s)`, res.stopped > 0 ? "warn" : "ok");
        await loadWorkers();
    } catch (e) { toast(e.message, "error"); }
});

/* -------------------------------------------------------------------------- */
/*  Document title progress badge                                             */
/* -------------------------------------------------------------------------- */

function refreshTitle() {
    let running = 0;
    let checked = 0;
    let total = 0;
    for (const w of state.workers.values()) {
        if (w.status === "running") running++;
        if (w.stats) {
            checked += w.stats.checked || 0;
            total   += w.total || w.stats.total || 0;
        }
    }
    if (running > 0) {
        const p = total > 0 ? Math.round((checked / total) * 100) : 0;
        document.title = `▶ ${running} · ${p}% — ${state.titleBase}`;
    } else {
        document.title = state.titleBase;
    }
}

/* -------------------------------------------------------------------------- */
/*  Live tile updates                                                         */
/* -------------------------------------------------------------------------- */

// Tiles are refreshed by polling GET /api/workers once a second. The panel
// used to open one EventSource per worker, but a browser allows only six
// HTTP/1.1 connections per host: with five or more workers (plus the log
// stream) the streams took every connection and the panel's own requests
// (buttons, settings, results) hung. The per-worker stream also appended
// result rows that the 2 s results poll added again (duplicates at debug).
let workersPollBusy = false;
async function pollWorkers() {
    if (workersPollBusy) return;
    workersPollBusy = true;
    try {
        await loadWorkers(true);
    } finally {
        workersPollBusy = false;
    }
}

/* -------------------------------------------------------------------------- */
/*  Worker editor modal                                                       */
/* -------------------------------------------------------------------------- */

const workerModal = $("#worker-modal");
$("#add-worker").addEventListener("click", () => openWorkerModal(null));
$("#add-worker-empty").addEventListener("click", () => openWorkerModal(null));
$$("[data-close]", workerModal).forEach((b) => b.addEventListener("click", () => workerModal.setAttribute("hidden", "")));

function openWorkerModal(w) {
    const title = w ? `Edit worker — ${w.name}` : "New worker";
    $("#worker-modal-title").textContent = title;
    $("#worker-form-id").value = w ? w.id : "";
    $("#worker-name").value = w ? w.name : suggestWorkerName();

    const def = defaultConfig();
    const cfg = (w && w.config) || def;
    $("#worker-workers").value = cfg.workers ?? def.workers;
    $("#worker-conns").value = cfg.max_conns ?? def.max_conns;
    $("#worker-rate").value = cfg.rate_rps ?? 0;
    $("#worker-retries").value = cfg.retries ?? def.retries;
    $("#worker-retry-delay").value = nsToMs(cfg.retry_delay_ns ?? def.retry_delay_ns ?? 500_000_000);
    $("#worker-timeout").value = nsToSec(cfg.timeout_ns ?? def.timeout_ns ?? 15_000_000_000);
    $("#worker-ua").value = cfg.user_agent ?? "";

    workerModal.removeAttribute("hidden");
    setTimeout(() => $("#worker-name").focus(), 30);
}

function suggestWorkerName() {
    const i = state.workers.size + 1;
    return `worker-${i}`;
}

function defaultConfig() {
    const s = state.settings || {};
    return {
        workers: s.default_workers || 50,
        max_conns: 8,
        rate_rps: s.default_rate_rps || 0,
        retries: s.default_retries ?? 2,
        retry_delay_ns: 500_000_000,
        rl_wait_max_ns: 30_000_000_000,
        timeout_ns: (s.default_timeout_s || 15) * 1_000_000_000,
        read_limit: 16384,
        user_agent: "",
        insecure_tls: false,
    };
}

function nsToMs(ns) { return Math.round(Number(ns) / 1_000_000); }
function nsToSec(ns) { return Math.round(Number(ns) / 1_000_000_000); }

$("#worker-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const id = $("#worker-form-id").value;
    // The form shows only some fields. Start from the worker's current
    // config so the others (read_limit, insecure_tls, rl_wait_max, cache
    // settings) keep their values; a new worker gets them from the server.
    // Saving used to reset them to 16384 / false / 30 s.
    const cfg = id ? { ...(state.workers.get(id)?.config || {}) } : {};
    Object.assign(cfg, {
        workers: +$("#worker-workers").value || 1,
        max_conns: +$("#worker-conns").value || 1,
        rate_rps: +$("#worker-rate").value || 0,
        retries: +$("#worker-retries").value || 0,
        retry_delay_ns: (+$("#worker-retry-delay").value || 500) * 1_000_000,
        timeout_ns: (+$("#worker-timeout").value || 15) * 1_000_000_000,
        user_agent: $("#worker-ua").value.trim(),
    });
    const body = { name: $("#worker-name").value.trim(), config: cfg };
    try {
        if (id) await api("PATCH", `/api/workers/${id}`, body);
        else await api("POST", "/api/workers", body);
        workerModal.setAttribute("hidden", "");
        toast(id ? "Worker updated" : "Worker created", "ok");
        await loadWorkers();
    } catch (e) {
        toast(e.message, "error");
    }
});

/* -------------------------------------------------------------------------- */
/*  Details modal: input + results                                            */
/* -------------------------------------------------------------------------- */

const detailsModal = $("#details-modal");
$$("[data-close]", detailsModal).forEach((b) => b.addEventListener("click", () => detailsModal.setAttribute("hidden", "")));
let detailsCurrentId = null;

$$("#details-filters .chip").forEach((c) => {
    c.addEventListener("click", () => {
        $$("#details-filters .chip").forEach((x) => x.classList.toggle("active", x === c));
        state.detailsFilter = c.dataset.filter;
        applyDetailsFilter();
    });
});

function applyDetailsFilter() {
    const f = state.detailsFilter;
    const list = $("#details-results");
    for (const row of list.children) {
        if (f === "all" || row.dataset.s === f) row.style.display = "";
        else row.style.display = "none";
    }
}

async function openDetails(id) {
    detailsCurrentId = id;
    const w = state.workers.get(id);
    if (!w) return;
    $("#details-title").textContent = `Worker — ${w.name}`;
    $("#details-textarea").value = "";
    $("#details-input-info").textContent = "";
    $("#details-results").innerHTML = "";
    state.detailsResults.set(id, 0);
    state.detailsCounts.set(id, {});
    $("#details-counts").textContent = "";

    state.detailsFilter = "all";
    $$("#details-filters .chip").forEach((c) =>
        c.classList.toggle("active", c.dataset.filter === "all"));

    $("#details-download").href = `/api/workers/${id}/results/download`;

    detailsModal.removeAttribute("hidden");
    await loadDetailsResults(id);
}

async function loadDetailsResults(id) {
    try {
        const data = await api("GET", `/api/workers/${id}/results?limit=500&after=${state.detailsResults.get(id) || 0}`);
        const counts = data.counts || {};
        state.detailsCounts.set(id, counts);
        renderDetailsCounts(id);
        const rs = data.results || [];
        if (rs.length === 0) return;
        const last = rs[rs.length - 1].id;
        state.detailsResults.set(id, last);
        const list = $("#details-results");
        const frag = document.createDocumentFragment();
        for (const r of rs) frag.appendChild(buildResultRow(r));
        list.appendChild(frag);
        // Cap visible rows to avoid heavy DOMs.
        const limit = (state.settings && state.settings.history_limit_max) || 500;
        while (list.children.length > limit) list.removeChild(list.firstChild);
        list.scrollTop = list.scrollHeight;
    } catch (e) {
        toast("Load results: " + e.message, "error");
    }
}

function renderDetailsCounts(id) {
    const c = state.detailsCounts.get(id) || {};
    const total = Object.values(c).reduce((a, b) => a + b, 0);
    if (total === 0) {
        $("#details-counts").textContent = "";
        return;
    }
    const parts = STATUSES.map((s) => `${s}=${c[s] || 0}`).filter((s) => !s.endsWith("=0"));
    $("#details-counts").textContent = `(${total} total · ${parts.join(", ")})`;
}

function buildResultRow(r) {
    const div = document.createElement("div");
    div.className = "result-row";
    div.dataset.s = r.status;
    const conf = r.confidence ? ` <span class="muted">${r.confidence}%</span>` : "";
    const cache = r.cached ? ` <span class="muted" title="served from page cache">cache</span>` : "";
    const evidence = r.evidence ? ` title="${escapeHTML(r.evidence)}"` : "";
    div.innerHTML = `
        <span class="rname"${evidence}>@${escapeHTML(r.username)}${cache}${r.error ? ` <span class="muted">${escapeHTML(r.error)}</span>` : ""}</span>
        <span class="rstatus">${r.status}${conf}</span>
    `;
    if (state.detailsFilter !== "all" && state.detailsFilter !== r.status) {
        div.style.display = "none";
    }
    return div;
}

/* -------------------------------------------------------------------------- */
/*  Browser notification on worker completion                                 */
/* -------------------------------------------------------------------------- */

function notifyComplete(w) {
    if (!state.notifyOnComplete) return;
    if (typeof Notification === "undefined") return;
    if (Notification.permission !== "granted") return;
    const s = w.stats || {};
    const total = s.total || w.total || 0;
    const checked = s.checked || 0;
    const verb = w.status === "error" ? "errored" : "completed";
    const body = `${checked}/${total} · free=${s.free||0} taken=${s.taken||0} sold=${s.sold||0} err=${s.errors||0}`;
    try {
        new Notification(`Worker ${w.name} ${verb}`, { body, silent: false });
    } catch {}
}

function maybeRequestNotificationPermission() {
    if (typeof Notification === "undefined") return;
    if (Notification.permission === "default") {
        // Best-effort; some browsers require a user gesture.
        try { Notification.requestPermission().catch(() => {}); } catch {}
    }
}

// confirmNewList asks before a new list replaces one that has results:
// the results of names that are not in the new list are deleted.
async function confirmNewList(id) {
    const n = await resultCount(id);
    return n === 0 || confirm(`This worker has ${n} results.\n\n` +
        "Results of names that are also in the new list are kept, the others are deleted " +
        "(download them first if you need them). Load the new list?");
}

$("#details-load-text").addEventListener("click", async () => {
    if (!detailsCurrentId) return;
    const text = $("#details-textarea").value;
    try {
        if (!(await confirmNewList(detailsCurrentId))) return;
        const res = await api("POST", `/api/workers/${detailsCurrentId}/input`, { text });
        $("#details-input-info").textContent =
            `Accepted ${res.accepted}, rejected ${res.rejected}`;
        toast(`Loaded ${res.accepted} names`, "ok");
        await loadWorkers();
    } catch (e) { toast(e.message, "error"); }
});

$("#details-file").addEventListener("change", async (ev) => {
    if (!detailsCurrentId) return;
    const f = ev.target.files[0];
    if (!f) return;
    const fd = new FormData();
    fd.append("file", f);
    try {
        if (!(await confirmNewList(detailsCurrentId))) return;
        const res = await api("POST", `/api/workers/${detailsCurrentId}/input/upload`, fd);
        $("#details-input-info").textContent =
            `Uploaded ${f.name}: accepted ${res.accepted}, rejected ${res.rejected}`;
        toast(`Uploaded ${res.accepted} names`, "ok");
        await loadWorkers();
    } catch (e) { toast(e.message, "error"); }
    finally { ev.target.value = ""; }
});

$("#details-clear").addEventListener("click", async () => {
    if (!detailsCurrentId) return;
    if (!confirm("Clear all results for this worker?")) return;
    try {
        await api("POST", `/api/workers/${detailsCurrentId}/clear`);
        $("#details-results").innerHTML = "";
        state.detailsCounts.set(detailsCurrentId, {});
        renderDetailsCounts(detailsCurrentId);
        toast("Results cleared", "ok");
    } catch (e) { toast(e.message, "error"); }
});

/* -------------------------------------------------------------------------- */
/*  Settings                                                                  */
/* -------------------------------------------------------------------------- */

async function loadSettings() {
    try {
        state.settings = await api("GET", "/api/settings");
        renderSettings();
        // Only seed the theme from the server when the user has not yet
        // expressed a local preference. After that, localStorage wins until
        // the user explicitly presses Save in the Settings tab.
        if (state.settings.theme && !storageGet("fc.theme")) {
            applyTheme(state.settings.theme, "server");
        }
    } catch (e) { toast("Load settings: " + e.message, "error"); }
}

function renderSettings() {
    const s = state.settings || {};
    $("#set-theme").value = s.theme || "dark";
    $("#set-log-level").value = s.log_level || "info";
    $("#set-default-workers").value = s.default_workers ?? 50;
    $("#set-default-rate").value = s.default_rate_rps ?? 0;
    $("#set-default-retries").value = s.default_retries ?? 2;
    $("#set-default-timeout").value = s.default_timeout_s ?? 15;
    $("#set-history-limit").value = s.history_limit_max ?? 500;
}

$("#settings-save").addEventListener("click", async () => {
    const body = {
        theme: $("#set-theme").value,
        log_level: $("#set-log-level").value,
        default_workers: +$("#set-default-workers").value || 50,
        default_rate_rps: +$("#set-default-rate").value || 0,
        default_retries: +$("#set-default-retries").value || 0,
        default_timeout_s: +$("#set-default-timeout").value || 15,
        history_limit_max: +$("#set-history-limit").value || 500,
    };
    try {
        state.settings = await api("PUT", "/api/settings", body);
        // User explicitly pressed Save — they want this theme persisted.
        applyTheme(state.settings.theme, "user");
        toast("Settings saved", "ok");
    } catch (e) { toast(e.message, "error"); }
});

$("#settings-reset").addEventListener("click", () => loadSettings());

/* -------------------------------------------------------------------------- */
/*  Logs                                                                      */
/* -------------------------------------------------------------------------- */

let logStream = null;

function refreshLogsTopicFilter() {
    const sel = $("#logs-topic-filter");
    const current = sel.value;
    sel.innerHTML = `<option value="">All topics</option>`;
    for (const w of state.workers.values()) {
        const opt = document.createElement("option");
        opt.value = w.id;
        opt.textContent = w.name + " (#" + w.id.slice(0, 6) + ")";
        sel.appendChild(opt);
    }
    sel.value = current;
}

async function loadRecentLogs() {
    try {
        const topic = state.logsTopic;
        const url = "/api/logs?limit=200" + (topic ? `&topic=${encodeURIComponent(topic)}` : "");
        const entries = await api("GET", url);
        const view = $("#logs-view");
        view.innerHTML = "";
        for (const e of entries) view.appendChild(buildLogEntry(e));
        view.scrollTop = view.scrollHeight;
    } catch (e) { toast("Logs: " + e.message, "error"); }
}

function openLogStream() {
    if (logStream) logStream.close();
    const topic = state.logsTopic;
    const url = "/api/logs/stream" + (topic ? `?topic=${encodeURIComponent(topic)}` : "");
    logStream = new EventSource(url);
    logStream.addEventListener("log", (e) => {
        if (state.logsPaused) return;
        let entry;
        try { entry = JSON.parse(e.data); } catch { return; }
        const view = $("#logs-view");
        view.appendChild(buildLogEntry(entry));
        while (view.children.length > 2000) view.removeChild(view.firstChild);
        view.scrollTop = view.scrollHeight;
    });
}

function buildLogEntry(e) {
    const div = document.createElement("div");
    div.className = "log-entry";
    div.innerHTML = `
        <span class="log-time">${escapeHTML(fmtTime(e.time))}</span>
        <span class="log-level ${escapeHTML(e.level)}">${escapeHTML(e.level)}</span>
        <span class="log-topic">${e.topic ? "#" + escapeHTML(e.topic.slice(0, 6)) : ""}</span>
        <span class="log-msg">${escapeHTML(e.message)}</span>
    `;
    return div;
}

$("#logs-topic-filter").addEventListener("change", (ev) => {
    state.logsTopic = ev.target.value;
    loadRecentLogs();
    openLogStream();
});
$("#logs-clear").addEventListener("click", () => { $("#logs-view").innerHTML = ""; });
$("#logs-pause").addEventListener("click", (ev) => {
    state.logsPaused = !state.logsPaused;
    ev.target.textContent = state.logsPaused ? "Resume" : "Pause";
});

/* -------------------------------------------------------------------------- */
/*  Debug console                                                             */
/* -------------------------------------------------------------------------- */

$("#console-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const raw = $("#console-input").value.trim();
    if (!raw) return;
    const out = $("#console-output");
    const requestEl = document.createElement("div");
    requestEl.className = "console-entry request";
    requestEl.textContent = "> " + raw;
    out.appendChild(requestEl);

    const parts = raw.match(/^(\S+)\s+(\S+)(?:\s+(.*))?$/);
    if (!parts) {
        const err = document.createElement("div");
        err.className = "console-entry fail";
        err.textContent = "Syntax: METHOD URL [JSON]";
        out.appendChild(err);
        return;
    }
    const [, method, url, jsonStr] = parts;
    let body;
    if (jsonStr) {
        try { body = JSON.parse(jsonStr); }
        catch (e) {
            const err = document.createElement("div");
            err.className = "console-entry fail";
            err.textContent = "Invalid JSON: " + e.message;
            out.appendChild(err);
            return;
        }
    }
    try {
        const res = await api(method.toUpperCase(), url, body);
        const ok = document.createElement("div");
        ok.className = "console-entry ok";
        ok.innerHTML = `200 OK<pre>${escapeHTML(JSON.stringify(res, null, 2))}</pre>`;
        out.appendChild(ok);
    } catch (e) {
        const err = document.createElement("div");
        err.className = "console-entry fail";
        err.textContent = "ERROR: " + e.message;
        out.appendChild(err);
    }
    out.scrollTop = out.scrollHeight;
    $("#console-input").value = "";
});

$("#console-clear").addEventListener("click", () => { $("#console-output").innerHTML = ""; });

/* -------------------------------------------------------------------------- */
/*  Boot                                                                      */
/* -------------------------------------------------------------------------- */

(async () => {
    await loadSettings();
    await loadWorkers();
    await loadRecentLogs();
    openLogStream();
    refreshTitle();
    maybeRequestNotificationPermission();
    // Live tile updates (see pollWorkers).
    setInterval(pollWorkers, 1000);
    setInterval(refreshTitle, 1500);
    // When details modal is open, pull incremental results too.
    setInterval(() => {
        if (!detailsCurrentId) return;
        if (detailsModal.hasAttribute("hidden")) return;
        loadDetailsResults(detailsCurrentId);
    }, 2000);
})();
