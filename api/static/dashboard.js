const POLL_MS = 4000;
let reportDayTouched = false;

function el(id) { return document.getElementById(id); }

// --- theme toggle (light/dark, on top of the automatic OS-preference default) ---
function getStoredTheme() {
    try { return localStorage.getItem("fleetiq-theme"); } catch (e) { return null; }
}
function setStoredTheme(theme) {
    try { localStorage.setItem("fleetiq-theme", theme); } catch (e) { /* ignore */ }
}
function effectiveTheme() {
    const saved = getStoredTheme();
    if (saved === "light" || saved === "dark") return saved;
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}
function syncThemeToggleButton() {
    const btn = el("theme-toggle");
    if (!btn) return;
    const isDark = effectiveTheme() === "dark";
    btn.innerHTML = isDark ? "&#9728; Light" : "&#127769; Dark";
    btn.setAttribute("aria-label", isDark ? "Switch to light theme" : "Switch to dark theme");
}
function initThemeToggle() {
    syncThemeToggleButton();
    el("theme-toggle").addEventListener("click", () => {
        const next = effectiveTheme() === "dark" ? "light" : "dark";
        document.documentElement.setAttribute("data-theme", next);
        setStoredTheme(next);
        syncThemeToggleButton();
    });
}
initThemeToggle();

function fmtMoney(n) { return n === null || n === undefined ? "-" : "$" + Number(n).toFixed(2); }
function fmtNum(n, d = 1) { return n === null || n === undefined ? "-" : Number(n).toFixed(d); }

async function fetchJson(path) {
    const res = await fetch(path);
    if (!res.ok) throw new Error(`${path} -> ${res.status}`);
    return res.json();
}

async function refreshHealth() {
    try {
        const h = await fetchJson("/health");
        el("sim-day").textContent = h.sim_day;
        const badge = el("status-badge");
        badge.textContent = h.status;
        badge.className = h.status === "healthy" ? "status-healthy" : "status-degraded";
        if (!reportDayTouched) el("report-day").value = h.sim_day;
    } catch (e) {
        el("status-badge").textContent = "unreachable";
        el("status-badge").className = "status-degraded";
    }
}

async function refreshRealtime() {
    try {
        const m = await fetchJson("/metrics/realtime");
        el("active-vehicles").textContent = m.active_vehicles;
        el("idle-ratio").textContent = (m.idle_ratio * 100).toFixed(0) + "%";
        el("trips-hour").textContent = m.trips_last_hour;

        const tbody = el("zone-table");
        tbody.innerHTML = "";
        el("zone-empty").hidden = m.zones.length > 0;
        for (const z of m.zones) {
            const tr = document.createElement("tr");
            // Zone metrics carry no cost data (only vehicles have fuel/maintenance),
            // so "profit" here means the zone actually earned something this window.
            const isProfit = Number(z.total_earnings) > 0;
            tr.className = isProfit ? "row-profit" : "row-unprofit";
            const earningsDisplay = isProfit ? fmtMoney(z.total_earnings) : "-" + fmtMoney(z.total_earnings);
            tr.innerHTML = `<td>${z.zone}</td><td>${z.active_vehicles}</td><td>${z.idle_count}</td>
                <td>${z.enroute_count}</td><td>${z.on_trip_count}</td><td>${z.trip_count}</td>
                <td>${earningsDisplay}</td><td>${fmtNum(z.avg_speed)} km/h</td>`;
            tbody.appendChild(tr);
        }
    } catch (e) { /* API not up yet - leave stale values */ }
}

async function refreshAlerts() {
    try {
        const a = await fetchJson("/alerts");
        el("alert-count").textContent = a.count;
        const tbody = el("alerts-table");
        tbody.innerHTML = "";
        el("alerts-empty").hidden = a.alerts.length > 0;
        for (const alert of a.alerts) {
            const tr = document.createElement("tr");
            const sevClass = alert.severity === "critical" ? "badge-critical" : "badge-warning";
            tr.innerHTML = `<td><span class="badge ${sevClass}">${alert.severity}</span></td>
                <td>${alert.alert_type}</td><td>${alert.entity_id ?? "-"}</td>
                <td>${alert.message}</td><td>${new Date(alert.created_at).toLocaleTimeString()}</td>`;
            tbody.appendChild(tr);
        }
    } catch (e) { /* ignore */ }
}

function renderProfitChart(vehicles) {
    const container = el("profit-chart");
    if (!container) return;
    el("profit-chart-empty").hidden = vehicles.length > 0;
    if (!vehicles.length) {
        container.innerHTML = "";
        return;
    }

    const width = 800, height = 340, padTop = 10, padBottom = 24, padSide = 4;
    const usableHeight = height - padTop - padBottom;
    const zeroY = padTop + usableHeight / 2;
    const maxAbs = Math.max(1, ...vehicles.map((v) => Math.abs(Number(v.net_profit))));
    const barGap = 3;
    const barWidth = Math.max((width - padSide * 2) / vehicles.length - barGap, 1);

    let bars = "";
    let labels = "";
    vehicles.forEach((v, i) => {
        const val = Number(v.net_profit);
        const barH = (Math.abs(val) / maxAbs) * (usableHeight / 2);
        const x = padSide + i * (barWidth + barGap);
        const y = val >= 0 ? zeroY - barH : zeroY;
        const color = v.profitable ? "var(--good)" : "var(--bad)";
        bars += `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barH.toFixed(1)}" rx="1.5" style="fill:${color}"><title>${v.vehicle_id}: ${fmtMoney(val)} (${v.profitable ? "profitable" : "unprofitable"})</title></rect>`;
        if (barWidth > 10) {
            labels += `<text x="${(x + barWidth / 2).toFixed(1)}" y="${height - 4}" font-size="8" text-anchor="middle" style="fill:var(--muted)">${v.vehicle_id}</text>`;
        }
    });

    container.innerHTML = `<svg viewBox="0 0 ${width} ${height}" width="100%" height="${height}" preserveAspectRatio="none">
        <line x1="${padSide}" y1="${zeroY.toFixed(1)}" x2="${width - padSide}" y2="${zeroY.toFixed(1)}" style="stroke:var(--border)" stroke-width="1" />
        ${bars}
        ${labels}
    </svg>`;
}

let lastReportVehicles = [];

function filteredReportVehicles() {
    const showProfitable = el("filter-profitable").checked;
    const showUnprofitable = el("filter-unprofitable").checked;
    return lastReportVehicles.filter((v) => (v.profitable ? showProfitable : showUnprofitable));
}

function renderReport() {
    const vehicles = filteredReportVehicles();
    renderProfitChart(vehicles);
    const tbody = el("report-table");
    tbody.innerHTML = "";
    el("report-empty").hidden = vehicles.length > 0;
    for (const v of vehicles) {
        const tr = document.createElement("tr");
        const statusClass = v.profitable ? "badge-good" : "badge-bad";
        const statusText = v.profitable ? "profitable" : "unprofitable";
        tr.innerHTML = `<td>${v.vehicle_id}</td><td>${fmtMoney(v.total_fare)}</td>
            <td>${fmtMoney(v.fuel_cost)}</td><td>${fmtMoney(v.maintenance_cost)}</td>
            <td>${fmtMoney(v.net_profit)}</td><td>${fmtNum(v.distance_covered)}</td>
            <td>${v.cost_per_km ? fmtMoney(v.cost_per_km) : "-"}</td>
            <td><span class="badge ${statusClass}">${statusText}</span></td>`;
        tbody.appendChild(tr);
    }
}

async function refreshReport() {
    const day = el("report-day").value;
    try {
        const r = await fetchJson(`/reports/daily/${day}`);
        lastReportVehicles = r.vehicles;
        renderReport();
    } catch (e) { /* ignore */ }
}

el("report-day").addEventListener("change", () => {
    reportDayTouched = true;
    refreshReport();
});
el("filter-profitable").addEventListener("change", renderReport);
el("filter-unprofitable").addEventListener("change", renderReport);

// --- income trend chart (daily / monthly, fleet-wide or one vehicle) ---
async function loadVehicleOptions() {
    try {
        const v = await fetchJson("/vehicles");
        const sel = el("income-scope");
        const current = sel.value;
        sel.innerHTML = '<option value="">All vehicles</option>' +
            v.vehicles.map((id) => `<option value="${id}">${id}</option>`).join("");
        sel.value = v.vehicles.includes(current) ? current : "";
    } catch (e) { /* ignore */ }
}

function groupByMonth(days) {
    // No real calendar in a simulated pipeline - 30 sim_days = one "month" bucket.
    const buckets = new Map();
    for (const d of days) {
        const month = Math.floor(d.sim_day / 30);
        const prev = buckets.get(month) || { month, total_fare: 0, trip_count: 0, net_profit: 0, hasProfitData: false };
        prev.total_fare += Number(d.total_fare);
        prev.trip_count += Number(d.trip_count);
        if (d.net_profit !== null && d.net_profit !== undefined) {
            prev.net_profit += Number(d.net_profit);
            prev.hasProfitData = true;
        }
        buckets.set(month, prev);
    }
    return Array.from(buckets.values())
        .sort((a, b) => a.month - b.month)
        .map((b) => ({ ...b, profitable: b.hasProfitData ? b.net_profit > 0 : null }));
}

function renderIncomeChart(data, labelFn) {
    const container = el("income-chart");
    el("income-empty").hidden = data.length > 0;
    if (!data.length) {
        container.innerHTML = "";
        el("income-total").textContent = "-";
        return;
    }

    const width = 800, height = 380, padTop = 10, padBottom = 24, padSide = 4;
    const maxVal = Math.max(1, ...data.map((d) => Number(d.total_fare)));
    const barGap = 2;
    const barWidth = Math.max((width - padSide * 2) / data.length - barGap, 1);
    const usableHeight = height - padTop - padBottom;

    let total = 0;
    let bars = "";
    data.forEach((d, i) => {
        const val = Number(d.total_fare);
        total += val;
        const barH = Math.max((val / maxVal) * usableHeight, 0);
        const x = padSide + i * (barWidth + barGap);
        const y = padTop + (usableHeight - barH);
        const isLoss = d.profitable === false;
        const color = isLoss ? "var(--bad)" : "var(--accent)";
        const lossNote = isLoss ? " (net loss)" : "";
        bars += `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barH.toFixed(1)}" rx="1.5" style="fill:${color}"><title>${labelFn(d)}: ${fmtMoney(val)}${lossNote}</title></rect>`;
    });

    const labelIdxs = data.length > 1 ? [0, Math.floor((data.length - 1) / 2), data.length - 1] : [0];
    let labels = "";
    for (const i of labelIdxs) {
        const x = padSide + i * (barWidth + barGap) + barWidth / 2;
        labels += `<text x="${x.toFixed(1)}" y="${height - 4}" font-size="9" text-anchor="middle" style="fill:var(--muted)">${labelFn(data[i])}</text>`;
    }

    container.innerHTML = `<svg viewBox="0 0 ${width} ${height}" width="100%" height="${height}" preserveAspectRatio="none">
        <line x1="${padSide}" y1="${(padTop + usableHeight).toFixed(1)}" x2="${width - padSide}" y2="${(padTop + usableHeight).toFixed(1)}" style="stroke:var(--border)" stroke-width="1" />
        ${bars}
        ${labels}
    </svg>`;
    el("income-total").textContent = fmtMoney(total);
}

let lastIncomeDays = [];

function filteredIncomeDays() {
    const showProfitable = el("income-filter-profitable").checked;
    const showUnprofitable = el("income-filter-unprofitable").checked;
    return lastIncomeDays.filter((d) => {
        if (d.profitable === true) return showProfitable;
        if (d.profitable === false) return showUnprofitable;
        return true; // not yet reconciled by the batch layer - always show
    });
}

function renderIncomeTrendView() {
    const granularity = el("income-granularity").value;
    const days = filteredIncomeDays();
    if (granularity === "month") {
        renderIncomeChart(groupByMonth(days), (d) => `month ${d.month}`);
    } else {
        renderIncomeChart(days, (d) => `day ${d.sim_day}`);
    }
}

async function refreshIncomeTrend() {
    const vehicleId = el("income-scope").value;
    try {
        const path = vehicleId
            ? `/reports/income-trend?vehicle_id=${encodeURIComponent(vehicleId)}`
            : "/reports/income-trend";
        const r = await fetchJson(path);
        lastIncomeDays = r.days || [];
        renderIncomeTrendView();
    } catch (e) { /* ignore */ }
}

el("income-scope").addEventListener("change", refreshIncomeTrend);
el("income-granularity").addEventListener("change", renderIncomeTrendView);
el("income-filter-profitable").addEventListener("change", renderIncomeTrendView);
el("income-filter-unprofitable").addEventListener("change", renderIncomeTrendView);

async function tick() {
    await Promise.all([refreshHealth(), refreshRealtime(), refreshAlerts(), refreshReport(), refreshIncomeTrend()]);
}

loadVehicleOptions();
tick();
setInterval(tick, POLL_MS);
