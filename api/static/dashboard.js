const POLL_MS = 4000;
let reportDayTouched = false;

function el(id) { return document.getElementById(id); }

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
            tr.innerHTML = `<td>${z.zone}</td><td>${z.active_vehicles}</td><td>${z.idle_count}</td>
                <td>${z.enroute_count}</td><td>${z.on_trip_count}</td><td>${z.trip_count}</td>
                <td>${fmtMoney(z.total_earnings)}</td><td>${fmtNum(z.avg_speed)} km/h</td>`;
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

async function refreshReport() {
    const day = el("report-day").value;
    try {
        const r = await fetchJson(`/reports/daily/${day}`);
        const tbody = el("report-table");
        tbody.innerHTML = "";
        el("report-empty").hidden = r.vehicles.length > 0;
        for (const v of r.vehicles) {
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
    } catch (e) { /* ignore */ }
}

el("report-day").addEventListener("change", () => {
    reportDayTouched = true;
    refreshReport();
});

async function tick() {
    await Promise.all([refreshHealth(), refreshRealtime(), refreshAlerts(), refreshReport()]);
}

tick();
setInterval(tick, POLL_MS);
