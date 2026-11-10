/* Task Portal - single-page app on Supabase (auth + Postgres with row-level security). */
(() => {
  "use strict";
  const CFG = window.PORTAL_CONFIG;
  const sb = window.supabase.createClient(CFG.supabaseUrl, CFG.supabaseKey);
  const app = document.getElementById("app");

  // ---------- reference data ----------
  const TYPES = {
    cdc_onboarding: "CDC table onboarding",
    kafka_stream: "Kafka topic / stream",
    informatica: "Informatica mapping / workflow",
    incident: "Incident / job failure",
    data_quality: "Data quality / reconciliation",
    report: "Report / MicroStrategy request",
    access: "Access / environment",
    documentation: "Documentation / runbook",
    learning: "Learning / KT session",
    enhancement: "Enhancement / change request",
    admin: "Admin / other",
  };
  const STATUSES = { new: "New", in_progress: "In progress", blocked: "Blocked", in_review: "In review", done: "Done", cancelled: "Cancelled" };
  const OPEN = ["new", "in_progress", "blocked", "in_review"];
  const PRIORITIES = { P1: "P1 · Critical", P2: "P2 · High", P3: "P3 · Medium", P4: "P4 · Low" };
  const SYSTEMS = ["Informatica", "Kafka", "PostgreSQL", "Oracle", "Cloudera", "Greenplum", "MicroStrategy", "Linux / Servers", "Other"];
  const LAYERS = ["Source", "Raw", "Enrich", "Staging", "Core", "Semantic", "Reporting", "N/A"];
  const ROLES = { admin: "Admin (team)", engineer: "Team member", manager: "Manager", pending: "Pending approval" };

  let session = null;
  let me = null;
  let people = {};
  let ticketFilters = { q: "", status: "open", type: "", priority: "", assignee: "", system: "" };

  // ---------- helpers ----------
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const localISO = (d = new Date()) => new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 10);
  const addDays = (iso, n) => { const d = new Date(iso + "T00:00:00"); d.setDate(d.getDate() + n); return localISO(d); };
  const fmtDate = (d) => (d ? new Date(d.length === 10 ? d + "T00:00:00" : d).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" }) : "—");
  const fmtDT = (d) => (d ? new Date(d).toLocaleString("en-GB", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" }) : "—");
  const dayOf = (ts) => (ts ? localISO(new Date(ts)) : null);
  const name = (id) => (id && people[id] ? people[id].full_name : "Unassigned");
  const initials = (n) => (n || "?").split(/\s+/).filter(Boolean).slice(0, 2).map((w) => w[0].toUpperCase()).join("");
  const isOverdue = (t) => t.due_date && OPEN.includes(t.status) && t.due_date < localISO();
  const isTeam = () => me && (me.role === "engineer" || me.role === "admin");
  const isManager = () => me && (me.role === "manager" || me.role === "admin");
  const statusPill = (s) => `<span class="pill s-${s}">${STATUSES[s] || s}</span>`;
  const prioPill = (p) => `<span class="pill p-${p}">${p}</span>`;
  const hrs = (n) => (Math.round(Number(n || 0) * 100) / 100).toString();
  const options = (map, selected, blank) => {
    const entries = Array.isArray(map) ? map.map((v) => [v, v]) : Object.entries(map);
    return (blank !== undefined ? `<option value="">${esc(blank)}</option>` : "") +
      entries.map(([v, l]) => `<option value="${esc(v)}" ${v === selected ? "selected" : ""}>${esc(l)}</option>`).join("");
  };
  const teamOptions = (selected, blank = "Unassigned") =>
    `<option value="">${esc(blank)}</option>` +
    Object.values(people).filter((p) => p.role !== "pending").sort((a, b) => a.full_name.localeCompare(b.full_name))
      .map((p) => `<option value="${p.id}" ${p.id === selected ? "selected" : ""}>${esc(p.full_name)}${p.role === "manager" ? " (manager)" : ""}</option>`).join("");
  const prioRank = { P1: 1, P2: 2, P3: 3, P4: 4 };
  const sortTickets = (a, b) => prioRank[a.priority] - prioRank[b.priority] || (a.due_date || "9999").localeCompare(b.due_date || "9999") || b.id - a.id;

  function toast(msg) {
    const t = document.getElementById("toast");
    t.textContent = msg; t.hidden = false;
    clearTimeout(toast._t); toast._t = setTimeout(() => (t.hidden = true), 3200);
  }
  async function q(promise) {
    const { data, error } = await promise;
    if (error) { toast(error.message); throw error; }
    return data;
  }
  function modal(title, bodyHtml, okLabel = "OK") {
    const m = document.getElementById("modal");
    document.getElementById("modal-title").textContent = title;
    document.getElementById("modal-body").innerHTML = bodyHtml;
    document.getElementById("modal-ok").textContent = okLabel;
    m.hidden = false;
    const first = m.querySelector("input, textarea, select"); if (first) first.focus();
    return new Promise((resolve) => {
      const done = (val) => { m.hidden = true; ok.onclick = cancel.onclick = null; resolve(val); };
      const ok = document.getElementById("modal-ok"), cancel = document.getElementById("modal-cancel");
      ok.onclick = () => done(document.getElementById("modal-body"));
      cancel.onclick = () => done(null);
    });
  }
  async function askBlockedReason() {
    const body = await modal("Why is it blocked?", `<label class="field">Reason<textarea id="block-reason" placeholder="e.g. Waiting for VPN access from IT"></textarea></label>`, "Mark blocked");
    if (!body) return null;
    const r = body.querySelector("#block-reason").value.trim();
    return r || "No reason given";
  }
  function downloadCSV(filename, rows) {
    const csv = rows.map((r) => r.map((c) => `"${String(c ?? "").replace(/"/g, '""')}"`).join(",")).join("\r\n");
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob(["﻿" + csv], { type: "text/csv" }));
    a.download = filename; document.body.appendChild(a); a.click(); a.remove();
  }

  // ---------- data ----------
  async function loadMe() {
    me = null; people = {};
    if (!session) return;
    const { data } = await sb.from("profiles").select("*").eq("id", session.user.id).maybeSingle();
    me = data;
    if (me && me.role !== "pending") {
      const all = await q(sb.from("profiles").select("*"));
      all.forEach((p) => (people[p.id] = p));
    }
  }
  const fetchTickets = () => q(sb.from("tickets").select("*").order("id", { ascending: false }));

  // ---------- routing ----------
  async function render() {
    const [, view = "dashboard", arg] = (location.hash || "#/dashboard").split("/");
    const top = document.getElementById("topbar");
    if (!session) { top.hidden = true; return viewLogin(); }
    if (!me) await loadMe();
    if (!me || me.role === "pending") { top.hidden = true; return viewPending(); }
    top.hidden = false;
    document.getElementById("whoami").innerHTML = `<span class="avatar">${esc(initials(me.full_name))}</span><span>${esc(me.full_name)}<br><span class="muted" style="font-size:.78rem">${esc(ROLES[me.role])}</span></span>`;
    document.querySelectorAll("[data-admin]").forEach((a) => (a.hidden = me.role !== "admin"));
    document.querySelectorAll("#mainnav a").forEach((a) => a.classList.toggle("active", a.dataset.nav === view));
    const views = { dashboard: viewDashboard, tickets: viewTickets, board: viewBoard, ticket: viewTicket, new: viewForm, edit: viewForm, report: viewReport, team: viewTeam, account: viewAccount };
    app.innerHTML = `<p class="muted">Loading…</p>`;
    try { await (views[view] || viewDashboard)(arg); }
    catch (e) { console.error(e); app.innerHTML = `<div class="card"><p class="error">Something went wrong: ${esc(e.message || e)}</p><p><a href="#/dashboard">Back to dashboard</a></p></div>`; }
    window.scrollTo(0, 0);
  }
  window.addEventListener("hashchange", render);

  // ---------- login / pending ----------
  function viewLogin() {
    let signup = false;
    const draw = () => {
      app.innerHTML = `
      <div class="auth">
        <div class="card">
          <div class="brand"><span class="mark">✓</span> Task Portal</div>
          <p class="muted">${esc(CFG.orgName)} · tickets, progress and reports</p>
          <form id="authform" class="stack">
            ${signup ? `<label class="field">Full name<input id="a-name" required autocomplete="name"></label>` : ""}
            <label class="field">Email<input id="a-email" type="email" required autocomplete="username"></label>
            <label class="field">Password<input id="a-pass" type="password" required minlength="8" autocomplete="${signup ? "new-password" : "current-password"}"></label>
            <p class="error" id="a-err" hidden></p>
            <button class="btn primary" type="submit">${signup ? "Create account" : "Sign in"}</button>
          </form>
          <p class="muted" style="font-size:.88rem">${signup
            ? `Already have an account? <a href="#" id="a-toggle">Sign in</a>`
            : `New here? <a href="#" id="a-toggle">Request an account</a>. An admin approves new accounts.`}</p>
        </div>
      </div>`;
      document.getElementById("a-toggle").onclick = (e) => { e.preventDefault(); signup = !signup; draw(); };
      document.getElementById("authform").onsubmit = async (e) => {
        e.preventDefault();
        const err = document.getElementById("a-err"); err.hidden = true;
        const email = document.getElementById("a-email").value.trim();
        const password = document.getElementById("a-pass").value;
        const btn = e.target.querySelector("button"); btn.disabled = true;
        if (signup) {
          const full_name = document.getElementById("a-name").value.trim();
          const { error } = await sb.auth.signUp({ email, password, options: { data: { full_name }, emailRedirectTo: location.origin + location.pathname } });
          btn.disabled = false;
          if (error) { err.textContent = error.message; err.hidden = false; return; }
          app.innerHTML = `<div class="auth"><div class="card"><h2>Account requested</h2><p>If asked, confirm your email from the message we sent. Then an admin approves your account and you can sign in.</p><p><a href="#/login" onclick="location.reload()">Back to sign in</a></p></div></div>`;
        } else {
          const { error } = await sb.auth.signInWithPassword({ email, password });
          btn.disabled = false;
          if (error) { err.textContent = error.message === "Invalid login credentials" ? "Wrong email or password." : error.message; err.hidden = false; }
        }
      };
    };
    draw();
  }
  function viewPending() {
    app.innerHTML = `<div class="auth"><div class="card">
      <h2>Waiting for approval</h2>
      <p>Your account (${esc(session.user.email)}) is set up. An admin needs to approve it before you can see tickets.</p>
      <div class="row"><button class="btn" id="p-refresh">Check again</button><button class="btn" id="p-out">Sign out</button></div>
    </div></div>`;
    document.getElementById("p-refresh").onclick = async () => { await loadMe(); render(); };
    document.getElementById("p-out").onclick = () => sb.auth.signOut();
  }

  // ---------- dashboard ----------
  async function viewDashboard() {
    const weekAgo = addDays(localISO(), -6);
    const [tickets, logs, events] = await Promise.all([
      fetchTickets(),
      q(sb.from("work_logs").select("*").gte("work_date", weekAgo)),
      q(sb.from("ticket_events").select("*, tickets(key,title)").order("created_at", { ascending: false }).limit(15)),
    ]);
    const open = tickets.filter((t) => OPEN.includes(t.status));
    const doneWeek = tickets.filter((t) => t.status === "done" && dayOf(t.resolved_at) >= weekAgo);
    const overdue = tickets.filter(isOverdue);
    const awaitingSign = tickets.filter((t) => t.status === "done" && !t.approved_at);
    const hoursWeek = logs.reduce((s, l) => s + Number(l.hours), 0);
    const mine = open.filter((t) => t.assignee_id === me.id).sort(sortTickets);
    const blocked = open.filter((t) => t.status === "blocked");

    const listRows = (arr, empty) => arr.length ? `<div class="table-wrap"><table><tbody>${arr.map((t) => `
      <tr class="clickable" data-id="${t.id}"><td class="key">${esc(t.key)}</td><td>${esc(t.title)}${t.blocked_reason ? `<div class="muted" style="font-size:.82rem">Blocked: ${esc(t.blocked_reason)}</div>` : ""}</td>
      <td>${prioPill(t.priority)}</td><td>${statusPill(t.status)}</td><td class="${isOverdue(t) ? "overdue" : "muted"}">${t.due_date ? fmtDate(t.due_date) : ""}</td></tr>`).join("")}</tbody></table></div>`
      : `<p class="muted">${empty}</p>`;

    const eventText = (e) => {
      const who = esc(name(e.actor_id) === "Unassigned" ? "System" : name(e.actor_id));
      const t = e.tickets ? `<a href="#/ticket/${e.ticket_id}">${esc(e.tickets.key)}</a>` : "";
      if (e.event === "created") return `<b>${who}</b> created ${t} ${esc(e.tickets?.title || "")}`;
      if (e.event === "status") return `<b>${who}</b> moved ${t} to ${statusPill(e.to_value)}`;
      if (e.event === "assignee") return `<b>${who}</b> assigned ${t} to ${esc(e.to_value || "nobody")}`;
      if (e.event === "priority") return `<b>${who}</b> set ${t} priority ${esc(e.to_value)}`;
      if (e.event === "due_date") return `<b>${who}</b> set ${t} due ${fmtDate(e.to_value)}`;
      if (e.event === "signed_off") return `<b>${who}</b> signed off ${t}`;
      return `<b>${who}</b> updated ${t}`;
    };

    app.innerHTML = `
      <div class="page-head">
        <div><h1>Good ${new Date().getHours() < 12 ? "morning" : new Date().getHours() < 17 ? "afternoon" : "evening"}, ${esc(me.full_name.split(" ")[0])}</h1>
        <p class="muted">${new Date().toLocaleDateString("en-GB", { weekday: "long", day: "numeric", month: "long", year: "numeric" })}</p></div>
        <div class="actions"><a class="btn" href="#/report">Weekly report</a></div>
      </div>
      <div class="kpis">
        <a class="kpi" href="#/tickets"><span class="n">${open.length}</span><span class="l">Open tickets</span></a>
        <a class="kpi" href="#/board"><span class="n">${open.filter((t) => t.status === "in_progress").length}</span><span class="l">In progress</span></a>
        <a class="kpi ${blocked.length ? "danger" : ""}" href="#/board"><span class="n">${blocked.length}</span><span class="l">Blocked</span></a>
        <a class="kpi ${overdue.length ? "warn" : ""}" href="#/tickets"><span class="n">${overdue.length}</span><span class="l">Overdue</span></a>
        <div class="kpi"><span class="n">${doneWeek.length}</span><span class="l">Done, last 7 days</span></div>
        <div class="kpi"><span class="n">${hrs(hoursWeek)}</span><span class="l">Hours logged, last 7 days</span></div>
      </div>
      <div class="grid-2">
        <div class="stack">
          ${me.role === "manager" ? `<div class="card"><h2>Waiting for your sign-off (${awaitingSign.length})</h2>${listRows(awaitingSign, "Nothing to sign off.")}</div>` : ""}
          ${isTeam() ? `<div class="card"><h2>My open tickets (${mine.length})</h2>${listRows(mine, "No open tickets assigned to you.")}</div>` : ""}
          <div class="card"><h2>Blocked (${blocked.length})</h2>${listRows(blocked, "Nothing is blocked.")}</div>
          ${overdue.length ? `<div class="card"><h2>Overdue (${overdue.length})</h2>${listRows(overdue.sort(sortTickets), "")}</div>` : ""}
        </div>
        <div class="card"><h2>Recent activity</h2><div class="feed">${events.length ? events.map((e) => `<div class="event">${eventText(e)}<div class="muted" style="font-size:.76rem">${fmtDT(e.created_at)}</div></div>`).join("") : `<p class="muted">No activity yet.</p>`}</div></div>
      </div>`;
    app.querySelectorAll("tr[data-id]").forEach((tr) => (tr.onclick = () => (location.hash = `#/ticket/${tr.dataset.id}`)));
  }

  // ---------- ticket list ----------
  async function viewTickets() {
    const tickets = await fetchTickets();
    const f = ticketFilters;
    app.innerHTML = `
      <div class="page-head"><div><h1>Tickets</h1><p class="muted" id="t-count"></p></div>
        <div class="actions no-print"><button class="btn" id="t-csv">Export CSV</button><button class="btn" onclick="window.print()">Print / PDF</button><a class="btn primary" href="#/new">+ New ticket</a></div></div>
      <div class="print-only"><p class="muted">${esc(CFG.orgName)} · printed ${fmtDT(new Date())}</p></div>
      <div class="filters no-print card">
        <label class="field">Search<input id="f-q" value="${esc(f.q)}" placeholder="Title, key, description"></label>
        <label class="field">Status<select id="f-status"><option value="open" ${f.status === "open" ? "selected" : ""}>All open</option><option value="all" ${f.status === "all" ? "selected" : ""}>Everything</option>${options(STATUSES, f.status)}</select></label>
        <label class="field">Type<select id="f-type">${options(TYPES, f.type, "Any type")}</select></label>
        <label class="field">Priority<select id="f-priority">${options(PRIORITIES, f.priority, "Any priority")}</select></label>
        <label class="field">Assignee<select id="f-assignee">${teamOptions(f.assignee, "Anyone")}<option value="none" ${f.assignee === "none" ? "selected" : ""}>Unassigned only</option></select></label>
        <label class="field">System<select id="f-system">${options(SYSTEMS, f.system, "Any system")}</select></label>
      </div>
      <div class="table-wrap"><table>
        <thead><tr><th>Key</th><th>Title</th><th>Type</th><th>Priority</th><th>Status</th><th>Assignee</th><th>Due</th><th>Updated</th></tr></thead>
        <tbody id="t-body"></tbody></table></div>`;
    const draw = () => {
      const s = f.q.toLowerCase();
      const rows = tickets.filter((t) =>
        (f.status === "all" || (f.status === "open" ? OPEN.includes(t.status) : t.status === f.status)) &&
        (!f.type || t.type === f.type) && (!f.priority || t.priority === f.priority) && (!f.system || t.system === f.system) &&
        (!f.assignee || (f.assignee === "none" ? !t.assignee_id : t.assignee_id === f.assignee)) &&
        (!s || (t.title + " " + t.key + " " + t.description).toLowerCase().includes(s))).sort(sortTickets);
      document.getElementById("t-count").textContent = `${rows.length} of ${tickets.length} tickets`;
      document.getElementById("t-body").innerHTML = rows.length ? rows.map((t) => `
        <tr class="clickable" data-id="${t.id}"><td class="key">${esc(t.key)}</td><td>${esc(t.title)}</td><td><span class="tag">${esc(TYPES[t.type])}</span></td>
        <td>${prioPill(t.priority)}</td><td>${statusPill(t.status)}${t.approved_at ? ` <span class="signed">✓ signed off</span>` : ""}</td><td>${esc(name(t.assignee_id))}</td>
        <td class="${isOverdue(t) ? "overdue" : ""}">${t.due_date ? fmtDate(t.due_date) : "—"}</td><td class="muted">${fmtDT(t.updated_at)}</td></tr>`).join("")
        : `<tr><td colspan="8" class="muted">No tickets match these filters.</td></tr>`;
      app.querySelectorAll("tr[data-id]").forEach((tr) => (tr.onclick = () => (location.hash = `#/ticket/${tr.dataset.id}`)));
      draw.rows = rows;
    };
    ["q", "status", "type", "priority", "assignee", "system"].forEach((k) => {
      const el = document.getElementById("f-" + k);
      el.addEventListener(k === "q" ? "input" : "change", () => { f[k] = el.value; draw(); });
    });
    document.getElementById("t-csv").onclick = () => downloadCSV(`tickets-${localISO()}.csv`, [
      ["Key", "Title", "Type", "Priority", "Status", "Assignee", "Reporter", "System", "Layer", "Due", "Estimate h", "Created", "Resolved", "Signed off"],
      ...draw.rows.map((t) => [t.key, t.title, TYPES[t.type], t.priority, STATUSES[t.status], name(t.assignee_id), name(t.reporter_id), t.system, t.layer, t.due_date, t.estimate_hours, dayOf(t.created_at), dayOf(t.resolved_at), t.approved_at ? dayOf(t.approved_at) : ""]),
    ]);
    draw();
  }

  // ---------- board ----------
  async function setStatus(t, status) {
    const patch = { status };
    if (status === "blocked") { const r = await askBlockedReason(); if (r === null) return false; patch.blocked_reason = r; }
    await q(sb.from("tickets").update(patch).eq("id", t.id));
    toast(`${t.key} moved to ${STATUSES[status]}`);
    return true;
  }
  async function viewBoard() {
    const tickets = await fetchTickets();
    const cutoff = addDays(localISO(), -14);
    const cols = ["new", "in_progress", "blocked", "in_review", "done"];
    const inCol = (s) => tickets.filter((t) => t.status === s && (s !== "done" || dayOf(t.resolved_at) >= cutoff)).sort(sortTickets);
    app.innerHTML = `
      <div class="page-head"><div><h1>Board</h1><p class="muted">Drag a card to change its status. Done shows the last 14 days.</p></div>
        <div class="actions"><a class="btn primary" href="#/new">+ New ticket</a></div></div>
      <div class="board">${cols.map((s) => `
        <div class="col" data-status="${s}">
          <div class="col-head">${statusPill(s)}<span class="muted">${inCol(s).length}</span></div>
          ${inCol(s).map((t) => `
            <a class="tcard" draggable="true" data-id="${t.id}" href="#/ticket/${t.id}">
              <span class="t">${esc(t.title)}</span>
              <span class="m"><span class="key">${esc(t.key)}</span>${prioPill(t.priority)}<span class="tag">${esc(t.system)}</span></span>
              <span class="m"><span class="avatar" style="width:22px;height:22px;font-size:.66rem">${esc(initials(name(t.assignee_id)))}</span>${esc(name(t.assignee_id))}
              ${t.due_date ? `<span class="${isOverdue(t) ? "overdue" : ""}">· due ${fmtDate(t.due_date)}</span>` : ""}${t.approved_at ? `<span class="signed">✓</span>` : ""}</span>
            </a>`).join("")}
        </div>`).join("")}</div>`;
    let dragId = null;
    app.querySelectorAll(".tcard").forEach((c) => c.addEventListener("dragstart", (e) => { dragId = c.dataset.id; e.dataTransfer.setData("text/plain", dragId); }));
    app.querySelectorAll(".col").forEach((col) => {
      col.addEventListener("dragover", (e) => { e.preventDefault(); col.classList.add("drop"); });
      col.addEventListener("dragleave", () => col.classList.remove("drop"));
      col.addEventListener("drop", async (e) => {
        e.preventDefault(); col.classList.remove("drop");
        const t = tickets.find((x) => String(x.id) === (e.dataTransfer.getData("text/plain") || dragId));
        if (t && t.status !== col.dataset.status && (await setStatus(t, col.dataset.status))) viewBoard();
      });
    });
  }

  // ---------- ticket detail ----------
  async function viewTicket(id) {
    const [t, comments, logs, events] = await Promise.all([
      q(sb.from("tickets").select("*").eq("id", id).maybeSingle()),
      q(sb.from("ticket_comments").select("*").eq("ticket_id", id).order("created_at")),
      q(sb.from("work_logs").select("*").eq("ticket_id", id).order("work_date", { ascending: false })),
      q(sb.from("ticket_events").select("*").eq("ticket_id", id).order("created_at")),
    ]);
    if (!t) { app.innerHTML = `<div class="card"><p>Ticket not found.</p><a href="#/tickets">Back to tickets</a></div>`; return; }
    const logged = logs.reduce((s, l) => s + Number(l.hours), 0);
    const evText = (e) => {
      if (e.event === "created") return `created the ticket`;
      if (e.event === "status") return `changed status ${STATUSES[e.from_value] || ""} → <b>${STATUSES[e.to_value]}</b>`;
      if (e.event === "assignee") return `assigned to <b>${esc(e.to_value || "nobody")}</b>`;
      if (e.event === "priority") return `changed priority ${esc(e.from_value)} → <b>${esc(e.to_value)}</b>`;
      if (e.event === "due_date") return `set due date <b>${fmtDate(e.to_value)}</b>`;
      if (e.event === "signed_off") return `<b>signed off</b> the ticket`;
      return esc(e.event);
    };
    app.innerHTML = `
      <div class="print-only"><p class="muted">${esc(CFG.orgName)} · ticket printed ${fmtDT(new Date())}</p></div>
      <div class="page-head">
        <div><p class="key">${esc(t.key)} · ${esc(TYPES[t.type])}</p><h1>${esc(t.title)}</h1></div>
        <div class="actions no-print">
          ${me.role === "manager" && t.status === "done" && !t.approved_at ? `<button class="btn primary" id="d-sign">✓ Sign off</button>` : ""}
          <a class="btn" href="#/edit/${t.id}">Edit</a>
          <button class="btn" onclick="window.print()">Print / PDF</button>
          ${me.role === "admin" ? `<button class="btn danger" id="d-del">Delete</button>` : ""}
        </div>
      </div>
      <div class="grid-2">
        <div class="stack">
          <div class="card"><h2>Description</h2><p class="desc">${t.description ? esc(t.description) : `<span class="muted">No description.</span>`}</p>
            ${t.status === "blocked" && t.blocked_reason ? `<p class="error" style="margin-top:10px"><b>Blocked:</b> ${esc(t.blocked_reason)}</p>` : ""}</div>

          <div class="card"><h2>Comments (${comments.length})</h2>
            <div class="feed">${comments.map((c) => `
              <div class="entry"><span class="avatar">${esc(initials(name(c.author_id)))}</span>
                <div class="body"><div class="meta"><b>${esc(name(c.author_id))}</b> · ${fmtDT(c.created_at)}
                ${c.author_id === me.id ? ` · <a href="#" class="no-print" data-delc="${c.id}">delete</a>` : ""}</div><div class="text">${esc(c.body)}</div></div></div>`).join("") || `<p class="muted">No comments yet.</p>`}</div>
            <form id="c-form" class="stack no-print" style="margin-top:12px"><textarea id="c-body" placeholder="Write an update or question…" required></textarea>
              <div><button class="btn primary" type="submit">Add comment</button></div></form>
          </div>

          <div class="card"><h2>Work log · ${hrs(logged)} h${t.estimate_hours ? ` of ${hrs(t.estimate_hours)} h estimated` : ""}</h2>
            ${logs.length ? `<div class="table-wrap"><table><thead><tr><th>Date</th><th>Who</th><th class="num">Hours</th><th>What was done</th><th class="no-print"></th></tr></thead><tbody>
              ${logs.map((l) => `<tr><td>${fmtDate(l.work_date)}</td><td>${esc(name(l.user_id))}</td><td class="num">${hrs(l.hours)}</td><td>${esc(l.note)}</td>
              <td class="no-print">${l.user_id === me.id ? `<a href="#" data-dell="${l.id}">delete</a>` : ""}</td></tr>`).join("")}</tbody></table></div>` : `<p class="muted">No time logged yet.</p>`}
            ${isTeam() ? `<form id="l-form" class="form-grid no-print" style="margin-top:12px">
              <label class="field">Date<input type="date" id="l-date" value="${localISO()}" required></label>
              <label class="field">Hours<input type="number" id="l-hours" min="0.25" max="24" step="0.25" value="1" required></label>
              <label class="field" style="grid-column:1/-1">What was done<input id="l-note" required placeholder="e.g. Mapped CMF columns to staging table"></label>
              <div><button class="btn primary" type="submit">Log time</button></div></form>` : ""}
          </div>

          <div class="card"><h2>History</h2><div class="feed">${events.map((e) => `<div class="event"><b>${esc(e.actor_id ? name(e.actor_id) : "System")}</b> ${evText(e)} <span class="muted">· ${fmtDT(e.created_at)}</span></div>`).join("")}</div></div>
        </div>

        <div class="card">
          <h2>Details</h2>
          <dl class="detail-meta">
            <dt>Status</dt><dd><select id="d-status" class="no-print">${options(STATUSES, t.status)}</select><span class="print-only">${STATUSES[t.status]}</span></dd>
            <dt>Priority</dt><dd><select id="d-priority" class="no-print">${options(PRIORITIES, t.priority)}</select><span class="print-only">${PRIORITIES[t.priority]}</span></dd>
            <dt>Assignee</dt><dd><select id="d-assignee" class="no-print">${teamOptions(t.assignee_id)}</select><span class="print-only">${esc(name(t.assignee_id))}</span></dd>
            <dt>Due date</dt><dd><input type="date" id="d-due" class="no-print" value="${t.due_date || ""}"><span class="print-only">${fmtDate(t.due_date)}</span>${isOverdue(t) ? ` <span class="overdue">overdue</span>` : ""}</dd>
            <dt>Reporter</dt><dd>${esc(name(t.reporter_id))}</dd>
            <dt>System</dt><dd>${esc(t.system)}</dd>
            <dt>Layer</dt><dd>${esc(t.layer)}</dd>
            <dt>Estimate</dt><dd>${t.estimate_hours ? hrs(t.estimate_hours) + " h" : "—"}</dd>
            <dt>Logged</dt><dd>${hrs(logged)} h</dd>
            <dt>Created</dt><dd>${fmtDT(t.created_at)}</dd>
            <dt>Started</dt><dd>${fmtDT(t.started_at)}</dd>
            <dt>Resolved</dt><dd>${fmtDT(t.resolved_at)}</dd>
            <dt>Sign-off</dt><dd>${t.approved_at ? `<span class="signed">✓ ${esc(name(t.approved_by))}, ${fmtDT(t.approved_at)}</span>` : `<span class="muted">${t.status === "done" ? "Waiting for manager" : "After it's done"}</span>`}</dd>
          </dl>
        </div>
      </div>`;

    const patch = async (fields, msg) => { await q(sb.from("tickets").update(fields).eq("id", t.id)); toast(msg); viewTicket(id); };
    document.getElementById("d-status").onchange = async (e) => { if (!(await setStatus(t, e.target.value))) e.target.value = t.status; else viewTicket(id); };
    document.getElementById("d-priority").onchange = (e) => patch({ priority: e.target.value }, "Priority updated");
    document.getElementById("d-assignee").onchange = (e) => patch({ assignee_id: e.target.value || null }, "Assignee updated");
    document.getElementById("d-due").onchange = (e) => patch({ due_date: e.target.value || null }, "Due date updated");
    const sign = document.getElementById("d-sign");
    if (sign) sign.onclick = async () => { await q(sb.rpc("sign_off", { t: t.id })); toast("Signed off"); viewTicket(id); };
    const del = document.getElementById("d-del");
    if (del) del.onclick = async () => {
      if (!(await modal("Delete this ticket?", `<p>${esc(t.key)} and all its comments and work logs will be removed permanently.</p>`, "Delete"))) return;
      await q(sb.from("tickets").delete().eq("id", t.id)); toast("Ticket deleted"); location.hash = "#/tickets";
    };
    document.getElementById("c-form").onsubmit = async (e) => {
      e.preventDefault();
      const body = document.getElementById("c-body").value.trim(); if (!body) return;
      await q(sb.from("ticket_comments").insert({ ticket_id: t.id, body, author_id: me.id })); viewTicket(id);
    };
    const lf = document.getElementById("l-form");
    if (lf) lf.onsubmit = async (e) => {
      e.preventDefault();
      await q(sb.from("work_logs").insert({ ticket_id: t.id, user_id: me.id, work_date: document.getElementById("l-date").value,
        hours: Number(document.getElementById("l-hours").value), note: document.getElementById("l-note").value.trim() }));
      toast("Time logged");
      if (t.status === "new") await q(sb.from("tickets").update({ status: "in_progress" }).eq("id", t.id));
      viewTicket(id);
    };
    app.querySelectorAll("[data-delc]").forEach((a) => (a.onclick = async (e) => { e.preventDefault(); await q(sb.from("ticket_comments").delete().eq("id", a.dataset.delc)); viewTicket(id); }));
    app.querySelectorAll("[data-dell]").forEach((a) => (a.onclick = async (e) => { e.preventDefault(); await q(sb.from("work_logs").delete().eq("id", a.dataset.dell)); viewTicket(id); }));
  }

  // ---------- new / edit ----------
  async function viewForm(id) {
    const t = id ? await q(sb.from("tickets").select("*").eq("id", id).maybeSingle()) : null;
    const v = t || { title: "", description: "", type: isManager() && !isTeam() ? "enhancement" : "admin", priority: "P3", status: "new", system: "Other", layer: "N/A", assignee_id: isTeam() ? me.id : "", due_date: "", estimate_hours: "" };
    app.innerHTML = `
      <div class="page-head"><div><h1>${t ? `Edit ${esc(t.key)}` : "New ticket"}</h1>
        ${!t && me.role === "manager" ? `<p class="muted">Describe what you need. The team will pick it up, estimate it and keep you updated here.</p>` : ""}</div></div>
      <form id="tf" class="card stack">
        <label class="field">Title<input id="tf-title" required maxlength="200" value="${esc(v.title)}" placeholder="e.g. Onboard CMF_BALANCE table to real-time CDC"></label>
        <label class="field">Description<textarea id="tf-desc" placeholder="What needs doing, why, and how we'll know it's done.">${esc(v.description)}</textarea></label>
        <div class="form-grid">
          <label class="field">Type<select id="tf-type">${options(TYPES, v.type)}</select></label>
          <label class="field">Priority<select id="tf-priority">${options(PRIORITIES, v.priority)}</select></label>
          <label class="field">Status<select id="tf-status">${options(STATUSES, v.status)}</select></label>
          <label class="field">Assignee<select id="tf-assignee">${teamOptions(v.assignee_id)}</select></label>
          <label class="field">System<select id="tf-system">${options(SYSTEMS, v.system)}</select></label>
          <label class="field">Data layer<select id="tf-layer">${options(LAYERS, v.layer)}</select></label>
          <label class="field">Due date<input type="date" id="tf-due" value="${v.due_date || ""}"></label>
          <label class="field">Estimate (hours)<input type="number" id="tf-est" min="0" step="0.5" value="${v.estimate_hours ?? ""}"></label>
        </div>
        <div class="row"><button class="btn primary" type="submit">${t ? "Save changes" : "Create ticket"}</button><a class="btn" href="${t ? `#/ticket/${t.id}` : "#/tickets"}">Cancel</a></div>
      </form>`;
    document.getElementById("tf").onsubmit = async (e) => {
      e.preventDefault();
      const val = (k) => document.getElementById("tf-" + k).value;
      const fields = { title: val("title").trim(), description: val("desc").trim(), type: val("type"), priority: val("priority"), status: val("status"),
        assignee_id: val("assignee") || null, system: val("system"), layer: val("layer"), due_date: val("due") || null, estimate_hours: val("est") === "" ? null : Number(val("est")) };
      if (fields.status === "blocked" && (!t || t.status !== "blocked")) { const r = await askBlockedReason(); if (r === null) return; fields.blocked_reason = r; }
      if (t) { await q(sb.from("tickets").update(fields).eq("id", t.id)); toast("Saved"); location.hash = `#/ticket/${t.id}`; }
      else { const row = await q(sb.from("tickets").insert({ ...fields, reporter_id: me.id }).select().single()); toast(`${row.key} created`); location.hash = `#/ticket/${row.id}`; }
    };
  }

  // ---------- report ----------
  let reportRange = { from: addDays(localISO(), -6), to: localISO(), person: "" };
  async function viewReport() {
    const r = reportRange;
    const [tickets, logs] = await Promise.all([
      fetchTickets(),
      q(sb.from("work_logs").select("*").gte("work_date", r.from).lte("work_date", r.to).order("work_date")),
    ]);
    const inRange = (ts) => ts && dayOf(ts) >= r.from && dayOf(ts) <= r.to;
    const byPerson = (t) => !r.person || t.assignee_id === r.person;
    const T = tickets.filter(byPerson);
    const L = logs.filter((l) => !r.person || l.user_id === r.person);
    const completed = T.filter((t) => t.status === "done" && inRange(t.resolved_at)).sort((a, b) => a.resolved_at.localeCompare(b.resolved_at));
    const created = T.filter((t) => inRange(t.created_at));
    const active = T.filter((t) => ["in_progress", "in_review"].includes(t.status)).sort(sortTickets);
    const blocked = T.filter((t) => t.status === "blocked").sort(sortTickets);
    const overdue = T.filter(isOverdue).sort(sortTickets);
    const upcoming = T.filter((t) => OPEN.includes(t.status) && t.due_date && t.due_date >= localISO() && t.due_date <= addDays(localISO(), 7)).sort((a, b) => a.due_date.localeCompare(b.due_date));
    const totalHours = L.reduce((s, l) => s + Number(l.hours), 0);
    const tById = Object.fromEntries(tickets.map((t) => [t.id, t]));
    const perPerson = {};
    L.forEach((l) => (perPerson[l.user_id] = (perPerson[l.user_id] || 0) + Number(l.hours)));
    const perTicket = {};
    L.forEach((l) => (perTicket[l.ticket_id] = (perTicket[l.ticket_id] || 0) + Number(l.hours)));

    const table = (rows, cols, empty) => rows.length ? `<div class="table-wrap"><table><thead><tr>${cols.map((c) => `<th class="${c.num ? "num" : ""}">${c.h}</th>`).join("")}</tr></thead>
      <tbody>${rows.map((x) => `<tr>${cols.map((c) => `<td class="${c.num ? "num" : ""}">${c.v(x)}</td>`).join("")}</tr>`).join("")}</tbody></table></div>` : `<p class="muted">${empty}</p>`;
    const C = {
      key: { h: "Key", v: (t) => `<span class="key">${esc(t.key)}</span>` },
      title: { h: "Ticket", v: (t) => esc(t.title) },
      type: { h: "Type", v: (t) => esc(TYPES[t.type]) },
      who: { h: "Assignee", v: (t) => esc(name(t.assignee_id)) },
      prio: { h: "Priority", v: (t) => t.priority },
      status: { h: "Status", v: (t) => statusPill(t.status) },
      due: { h: "Due", v: (t) => `<span class="${isOverdue(t) ? "overdue" : ""}">${fmtDate(t.due_date)}</span>` },
      hrs: { h: "Hours", num: true, v: (t) => hrs(perTicket[t.id] || 0) },
    };
    const periodLabel = `${fmtDate(r.from)} – ${fmtDate(r.to)}`;
    app.innerHTML = `
      <div class="card no-print stack">
        <div class="page-head"><div><h1>Progress report</h1><p class="muted">Pick a period, then Print / Save as PDF to send or file it.</p></div>
          <div class="actions"><button class="btn" id="r-csv">Time log CSV</button><button class="btn primary" onclick="window.print()">Print / Save as PDF</button></div></div>
        <div class="filters">
          <label class="field">From<input type="date" id="r-from" value="${r.from}"></label>
          <label class="field">To<input type="date" id="r-to" value="${r.to}"></label>
          <label class="field">Person<select id="r-person">${teamOptions(r.person, "Whole team")}</select></label>
          <label class="field">Quick range<select id="r-quick"><option value="">Choose…</option><option value="7">Last 7 days</option><option value="week">This work week (Sun–Thu)</option><option value="lastweek">Last work week</option><option value="month">This month</option><option value="30">Last 30 days</option></select></label>
        </div>
      </div>

      <article class="report stack">
        <header class="report-head">
          <div><h1>${r.person ? "Individual progress report" : "Team progress report"}</h1>
            <p>${esc(CFG.orgName)}${r.person ? ` · ${esc(name(r.person))}` : ""}</p></div>
          <div class="r"><b>Period:</b> ${periodLabel}<br><b>Prepared by:</b> ${esc(me.full_name)}<br><b>Generated:</b> ${fmtDT(new Date())}</div>
        </header>

        <div class="kpis">
          <div class="kpi"><span class="n">${completed.length}</span><span class="l">Completed</span></div>
          <div class="kpi"><span class="n">${created.length}</span><span class="l">New tickets</span></div>
          <div class="kpi"><span class="n">${active.length}</span><span class="l">In progress / review</span></div>
          <div class="kpi ${blocked.length ? "danger" : ""}"><span class="n">${blocked.length}</span><span class="l">Blocked</span></div>
          <div class="kpi ${overdue.length ? "warn" : ""}"><span class="n">${overdue.length}</span><span class="l">Overdue</span></div>
          <div class="kpi"><span class="n">${hrs(totalHours)}</span><span class="l">Hours logged</span></div>
        </div>

        <section><h2>1. Completed in this period</h2>
          ${table(completed, [C.key, C.title, C.type, C.who, { h: "Completed", v: (t) => fmtDate(t.resolved_at) }, { h: "Sign-off", v: (t) => (t.approved_at ? `<span class="signed">✓ ${esc(name(t.approved_by))}</span>` : `<span class="muted">Pending</span>`) }, C.hrs], "Nothing completed in this period.")}</section>
        <section><h2>2. In progress and in review</h2>${table(active, [C.key, C.title, C.who, C.prio, C.status, C.due, C.hrs], "Nothing in progress.")}</section>
        <section><h2>3. Blocked</h2>${table(blocked, [C.key, C.title, C.who, { h: "Blocked because", v: (t) => esc(t.blocked_reason || "—") }, C.due], "Nothing is blocked.")}</section>
        <section><h2>4. Overdue</h2>${table(overdue, [C.key, C.title, C.who, C.prio, C.status, C.due], "Nothing is overdue.")}</section>
        <section><h2>5. Due in the next 7 days</h2>${table(upcoming, [C.key, C.title, C.who, C.prio, C.status, C.due], "Nothing due in the next 7 days.")}</section>
        <section><h2>6. Time logged</h2>
          ${table(Object.entries(perPerson).sort((a, b) => b[1] - a[1]), [{ h: "Person", v: ([p]) => esc(name(p)) }, { h: "Hours", num: true, v: ([, h]) => hrs(h) }], "No time logged in this period.")}
          ${L.length ? table(L, [{ h: "Date", v: (l) => fmtDate(l.work_date) }, { h: "Person", v: (l) => esc(name(l.user_id)) }, { h: "Ticket", v: (l) => `<span class="key">${esc(tById[l.ticket_id]?.key || "")}</span> ${esc(tById[l.ticket_id]?.title || "")}` }, { h: "Work done", v: (l) => esc(l.note) }, { h: "Hours", num: true, v: (l) => hrs(l.hours) }], "") : ""}
        </section>
        <div class="sign"><div>Prepared by: ${esc(me.full_name)}</div><div>Reviewed and approved by (name, signature, date)</div></div>
      </article>`;
    const set = (k, v) => { reportRange[k] = v; viewReport(); };
    document.getElementById("r-from").onchange = (e) => set("from", e.target.value);
    document.getElementById("r-to").onchange = (e) => set("to", e.target.value);
    document.getElementById("r-person").onchange = (e) => set("person", e.target.value);
    document.getElementById("r-quick").onchange = (e) => {
      const t = localISO(), d = new Date(), dow = d.getDay();
      const sunday = addDays(t, -dow);
      const map = {
        7: [addDays(t, -6), t], 30: [addDays(t, -29), t],
        week: [sunday, addDays(sunday, 4)], lastweek: [addDays(sunday, -7), addDays(sunday, -3)],
        month: [t.slice(0, 8) + "01", t],
      };
      const v = map[e.target.value]; if (!v) return;
      reportRange.from = v[0]; reportRange.to = v[1]; viewReport();
    };
    document.getElementById("r-csv").onclick = () => downloadCSV(`timelog-${r.from}-to-${r.to}.csv`, [
      ["Date", "Person", "Ticket", "Title", "Work done", "Hours"],
      ...L.map((l) => [l.work_date, name(l.user_id), tById[l.ticket_id]?.key, tById[l.ticket_id]?.title, l.note, l.hours]),
    ]);
  }

  // ---------- team (admin) ----------
  async function viewTeam() {
    if (me.role !== "admin") { location.hash = "#/dashboard"; return; }
    const all = await q(sb.from("profiles").select("*").order("created_at"));
    const pending = all.filter((p) => p.role === "pending");
    app.innerHTML = `
      <div class="page-head"><div><h1>Team and access</h1><p class="muted">New people use “Request an account” on the sign-in page. Approve them here and choose their role.</p></div></div>
      ${pending.length ? `<div class="card" style="border-color:var(--warn)"><h2>${pending.length} waiting for approval</h2><p class="muted">Set a role to let them in.</p></div>` : ""}
      <div class="table-wrap"><table><thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Joined</th></tr></thead><tbody>
        ${all.map((p) => `<tr><td><span class="row"><span class="avatar">${esc(initials(p.full_name))}</span>${esc(p.full_name)}</span></td><td>${esc(p.email)}</td>
          <td><select data-role="${p.id}" ${p.id === me.id ? "disabled" : ""}>${options(ROLES, p.role)}</select></td><td class="muted">${fmtDate(p.created_at)}</td></tr>`).join("")}
      </tbody></table></div>
      <div class="card"><h2>What each role can do</h2><div class="table-wrap"><table><thead><tr><th>Role</th><th>Can do</th></tr></thead><tbody>
        <tr><td>Admin (team)</td><td>Everything a team member can, plus approve accounts, change roles and delete tickets.</td></tr>
        <tr><td>Team member</td><td>Create and update tickets, comment, log time, print reports.</td></tr>
        <tr><td>Manager</td><td>See everything, raise requests, comment, change priority and due dates, sign off completed work, print reports.</td></tr>
        <tr><td>Pending approval</td><td>Nothing until approved.</td></tr></tbody></table></div></div>`;
    app.querySelectorAll("[data-role]").forEach((s) => (s.onchange = async () => {
      await q(sb.rpc("set_user_role", { target: s.dataset.role, new_role: s.value }));
      toast("Role updated"); await loadMe(); viewTeam();
    }));
  }

  // ---------- account ----------
  async function viewAccount() {
    app.innerHTML = `
      <div class="page-head"><div><h1>Your account</h1><p class="muted">${esc(session.user.email)} · ${esc(ROLES[me.role])}</p></div>
        <div class="actions"><button class="btn" id="ac-out">Sign out</button></div></div>
      <div class="grid-2"><div class="stack">
        <form class="card stack" id="ac-name"><h2>Display name</h2><label class="field">Full name<input id="ac-n" required value="${esc(me.full_name)}"></label><div><button class="btn primary">Save name</button></div></form>
        <form class="card stack" id="ac-pass"><h2>Change password</h2>
          <label class="field">New password<input type="password" id="ac-p1" minlength="8" required autocomplete="new-password"></label>
          <label class="field">Repeat new password<input type="password" id="ac-p2" minlength="8" required autocomplete="new-password"></label>
          <p class="error" id="ac-err" hidden></p><div><button class="btn primary">Change password</button></div></form>
      </div></div>`;
    document.getElementById("ac-out").onclick = () => sb.auth.signOut();
    document.getElementById("ac-name").onsubmit = async (e) => {
      e.preventDefault();
      await q(sb.from("profiles").update({ full_name: document.getElementById("ac-n").value.trim() }).eq("id", me.id));
      toast("Name saved"); await loadMe(); render();
    };
    document.getElementById("ac-pass").onsubmit = async (e) => {
      e.preventDefault();
      const p1 = document.getElementById("ac-p1").value, p2 = document.getElementById("ac-p2").value, err = document.getElementById("ac-err");
      if (p1 !== p2) { err.textContent = "The two passwords don't match."; err.hidden = false; return; }
      const { error } = await sb.auth.updateUser({ password: p1 });
      if (error) { err.textContent = error.message; err.hidden = false; return; }
      err.hidden = true; e.target.reset(); toast("Password changed");
    };
  }

  // ---------- start ----------
  // Never await Supabase calls inside the auth callback itself (it can deadlock), so defer the work.
  sb.auth.onAuthStateChange((event, s) => {
    const changed = (s?.user?.id || null) !== (session?.user?.id || null);
    session = s;
    if (event === "INITIAL_SESSION" || event === "SIGNED_OUT" || changed) {
      setTimeout(async () => {
        await loadMe();
        if (session && (!location.hash || location.hash === "#/login")) location.hash = "#/dashboard";
        render();
      }, 0);
    }
  });
  // Refresh live views when you come back to the tab
  window.addEventListener("focus", () => {
    const v = (location.hash || "#/dashboard").split("/")[1];
    if (session && me && me.role !== "pending" && (v === "dashboard" || v === "board")) render();
  });
})();
