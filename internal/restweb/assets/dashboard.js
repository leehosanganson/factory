(() => {
  const byId = (id) => document.getElementById(id);
  let csrf = "";
  let selected = "";
  let stream = null;
  let refreshTimer = null;
  let pollingTimer = null;
  const api = async (path, options = {}) => {
    const headers = new Headers(options.headers || {});
    if (options.method && options.method !== "GET") headers.set("X-CSRF-Token", csrf);
    const response = await fetch(`/ui/api${path}`, { ...options, headers, credentials: "same-origin", cache: "no-store" });
    if (!response.ok) {
      let message = `Request failed (${response.status}).`;
      try { const body = await response.json(); if (body.message) message = body.message; } catch (_) {}
      throw new Error(message);
    }
    return response.status === 204 ? null : response.json();
  };
  const node = (tag, text, className) => { const element = document.createElement(tag); if (text !== undefined) element.textContent = text; if (className) element.className = className; return element; };
  const showNotice = (message) => { byId("notice").textContent = message || ""; };
  const authenticated = (session) => {
    csrf = session.csrf || "";
    byId("login-panel").classList.add("hidden");
    byId("dashboard").classList.remove("hidden");
    byId("logout").classList.remove("hidden");
    refreshJobs();
    if (!pollingTimer) pollingTimer = setInterval(() => { if (!byId("dashboard").classList.contains("hidden")) refreshJobs(); }, 10000);
  };
  async function refreshJobs() {
    byId("loading").textContent = "Refreshing…";
    try {
      let page = await api("/v1/jobs?limit=100");
      let jobs = page.jobs || [];
      for (let pages = 1; page.has_more && pages < 10; pages++) {
        page = await api(`/v1/jobs?limit=100&after=${page.next_sequence}&snapshot=${page.snapshot_sequence}`);
        jobs = jobs.concat(page.jobs || []);
      }
      jobs = jobs.slice(-50);
      const list = byId("job-list"); list.replaceChildren();
      if (jobs.length === 0) {
        list.append(node("p", "No jobs yet. Submit a task to create the first job.", "empty"));
      } else {
        for (const job of jobs) {
          const row = node("article", undefined, "job-row");
          const link = node("a", job.id, "job-link"); link.href = "#job-" + encodeURIComponent(job.id); link.addEventListener("click", (event) => { event.preventDefault(); inspect(job.id); });
          const status = node("span", job.status, "status " + job.status);
          const meta = node("span", `${new Date(job.created_at).toLocaleString()} · Updated ${new Date(job.updated_at).toLocaleString()}`, "job-meta");
          row.append(link, status, meta); list.append(row);
        }
      }
      byId("loading").textContent = "";
      if (selected) await inspect(selected, false);
    } catch (error) { byId("loading").textContent = ""; showNotice(error.message); }
  }
  async function inspect(id, scroll = true) {
    const changedSelection = selected !== id;
    selected = id;
    try {
      const [job, history] = await Promise.all([api(`/v1/jobs/${encodeURIComponent(id)}`), api(`/v1/jobs/${encodeURIComponent(id)}/history`)]);
      const panel = byId("detail"); panel.replaceChildren(); panel.classList.remove("hidden");
      panel.append(node("p", "JOB INSPECTION", "eyebrow"), node("h2", `Job ${job.id}`, ""));
      const grid = node("div", undefined, "detail-grid");
      const field = (label, value) => { const wrap = node("div"); wrap.append(node("span", label, "detail-label"), node("span", value || "Not available", "detail-value")); return wrap; };
      grid.append(field("Status", job.status), field("Submitted by", job.submitted_by), field("Created", new Date(job.created_at).toLocaleString()), field("Updated", new Date(job.updated_at).toLocaleString()));
      if (job.worker) grid.append(field("Worker", job.worker.worker_id), field("Worker boot", job.worker.boot_id));
      if (job.progress) grid.append(field("Stage", `${job.progress.stage} · ${job.progress.state}`));
      if (job.provider) grid.append(field("Pull request", `${job.provider.repository}#${job.provider.number}`));
      panel.append(grid);
      if (job.status === "running" && job.progress) { panel.append(node("p", `Current stage: ${job.progress.stage} — ${job.progress.state}`)); const track = node("div", undefined, "progress-track"); track.append(node("span")); panel.append(track); }
      if (job.verification) {
        panel.append(node("h3", "Verification"));
        for (const check of job.verification.checks || []) panel.append(node("p", `${check.name}: ${check.outcome}`));
        for (const limitation of job.verification.limitations || []) panel.append(node("p", limitation, "muted"));
      }
      if (job.provider && job.provider.url) { const link = node("a", "Open pull request"); link.href = job.provider.url; link.target = "_blank"; link.rel = "noopener noreferrer"; panel.append(link); }
      panel.append(node("h3", "Lifecycle"));
      if (!history.events || history.events.length === 0) panel.append(node("p", "No retained lifecycle events.", "muted"));
      else {
        const events = node("ol", undefined, "event-list");
        for (const event of history.events) events.append(node("li", `${new Date(event.at).toLocaleString()} · ${event.type}${event.stage ? ` · ${event.stage} ${event.state || ""}` : ""}`));
        panel.append(events);
      }
      if (!job.status || !["succeeded", "failed", "canceled"].includes(job.status)) {
        const actions = node("div", undefined, "actions");
        if (job.status === "queued" || job.status === "running") {
          const cancel = node("button", "Request cancellation", "danger"); cancel.type = "button"; cancel.addEventListener("click", async () => { cancel.disabled = true; try { await api(`/v1/jobs/${encodeURIComponent(id)}/cancel`, { method: "POST" }); showNotice("Cancellation requested. Work stops cooperatively; refresh to confirm the outcome."); await inspect(id, false); await refreshJobs(); } catch (error) { showNotice(error.message); cancel.disabled = false; } }); actions.append(cancel);
        }
        panel.append(actions);
      }
      if (scroll) panel.scrollIntoView({ behavior: "smooth", block: "start" });
      if (changedSelection && stream) { stream.close(); stream = null; }
      if (!stream && !["succeeded", "failed", "canceled"].includes(job.status)) {
        stream = new EventSource(`/ui/api/v1/jobs/${encodeURIComponent(id)}/events`);
        stream.addEventListener("queued", update); stream.addEventListener("running", update); stream.addEventListener("canceled", update); stream.addEventListener("failed", update); stream.addEventListener("succeeded", update); stream.addEventListener("workflow_progress", update); stream.addEventListener("terminal_snapshot", update);
      }
      if (["succeeded", "failed", "canceled"].includes(job.status) && stream) { stream.close(); stream = null; }
    } catch (error) { showNotice(error.message); }
  }
  function update() { if (selected) { clearTimeout(refreshTimer); refreshTimer = setTimeout(() => { refreshJobs(); }, 200); } }
  byId("login-form").addEventListener("submit", async (event) => {
    event.preventDefault(); byId("login-error").textContent = "";
    const input = byId("token"); const token = input.value; input.value = "";
    try {
      const response = await fetch("/ui/login", { method: "POST", credentials: "same-origin", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body: new URLSearchParams({ token }), cache: "no-store" });
      if (!response.ok) throw new Error("The configured named operator token was not accepted.");
      authenticated(await response.json());
    } catch (error) { byId("login-error").textContent = error.message; }
  });
  byId("admit-form").addEventListener("submit", async (event) => {
    event.preventDefault(); showNotice("");
    const form = new FormData(event.currentTarget); const task = String(form.get("task") || "");
    try {
      const response = await api("/v1/jobs", { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": crypto.randomUUID() }, body: JSON.stringify({ repository: String(form.get("repository") || ""), task }) });
      event.currentTarget.reset(); showNotice(`Job ${response.job.id} admitted.`); await refreshJobs(); await inspect(response.job.id);
    } catch (error) { showNotice(error.message); }
  });
  byId("refresh").addEventListener("click", refreshJobs);
  byId("logout").addEventListener("click", async () => {
    try { await fetch("/ui/logout", { method: "POST", credentials: "same-origin", headers: { "X-CSRF-Token": csrf }, cache: "no-store" }); } finally { if (stream) stream.close(); if (pollingTimer) clearInterval(pollingTimer); pollingTimer = null; csrf = ""; selected = ""; byId("dashboard").classList.add("hidden"); byId("login-panel").classList.remove("hidden"); byId("logout").classList.add("hidden"); byId("detail").classList.add("hidden"); }
  });
  fetch("/ui/session", { credentials: "same-origin", cache: "no-store" }).then((response) => response.json()).then((session) => { if (session.authenticated) authenticated(session); }).catch(() => {});
})();
