(() => {
  const byId = (id) => document.getElementById(id);
  let csrf = "";
  let selected = "";
  let currentThread = "";
  let threadAttempt = null;
  let stream = null;
  let refreshTimer = null;
  let pollingTimer = null;
  let schedulesRefreshing = false;
  let scheduleRefreshPending = false;
  let scheduleRefreshGeneration = 0;
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
    refreshSchedules();
    if (currentThread) inspectThread(currentThread);
    if (!pollingTimer) pollingTimer = setInterval(() => { if (!byId("dashboard").classList.contains("hidden")) { refreshJobs(); refreshSchedules(); } }, 10000);
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
  async function refreshSchedules() {
    if (schedulesRefreshing) { scheduleRefreshPending = true; return; }
    schedulesRefreshing = true;
    const generation = ++scheduleRefreshGeneration;
    const isCurrent = () => generation === scheduleRefreshGeneration && !byId("dashboard").classList.contains("hidden");
    const loading = byId("schedule-loading");
    loading.textContent = "Refreshing…";
    try {
      const page = await api("/v1/schedules");
      if (!isCurrent()) return;
      const schedules = page.schedules || [];
      const list = byId("schedule-list"); list.replaceChildren();
      if (schedules.length === 0) {
        list.append(node("p", "No schedules are configured.", "empty"));
      } else {
        const rows = await Promise.all(schedules.map(async (schedule) => {
          let runs = [];
          try {
            const runPage = await api(`/v1/schedules/${encodeURIComponent(schedule.id)}/runs?limit=5`);
            runs = runPage.runs || [];
          } catch (error) { if (isCurrent()) showNotice(error.message); }
          return { schedule, runs };
        }));
        if (!isCurrent()) return;
        for (const { schedule, runs } of rows) {
          const card = node("article", undefined, "schedule-row");
          const heading = node("div", undefined, "schedule-heading");
          heading.append(node("h3", schedule.id), node("span", schedule.status, "status " + schedule.status));
          card.append(heading);
          const detail = node("p", undefined, "schedule-meta");
          detail.append(node("span", `UTC · ${schedule.cron}`, "schedule-cron"));
          card.append(detail);
          if (schedule.pending_occurrence_at) card.append(node("p", `Pending occurrence: ${new Date(schedule.pending_occurrence_at).toISOString()}`, "schedule-summary"));
          if (schedule.skipped_occurrences) {
            const skipped = schedule.skipped_occurrences;
            card.append(node("p", `Skipped ${skipped.count} occurrence${skipped.count === 1 ? "" : "s"} · ${new Date(skipped.first_at).toISOString()} to ${new Date(skipped.last_at).toISOString()}`, "schedule-summary"));
          }
          card.append(node("h4", "Recent runs", "schedule-runs-title"));
          if (runs.length === 0) card.append(node("p", "No retained runs.", "muted"));
          else {
            const runList = node("ul", undefined, "schedule-runs");
            for (const run of runs.slice(0, 5)) {
              const item = node("li");
              const time = node("span", new Date(run.occurrence_at).toISOString(), "schedule-run-time");
              const status = node("span", run.status, "status " + run.status);
              const link = node("a", `Job ${run.job_id}`, "schedule-run-link");
              link.href = `#job-${encodeURIComponent(run.job_id)}`;
              link.addEventListener("click", (event) => { event.preventDefault(); inspect(run.job_id); });
              item.append(time, status, link); runList.append(item);
            }
            card.append(runList);
          }
          list.append(card);
        }
      }
      loading.textContent = "";
    } catch (error) { if (isCurrent()) { loading.textContent = ""; showNotice(error.message); } }
    finally {
      schedulesRefreshing = false;
      if (scheduleRefreshPending) {
        scheduleRefreshPending = false;
        if (!byId("dashboard").classList.contains("hidden")) refreshSchedules();
      }
    }
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
      if (scroll) panel.scrollIntoView({ behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth", block: "start" });
      if (changedSelection && stream) { stream.close(); stream = null; }
      if (!stream && !["succeeded", "failed", "canceled"].includes(job.status)) {
        stream = new EventSource(`/ui/api/v1/jobs/${encodeURIComponent(id)}/events`);
        stream.addEventListener("queued", update); stream.addEventListener("running", update); stream.addEventListener("canceled", update); stream.addEventListener("failed", update); stream.addEventListener("succeeded", update); stream.addEventListener("workflow_progress", update); stream.addEventListener("terminal_snapshot", update);
      }
      if (["succeeded", "failed", "canceled"].includes(job.status) && stream) { stream.close(); stream = null; }
    } catch (error) { showNotice(error.message); }
  }
  async function inspectThread(id) {
    try {
      const result = await api(`/v1/threads/${encodeURIComponent(id)}`);
      const panel = byId("thread-result"); panel.replaceChildren();
      const thread = result.thread;
      panel.append(node("h3", `Thread ${thread.id}`), node("span", thread.status, "status " + thread.status));
      for (const job of result.jobs || []) {
        const row = node("article", undefined, "thread-job");
        row.append(node("strong", `Job ${job.id}`), node("span", job.status, "status " + job.status));
        row.append(node("span", `Created ${new Date(job.created_at).toLocaleString()} · Updated ${new Date(job.updated_at).toLocaleString()}`, "job-meta"));
        panel.append(row);
      }
      showNotice("");
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
  byId("thread-form").addEventListener("submit", async (event) => {
    event.preventDefault(); showNotice("");
    const form = new FormData(event.currentTarget);
    const input = { repository: String(form.get("repository") || "").trim(), task: String(form.get("task") || "") };
    const fingerprint = JSON.stringify(input);
    if (!threadAttempt || threadAttempt.fingerprint !== fingerprint) threadAttempt = { fingerprint, key: crypto.randomUUID() };
    try {
      const response = await api("/v1/threads", { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": threadAttempt.key }, body: fingerprint });
      currentThread = response.thread.id;
      threadAttempt = null;
      event.currentTarget.reset();
      byId("thread-result").replaceChildren();
      showNotice(`Thread ${response.thread.id} created with Job ${response.job.id}.`);
      await inspectThread(currentThread);
    } catch (error) { showNotice(error.message); }
  });
  byId("refresh").addEventListener("click", () => { refreshJobs(); refreshSchedules(); if (currentThread) inspectThread(currentThread); });
  byId("logout").addEventListener("click", async () => {
    try { await fetch("/ui/logout", { method: "POST", credentials: "same-origin", headers: { "X-CSRF-Token": csrf }, cache: "no-store" }); } finally { if (stream) stream.close(); if (pollingTimer) clearInterval(pollingTimer); pollingTimer = null; csrf = ""; selected = ""; currentThread = ""; threadAttempt = null; byId("thread-result").replaceChildren(); byId("dashboard").classList.add("hidden"); byId("login-panel").classList.remove("hidden"); byId("logout").classList.add("hidden"); byId("schedule-loading").textContent = ""; byId("schedule-list").replaceChildren(); scheduleRefreshGeneration++; scheduleRefreshPending = false; byId("detail").classList.add("hidden"); }
  });
  fetch("/ui/session", { credentials: "same-origin", cache: "no-store" }).then((response) => response.json()).then((session) => { if (session.authenticated) authenticated(session); }).catch(() => {});
})();
