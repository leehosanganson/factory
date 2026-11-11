import { useCallback, useEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Session = { authenticated: boolean; csrf?: string };
type Job = { id: string; status: string; created_at: string; updated_at: string; submitted_by?: string; worker?: { worker_id: string; boot_id: string }; progress?: { stage: string; state: string }; provider?: { repository: string; number: number; url?: string }; verification?: { checks?: { name: string; outcome: string }[]; limitations?: string[] } };
type History = { events?: { at: string; type: string; stage?: string; state?: string }[] };
type Schedule = { id: string; status: string; cron: string; revision: number; pending_occurrence_at?: string; skipped_occurrences?: { count: number; first_at: string; last_at: string } };
type ScheduleDefinition = { id: string; status: string; cron: string; repository: string; task: string };
type Thread = { id: string; status: string; created_at: string; updated_at: string; completed_at?: string };
type ThreadPage = { threads?: Thread[]; has_more?: boolean; next_sequence?: number; snapshot_sequence?: number };
type ApiError = Error & { code?: string };
const api = async <T,>(path: string, options: RequestInit = {}, csrf = '', onResponse?: (response: Response) => void): Promise<T> => {
  const headers = new Headers(options.headers || {});
  if (options.method && options.method !== 'GET') headers.set('X-CSRF-Token', csrf);
  const response = await fetch(`/ui/api${path}`, { ...options, headers, credentials: 'same-origin', cache: 'no-store' });
  onResponse?.(response);
  if (!response.ok) {
    let message = `Request failed (${response.status}).`;
    let code = '';
    try {
      const body = await response.json();
      if (body.message) message = body.message;
      if (body.error?.code) { code = body.error.code; message = body.error.message || message; }
    } catch { /* Use the bounded status message. */ }
    const error = new Error(message) as ApiError;
    error.code = code;
    throw error;
  }
  return (response.status === 204 ? null : response.json()) as Promise<T>;
};
const idempotency = () => crypto.randomUUID();
const date = (value: string) => new Date(value).toLocaleString();

function App() {
  const [session, setSession] = useState<Session>({ authenticated: false });
  const [token, setToken] = useState('');
  const loginGeneration = useRef(0);
  const [loginPending, setLoginPending] = useState(false);
  const [loginError, setLoginError] = useState('');
  const [notice, setNotice] = useState('');
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState('');
  const [admissionPending, setAdmissionPending] = useState(false);
  const selectionGeneration = useRef(0);
  const activeSelection = useRef('');
  const [detail, setDetail] = useState<{ job: Job; history: History } | null>(null);
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [scheduleLoading, setScheduleLoading] = useState('');
  const [scheduleRevision, setScheduleRevision] = useState(0);
  const [thread, setThread] = useState<Thread | null>(null);
  const [threads, setThreads] = useState<Thread[]>([]);
  const [threadPage, setThreadPage] = useState<ThreadPage>({});
  const threadPageRef = useRef<ThreadPage>({});
  const [threadLoading, setThreadLoading] = useState('');
  const [threadJobs, setThreadJobs] = useState<Job[]>([]);
  const [threadPending, setThreadPending] = useState(false);
  const threadMutationGeneration = useRef(0);
  const threadCreationGeneration = useRef(0);
  const mounted = useRef(true);
  const activeThreadID = useRef('');
  const [schedulePending, setSchedulePending] = useState(false);
  const scheduleKey = useRef('');
  const stream = useRef<EventSource | null>(null);
  const refreshTimer = useRef<number | undefined>(undefined);
  const threadAttempt = useRef<{ fingerprint: string; key: string } | null>(null);
  const appendAttempt = useRef<{ thread: string; fingerprint: string; key: string } | null>(null);
  const scheduleAttempt = useRef<{ fingerprint: string; key: string } | null>(null);
  const admissionAttempt = useRef<{ fingerprint: string; key: string } | null>(null);
  const scheduleGeneration = useRef(0);
  const jobRefreshGeneration = useRef(0);
  const scheduleRefreshGeneration = useRef(0);
  const threadRefreshGeneration = useRef(0);

  const refreshJobs = useCallback(async () => {
    const generation = ++jobRefreshGeneration.current;
    setLoading('Refreshing…');
    try {
      let page = await api<{ jobs?: Job[]; has_more?: boolean; next_sequence?: number; snapshot_sequence?: number }>('/v1/jobs?limit=100', {}, session.csrf);
      let rows = page.jobs || [];
      for (let pages = 1; page.has_more && pages < 10; pages++) {
        page = await api(`/v1/jobs?limit=100&after=${page.next_sequence}&snapshot=${page.snapshot_sequence}`, {}, session.csrf);
        rows = rows.concat(page.jobs || []);
      }
      if (generation !== jobRefreshGeneration.current || !mounted.current) return;
      setJobs(rows.slice(-50));
      setLoading('');
      if (activeSelection.current) await inspect(activeSelection.current, false);
    } catch (error) { if (generation === jobRefreshGeneration.current && mounted.current) { setLoading(''); setNotice((error as Error).message); } }
  }, [session.csrf]);

  const refreshThreads = useCallback(async (older = false) => {
    const generation = ++threadRefreshGeneration.current;
    const current = () => generation === threadRefreshGeneration.current;
    setThreadLoading(older ? 'Loading older Threads…' : 'Refreshing…');
    try {
      const currentPage = threadPageRef.current;
      const query = older && currentPage.has_more ? `?limit=50&before_sequence=${currentPage.next_sequence}&snapshot_sequence=${currentPage.snapshot_sequence}` : '?limit=50';
      const page = await api<ThreadPage>(`/v1/threads${query}`, {}, session.csrf);
      if (!current() || !mounted.current) return;
      setThreads((rows) => older ? rows.concat(page.threads || []) : page.threads || []);
      threadPageRef.current = page;
      setThreadPage(page);
      setThreadLoading('');
    } catch (error) { if (current() && mounted.current) { setThreadLoading(''); setNotice((error as Error).message); } }
  }, [session.csrf]);

  const refreshSchedules = useCallback(async (): Promise<Schedule[]> => {
    const generation = ++scheduleRefreshGeneration.current;
    const current = () => generation === scheduleRefreshGeneration.current;
    setScheduleLoading('Refreshing…');
    try {
      const page = await api<{ schedules?: Schedule[] }>('/v1/schedules', {}, session.csrf);
      if (!current() || !mounted.current) return [];
      const rows = page.schedules || [];
      setSchedules(rows);
      setScheduleRevision(generation);
      if (current()) setScheduleLoading('');
      return rows;
    } catch (error) { if (current() && mounted.current) { setScheduleLoading(''); setNotice((error as Error).message); } return []; }
  }, [session.csrf]);

  async function inspect(id: string, scroll = true) {
    const changed = activeSelection.current !== id;
    const generation = ++selectionGeneration.current;
    activeSelection.current = id;
    try {
      if (changed && stream.current) { stream.current.close(); stream.current = null; }
      const [job, history] = await Promise.all([api<Job>(`/v1/jobs/${encodeURIComponent(id)}`, {}, session.csrf), api<History>(`/v1/jobs/${encodeURIComponent(id)}/history`, {}, session.csrf)]);
      if (generation !== selectionGeneration.current || activeSelection.current !== id) return;
      setDetail({ job, history });
      if (scroll) window.setTimeout(() => document.getElementById('detail')?.scrollIntoView({ behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' }), 0);
      if (!stream.current && !['succeeded', 'failed', 'canceled'].includes(job.status)) {
        const source = new EventSource(`/ui/api/v1/jobs/${encodeURIComponent(id)}/events`);
        for (const event of ['queued', 'running', 'canceled', 'failed', 'succeeded', 'workflow_progress', 'terminal_snapshot']) source.addEventListener(event, () => { window.clearTimeout(refreshTimer.current); refreshTimer.current = window.setTimeout(() => void refreshJobs(), 200); });
        stream.current = source;
      }
      if (['succeeded', 'failed', 'canceled'].includes(job.status) && stream.current) { stream.current.close(); stream.current = null; }
    } catch (error) { if (generation === selectionGeneration.current && activeSelection.current === id) setNotice((error as Error).message); }
  }

  useEffect(() => {
    void fetch('/ui/session', { credentials: 'same-origin', cache: 'no-store' }).then((response) => response.json()).then((value: Session) => { if (value.authenticated) setSession(value); }).catch(() => undefined);
  }, []);
  useEffect(() => {
    if (!session.authenticated) return;
    void refreshJobs(); void refreshSchedules(); void refreshThreads();
    const timer = window.setInterval(() => { void refreshJobs(); void refreshSchedules(); }, 10000);
    return () => window.clearInterval(timer);
  }, [session.authenticated, refreshJobs, refreshSchedules, refreshThreads]);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      stream.current?.close();
      window.clearTimeout(refreshTimer.current);
    };
  }, []);

  async function login(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); if (loginPending) return; const generation = ++loginGeneration.current; mounted.current = true; setLoginPending(true); setLoginError(''); const value = token; setToken('');
    try {
      const response = await fetch('/ui/login', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ token: value }), cache: 'no-store' });
      if (!response.ok) throw new Error('The configured named operator token was not accepted.');
      const newSession = await response.json();
      if (mounted.current && loginGeneration.current === generation) setSession(newSession);
    } catch (error) { if (mounted.current && loginGeneration.current === generation) setLoginError((error as Error).message); }
    finally { if (mounted.current && loginGeneration.current === generation) setLoginPending(false); }
  }
  async function logout() {
    mounted.current = false;
    try { await fetch('/ui/logout', { method: 'POST', credentials: 'same-origin', headers: { 'X-CSRF-Token': session.csrf || '' }, cache: 'no-store' }); }
    finally {
      stream.current?.close(); stream.current = null; activeSelection.current = ''; selectionGeneration.current++; activeThreadID.current = ''; threadMutationGeneration.current++; threadCreationGeneration.current++; loginGeneration.current++; jobRefreshGeneration.current++; scheduleRefreshGeneration.current++; threadRefreshGeneration.current++; scheduleGeneration.current++;
      setThreadPending(false); setSession({ authenticated: false }); setDetail(null); setThread(null); setThreads([]); threadPageRef.current = {}; setThreadPage({}); setThreadJobs([]); setSchedules([]); setNotice('');
      threadAttempt.current = null; appendAttempt.current = null; scheduleAttempt.current = null; scheduleKey.current = ''; admissionAttempt.current = null; setAdmissionPending(false);
    }
  }
  async function admit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); if (admissionPending) return; setAdmissionPending(true); setNotice(''); const formElement = event.currentTarget; const form = new FormData(formElement); const input = { repository: String(form.get('repository') || '').trim(), task: String(form.get('task') || '') }; const fingerprint = JSON.stringify(input);
    if (!admissionAttempt.current || admissionAttempt.current.fingerprint !== fingerprint) admissionAttempt.current = { fingerprint, key: idempotency() };
    const key = admissionAttempt.current.key;
    try {
      const response = await api<{ job: Job }>('/v1/jobs', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: fingerprint }, session.csrf);
      if (!mounted.current || admissionAttempt.current?.key !== key) return;
      admissionAttempt.current = null; formElement.reset(); setNotice(`Job ${response.job.id} admitted.`); await refreshJobs(); await inspect(response.job.id);
    } catch (error) { if (mounted.current) setNotice((error as Error).message); }
    finally { if (mounted.current) setAdmissionPending(false); }
  }
  async function createThread(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); threadCreationGeneration.current++; setNotice(''); const formElement = event.currentTarget; const form = new FormData(formElement); const input = { repository: String(form.get('repository') || '').trim(), task: String(form.get('task') || '') };
    const fingerprint = JSON.stringify(input);
    if (!threadAttempt.current || threadAttempt.current.fingerprint !== fingerprint) threadAttempt.current = { fingerprint, key: idempotency() };
    try {
      const result = await api<{ thread: Thread; job: Job }>('/v1/threads', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': threadAttempt.current.key }, body: fingerprint }, session.csrf);
      if (!mounted.current) return;
      threadCreationGeneration.current++;
      threadMutationGeneration.current++; activeThreadID.current = result.thread.id; setThreadJobs([]); setThread(result.thread); setThreadPending(false); appendAttempt.current = null; threadAttempt.current = null; formElement.reset(); setNotice(`Thread ${result.thread.id} created with Job ${result.job.id}.`); await refreshThreads(); await inspectThread(result.thread.id);
    } catch (error) { if (mounted.current) { setNotice((error as Error).message); setThreadPending(false); } }
  }
  async function inspectThread(id: string) {
    const generation = threadMutationGeneration.current;
    try { const result = await api<{ thread: Thread; jobs?: Job[] }>(`/v1/threads/${encodeURIComponent(id)}`, {}, session.csrf); if (generation === threadMutationGeneration.current && activeThreadID.current === id && mounted.current) { setThread(result.thread); setThreadJobs(result.jobs || []); } }
    catch (error) { if (generation === threadMutationGeneration.current && activeThreadID.current === id && mounted.current) setNotice((error as Error).message); }
  }
  async function appendThread(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); if (!thread || thread.completed_at || threadPending) return;
    const formElement = event.currentTarget; const form = new FormData(formElement); const input = { repository: String(form.get('repository') || '').trim(), task: String(form.get('task') || '') }; const fingerprint = JSON.stringify(input);
    if (!appendAttempt.current || appendAttempt.current.thread !== thread.id || appendAttempt.current.fingerprint !== fingerprint) appendAttempt.current = { thread: thread.id, fingerprint, key: idempotency() };
    const id = thread.id; setThreadPending(true);
    try {
      const result = await api<{ job: Job; replayed: boolean }>(`/v1/threads/${encodeURIComponent(id)}/jobs`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': appendAttempt.current.key }, body: fingerprint }, session.csrf);
      if (activeThreadID.current === id && mounted.current) {
        threadMutationGeneration.current++;
        appendAttempt.current = null;
        formElement.reset();
        setNotice(result.replayed ? `Job ${result.job.id} was already appended to Thread ${id}.` : `Job ${result.job.id} appended to Thread ${id}.`);
        await inspectThread(id);
      }
    } catch (error) {
      if (activeThreadID.current === id && mounted.current) {
        if ((error as ApiError).code === 'thread_closed') { await inspectThread(id); setNotice('This Thread is already completed. Its current state has been refreshed.'); }
        else setNotice((error as Error).message);
      }
    } finally { if (activeThreadID.current === id && mounted.current) setThreadPending(false); }
  }
  async function completeThread() {
    if (!thread || thread.completed_at || threadPending) return; const id = thread.id; setThreadPending(true);
    try {
      await api(`/v1/threads/${encodeURIComponent(id)}/complete`, { method: 'POST' }, session.csrf);
      if (activeThreadID.current === id && mounted.current) { threadMutationGeneration.current++; setNotice(`Thread ${id} completed.`); await inspectThread(id); }
    } catch (error) {
      if (activeThreadID.current === id && mounted.current) {
        if ((error as ApiError).code === 'thread_closed') { await inspectThread(id); setNotice('This Thread is already completed. Its current state has been refreshed.'); }
        else setNotice((error as Error).message);
      }
    } finally { if (activeThreadID.current === id && mounted.current) setThreadPending(false); }
  }
  async function createSchedule(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); if (schedulePending) return; setNotice(''); const formElement = event.currentTarget; const form = new FormData(formElement);
    const input = { cron: String(form.get('cron') || '').trim(), repository: String(form.get('repository') || '').trim(), task: String(form.get('task') || '') }; const fingerprint = JSON.stringify(input);
    if (!scheduleAttempt.current || scheduleAttempt.current.fingerprint !== fingerprint) scheduleAttempt.current = { fingerprint, key: idempotency() };
    const key = scheduleAttempt.current.key;
    scheduleKey.current = key;
    setSchedulePending(true);
    try {
      const response = await api<{ schedule: { id: string }; replayed?: boolean }>('/v1/schedules', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: fingerprint }, session.csrf);
      if (!mounted.current || scheduleAttempt.current?.key !== key) return;
      formElement.reset(); scheduleAttempt.current = null; scheduleKey.current = ''; setNotice(response.replayed ? `Schedule ${response.schedule.id} was already created.` : `Schedule ${response.schedule.id} created.`); await refreshSchedules();
    } catch (error) { if (mounted.current && scheduleAttempt.current?.key === key) setNotice((error as Error).message); }
    finally { if (mounted.current && scheduleKey.current === key) setSchedulePending(false); }
  }

  if (!session.authenticated) return <><header className="topbar"><a className="brand" href="/ui">Factory <span>Control Plane</span></a></header><main><section className="panel narrow" aria-labelledby="login-title"><p className="eyebrow">OPERATOR ACCESS</p><h1 id="login-title">Sign in</h1><p className="muted">Use the configured named operator token. It is held only in this form and is not saved.</p><form onSubmit={login}><label htmlFor="token">Operator token</label><input id="token" name="token" type="password" autoComplete="current-password" required value={token} onChange={(e) => setToken(e.target.value)} /><button type="submit" disabled={loginPending}>Sign in</button></form><p className="error" role="alert">{loginError}</p></section></main></>;

  return <><header className="topbar"><a className="brand" href="/ui">Factory <span>Control Plane</span></a><button className="quiet" type="button" onClick={() => void logout()}>Sign out</button></header><main>
    <div className="welcome"><div><p className="eyebrow">LOCAL JOB SERVICE</p><h1>Jobs</h1><p className="muted">Admit work, monitor safe lifecycle progress, and inspect outcomes.</p></div><button className="secondary" type="button" onClick={() => { void refreshJobs(); void refreshSchedules(); void refreshThreads(); if (thread) void inspectThread(thread.id); }}>Refresh</button></div>
    <div className="notice" role="status" aria-live="polite">{notice}</div>
    <div className="layout"><section className="panel admission" aria-labelledby="admit-title"><h2 id="admit-title">Admit a job</h2><form onSubmit={admit}><label htmlFor="repository">Repository alias</label><input id="repository" name="repository" maxLength={64} required autoComplete="off"/><label htmlFor="task">Task</label><textarea id="task" name="task" rows={6} maxLength={262144} required/><button type="submit" disabled={admissionPending}>{admissionPending ? 'Queueing…' : 'Queue job'}</button></form></section>
    <section className="panel jobs-panel" aria-labelledby="jobs-title"><div className="section-heading"><h2 id="jobs-title">Recent jobs</h2><span className="muted" role="status">{loading}</span></div><div className="job-list" aria-live="polite">{jobs.length ? jobs.map((job) => <article className="job-row" key={job.id}><a className="job-link" href={`#job-${encodeURIComponent(job.id)}`} onClick={(e) => { e.preventDefault(); void inspect(job.id); }}>{job.id}</a><span className={`status ${job.status}`}>{job.status}</span><span className="job-meta">{date(job.created_at)} · Updated {date(job.updated_at)}</span></article>) : <p className="empty">No jobs yet. Submit a task to create the first job.</p>}</div></section></div>
    <section className="panel threads-panel" aria-labelledby="threads-title"><div className="section-heading"><h2 id="threads-title">Threads</h2><span className="muted" role="status">{threadLoading}</span></div><p className="muted">Browse recent Threads and select one to reopen its safe job summary.</p><div className="thread-list" aria-live="polite">{threads.length ? threads.map((item) => <article className="thread-row" key={item.id}><button className="thread-select" type="button" onClick={() => { activeThreadID.current = item.id; threadMutationGeneration.current++; appendAttempt.current = null; void inspectThread(item.id); }}>{item.id}</button><span className={`status ${item.status}`}>{item.status}</span><span className="job-meta">{date(item.created_at)} · Updated {date(item.updated_at)}{item.completed_at ? ` · Completed ${date(item.completed_at)}` : ''}</span></article>) : <p className="empty">No retained Threads yet.</p>}</div>{threadPage.has_more && <button className="secondary" type="button" onClick={() => void refreshThreads(true)}>Load older Threads</button>}<h3>Start a Thread</h3><form className="thread-form" onSubmit={createThread}><label htmlFor="thread-repository">Repository alias</label><input id="thread-repository" name="repository" maxLength={64} required autoComplete="off"/><label htmlFor="thread-task">Task</label><textarea id="thread-task" name="task" rows={4} maxLength={262144} required/><button type="submit">Create Thread</button></form>
      <div className="thread-result" aria-live="polite">{thread && <><h3>Thread {thread.id}</h3><span className={`status ${thread.status}`}>{thread.status}</span>{thread.completed_at && <p className="job-meta">Completed {date(thread.completed_at)}</p>}{threadJobs.map((job) => <article className="thread-job" key={job.id}><strong>Job {job.id}</strong><span className={`status ${job.status}`}>{job.status}</span><span className="job-meta">Created {date(job.created_at)} · Updated {date(job.updated_at)}</span></article>)}</>}</div>
      {thread && !thread.completed_at && <section className="thread-controls" aria-label="Current Thread actions"><h3>Continue this Thread</h3><form className="thread-form" onSubmit={appendThread}><label htmlFor="thread-append-repository">Repository alias</label><input id="thread-append-repository" name="repository" maxLength={64} required autoComplete="off"/><label htmlFor="thread-append-task">Follow-up task</label><textarea id="thread-append-task" name="task" rows={3} maxLength={262144} required/><button type="submit" disabled={threadPending}>Append Job</button></form><button className="danger" type="button" disabled={threadPending} onClick={() => void completeThread()}>Complete Thread</button></section>}</section>
    <section className="panel schedules-panel" aria-labelledby="schedules-title"><div className="section-heading"><h2 id="schedules-title">Schedules</h2><span className="muted" role="status">{scheduleLoading}</span></div><p className="muted">Schedule tasks run on their cron times. Cron uses UTC and supports one-minute minimum recurrence.</p><form className="schedule-form" onSubmit={createSchedule}><label htmlFor="schedule-cron">Cron (UTC)</label><input id="schedule-cron" name="cron" maxLength={128} required autoComplete="off" placeholder="0 9 * * 1-5"/><span className="muted">Five fields; UTC; one-minute minimum recurrence.</span><label htmlFor="schedule-repository">Repository alias</label><input id="schedule-repository" name="repository" maxLength={64} required autoComplete="off"/><label htmlFor="schedule-task">Task</label><textarea id="schedule-task" name="task" rows={4} maxLength={262144} required/><button id="schedule-submit" type="submit" disabled={schedulePending}>Create schedule</button></form>
      <div className="schedule-list" aria-live="polite">{schedules.length ? schedules.map((schedule) => <ScheduleCard key={schedule.id} schedule={schedule} revision={scheduleRevision} csrf={session.csrf || ''} onInspect={(id) => void inspect(id)} onRefresh={refreshSchedules} onError={setNotice}/> ) : <p className="empty">No schedules are configured.</p>}</div></section>
    {detail && <JobDetail key={detail.job.id} job={detail.job} history={detail.history} csrf={session.csrf || ''} onRefresh={() => { void refreshJobs(); }} onError={setNotice} />}
  </main></>;
}

function ScheduleCard({ schedule, revision, csrf, onInspect, onRefresh, onError }: { schedule: Schedule; revision: number; csrf: string; onInspect: (id: string) => void; onRefresh: () => Promise<Schedule[]>; onError: (message: string) => void }) {
  const [runs, setRuns] = useState<{ occurrence_at: string; status: string; job_id: string }[]>([]);
  const [edit, setEdit] = useState<ScheduleDefinition | null>(null);
  const [editETag, setEditETag] = useState('');
  const [busy, setBusy] = useState(false);
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => { let active = true; void api<{ runs?: typeof runs }>(`/v1/schedules/${encodeURIComponent(schedule.id)}/runs?limit=5`, {}, csrf).then((page) => { if (active) setRuns(page.runs || []); }).catch((error) => { if (active) onError((error as Error).message); }); return () => { active = false; }; }, [schedule.id, revision, csrf, onError]);
  async function openEdit() {
    setBusy(true);
    try {
      let etag = '';
      const definition = await api<ScheduleDefinition>(`/v1/schedules/${encodeURIComponent(schedule.id)}`, {}, csrf, (response) => { etag = response.headers.get('ETag') || ''; });
      if (!etag) throw new Error('The schedule response did not include a revision ETag. Refresh and try again.');
      if (mounted.current) { setEdit(definition); setEditETag(etag); }
    } catch (error) { if (mounted.current) onError((error as Error).message); }
    finally { if (mounted.current) setBusy(false); }
  }
  async function mutate(action: 'edit' | 'pause' | 'resume' | 'remove', form?: HTMLFormElement) {
    if (busy) return;
    setBusy(true);
    try {
      let etag = editETag;
      if (action !== 'edit') {
        const latest = await onRefresh();
        const current = latest.find((item) => item.id === schedule.id);
        if (!current) throw new Error('Schedule is no longer available. The list has been refreshed.');
        etag = `"${current.revision}"`;
      }
      let body: string | undefined;
      if (action === 'edit' && form) {
        const values = new FormData(form);
        body = JSON.stringify({ action, cron: String(values.get('cron') || '').trim(), repository: String(values.get('repository') || '').trim(), task: String(values.get('task') || '') });
      } else if (action !== 'remove') body = JSON.stringify({ action });
      await api(`/v1/schedules/${encodeURIComponent(schedule.id)}`, { method: action === 'remove' ? 'DELETE' : 'PATCH', headers: { ...(body ? { 'Content-Type': 'application/json' } : {}), 'If-Match': etag }, ...(body ? { body } : {}) }, csrf);
      if (!mounted.current) return;
      setEdit(null); setEditETag(''); onError(`Schedule ${action === 'remove' ? 'removed' : action === 'pause' ? 'paused' : action === 'resume' ? 'resumed' : 'updated'}.`); await onRefresh();
    } catch (error) {
      if (!mounted.current) return;
      const failure = error as ApiError;
      if (failure.code === 'revision_conflict' || failure.code === 'precondition_required') {
        setEdit(null); setEditETag('');
        await onRefresh();
        onError(failure.code === 'revision_conflict' ? 'Schedule changed since this view was loaded. The list is refreshed; review it and open edit again before trying again.' : 'The server requires a current revision. The list is refreshed; review it and try the action again.');
      } else onError(failure.message);
    } finally { if (mounted.current) setBusy(false); }
  }
  return <article className="schedule-row"><div className="schedule-heading"><h3>{schedule.id}</h3><span className={`status ${schedule.status}`}>{schedule.status}</span></div><p className="schedule-meta"><span className="schedule-cron">UTC · {schedule.cron}</span></p>{schedule.pending_occurrence_at && <p className="schedule-summary">Pending occurrence: {new Date(schedule.pending_occurrence_at).toISOString()}</p>}{schedule.skipped_occurrences && <p className="schedule-summary">Skipped {schedule.skipped_occurrences.count} occurrence{schedule.skipped_occurrences.count === 1 ? '' : 's'} · {new Date(schedule.skipped_occurrences.first_at).toISOString()} to {new Date(schedule.skipped_occurrences.last_at).toISOString()}</p>}<div className="actions" aria-label={`Schedule ${schedule.id} controls`}><button className="secondary" type="button" disabled={busy || schedule.status === 'removed'} onClick={() => void openEdit()}>Edit</button>{schedule.status !== 'removed' && <button className="secondary" type="button" disabled={busy} onClick={() => void mutate(schedule.status === 'paused' ? 'resume' : 'pause')}>{schedule.status === 'paused' ? 'Resume' : 'Pause'}</button>}{schedule.status !== 'removed' && <button className="danger" type="button" disabled={busy} onClick={() => { if (window.confirm('Remove this schedule? Existing Jobs are not changed.')) void mutate('remove'); }}>Remove</button>}</div>{edit && <form className="schedule-form schedule-edit" aria-label={`Edit schedule ${schedule.id}`} onSubmit={(event) => { event.preventDefault(); void mutate('edit', event.currentTarget); }}><h4>Edit schedule</h4><label htmlFor={`edit-cron-${schedule.id}`}>Cron (UTC)</label><input id={`edit-cron-${schedule.id}`} name="cron" maxLength={128} required autoComplete="off" value={edit.cron} onChange={(event) => setEdit({ ...edit, cron: event.target.value })}/><label htmlFor={`edit-repository-${schedule.id}`}>Repository alias</label><input id={`edit-repository-${schedule.id}`} name="repository" maxLength={64} required autoComplete="off" value={edit.repository} onChange={(event) => setEdit({ ...edit, repository: event.target.value })}/><label htmlFor={`edit-task-${schedule.id}`}>Task</label><textarea id={`edit-task-${schedule.id}`} name="task" rows={4} maxLength={262144} required value={edit.task} onChange={(event) => setEdit({ ...edit, task: event.target.value })}/><div className="actions"><button type="submit" disabled={busy}>Save schedule</button><button className="secondary" type="button" disabled={busy} onClick={() => { setEdit(null); setEditETag(''); }}>Cancel</button></div></form>}<h4 className="schedule-runs-title">Recent runs</h4>{runs.length === 0 ? <p className="muted">No retained runs.</p> : <ul className="schedule-runs">{runs.map((run) => <li key={run.job_id}><span className="schedule-run-time">{new Date(run.occurrence_at).toISOString()}</span><span className={`status ${run.status}`}>{run.status}</span><a className="schedule-run-link" href={`#job-${encodeURIComponent(run.job_id)}`} onClick={(e) => { e.preventDefault(); onInspect(run.job_id); }}>Job {run.job_id}</a></li>)}</ul>}</article>;
}
function JobDetail({ job, history, csrf, onRefresh, onError }: { job: Job; history: History; csrf: string; onRefresh: () => void; onError: (message: string) => void }) {
  const [busy, setBusy] = useState(false);
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  const field = (label: string, value?: string) => <div key={label}><span className="detail-label">{label}</span><span className="detail-value">{value || 'Not available'}</span></div>;
  async function cancel() {
    setBusy(true);
    try {
      await api(`/v1/jobs/${encodeURIComponent(job.id)}/cancel`, { method: 'POST' }, csrf);
      onRefresh();
    } catch (error) {
      onError((error as Error).message);
    } finally {
      if (mounted.current) setBusy(false);
    }
  }
  return <section id="detail" className="panel detail" aria-labelledby="detail-title" aria-live="polite"><p className="eyebrow">JOB INSPECTION</p><h2 id="detail-title">Job {job.id}</h2><div className="detail-grid">{field('Status', job.status)}{field('Submitted by', job.submitted_by)}{field('Created', date(job.created_at))}{field('Updated', date(job.updated_at))}{job.worker && field('Worker', job.worker.worker_id)}{job.worker && field('Worker boot', job.worker.boot_id)}{job.progress && field('Stage', `${job.progress.stage} · ${job.progress.state}`)}{job.provider && field('Pull request', `${job.provider.repository}#${job.provider.number}`)}</div>{job.status === 'running' && job.progress && <><p>Current stage: {job.progress.stage} — {job.progress.state}</p><div className="progress-track"><span/></div></>}{job.verification && <><h3>Verification</h3>{(job.verification.checks || []).map((check) => <p key={check.name}>{check.name}: {check.outcome}</p>)}{(job.verification.limitations || []).map((limitation) => <p className="muted" key={limitation}>{limitation}</p>)}</>}{job.provider?.url && <a href={job.provider.url} target="_blank" rel="noopener noreferrer">Open pull request</a>}<h3>Lifecycle</h3>{history.events?.length ? <ol className="event-list">{history.events.map((event, index) => <li key={`${event.at}-${index}`}>{date(event.at)} · {event.type}{event.stage ? ` · ${event.stage} ${event.state || ''}` : ''}</li>)}</ol> : <p className="muted">No retained lifecycle events.</p>}{!['succeeded', 'failed', 'canceled'].includes(job.status) && <div className="actions">{['queued', 'running'].includes(job.status) && <button className="danger" type="button" disabled={busy} onClick={() => void cancel()}>Request cancellation</button>}</div>}</section>;
}

createRoot(document.getElementById('root')!).render(<StrictMode><App/></StrictMode>);
