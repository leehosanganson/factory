import { useCallback, useLayoutEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';

export type Thread = { id: string; status: string; created_at: string; updated_at: string; completed_at?: string };
export type ThreadRequest = { repository: string; task: string };
type Draft = { key: string; request: ThreadRequest; error?: string };

const submittedLanes = [
  { id: 'work', title: 'Work', status: 'in_progress', description: 'Jobs queued or running.' },
  { id: 'attention', title: 'Needs Attention', status: 'need_attention', description: 'The server recorded a need for attention.' },
  { id: 'finished', title: 'Finished', status: 'done', description: 'Jobs reached an outcome. Not an approval.' },
  { id: 'unknown', title: 'Unclassified', status: null, description: 'The server returned a status this board does not recognize.' },
] as const;

export function ThreadBoard({ threads, onSelect, onQueue }: {
  threads: Thread[];
  onSelect: (id: string) => void;
  onQueue: (request: ThreadRequest, key: string) => Promise<void>;
}) {
  const [drafts, setDrafts] = useState<Draft[]>([]);
  const [pending, setPending] = useState<string | null>(null);
  const [dragging, setDragging] = useState<string | null>(null);
  const [overWork, setOverWork] = useState(false);
  const inFlight = useRef(false);
  const draggedKey = useRef<string | null>(null);
  const ticketButtons = useRef(new Map<string, HTMLButtonElement>());
  const detachedFocus = useRef<{ id: string; button: HTMLButtonElement } | null>(null);

  const registerTicket = useCallback((button: HTMLButtonElement | null) => {
    if (!button) return;
    const id = button.dataset.threadId!;
    ticketButtons.current.set(id, button);
    return () => {
      // A lane change unmounts the old button. Capture focus before React removes it.
      if (button.ownerDocument.activeElement === button) detachedFocus.current = { id, button };
      ticketButtons.current.delete(id);
    };
  }, []);

  useLayoutEffect(() => {
    const previous = detachedFocus.current;
    detachedFocus.current = null;
    if (!previous || previous.button.isConnected) return;
    const doc = previous.button.ownerDocument;
    if (doc.activeElement === doc.body) ticketButtons.current.get(previous.id)?.focus({ preventScroll: true });
  }, [threads]);

  function addDraft(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const repository = String(form.get('repository') || '').trim();
    const task = String(form.get('task') || '');
    if (!repository || !task.trim()) return;
    const draft = { key: crypto.randomUUID(), request: { repository, task } };
    setDrafts((rows) => [...rows, draft]);
    event.currentTarget.reset();
  }

  function endDrag() {
    draggedKey.current = null;
    setDragging(null);
    setOverWork(false);
  }

  async function queue(key: string) {
    const draft = drafts.find((item) => item.key === key);
    if (!draft || inFlight.current) return;
    inFlight.current = true;
    endDrag();
    setPending(key);
    setDrafts((rows) => rows.map((row) => row.key === key ? { ...row, error: undefined } : row));
    try {
      await onQueue(draft.request, key);
      setDrafts((rows) => rows.filter((row) => row.key !== key));
    } catch (error) {
      setDrafts((rows) => rows.map((row) => row.key === key ? { ...row, error: (error as Error).message } : row));
    } finally {
      inFlight.current = false;
      setPending(null);
    }
  }

  return <div className="ticket-board">
    <section className="ticket-lane" data-lane="drafts" aria-labelledby="lane-drafts">
      <div className="lane-heading"><h3 id="lane-drafts">Drafts</h3><span className="lane-count">{drafts.length}</span></div>
      <p className="lane-caption">Only in this tab. No Job starts until you queue it.</p>
      <form className="draft-form" onSubmit={addDraft} aria-label="New draft" autoComplete="off">
        <label htmlFor="draft-repository">Repository alias</label>
        <input id="draft-repository" name="repository" maxLength={64} required onChange={(event) => event.currentTarget.setCustomValidity(event.currentTarget.value.trim() ? '' : 'Enter a repository alias.')}/>
        <label htmlFor="draft-task">Task</label>
        <textarea id="draft-task" name="task" rows={3} maxLength={262144} required onChange={(event) => event.currentTarget.setCustomValidity(event.currentTarget.value.trim() ? '' : 'Enter a task.')}/>
        <button className="secondary" type="submit">Add draft</button>
      </form>
      <div className="ticket-stack">
        {drafts.map((draft) => <article className={`ticket draft-ticket${dragging === draft.key ? ' is-dragging' : ''}`} key={draft.key}
          draggable={pending === null} onDragStart={(event) => {
            if (inFlight.current) { event.preventDefault(); return; }
            draggedKey.current = draft.key;
            event.dataTransfer.effectAllowed = 'move';
            event.dataTransfer.setData('application/x-factory-draft', draft.key);
            setDragging(draft.key);
          }} onDragEnd={endDrag}>
          <div className="ticket-label"><span>Local draft</span><span className="drag-grip" aria-hidden="true"/></div>
          <p className="draft-task">{draft.request.task}</p>
          <span className="ticket-meta">Repo / {draft.request.repository}</span>
          {draft.error && <p className="draft-error" role="alert">{draft.error} You can retry with Queue.</p>}
          <div className="ticket-actions">
            <button type="button" aria-disabled={pending !== null} onClick={() => void queue(draft.key)}>{pending === draft.key ? 'Queueing…' : 'Queue'}</button>
            <button type="button" className="secondary" disabled={pending === draft.key} onClick={() => setDrafts((rows) => rows.filter((row) => row.key !== draft.key))}>Discard</button>
          </div>
        </article>)}
      </div>
    </section>
    {submittedLanes.map(({ id, title, status, description }) => {
      const rows = threads.filter((thread) => status === null ? !submittedLanes.some((lane) => lane.status === thread.status) : thread.status === status);
      if (status === null && !rows.length) return null;
      return <section className="ticket-lane" data-lane={id} key={id} aria-labelledby={`lane-${id}`}>
        <div className="lane-heading">
          {id === 'attention' && <span className="attention-mark" aria-hidden="true">!</span>}
          <h3 id={`lane-${id}`}>{title}</h3><span className="lane-count">{rows.length}</span>
        </div>
        <p className="lane-caption">{description}</p>
        {id === 'work' && <div role="region" aria-label="Work drop zone" className={`work-drop${dragging ? ' is-ready' : ''}${overWork ? ' is-over' : ''}`}
          onDragOver={(event) => {
            if (!draggedKey.current || inFlight.current) return;
            event.preventDefault();
            event.dataTransfer.dropEffect = 'move';
            setOverWork(true);
          }} onDragLeave={() => setOverWork(false)} onDrop={(event) => {
            if (!draggedKey.current || inFlight.current || event.dataTransfer.getData('application/x-factory-draft') !== draggedKey.current) return;
            event.preventDefault();
            void queue(draggedKey.current);
          }}>
          <span aria-hidden="true">+</span> Drop a draft here to queue it
        </div>}
        <div className="ticket-stack">
          {rows.length ? rows.map((thread) => <article className="ticket" key={thread.id}>
            <div className="ticket-label"><span>Thread</span><span className="ticket-state">{thread.status}</span></div>
            <button type="button" className="ticket-open thread-select" data-thread-id={thread.id} ref={registerTicket} onClick={() => onSelect(thread.id)}>{thread.id}</button>
            <time className="ticket-meta" dateTime={thread.updated_at}>Updated {new Date(thread.updated_at).toLocaleString()}</time>
          </article>) : <p className="lane-empty">No Threads here.</p>}
        </div>
      </section>;
    })}
  </div>;
}
