import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';

function stream(response, delta) {
  const tool = Boolean(delta.tool_calls);
  response.writeHead(200, { 'Content-Type': 'text/event-stream' });
  for (const [part, finish_reason] of [[{ role: 'assistant', ...delta }, null], [{}, tool ? 'tool_calls' : 'stop']]) {
    response.write(`data: ${JSON.stringify({
      id: 'fixture', object: 'chat.completion.chunk', created: 0, model: 'deterministic',
      choices: [{ index: 0, delta: part, finish_reason }],
    })}\n\n`);
  }
  response.end('data: [DONE]\n\n');
}

function toolCall(name, args) {
  return { tool_calls: [{
    index: 0, id: `fixture-${name}`, type: 'function',
    function: { name, arguments: JSON.stringify(args) },
  }] };
}

function content(message) {
  if (typeof message?.content === 'string') return message.content;
  return (message?.content ?? []).map((block) => block.text ?? '').join('');
}

const alphaTask = 'start alpha: replace the seeded README.md line with alpha-workspace; change no other files';

async function complete({ messages }) {
  const tasks = messages.filter(({ role }) => role === 'user').map(content);
  const task = tasks.at(-1);
  const last = messages.at(-1);
  const startTask = tasks[0];
  assert.ok(startTask === alphaTask || startTask === 'start beta', 'missing original Thread prompt');
  const alpha = startTask === alphaTask;
  const name = alpha ? 'alpha' : 'beta';
  const workspace = `${name}-workspace`;
  const session = `${name}-session`;
  const path = alpha ? 'README.md' : 'workspace-note';

  if (task === startTask) {
    assert.equal(tasks.length, 1);
    if (last.role === 'tool') {
      assert.match(content(last), alpha ? /Successfully replaced 1 block\(s\) in README\.md/ : /Successfully wrote/);
      return { content: `started ${name}; remember ${session}` };
    }
    assert.equal(last.role, 'user');
    await writeFile(`/tmp/${name}-ready`, '');
    const deadline = Date.now() + 60_000;
    while (!existsSync('/tmp/release')) {
      assert.ok(Date.now() < deadline, `timed out waiting to release ${name}`);
      await delay(100);
    }
    if (alpha) {
      return toolCall('edit', { path, edits: [{ oldText: 'Disposable local Compose worker checkout.', newText: workspace }] });
    }
    return toolCall('write', { path, content: workspace });
  }

  assert.deepEqual(tasks, !alpha ? [startTask, 'recall both'] :
    task === 'check first' ? [startTask, 'check first'] : [startTask, 'check first', 'recall both']);
  const initialReply = messages.find((msg) => msg.role === 'assistant' && content(msg).startsWith('started '));
  assert.equal(content(initialReply), `started ${name}; remember ${session}`);
  if (last.role === 'user') return toolCall('read', { path });
  assert.equal(last.role, 'tool');
  assert.equal(content(last).trim(), workspace);
  const recalled = `${content(last).trim()} / ${content(initialReply).split('remember ')[1]}`;
  return { content: task === 'check first' ? `checked ${recalled}` : `${recalled}\n${'0'.repeat(9000)}` };
}

createServer(async (request, response) => {
  if (request.method === 'GET' && request.url === '/health') {
    response.end('ok');
    return;
  }
  if (request.method !== 'POST' || request.url !== '/v1/chat/completions') {
    response.writeHead(404).end();
    return;
  }
  try {
    assert.equal(request.headers.authorization, 'Bearer task-board-fixture-only');
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const input = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    assert.equal(input.model, 'deterministic');
    assert.equal(input.stream, true);
    const result = process.env.FACTORY_TASK_BOARD_DEMO === '1'
      ? { content: 'Mock model completed this turn. No external model was called and no files were changed.' }
      : await complete(input);
    stream(response, result);
  } catch (error) {
    console.error(error);
    if (!response.headersSent) response.writeHead(400);
    response.end('Unexpected fixture request');
  }
}).listen(8080);
