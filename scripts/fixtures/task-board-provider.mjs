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

async function complete({ model, messages, stream: streaming }) {
  assert.equal(model, 'deterministic');
  assert.equal(streaming, true);
  const tasks = messages.filter(({ role }) => role === 'user').map(content);
  const task = tasks.at(-1);
  const last = messages.at(-1);
  const name = /^start (alpha|beta)$/.exec(tasks[0])?.[1];
  assert.ok(name, 'missing original Thread prompt');
  const workspace = `${name}-workspace`;
  const session = `${name}-session`;
  const path = 'workspace-note';

  if (task === `start ${name}`) {
    assert.equal(tasks.length, 1);
    if (last.role === 'tool') {
      assert.match(content(last), /Successfully wrote/);
      return { content: `started ${name}; remember ${session}` };
    }
    assert.equal(last.role, 'user');
    await writeFile(`/tmp/${name}-ready`, '');
    const deadline = Date.now() + 60_000;
    while (!existsSync('/tmp/release')) {
      assert.ok(Date.now() < deadline, `timed out waiting to release ${name}`);
      await delay(100);
    }
    return toolCall('write', { path, content: workspace });
  }

  assert.deepEqual(tasks, name === 'beta' ? ['start beta', 'recall both'] :
    task === 'check first' ? ['start alpha', 'check first'] : ['start alpha', 'check first', 'recall both']);
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
    const result = await complete(JSON.parse(Buffer.concat(chunks).toString('utf8')));
    stream(response, result);
  } catch (error) {
    console.error(error);
    if (!response.headersSent) response.writeHead(400);
    response.end('Unexpected fixture request');
  }
}).listen(8080);
