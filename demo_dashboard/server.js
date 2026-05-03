#!/usr/bin/env node
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const {
  PORT = '3000',
  BROADCAST_AUTH_TOKEN,
  OPENAI_API_KEY,
  OPENAI_MODEL = 'gpt-4o',
  DELTASTREAM_MCP_URL,
  DELTASTREAM_MCP_TOKEN,
  SUSPEND_PRINCIPAL_MCP_URL,
  SUSPEND_PRINCIPAL_MCP_TOKEN,
} = process.env;

const SYSTEM_PROMPT = `You are an OCI operations assistant with access to real-time data from an OCI tenancy.

You have four tools available:
- oci_audit_mv: real-time feed of audit events — API calls, policy changes, resource modifications, identity actions
- oci_logging_mv: real-time feed of OCI Functions invocation logs — what functions ran, what they logged, and any errors
- agent_actions_mv: log of actions you have taken this session — query this to verify what you have already done
- suspend_principal: suspends an OCI principal by adding them to the restricted-access IAM group

When answering questions:
- Always query with a recent time window (last 15–30 minutes unless asked otherwise)
- For failure questions, check responsestatus for 4xx/5xx in audit, and search message for "error", "failed", or "exception" in logging
- For incident questions, use besttraceid to correlate audit events with function logs
- Be specific: name principals, function IDs, timestamps, and status codes in your answers
- If you don't find relevant events in the time window, say so and suggest widening the window

When taking action:
- Only call suspend_principal after you have identified the responsible principal from oci_audit_mv or oci_logging_mv
- State your reasoning explicitly before acting — name the principal, the evidence, and the time window
- After suspending a principal, verify the action by querying agent_actions_mv and confirm status = 'confirmed'
- Before suspending a principal a second time, query agent_actions_mv to confirm you have not already done so`;

function buildMcpTools() {
  const tools = [];
  if (DELTASTREAM_MCP_URL) {
    tools.push({
      type: 'mcp',
      server_label: 'deltastream',
      server_url: DELTASTREAM_MCP_URL,
      require_approval: 'never',
      ...(DELTASTREAM_MCP_TOKEN
        ? { headers: { Authorization: `Bearer ${DELTASTREAM_MCP_TOKEN}` } }
        : {}),
    });
  }
  if (SUSPEND_PRINCIPAL_MCP_URL) {
    tools.push({
      type: 'mcp',
      server_label: 'suspend-principal',
      server_url: SUSPEND_PRINCIPAL_MCP_URL,
      require_approval: 'never',
      ...(SUSPEND_PRINCIPAL_MCP_TOKEN
        ? { headers: { Authorization: `Bearer ${SUSPEND_PRINCIPAL_MCP_TOKEN}` } }
        : {}),
    });
  }
  return tools;
}

const clients = new Set();
const conversations = new Map();

function broadcast(type, data) {
  const payload = `data: ${JSON.stringify({ type, ...data })}\n\n`;
  for (const res of clients) res.write(payload);
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function extractAssistantText(response) {
  if (!response?.output) return '';
  const parts = [];
  for (const item of response.output) {
    if (item.type === 'message' && item.role === 'assistant') {
      for (const c of item.content || []) {
        if (c.type === 'output_text' && c.text) parts.push(c.text);
      }
    }
  }
  return parts.join('\n').trim();
}

const indexPath = path.join(__dirname, 'public', 'index.html');

const httpServer = http.createServer(async (req, res) => {
  if (req.url === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    res.write('retry: 3000\n\n');
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }

  if (req.url === '/broadcast' && req.method === 'POST') {
    if (BROADCAST_AUTH_TOKEN && req.headers['authorization'] !== `Bearer ${BROADCAST_AUTH_TOKEN}`) {
      res.writeHead(401); res.end('Unauthorized'); return;
    }
    try {
      const { type = 'action', ...data } = await readJsonBody(req);
      broadcast(type, data);
      res.writeHead(204); res.end();
    } catch (err) {
      res.writeHead(400); res.end(`Bad request: ${err.message}`);
    }
    return;
  }

  if (req.url === '/chat' && req.method === 'POST') {
    if (!OPENAI_API_KEY) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'OPENAI_API_KEY not configured on dashboard' }));
      return;
    }
    let payload;
    try {
      payload = await readJsonBody(req);
    } catch (err) {
      res.writeHead(400); res.end(`Bad request: ${err.message}`); return;
    }
    const { sessionId, message } = payload;
    if (!sessionId || !message) {
      res.writeHead(400); res.end('sessionId and message required'); return;
    }

    const previous = conversations.get(sessionId);
    const body = {
      model: OPENAI_MODEL,
      instructions: SYSTEM_PROMPT,
      input: message,
      tools: buildMcpTools(),
      ...(previous ? { previous_response_id: previous } : {}),
    };

    try {
      const upstream = await fetch('https://api.openai.com/v1/responses', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${OPENAI_API_KEY}`,
        },
        body: JSON.stringify(body),
      });
      const data = await upstream.json();
      if (!upstream.ok) {
        process.stderr.write(`OpenAI error ${upstream.status}: ${JSON.stringify(data)}\n`);
        res.writeHead(upstream.status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(data));
        return;
      }
      if (data.id) conversations.set(sessionId, data.id);
      const text = extractAssistantText(data);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ text, response_id: data.id }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  if (req.url === '/chat/reset' && req.method === 'POST') {
    try {
      const { sessionId } = await readJsonBody(req);
      conversations.delete(sessionId);
      res.writeHead(204); res.end();
    } catch {
      res.writeHead(400); res.end('Bad request');
    }
    return;
  }

  fs.readFile(indexPath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(data);
  });
});

httpServer.listen(PORT, () => {
  process.stderr.write(`Dashboard → http://localhost:${PORT}\n`);
  process.stderr.write(`POST events to http://localhost:${PORT}/broadcast\n`);
  if (!OPENAI_API_KEY) {
    process.stderr.write(`(chat disabled: set OPENAI_API_KEY to enable /chat)\n`);
  }
});
