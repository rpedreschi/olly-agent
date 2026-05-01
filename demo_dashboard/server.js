#!/usr/bin/env node
import { Kafka, logLevel } from 'kafkajs';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Config — reuses the same Kafka credentials as the rest of the demo
// ---------------------------------------------------------------------------

const {
  KAFKA_BOOTSTRAP,
  KAFKA_SASL_USERNAME,
  KAFKA_SASL_PASSWORD,
  AGENT_ACTIONS_TOPIC = 'mcp_agent_actions',
  AUDIT_TOPIC         = 'mcp-oci-audit',
  PORT                = '3000',
} = process.env;

for (const [name, val] of Object.entries({ KAFKA_BOOTSTRAP, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD })) {
  if (!val) {
    process.stderr.write(`Missing required environment variable: ${name}\n`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// SSE broadcast
// ---------------------------------------------------------------------------

const clients = new Set();

function broadcast(type, data) {
  const payload = `data: ${JSON.stringify({ type, ...data })}\n\n`;
  for (const res of clients) res.write(payload);
}

// ---------------------------------------------------------------------------
// HTTP server — serves the dashboard and the /events SSE stream
// ---------------------------------------------------------------------------

const indexPath = path.join(__dirname, 'public', 'index.html');

const httpServer = http.createServer((req, res) => {
  if (req.url === '/events') {
    res.writeHead(200, {
      'Content-Type':                'text/event-stream',
      'Cache-Control':               'no-cache',
      'Connection':                  'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    res.write('retry: 3000\n\n');
    clients.add(res);
    req.on('close', () => clients.delete(res));
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
});

// ---------------------------------------------------------------------------
// Kafka consumers
// Use a timestamped group ID so restarts don't replay from a committed offset.
// ---------------------------------------------------------------------------

const kafka = new Kafka({
  clientId: 'demo-dashboard',
  brokers:  [KAFKA_BOOTSTRAP],
  ssl:      true,
  sasl: {
    mechanism: 'scram-sha-512',
    username:  KAFKA_SASL_USERNAME,
    password:  KAFKA_SASL_PASSWORD,
  },
  logLevel: logLevel.ERROR,
});

async function consume(topic, groupSuffix, handler) {
  const consumer = kafka.consumer({ groupId: `demo-dashboard-${groupSuffix}-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  await consumer.run({ eachMessage: async ({ message }) => {
    try { handler(JSON.parse(message.value.toString())); } catch {}
  }});
}

async function startConsumers() {
  // Agent actions — every record gets broadcast
  await consume(AGENT_ACTIONS_TOPIC, 'actions', (record) => {
    broadcast('action', record);
  });

  // Audit feed — only surface 4xx / 5xx events as anomalies
  await consume(AUDIT_TOPIC, 'audit', (record) => {
    const status = String(record.responsestatus ?? '');
    if (status.startsWith('4') || status.startsWith('5')) {
      broadcast('anomaly', record);
    }
  });
}

startConsumers().catch((err) => {
  process.stderr.write(`Kafka error: ${err.message}\n`);
  process.exit(1);
});
