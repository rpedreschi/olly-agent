#!/usr/bin/env node
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const {
  PORT = '3000',
  BROADCAST_AUTH_TOKEN,
} = process.env;

const clients = new Set();

function broadcast(type, data) {
  const payload = `data: ${JSON.stringify({ type, ...data })}\n\n`;
  for (const res of clients) res.write(payload);
}

const indexPath = path.join(__dirname, 'public', 'index.html');

const httpServer = http.createServer(async (req, res) => {
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

  if (req.url === '/broadcast' && req.method === 'POST') {
    if (BROADCAST_AUTH_TOKEN) {
      if (req.headers['authorization'] !== `Bearer ${BROADCAST_AUTH_TOKEN}`) {
        res.writeHead(401); res.end('Unauthorized'); return;
      }
    }
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    try {
      const { type = 'action', ...data } = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      broadcast(type, data);
      res.writeHead(204); res.end();
    } catch (err) {
      res.writeHead(400); res.end(`Bad request: ${err.message}`);
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
});
