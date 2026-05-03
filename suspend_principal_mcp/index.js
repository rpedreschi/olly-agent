#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { Kafka, logLevel } from 'kafkajs';
import { randomUUID } from 'crypto';
import http from 'http';

// ---------------------------------------------------------------------------
// Configuration — all values come from mcp_config.json env block
// ---------------------------------------------------------------------------

const {
  OCI_SUSPEND_FUNCTION_ENDPOINT,
  OCI_FUNCTION_AUTH_TOKEN,
  KAFKA_BOOTSTRAP,
  KAFKA_SASL_USERNAME,
  KAFKA_SASL_PASSWORD,
  AGENT_ACTIONS_TOPIC = 'mcp_agent_actions',
  MOCK_OCI_FUNCTION = 'false',
  KAFKA_SASL_MECHANISM = 'scram-sha-512',
  MCP_TRANSPORT = 'stdio',
  MCP_HTTP_PORT = '3001',
  MCP_HTTP_AUTH_TOKEN,
} = process.env;

const MOCK_MODE = MOCK_OCI_FUNCTION === 'true';

const REQUIRED = MOCK_MODE
  ? { KAFKA_BOOTSTRAP, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD }
  : { OCI_SUSPEND_FUNCTION_ENDPOINT, OCI_FUNCTION_AUTH_TOKEN, KAFKA_BOOTSTRAP, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD };

for (const [name, val] of Object.entries(REQUIRED)) {
  if (!val) {
    process.stderr.write(`Missing required environment variable: ${name}\n`);
    process.exit(1);
  }
}

if (MOCK_MODE) {
  process.stderr.write('suspend-principal MCP: running in MOCK mode — OCI Function calls are simulated\n');
}

// ---------------------------------------------------------------------------
// Kafka producer — stays connected for the lifetime of the process.
// Uses SASL/SCRAM-SHA-512, matching the oci_streaming_kafka store in 01_store.sql.
// ---------------------------------------------------------------------------

const kafka = new Kafka({
  clientId: 'suspend-principal-mcp',
  brokers: KAFKA_BOOTSTRAP.split(',')
    .map((b) => b.trim().replace(/^\[([^\]]+)\]\(https?:\/\/[^)]+\)/, '$1'))
    .filter(Boolean),
  ssl: true,
  sasl: {
    mechanism: KAFKA_SASL_MECHANISM,
    username: KAFKA_SASL_USERNAME,
    password: KAFKA_SASL_PASSWORD,
  },
  logLevel: logLevel.ERROR,
});

const producer = kafka.producer();
await producer.connect();

// ---------------------------------------------------------------------------
// OCI Function call
//
// In production: POSTs to the OCI Function endpoint. The function holds the
// IAM credentials and adds the principal to the restricted-access group.
//
// In mock mode (MOCK_OCI_FUNCTION=true): simulates a ~400ms round trip and
// returns a realistic confirmed response. The Kafka write still happens, so
// agent_actions_mv updates live exactly as it would in production.
// ---------------------------------------------------------------------------

async function callOciFunction(principal_id, reason, triggered_by) {
  if (MOCK_MODE) {
    await new Promise((r) => setTimeout(r, 400));
    return {
      ok: true,
      statusCode: 200,
      body: JSON.stringify({
        message: 'Principal added to restricted-access group (simulated)',
        principal_id,
        iam_group: 'restricted-access',
        operation_id: randomUUID(),
      }),
    };
  }

  const res = await fetch(OCI_SUSPEND_FUNCTION_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${OCI_FUNCTION_AUTH_TOKEN}`,
    },
    body: JSON.stringify({ principal_id, reason, triggered_by }),
  });

  const body = await res.text();
  return { ok: res.ok, statusCode: res.status, body };
}

// ---------------------------------------------------------------------------
// MCP server
// ---------------------------------------------------------------------------

const TOOL = {
  name: 'suspend_principal',
  description:
    'Suspends an OCI principal by adding them to the restricted-access IAM group. ' +
    'Use only when a principal is confirmed to be the source of anomalous or unauthorized ' +
    'activity. The suspension is recorded in the agent_actions stream so it can be audited ' +
    'and correlated against the operational data that triggered it. ' +
    'Reversal requires manual intervention by an IAM administrator.',
  inputSchema: {
    type: 'object',
    properties: {
      principal_id: {
        type: 'string',
        description: 'OCID or name of the OCI principal to suspend',
      },
      reason: {
        type: 'string',
        description:
          'Justification for the suspension — include the evidence observed ' +
          '(event names, time window, error patterns, etc.)',
      },
      triggered_by: {
        type: 'string',
        description: 'Identifier of the agent or process requesting the action',
      },
    },
    required: ['principal_id', 'reason', 'triggered_by'],
  },
};

const server = new Server(
  { name: 'suspend-principal', version: '1.0.0' },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: [TOOL] }));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  if (name !== 'suspend_principal') {
    throw new Error(`Unknown tool: ${name}`);
  }

  const { principal_id, reason, triggered_by } = args;
  const action_id = randomUUID();
  const action_time = new Date().toISOString();

  // Call the OCI Function — capture the outcome regardless of success/failure
  // so it always gets written to the audit stream.
  let oci_response;
  let status;
  try {
    const result = await callOciFunction(principal_id, reason, triggered_by);
    oci_response = result.body;
    status = result.ok ? 'confirmed' : 'failed';
  } catch (err) {
    oci_response = err.message;
    status = 'failed';
  }

  const record = {
    action_id,
    action_type: 'suspend_principal',
    target_id: principal_id,
    reason,
    triggered_by,
    action_time,
    status,
    oci_response,
  };

  // Write to agent_actions Kafka topic so the action appears in agent_actions_mv.
  // If the Kafka write fails, surface a warning alongside the result — the agent
  // should still know the OCI Function outcome even if the audit write failed.
  let auditWarning = '';
  try {
    await producer.send({
      topic: AGENT_ACTIONS_TOPIC,
      messages: [{ key: action_id, value: JSON.stringify(record) }],
    });
  } catch (err) {
    auditWarning =
      `\n\nAudit warning: the action was executed but the record could not be ` +
      `written to the ${AGENT_ACTIONS_TOPIC} topic: ${err.message}`;
  }

  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify(record, null, 2) + auditWarning,
      },
    ],
  };
});

// ---------------------------------------------------------------------------
// Startup and graceful shutdown
// ---------------------------------------------------------------------------

const shutdown = async () => {
  await producer.disconnect();
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

if (MCP_TRANSPORT === 'http') {
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  await server.connect(transport);

  const httpServer = http.createServer(async (req, res) => {
    if (req.url !== '/mcp') {
      res.statusCode = 404;
      res.end('Not found');
      return;
    }

    if (MCP_HTTP_AUTH_TOKEN) {
      const auth = req.headers['authorization'];
      if (auth !== `Bearer ${MCP_HTTP_AUTH_TOKEN}`) {
        res.statusCode = 401;
        res.end('Unauthorized');
        return;
      }
    }

    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : undefined;
    await transport.handleRequest(req, res, body);
  });

  httpServer.listen(Number(MCP_HTTP_PORT), () => {
    process.stderr.write(`suspend-principal MCP: HTTP transport listening on :${MCP_HTTP_PORT} at /mcp\n`);
  });
} else {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}
