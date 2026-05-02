# OCI Real-Time AI Agent — Reference Implementation

This repo contains everything you need to build a real-time AI agent on top of OCI Logging, using OCI Streaming for Apache Kafka, DeltaStream, and OpenAI Agent Builder.

The companion blog post and demo video walk through the architecture and show the agent in action. This repo is the code behind them.

**Architecture:**
```
OCI Logging → Connector Hub → OCI Streaming → DeltaStream → MCP → AI Agent
```

---

## What's in this repo

```
oci_logging_agent/
├── env.example                           # Credential placeholders — copy to .env
├── 01_store.sql                          # Connect DeltaStream to OCI Streaming
├── 02_raw_streams.sql                    # Define raw input streams from Connector Hub topics
├── 03_enriched_streams.sql               # Transform and enrich raw events
├── 04_materialized_views.sql             # Create agent-queryable views + set descriptions
├── 05_agent_actions_stream.sql           # Agent audit stream + view for suspend_principal actions
├── mcp_config.json                       # MCP server configuration (DeltaStream + suspend_principal)
├── suspend_principal_mcp/
│   ├── package.json                      # Node.js dependencies
│   └── index.js                          # MCP server: calls OCI Function + writes to agent_actions
├── demo_dashboard/
│   ├── package.json                      # Node.js dependencies (kafkajs only)
│   ├── server.js                         # SSE server: tails Kafka, broadcasts to browser
│   └── public/index.html                 # Single-page live dashboard (no build step)
└── connector_hub_setup.md               # Step-by-step Connector Hub configuration guide
```

---

## Prerequisites

- An OCI tenancy with OCI Logging enabled (audit logs and/or service logs)
- A DeltaStream account — see [Resources](#resources) below for how to get access
- An OpenAI account with access to Agent Builder
- Node.js 18+ (for the DeltaStream MCP server)

---

## Setup

### 1. Clone and configure credentials

```bash
git clone https://github.com/deltastreaminc/examples.git
cd examples/oci_logging_agent
cp env.example .env
```

Open `.env` and fill in your values:

| Variable | Where to find it |
|---|---|
| `OCI_STREAMING_BOOTSTRAP` | OCI Console → Streaming → Stream Pools → your pool → Kafka connection settings |
| `OCI_STREAM_POOL_OCID` | OCI Console → Streaming → Stream Pools → your pool |
| `OCI_SASL_USERNAME` | Format: `<tenancy>/<username>/<stream_pool_ocid>` |
| `OCI_AUTH_TOKEN` | OCI Console → Profile (top right) → Auth Tokens → Generate Token |
| `DELTASTREAM_TOKEN` | DeltaStream Console → Integrations → API Tokens |
| `OPENAI_API_KEY` | platform.openai.com → API Keys |

---

### 2. Configure OCI Connector Hub

Connector Hub routes your OCI Logging events into OCI Streaming topics with no code required.

See **[connector_hub_setup.md](connector_hub_setup.md)** for a step-by-step walkthrough.

You will create two service connectors:
- **Audit logs** → OCI Streaming topic (set `OCI_AUDIT_TOPIC` in your `.env`)
- **Service/function logs** → OCI Streaming topic (set `OCI_LOGGING_TOPIC` in your `.env`)

---

### 3. Run the DeltaStream SQL scripts

Run the scripts in order using the DeltaStream console or CLI. Each script builds on the previous one.

```sql
-- In the DeltaStream console, run each file in order:
-- 01_store.sql               → creates two stores (see below)
-- 02_raw_streams.sql         → defines input streams on your Connector Hub topics
-- 03_enriched_streams.sql    → transforms raw events into clean, enriched streams
-- 04_materialized_views.sql  → creates agent-queryable views with descriptions
-- 05_agent_actions_stream.sql → creates the agent audit stream and materialized view
```

**Why two stores?** OCI has two distinct Kafka-compatible streaming services with different auth patterns:

- **`oci_streaming`** (SASL/PLAIN + Auth Token) — this is OCI Streaming native, which is where Connector Hub delivers your raw log events. It uses your OCI tenancy credentials.
- **`oci_streaming_kafka`** (SASL/SCRAM-SHA-512 + super-user) — this is OCI Streaming for Apache Kafka, a fully managed Kafka cluster. DeltaStream writes the enriched output streams here, and the MCP server reads from it.

Both are in `01_store.sql`. Replace the placeholders in each with the corresponding values from your `.env`.

- `<OCI_STREAMING_BOOTSTRAP>` → your OCI Streaming bootstrap server
- `<OCI_SASL_USERNAME>` → your SASL username
- `<OCI_AUTH_TOKEN>` → your OCI auth token
- `<OCI_AUDIT_TOPIC>` → the topic name you configured in Connector Hub
- `<OCI_LOGGING_TOPIC>` → the topic name you configured in Connector Hub

> **Tip:** After running `04_materialized_views.sql`, give the views a minute to backfill before querying. The `starting.position = 'earliest'` setting will process historical events first, then stay current. You can remove this if you do NOT want backfill.

---

### 4. Configure the MCP server

Copy `mcp_config.json` to your OpenAI Agent Builder MCP configuration location and fill in your values.

**DeltaStream MCP server** (read-only — audit, logging, and agent action views):

| Placeholder | Value |
|---|---|
| `<YOUR_DELTASTREAM_API_TOKEN>` | Your DeltaStream API token |
| `<YOUR_DATABASE_NAME>` | The DeltaStream database containing your views |
| `<YOUR_SCHEMA_NAME>` | The DeltaStream schema containing your views |

**suspend-principal MCP server** (write — see step 5 below):

| Placeholder | Value |
|---|---|
| `<YOUR_OCI_FUNCTION_ENDPOINT>` | Invoke URL of the OCI Function that performs the IAM suspension |
| `<YOUR_OCI_FUNCTION_AUTH_TOKEN>` | Auth token for calling the OCI Function endpoint |
| `<OCI_KAFKA_BOOTSTRAP>` | Same bootstrap server used in `01_store.sql` for `oci_streaming_kafka` |
| `<OCI_KAFKA_USERNAME>` | Same SCRAM-SHA-512 username used in `01_store.sql` |
| `<OCI_KAFKA_PASSWORD>` | Same SCRAM-SHA-512 password used in `01_store.sql` |

### 5. Deploy the suspend_principal OCI Function and MCP tool

The `suspend_principal` tool is a two-part integration:

**Mock mode (no OCI Function required)** — set `MOCK_OCI_FUNCTION=true` in `mcp_config.json` (it ships enabled by default). The MCP server simulates a ~400ms OCI Function round trip and returns a realistic `confirmed` response. The Kafka write still happens, so `agent_actions_mv` updates live on screen exactly as it would in production — the audience sees the stream update in real time regardless. Switch to `false` and supply the two OCI vars below only when you want to wire up a real IAM suspension.

**OCI Function (production only)** — holds the IAM credentials and adds the principal to a zero-policy IAM group. No IAM credentials live in the MCP server. The function should accept:
```json
{ "principal_id": "...", "reason": "...", "triggered_by": "..." }
```
and return a JSON body confirming the operation.

**suspend_principal MCP server** (`suspend_principal_mcp/`) — a Node.js MCP server that bridges the agent to the OCI Function (or the mock) and the audit stream. Install it:

```bash
cd suspend_principal_mcp
npm install
# env vars are passed by the MCP host via mcp_config.json — no manual export needed
```

What it does on each tool call:
1. Calls the OCI Function endpoint with `principal_id`, `reason`, and `triggered_by`
2. Records the outcome (confirmed / failed) plus the raw OCI response
3. Writes the full action record to the `mcp_agent_actions` Kafka topic so it appears in `agent_actions_mv`
4. Returns the record to the agent so it can confirm the action and quote the result

The tool schema the agent sees:

```json
{
  "name": "suspend_principal",
  "description": "Suspends an OCI principal by adding them to the restricted-access group. Use when a principal is confirmed to be the source of anomalous or unauthorized activity.",
  "parameters": {
    "principal_id": "string",
    "reason": "string",
    "triggered_by": "string"
  }
}
```

---

### 6. Start the live demo dashboard

The dashboard tails `mcp_agent_actions` and the `mcp-oci-audit` anomaly feed and pushes events to the browser via SSE. No page refresh needed — new cards slide in as they arrive.

```bash
cd demo_dashboard
npm install
KAFKA_BOOTSTRAP=<OCI_KAFKA_BOOTSTRAP> \
KAFKA_SASL_USERNAME=<OCI_KAFKA_USERNAME> \
KAFKA_SASL_PASSWORD=<OCI_KAFKA_PASSWORD> \
node server.js
```

Then open `http://localhost:3000` — put it on the right half of your screen alongside the agent conversation.

**Left panel — Suspicious Activity:** 4xx and 5xx audit events arrive here in real time, building the case the agent is investigating.

**Right panel — Agent Actions:** when the agent calls `suspend_principal`, a new card flashes green within 2–3 seconds, independently of the agent's own confirmation message. That's the point.

### 7. Create your agent in OpenAI Agent Builder

1. Go to [platform.openai.com/agents](https://platform.openai.com/agents)
2. Create a new agent
3. Set the system prompt (see below)
4. Add both MCP servers using your `mcp_config.json`
5. The agent will automatically discover `oci_audit_mv`, `oci_logging_mv`, and `agent_actions_mv` as query tools, and `suspend_principal` as the write action

**Suggested system prompt:**

```
You are an OCI operations assistant with access to real-time data from an OCI tenancy.

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
- Before suspending a principal a second time, query agent_actions_mv to confirm you have not already done so
```

---

## Adapting this to your environment

The streams and tools in this repo are a starting point, not a prescription. Some directions to take it further:

- **Add more log sources** — extend `02_raw_streams.sql` to include API Gateway logs, VCN flow logs, or custom application logs
- **Narrow the filter** — modify the `WHERE` clause in `03_enriched_streams.sql` to focus on specific event types or compartments
- **Add derived fields** — extend the `SELECT` in `03_enriched_streams.sql` with additional `REGEXP_EXTRACT` or `COALESCE` logic specific to your naming conventions
- **Add more views** — create additional materialized views in `04_materialized_views.sql` focused on specific use cases (e.g. a view filtered to failed events only, or scoped to a single application)
- **Add more write actions** — follow the `suspend_principal` pattern to add other OCI Function-backed tools (e.g. revoke a token, quarantine a resource, open a PagerDuty incident)
- **Join agent actions with operational data** — because `agent_actions_mv` lives in the same platform, you can write DeltaStream SQL that joins it against `oci_audit_mv` to correlate agent interventions with the tenancy events that triggered them

The architecture is the same regardless of what you put in the streams.

---

## Troubleshooting

**Events not arriving in raw streams**
- Verify Connector Hub is in "Active" state in OCI Console
- Confirm the topic names in `02_raw_streams.sql` match what Connector Hub is writing to
- Check that your OCI Auth Token hasn't expired (they have a maximum 1-year lifetime)

**DeltaStream store connection failing**
- Confirm the SASL username format: `<tenancy_name>/<oci_username>/<stream_pool_ocid>`
- Verify the bootstrap server URL matches your OCI region
- Ensure the Auth Token was generated for the correct user

**Materialized views showing no data**
- Wait 1–2 minutes after creation for backfill to complete
- Verify the enriched streams (`oci_audit`, `oci_logging`) have data first
- Check that the `WHERE` filters in `03_enriched_streams.sql` match your actual event types

**Agent not finding the MCP tools**
- Confirm `DELTASTREAM_RELATIONS` in `mcp_config.json` matches the exact view names
- Verify the database and schema names are correct
- Check that the DeltaStream API token has read access to the views

**suspend_principal returning an error**
- Verify `OCI_SUSPEND_FUNCTION_ENDPOINT` is the full invoke URL (not the resource OCID)
- Confirm the OCI Function is deployed and in "Active" state
- Check that the auth token in `OCI_FUNCTION_AUTH_TOKEN` has not expired
- Confirm the IAM group the function adds principals to exists and has an attached zero-policy

**agent_actions_mv showing no data after a suspension**
- Confirm the `mcp_agent_actions` Kafka topic exists in your OCI Streaming for Apache Kafka cluster
- Verify the Kafka credentials in `mcp_config.json` match those used in `01_store.sql` for `oci_streaming_kafka`
- Check that `05_agent_actions_stream.sql` was run after the topic existed

---

## Resources

- [DeltaStream Documentation](https://docs.deltastream.io)
- [DeltaStream — get in touch if you'd like access](https://www.deltastream.io/contact-us)
- [OCI Streaming for Apache Kafka](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/kafkacompatibility.htm)
- [OCI Connector Hub Documentation](https://docs.oracle.com/en-us/iaas/Content/connector-hub/overview.htm)
- [OpenAI Agent Builder](https://platform.openai.com/agents)
- [Blog post — companion to this repo](#)
- [Demo video](#)
