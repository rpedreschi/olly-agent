-- ============================================================
-- Step 5: Create the Agent Actions Stream and Materialized View
--
-- This stream records every action taken by the AI agent.
-- The suspend_principal MCP tool writes here each time it acts,
-- creating an auditable, queryable log of agent behavior in the
-- same platform as the operational data the agent reads from.
--
-- The stream is written by the suspend_principal MCP tool (not
-- by a DeltaStream transformation). It points at the Kafka topic
-- that the MCP tool publishes to after each OCI Function call.
--
-- Run this after Step 4.
-- ============================================================


-- ------------------------------------------------------------
-- Agent Actions Stream
-- Source topic: mcp_agent_actions (written by suspend_principal MCP tool)
-- Store: oci_streaming_kafka (same output broker as enriched streams)
-- ------------------------------------------------------------
CREATE STREAM "agent_actions" (
  "action_id"    VARCHAR,
  "action_type"  VARCHAR,
  "target_id"    VARCHAR,
  "reason"       VARCHAR,
  "triggered_by" VARCHAR,
  "action_time"  TIMESTAMP(3) WITH TIME ZONE,
  "status"       VARCHAR,
  "oci_response" VARCHAR
)
WITH (
  'store'            = 'oci_streaming_kafka',
  'topic'            = 'mcp_agent_actions',
  'topic.partitions' = 1,
  'topic.replicas'   = 2,
  'value.format'     = 'JSON'
);


-- ------------------------------------------------------------
-- Agent Actions Materialized View
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW "agent_actions_mv" AS
SELECT * FROM "agent_actions"
WITH ('starting.position' = 'earliest');


-- ============================================================
-- Step 6: Set AI-Friendly Description for agent_actions_mv
-- ============================================================

ALTER RELATION "agent_actions_mv" SET description =
'Continuously updated log of actions taken by the AI agent.
Every call to suspend_principal writes a record here — so the
audit trail of agent behavior lives in the same platform as the
operational data the agent was reading from.

Use this view to answer questions about what the agent did,
when it acted, which principals it targeted, and whether those
actions succeeded.

Key fields:
- action_time: when the action was executed. Always filter by time window.
- action_type: the action taken (e.g. suspend_principal).
- target_id: the OCI principal OCID or name that was acted on.
- reason: the justification the agent supplied when calling the tool.
- triggered_by: the agent or process that initiated the action.
- status: outcome — confirmed, failed, or pending.
- oci_response: raw response from the OCI Function that executed the action.

Usage tips:
- To review your full session history: query without filters or with a wide time window.
- To verify a specific suspension: filter on target_id and check status = ''confirmed''.
- To correlate an action with what triggered it: join target_id against
  oci_audit_mv.principalid and match on overlapping time windows.
- To audit what you have done before acting again: always query this view
  before calling suspend_principal a second time on the same principal.';
