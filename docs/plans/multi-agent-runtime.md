# Plan: Agent Hub A2A Runtime Compatibility Layer (v2)

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Make Odysseus Agent Hub an A2A-compatible local agent coordination hub — expose internal agents (Codex, Hermes, Cursor) as A2A agents, consume external A2A agents as dispatch targets, while preserving the existing coordinator, approval flow, and role bindings.

**Architecture:** A2A as the wire protocol boundary. Odysseus keeps its internal coordinator and DB models; A2A adapters translate at the edges. External A2A agents talk to an A2A Gateway that maps to AgentTask. Internal agents are exposed via an A2A Server Adapter that serves Agent Cards and maps A2A SendMessage to task creation.

**Tech Stack:** `a2a-sdk` (Python, Apache 2.0), FastAPI, existing SQLite/SQLAlchemy models, existing coordinator `_tick()`.

**Status:** draft v2 — revised after Codex review. Supersedes `multi-agent-runtime.md` v1.

---

## What Changed from v1

v1 (`multi-agent-runtime.md`) proposed a custom protocol with Agent, AgentMessage, AgentThread, AgentArtifact models, custom wakeup, and capability routing. Codex review identified six issues, all addressed below:

| v1 Issue | v2 Resolution |
|---|---|
| Blurred owner/current_owner/assigned_agent_id | Explicit three-layer identity: `owner` = user (access control), `adapter_name` = execution backend, `agent_instance_id` = A2A identity. Invariant documented. |
| Missing auth for agent-to-agent endpoints | AgentInstance.auth_token_hash added. Agent auth enforced before registration, heartbeat, messaging, wakeup endpoints. |
| Agent-initiated actions bypass approval | All incoming A2A actions flow through existing approval gate. No autonomous execution without user approval unless explicitly allowlisted. |
| AgentThread created then recommended against | No AgentThread. Messages are task-scoped; reuse existing AgentEvent with A2A metadata. |
| awaiting_response status without transition rules | Deferred. Model response waiting with depends_on + message injection. Add response_to_task_id to AgentTask but no new status. |
| Wakeup bypasses polling without reentrancy guard | Wakeup requests a "tick soon" via a single-flight asyncio.Lock. No concurrent `_tick()` calls. |

Additionally, the entire custom protocol approach is replaced with A2A compliance, which covers agent discovery (Agent Cards), messaging (SendMessage), task lifecycle (A2A Task/TaskStatus), streaming (SSE), wakeup (Push Notifications), artifacts (A2A Artifacts), and auth (A2A security schemes).

---

## Architecture

```
External A2A agents
(ADK, LangGraph, CrewAI, BeeAI, custom)
        │
        │  A2A protocol (JSON-RPC, SSE, webhooks)
        │
        ▼
┌──────────────────────────────────┐
│     A2A Gateway                   │
│  ┌────────────┐ ┌──────────────┐ │
│  │ A2A Server │ │ A2A Client   │ │
│  │ Adapter    │ │ Adapter      │ │
│  │ (expose)   │ │ (consume)    │ │
│  └─────┬──────┘ └──────┬───────┘ │
│        │               │         │
│        ▼               ▼         │
│  ┌────────────────────────────┐  │
│  │  Compatibility Layer       │  │
│  │  (translates A2A ↔ AgentTask)│  │
│  └────────────┬───────────────┘  │
└───────────────┼──────────────────┘
                │
                ▼
┌──────────────────────────────────┐
│   Existing Agent Hub Runtime     │
│  ┌────────────────────────────┐  │
│  │  Coordinator (_tick)       │  │
│  │  Tasks, Events, Approvals  │  │
│  │  Role Bindings, Deps       │  │
│  └────────────┬───────────────┘  │
│               │                  │
│  ┌────────────▼───────────────┐  │
│  │  Local Adapters            │  │
│  │  Codex, Hermes, Cursor     │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘
```

**Key principle:** A2A is the boundary. The coordinator doesn't change. What's new is the translation layer that maps A2A concepts to existing Odysseus concepts.

---

## A2A ↔ Odysseus Mapping

| A2A Concept | Odysseus Equivalent | Action |
|---|---|---|
| AgentCard | AgentInstance model | New — agents register with capabilities, endpoint, auth |
| AgentCard.skills | AgentInstance.capabilities (JSON) | New — maps to role bindings for dispatch |
| AgentCard.url | AgentInstance.endpoint | New — for HTTP agents and webhook wakeup |
| SendMessage | AgentTask creation with `from_agent_id` | New path — incoming A2A message creates or updates a task |
| Task | AgentTask (existing) | Reuse — add `external_protocol`, `external_task_id`, `agent_card_url` |
| TaskStatus (working/completed/failed/cancelled/input-required) | AgentTask.status (running/done/blocked/cancelled/waiting_for_approval) | Map — A2A statuses are a superset; map known states, reject unmappable |
| TaskStatusUpdateEvent (SSE) | Existing SSE + AgentEvent | Extend — wrap in A2A-compatible SSE format |
| Artifact | AgentEvent with content + metadata | Extend — add artifact type, MIME, size to events |
| PushNotificationConfig | AgentInstance.wakeup_url + auth_token | New — stored on agent registration, used for wakeup |
| Security schemes (apiKey, bearer, OAuth2, OIDC) | AgentInstance.auth_token_hash | New — agent authenticates with token; hashed in DB |

---

## A2A Integration Constraints

These constraints are derived from Codex's v1 review, reframed as A2A safety rules:

### 1. Three-Layer Identity (never merge these)

```
owner          = authenticated USER (access control, scope, SSE filtering)
adapter_name   = execution BACKEND (hermes, codex, cursor — what runs the task)
agent_instance_id = A2A IDENTITY (UUID — which specific agent instance)
```

**Invariant:** `owner` gates who can see a task. `adapter_name` determines which adapter executes it. `agent_instance_id` is the A2A identity used for messaging and discovery. A task's owner never changes. Its adapter_name is resolved at claim time. Its agent_instance_id is set at creation and identifies the A2A agent that owns the task's lifecycle.

### 2. Agent Auth Before Any A2A Endpoint

```
POST /api/agent-hub/agents/register     → requires agent auth token
POST /api/agent-hub/agents/heartbeat    → requires agent auth token
POST /api/agent-hub/a2a/send-message    → requires A2A client auth
POST /api/agent-hub/a2a/webhook/*       → requires webhook secret validation
```

AgentInstance.auth_token_hash stores bcrypt(token). On register, the agent provides a token; the hub hashes and stores it. Subsequent authenticated endpoints compare against the hash. Without this, any registered agent could impersonate another or message across users.

### 3. Approval Gate Preserved

Incoming A2A SendMessage creates an AgentTask. That task flows through the existing approval model:

```
A2A SendMessage → AgentTask(status=queued) → coordinator claims →
adapter.run() → AgentAdapterResult(needs_approval=True/False) →
if needs_approval: waiting_for_approval → user approves → actions execute
if not: transition to proposed_status
```

**No A2A message can bypass the approval gate.** Agent-initiated task creation is a new path into the queue, not a new execution path. The existing `create_task` action already follows this pattern — A2A extends it to external callers.

### 4. Task-Scoped Messages, No AgentThread

v1 proposed an AgentThread table, then recommended against it. v2 follows the recommendation: messages are task-scoped. The existing `AgentEvent` table already provides a task timeline. For A2A messages that don't map to a specific task, use a lightweight `task_id` on the event or create a wrapper task.

**A2A `contextId`** (an optional grouping identifier in the spec) maps to a logical group, not a DB table. Use it for filtering in v1; add thread support in v2 only if needed.

### 5. No awaiting_response Status

A2A doesn't have an "awaiting response" status — it has `input-required` for human-in-the-loop and `working` for in-progress. For agent-to-agent delegation where Agent A waits for Agent B:

- Agent A creates task T for Agent B with `depends_on` or `response_to_task_id`
- Agent A's task stays `queued` until the dependency resolves
- Agent B's result is injected into Agent A's context brief via the existing `_inject_context_brief()`
- When Agent B's task hits `done`, Agent A's dependencies clear and it becomes claimable

No new status. The existing dependency gating + context brief injection handles this.

### 6. Single-Flight Coordinator Wakeup

`_tick()` is not reentrant. A wakeup request from a route handler MUST NOT call `_tick()` directly. Instead:

```python
_wakeup_requested = asyncio.Event()
_wakeup_lock = asyncio.Lock()

async def request_wakeup():
    """Called from route handlers when a task is created for an online agent."""
    _wakeup_requested.set()

# In _coordinator_loop():
while _running:
    await asyncio.wait(
        [asyncio.sleep(POLL_INTERVAL), _wakeup_requested.wait()],
        return_when=asyncio.FIRST_COMPLETED
    )
    _wakeup_requested.clear()
    async with _wakeup_lock:
        await _tick()
```

This ensures at most one `_tick()` runs at a time, wakeup requests are coalesced (N rapid creates = 1 tick), and the background poll still fires on schedule.

---

## Phase 1: A2A Foundation (Smallest Viable)

Delivers: agents register with A2A Agent Cards, Odysseus exposes internal agents as A2A endpoints, external A2A agents can send messages that create tasks, wakeup triggers immediate dispatch.

**Estimated:** 400-500 lines across 7-9 files

### Files

| File | Change |
|---|---|
| `core/database.py` | Add AgentInstance model. Add `agent_instance_id`, `external_protocol`, `external_task_id`, `agent_card_url`, `response_to_task_id` to AgentTask. Migration functions. |
| `routes/agent_hub_routes.py` | Add `/agents/register`, `/agents/heartbeat`, `/agents` endpoints with agent auth. Extend task creation to accept A2A SendMessage payload. |
| `routes/a2a_routes.py` | **New.** A2A Server endpoints: `/.well-known/agent-card`, `/a2a/send-message`, `/a2a/tasks/{id}/status`. A2A-compatible SSE stream. |
| `src/a2a_server.py` | **New.** A2A Server Adapter — builds AgentCards from AgentInstances, maps SendMessage → AgentTask, maps AgentTask status → A2A TaskStatus. |
| `src/a2a_client.py` | **New.** A2A Client Adapter — fetches remote AgentCards, sends SendMessage to external agents, polls/streams task status. Registers as an adapter target. |
| `src/agent_coordinator.py` | Add `_wakeup_requested` + `_wakeup_lock` + single-flight guard. Add `from_agent_id` to task context brief. Add `_resolve_agent_instance()` for A2A dispatch. |
| `src/adapters/base.py` | Add `agent_instance_id` field to AgentAdapterResult. |
| `static/js/agentHub.js` | Agent status panel showing registered A2A agents. Remote agent card viewer. |
| `static/style.css` | Agent panel styles. |
| `tests/test_a2a.py` | Agent registration, AgentCard serving, SendMessage → task creation, wakeup coalescing, auth rejection, status mapping, external task polling. |

### Task 1: AgentInstance model + registration

**What:** DB model for A2A agent instances with auth.

```python
# core/database.py — new model
class AgentInstance(TimestampMixin, Base):
    __tablename__ = "agent_instances"
    id = Column(String, primary_key=True, index=True)
    owner = Column(String, index=True)  # which user registered this agent
    name = Column(String)               # human-readable (e.g. "My Codex")
    kind = Column(String)               # "cli" | "sdk" | "http" | "a2a-remote"
    adapter_name = Column(String)       # "hermes" | "codex" | "cursor" | None (remote)
    status = Column(String, default="offline")  # "online" | "offline" | "busy"
    capabilities = Column(JSON)         # ["code-review", "testing", "planning"]
    endpoint = Column(String)           # A2A endpoint URL (for remote agents)
    auth_token_hash = Column(String)    # bcrypt(token) — agent authenticates with this
    last_heartbeat = Column(DateTime)
    agent_card_json = Column(JSON)      # cached AgentCard (for local agents)
```

**Route:** `POST /api/agent-hub/agents/register` — creates or updates AgentInstance. Requires `X-Agent-Token` header matching stored hash (on re-register) or sets it (on first register). Returns agent ID and token (on create only).

**Tests (5):** register new agent, re-register with token, invalid token rejected, list agents filtered by owner, A2A AgentCard auto-generated for local agents.

### Task 2: Agent heartbeat + auto-offline

**Route:** `POST /api/agent-hub/agents/heartbeat` — agent calls this every 30s. Updates `last_heartbeat`, sets `status="online"`. Requires auth token. Coordinator background task: every 60s, mark agents offline where `last_heartbeat < now - 90s`.

**Tests (4):** heartbeat updates timestamp, stale agent goes offline, online stays online, missing auth token rejected.

### Task 3: A2A Server Adapter (expose Odysseus as A2A)

**What:** `src/a2a_server.py` — builds A2A-compatible AgentCards from AgentInstances, serves `/.well-known/agent-card`, handles SendMessage by creating AgentTasks, maps AgentTask status to A2A TaskStatus.

**AgentCard for a local agent:**
```json
{
  "name": "Odysseus Codex",
  "description": "Codex CLI agent for code editing and testing",
  "url": "http://localhost:7000/a2a/agent/codex-<uuid>",
  "provider": {"organization": "Odysseus Agent Hub"},
  "capabilities": {"streaming": true, "pushNotifications": true},
  "skills": [
    {"id": "code-review", "name": "Code Review", "description": "Review code for bugs and style"},
    {"id": "testing", "name": "Test Runner", "description": "Run test suites and report results"}
  ],
  "defaultInputModes": ["text", "file"],
  "defaultOutputModes": ["text", "file"],
  "securitySchemes": {"apiKey": {"type": "apiKey", "in": "header", "name": "X-Agent-Token"}},
  "protocolVersion": "1.0"
}
```

**Routes:** `routes/a2a_routes.py` — three endpoints:
- `GET /.well-known/agent-card/{agent_id}` → serves AgentCard
- `POST /a2a/send-message` → creates AgentTask, returns A2A Task
- `GET /a2a/tasks/{task_id}/status` → returns A2A TaskStatus

**Tests (6):** AgentCard serves correctly, SendMessage creates task, status maps correctly, unknown agent returns 404, missing auth returns 401, SSE stream delivers A2A-compatible events.

### Task 4: A2A Client Adapter (consume external A2A agents)

**What:** `src/a2a_client.py` — fetches AgentCards from external A2A URLs, registers them as AgentInstances with `kind="a2a-remote"`, sends SendMessage when dispatched, polls for task completion.

**Registration flow:**
1. User provides A2A endpoint URL (e.g. `https://my-agent.example.com`)
2. Client fetches `GET {url}/.well-known/agent-card`
3. Validates protocol version, capabilities
4. Creates AgentInstance with `kind="a2a-remote"`, `endpoint=url`, `capabilities` from AgentCard.skills
5. Agent appears in dispatch targets

**Dispatch flow:**
1. Coordinator resolves role → external agent (via role binding or capability match)
2. Client adapter sends `POST {url}/a2a/send-message` with task content
3. Receives A2A Task with status and optional SSE stream URL
4. Polls `GET {url}/a2a/tasks/{id}/status` or subscribes to SSE
5. On completion, maps A2A TaskStatus → AgentTask.status, records artifacts as events

**Tests (4):** fetch AgentCard from mock server, register external agent, SendMessage succeeds, task completion maps correctly.

### Task 5: Single-flight coordinator wakeup

**What:** Add `_wakeup_requested` Event and `_wakeup_lock` to coordinator. Route handlers call `request_wakeup()` after creating tasks assigned to online agents. Coordinator loop uses `asyncio.wait` to break out of sleep on wakeup.

**Implementation (in `src/agent_coordinator.py`):**
```python
_wakeup_requested = asyncio.Event()
_wakeup_lock = asyncio.Lock()

def request_wakeup():
    """Signal the coordinator loop to tick immediately."""
    _wakeup_requested.set()

# In _coordinator_loop(), replace `await asyncio.sleep(POLL_INTERVAL)`:
async def _coordinator_loop():
    global _last_tick, _tasks_processed
    # ... startup probe ...
    while _running:
        try:
            async with _wakeup_lock:
                await _tick()
        except Exception:
            logger.exception("Coordinator tick failed")
        # Wait for next poll interval OR wakeup signal
        await asyncio.wait(
            [
                asyncio.create_task(asyncio.sleep(POLL_INTERVAL)),
                asyncio.create_task(_wakeup_requested.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        _wakeup_requested.clear()
```

**Tests (3):** wakeup triggers tick before poll interval, multiple rapid wakeups coalesce to single tick, wakeup during tick is queued (lock prevents reentrancy).

---

## Phase 2: Artifacts + Capability Routing (deferred)

Only after Phase 1 identity/auth/messaging/wakeup is solid.

1. Map A2A Artifacts to AgentEvent with typed metadata
2. Add capability-based routing: `POST /api/agent-hub/tasks` accepts `required_capabilities`
3. Coordinator `_match_by_capability()` — matches task requirements to AgentInstance.capabilities
4. Artifact browser UI panel

---

## Phase 3: Response Handoff (deferred)

Only after Phase 2 capability routing is working.

1. Add `response_to_task_id` to AgentTask (deferred from Phase 1)
2. On task completion, inject result into parent's context brief
3. Parent task becomes claimable when dependency clears

---

## Risks & Resolved Decisions

### Resolved (Codex architectural review, 2026-06-06)

| Decision | Resolution |
|---|---|
| **Local adapters vs A2A agents** | Keep distinct. Local Hermes/Codex/Cursor adapters stay internal dispatch targets. Odysseus itself exposes A2A AgentCards backed by local adapters — not per-adapter long-lived servers. |
| **Dual SSE streams** | Serve two endpoints. Existing `/api/agent-hub/stream` for the UI. New A2A-compatible stream for external A2A clients. Translate at boundary, don't force UI to consume A2A. |
| **`input-required` mapping** | Precise. `waiting_for_approval` → `input-required`. On approval, emit `working` while executing actions, then `completed`. No loose mapping. |
| **A2A SDK integration depth** | Thin in Phase 1. Import types, validation, AgentCard structures, serializers. Odysseus SQLAlchemy models remain source of truth. Add conformance tests against real A2A request/response examples. |

### Open Risks

1. **A2A SDK dependency** — `a2a-sdk` requires Python 3.10+. Odysseus uses 3.11. No issue, but verify SDK doesn't pull incompatible transitive deps.

2. **A2A TaskStatus mapping** — resolved: `waiting_for_approval` → `input-required` (precise). Remaining gap: `auth-required` and `rate-limited` are A2A-only; Odysseus doesn't have equivalents. Return `working` with metadata for these in v1.

3. **AgentInstance lifecycle** — what happens when an agent deregisters while tasks are running? Tasks complete normally (already claimed/locked). New tasks are not assigned to offline agents. AgentCard returns 404.

4. **Multi-user agent scope** — can agent A (registered by user X) be dispatched for user Y's tasks? No. AgentInstance.owner gates visibility. Cross-user agent sharing is a Phase 3 feature.

5. **A2A spec version pinning** — pin to v1.0.0. When A2A releases breaking changes, Odysseus pins the version and upgrades deliberately.
