# Plan: Chat-Integrated Agent Hub (Level 3 Multi-Agent Threads)

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Make the Odysseus chat the primary interface for agent collaboration — slash commands delegate work to agents, responses appear inline as streaming messages, and users see live agent-to-agent interactions without leaving the conversation.

**Architecture:** Chat becomes the frontend hub. Agent Hub becomes the backend it talks to. Slash commands create tasks, SSE streams results into the chat stream, and inline editing lets users interact with agent output directly.

**Tech Stack:** Existing FastAPI/SSE infrastructure, vanilla JS chat UI, Agent Hub coordinator, A2A routes.

**Status:** draft v1
**Date:** 2026-06-05
**Estimated effort:** 500-700 lines across 6-8 files

---

## Current State

Today the workflow requires context switching:

```
Chat with Hermes → plan something →
"want Codex to review?" →
switch to Agent Hub modal → create task → assign reviewer role →
wait for completion → open task detail → read review →
switch back to chat → continue
```

The user is the manual relay between agents. Every handoff requires leaving the conversation.

## Target State

```
Chat with Hermes → "/review plan.md" →
[Codex is reviewing...] → streaming response appears inline →
user: "good catch, apply that fix" → \apply → edits land →
"/verify" → verifier runs tests → "all passing" →
continue planning with Hermes, context intact
```

One conversation. Multiple agents. Clear attribution. Live streaming. No modal switches.

## Architecture

```
┌─────────────────────────────────────────────┐
│  Chat UI                                    │
│  ┌───────────────────────────────────────┐  │
│  │  Hermes: Here's the auth refactor...  │  │
│  │  User: /review auth-plan.md           │  │
│  │  ┌─────────────────────────────────┐  │  │
│  │  │ Codex (reviewing...)            │  │  │
│  │  │ Line 45: race condition on     │  │  │
│  │  │ token refresh. Use a lock.     │  │  │
│  │  │ [Accept] [Edit] [Reject]       │  │  │
│  │  └─────────────────────────────────┘  │  │
│  │  User: /implement fix for line 45    │  │
│  │  ┌─────────────────────────────────┐  │  │
│  │  │ Codex (implementing...)         │  │  │
│  │  │ + async with token_lock:        │  │  │
│  │  │     await refresh_if_needed()   │  │  │
│  │  │ [Accept Diff] [Reject]          │  │  │
│  │  └─────────────────────────────────┘  │  │
│  └───────────────────────────────────────┘  │
│                    │                         │
│           ┌────────▼────────┐                │
│           │  Slash Command  │                │
│           │  Parser         │                │
│           └────────┬────────┘                │
│                    │                         │
└────────────────────┼─────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│  Agent Hub Backend (existing)               │
│  ┌───────────────────────────────────────┐  │
│  │  Coordinator → Adapters → SSE        │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

## What Already Exists

| Piece | Location | Ready for chat integration? |
|---|---|---|
| Agent Hub task creation | `routes/agent_hub_routes.py` | Yes — POST to create, assign role/adapter |
| SSE streaming | `routes/agent_hub_routes.py` + `agentHub.js` | Yes — per-owner event stream, `init` + `delta` |
| Role dispatch | `src/agent_coordinator.py` | Yes — diagnoser/implementer/verifier |
| A2A AgentCards | `routes/a2a_routes.py` | Yes — agent discovery and capability listing |
| Wakeup | `src/agent_coordinator.py` | Yes — `request_wakeup()` triggers immediate tick |
| Inline editing | Chat UI (existing) | Yes — edit button already on messages |
| Streaming text | Chat SSE | Partial — chat streams model output, but not agent output |
| File diff view | None | New — needed for showing proposed code changes |

## What Needs Building

### 1. Slash Command Parser

The chat input already processes text. Add command detection:

```
/review <file-or-text>     → creates Agent Hub task, role=reviewer
/implement <description>   → creates Agent Hub task, role=implementer
/verify <description>      → creates Agent Hub task, role=verifier
/delegate <text> to <role> → creates task with specified role
/apply                      → accepts last agent's proposed changes
```

**Implementation:** Hook into the chat input handler. When a message starts with `/`, parse the command, create an Agent Hub task via `POST /api/agent-hub/tasks`, and inject a placeholder message into the chat stream.

### 2. Agent Message Renderer

When an Agent Hub task completes (or streams progress), the chat renders it as an attributed message:

```
┌─ Codex (reviewing plan.md) ──────────────────────┐
│ Line 45: race condition. Use a lock.             │
│                                                   │
│ [Accept changes] [Edit] [Dismiss]                 │
│ 72 tok · 3.2s                                     │
└───────────────────────────────────────────────────┘
```

**Key behaviors:**
- Agent name and role shown in the header (from AgentInstance + task role)
- Content streams in real-time via SSE (not just final result)
- Proposed file changes shown as a diff (green/red inline)
- Accept/Edit/Reject actions for code changes
- Token count and duration in the footer (like existing chat messages)

### 3. Live Agent-to-Agent Interaction View

When Agent A delegates to Agent B, both appear in the same thread:

```
Hermes: /review auth.py
  └─ Codex (review): Found race condition at line 45
       Hermes: /implement fix for line 45
         └─ Codex (implement): + async with token_lock: ...
              Hermes: /verify
                └─ Verifier: All 12 tests passing
User: Ship it.
```

**Implementation:** Tasks created by `/delegate` have a `parent_task_id` linking them. The chat UI renders nested messages with indentation. SSE updates from any task in the tree update the thread.

### 4. Apply/Edit Flow

When an agent proposes code changes, the user can interact without leaving chat:

- **Accept:** writes the proposed diff to the file, shows confirmation
- **Edit:** opens the inline code editor (existing Odysseus feature), user modifies, saves
- **Reject:** dismisses the suggestion, agent can try again

The `/apply` command accepts the most recent agent's proposed changes. Individual Accept buttons on each message handle per-suggestion acceptance.

### 5. Agent Presence Indicator

A small strip in the chat header or sidebar showing which agents are online:

```
● Codex  ● Hermes  ○ Verifier
```

Polls `GET /api/agent-hub/agents` alongside the existing chat poll. Shows status dots (green=online, grey=offline) and capability tooltips.

---

## Phase 1: Chat Commands + Inline Responses

Delivers: `/review`, `/implement`, `/verify` commands create tasks and show inline results. Streaming agent responses in chat.

**Files:** 4 new, 4 modified (~350 lines)

### Task 1: Slash command parser
- Hook into chat input `keydown` / submit handler
- Detect `/review`, `/implement`, `/verify`, `/delegate` patterns
- Extract target text (selected file, typed description, or last message)
- Create Agent Hub task via `POST /api/agent-hub/tasks` with appropriate role
- Inject placeholder message: "[Codex is reviewing...]"
- 4 tests: `/review` with file, `/implement` with description, `/verify` with last message, invalid command shows help

### Task 2: Agent SSE listener in chat
- Chat already has an SSE connection for model responses
- Add a parallel SSE listener for Agent Hub events (`/api/agent-hub/stream`)
- When a task transitions to `done`, render the result as an agent message
- When a task transitions to `running`, update the placeholder to "[Codex is working...]"
- Track which tasks the current chat session created (don't show all Agent Hub tasks)
- 3 tests: task created by this session appears, task from another session does not, streaming status transitions

### Task 3: Agent message renderer
- HTML/CSS template for agent messages in the chat log
- Shows: agent icon + name, role, streaming/done status, content, token count + time
- Code blocks inside agent responses get syntax highlighting (reuse existing)
- Proposed file changes detected via `[ACTIONS]` block or `file_write` events → rendered as inline diff
- 2 tests: text-only response renders, code diff response renders with Accept/Reject buttons

### Task 4: Accept/Edit/Reject flow
- Accept button: writes proposed content to file via existing `file_write` action
- Edit button: opens existing inline editor with proposed content pre-filled
- Reject button: dismisses message, records rejection (so agent doesn't loop)
- `/apply` command: accepts most recent agent's proposed changes
- 3 tests: accept writes file, reject dismisses, `/apply` targets most recent

---

## Phase 2: Live Interaction View (deferred)

- Nested message threading with indentation
- Parent-child task linking via `parent_task_id`
- Agent-to-agent messages visible inline
- User can jump in at any nesting level

## Phase 3: Agent Presence + Discovery (deferred)

- Agent status strip in chat header
- Click agent → show capabilities, recent tasks, AgentCard
- `/agents` command lists available agents with status

---

## Touchpoint Map

| Layer | File | What Changes |
|---|---|---|
| Chat UI | `static/js/chat.js` (or equivalent) | Slash command parser, SSE listener for agent events, agent message rendering |
| Chat CSS | `static/style.css` | Agent message styles, diff view, Accept/Edit/Reject buttons |
| Agent Hub JS | `static/js/agentHub.js` | Export `_taskMap` so chat can query task status; expose `_getFilteredTasks` |
| Routes | `routes/agent_hub_routes.py` | Add `?created_by_chat=true` filter to task list; add `/tasks/{id}/apply` endpoint |
| Chat HTML | `static/index.html` | Agent presence strip in header (optional Phase 1) |
| Coordinator | `src/agent_coordinator.py` | No changes — existing dispatch handles everything |
| A2A routes | `routes/a2a_routes.py` | No changes — existing AgentCard + SendMessage + SSE already works |

---

## Design Decisions

### Command syntax: `/review file.py` vs `/review "fix the auth bug"`

Both. If the argument looks like a file path (contains `.`, `/`, or exists on disk), treat it as a file review. Otherwise treat it as a free-text description. The task title is auto-generated: "Review `file.py`" or "Implement: fix the auth bug".

### Task scope: session-only vs global

Chat-created tasks are scoped to the chat session. When the user runs `/review`, the task gets `metadata: {chat_session_id: "..."}`. The chat SSE listener filters to only show tasks from the current session. The full Agent Hub modal still shows all tasks.

### Streaming: model output vs agent output vs task events

The chat already receives model output SSE. Agent Hub has its own SSE stream. The chat needs to listen to BOTH — model output for Hermes replies, agent output for delegated task results. They're separate event sources with different formats. Keep them separate rather than merging into one stream.

### File diffs: inline vs modal

Code changes from agents appear inline in the chat as a compact diff (like GitHub's PR view). No modal. The user shouldn't leave the conversation to see what the agent changed.
