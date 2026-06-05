# Plan: Recurring Scheduled Tasks for Agent Hub (v2 — Codex-reviewed)

**Status:** revised after Codex review
**Date:** 2026-06-05
**Estimated effort:** 500-650 lines across 8 files

## What Changed from v1

The original plan (v1, ~270 lines, 5 files) underestimated several areas. Codex review identified
eight gaps, all addressed below. The revised estimate reflects the real integration surface:
status machine changes across routes, UI, SSE, batch ops, and tests; clone lineage fields;
overlap policy; restart semantics; and shared scheduling with the existing `TaskScheduler`.

---

## What

Add recurring/scheduled task support to the Agent Hub. Tasks auto-activate on a schedule —
one-shot or recurring — via the existing coordinator `_tick()` polling loop. Scheduling
computation is shared with the existing `TaskScheduler` (`compute_next_run()` in
`src/task_scheduler.py`), not duplicated.

## Why

Manual task creation is the only path today. Recurring use cases ("verify every 4h",
"morning diagnoser scan", "deployment check at 2am") require a human on a timer —
the exact problem scheduling exists to solve.

## Architecture

**Approach: DB polling with schedule columns on AgentTask, reusing existing scheduler
helpers.** The coordinator `_tick()` loop already polls for queued tasks; adding a
schedule-activation gate before the claim loop is surgical (one query block). Schedule
computation (`compute_next_run()`) is shared with the existing `TaskScheduler` — no
duplicate parser, no second cron expression engine.

**Separation of concerns:** Agent Hub runs are NOT routed through `TaskScheduler`'s
execution engine (`_execute_task`). That engine handles assistant/LLM/action tasks
with a different lifecycle (serial execution, TaskRun records, semaphore). Agent Hub
tasks have role dispatch, dependency gating, SSE, RAG context, approval, and adapter
ownership — those stay in the Agent Hub coordinator. Only schedule *calculation* is shared.

---

## Design

### 1. Shared Schedule Computation

**Do not write a new `_compute_next()` in `agent_coordinator.py`.** Instead, the Agent
Hub coordinator imports and calls the existing `compute_next_run()` from
`src/task_scheduler.py` (line 67). This function already handles:

- `schedule="cron"` + `cron_expression` → `croniter`-based next run
- `schedule="daily"` + `scheduled_time` (HH:MM)
- `schedule="weekly"` + `scheduled_time` + `scheduled_day` (0=Monday)
- `schedule="monthly"` + day-of-month clamping
- `schedule="once"` + `scheduled_date`
- Timezone-aware computation (via `tz_name` parameter)
- Malformed-input resilience (returns `None` on bad cron or HH:MM)

The Agent Hub adds one new schedule type not in `compute_next_run`: **interval**
(`every 2h`, `every 1d`). This is a small wrapper — parse the interval expression
into a timedelta, advance from `after` or now, return naive UTC. Add this as a
companion function `compute_next_interval(expr: str, after=None) -> datetime | None`
alongside `compute_next_run()` in `task_scheduler.py`.

**Why not extend `compute_next_run()` directly:** Its signature is tied to
`ScheduledTask` fields (`schedule`, `scheduled_time`, `scheduled_day`, etc.).
Agent Hub uses a flatter schema (`schedule_type`, `schedule_expr`). The wrapper
pattern avoids coupling the two models while keeping the computation in one file.

### 2. Model Changes (`core/database.py`)

Six new columns on `AgentTask` (plus a migration with indexes):

```
schedule_type:           String(20), nullable       # 'once', 'interval', 'cron', null
schedule_expr:           String(100), nullable      # '30m', 'every 2h', '0 9 * * *', ISO ts
next_run_at:             DateTime, nullable          # computed next activation time
allow_overlap:           Boolean, default=False      # allow concurrent clones from same template
scheduled_template_id:   String, nullable, index     # FK → agent_tasks.id (set on clones, NOT templates)
scheduled_run_at:        DateTime, nullable          # the next_run_at that triggered this clone
```

Indexes:
- `ix_agent_tasks_next_run` on `(status, next_run_at)` — accelerates the due-task query
- `ix_agent_tasks_scheduled_template` on `(scheduled_template_id)` — enables "show all runs from this recurrence"

Note: `scheduled_template_id` and `scheduled_run_at` are set on **clones** (not the
template itself). The template carries the schedule fields; clones carry the lineage
pointers back to the template. See section 4.

Null `schedule_type` = immediate task (current behavior, unchanged).

### 3. Status Machine Changes

This is the largest integration surface. `scheduled` and `paused` are new statuses.

#### VALID_STATUSES and TRANSITIONS (`routes/agent_hub_routes.py:30`)

New statuses: `scheduled`, `paused`.

```
VALID_STATUSES = {
    "draft", "queued", "running", "waiting_for_approval",
    "blocked", "done", "cancelled",
    "scheduled", "paused",              # NEW
}
```

New transitions:

```
"scheduled":  {"queued", "paused", "cancelled"},
"paused":     {"scheduled", "cancelled"},
```

- `draft` → `scheduled` via create (schedule fields present)
- `scheduled` → `queued` via coordinator activation (automatic) or manual resume
- `scheduled` → `paused` via update/pause
- `paused` → `scheduled` via resume (recomputes `next_run_at`)
- `scheduled`/`paused` → `cancelled` (terminal)
- `done` and `cancelled` remain terminal (no transitions out)

#### Cascade: Every place that checks status must handle these two

| Touchpoint | File | Change |
|---|---|---|
| Create validation | `agent_hub_routes.py:586` | Accept `scheduled` (not just `queued`/`draft`) |
| Update validation | `agent_hub_routes.py:666` | Already delegates to `_validate_transition()` — works if TRANSITIONS is updated |
| Transition endpoint | `agent_hub_routes.py:1004` | Same — delegates |
| List filter | `agent_hub_routes.py:529` | Already validates against VALID_STATUSES — works |
| Batch cancel | `agent_hub_routes.py:1182` | Cancel sets `cancelled`, clears `locked_by` — works for `scheduled`/`paused` (no lock). Also clears `next_run_at` to `None` so the startup sweep doesn't revive it. |
| Batch retry | `agent_hub_routes.py:1186` | **Special-cased for scheduled/paused templates.** Retry on a `scheduled` or `paused` task means "run now once, then resume schedule": create a one-shot clone (`scheduled_template_id` + `scheduled_run_at` set, `schedule_type=null`, `status=queued`), leave the template untouched. This preserves the recurrence — the template still fires at its next `next_run_at`. Retry on `queued`/`blocked` tasks keeps existing behavior (reset to `queued`). Retry on `scheduled`/`paused` without this special case would silently destroy schedule intent by setting `status=queued` directly. |
| `_task_to_dict()` | `agent_hub_routes.py:130` | Include new fields: `schedule_type`, `schedule_expr`, `next_run_at`, `allow_overlap` |
| SSE publish | `agent_hub_events.py` | No change — `_task_to_dict()` feeds SSE |
| UI status dropdown | `agentHub.js:112` | Add `Scheduled` and `Paused` options |
| UI client-side filter | `agentHub.js:417` | No change (string match on `t.status`) |
| Status badge CSS | `style.css` | New colors: scheduled=#5b8abf (blue), paused=#888 (gray) |
| Timeline rendering | `agentHub.js` | No change — status is just a string in events |
| Stats/counts | `agentHub.js` | If derived from `_taskMap`, no change. If hardcoded status lists exist, add entries. |

### 4. Recurrence Model: Clone-on-Run with Lineage

Recurring tasks are **templates** (status=`scheduled`). When `next_run_at` arrives:

1. Clone the template (new UUID, same owner/role/objective/sandbox/priority/tags)
2. Set clone to `status='queued'` (enters normal dispatch flow)
3. Set clone lineage fields (see below)
4. Record event on clone: "Auto-activated by schedule from '[template title]'"
5. Advance template's `next_run_at` to the next computed time
6. Clone runs through the normal lifecycle independently

One-shot scheduled tasks transition `scheduled` → `queued` on activation (no clone).

#### Clone Lineage Fields

The clone carries these fields linking it back to the template:

```
scheduled_template_id:   Column(String, nullable)   # FK → agent_tasks.id (the template)
scheduled_run_at:        Column(DateTime, nullable)  # the next_run_at that triggered this clone
```

These are set on the clone (NOT the template). They enable:

- "Show all runs from this recurrence" (query by `scheduled_template_id`)
- "When was this particular run scheduled?" (display `scheduled_run_at`)
- Deduping activation (if `_tick()` double-runs, skip clones already created for this `next_run_at`)
- Debugging: trace a failed clone back to its template

**Do NOT reuse `created_by_task_id` for this.** That field already means "the agent task
that spawned this subtask via `create_task` action." Overloading it would blur agent-created
subtasks with schedule-created runs and break the existing `_execute_create_task_actions()`
logic that checks `created_by_task_id` for chain ownership.

#### Clone Field Inheritance

| Field | Inherited? | Notes |
|---|---|---|
| title, objective | Yes | |
| owner | Yes | critical: coordinator ignores unowned tasks |
| role | Yes | resolved to adapter at claim time |
| depends_on | **Conditional** | See dependency policy below |
| chain_task_id | Yes | chain activates normally after clone completes |
| sandbox_mode | Yes | |
| priority, tags | Yes | |
| schedule_type, schedule_expr, next_run_at | **No** | Clone is NOT scheduled |
| allow_overlap | No | Only template carries this |
| scheduled_template_id | Set to template's ID | |
| scheduled_run_at | Set to template's `next_run_at` at clone time | |
| created_by_task_id | No | Not set (not an agent-created subtask) |

### 5. Dependency Policy for Recurring Templates

CODEIX FLAGGED: "Inheriting `depends_on` means every future recurrence depends on the
original dependency tasks forever."

**Policy: Recurring template dependencies must be other due templates.**

At create time, if a template has `depends_on` set:

- Validate that each dependency ID refers to a task with `schedule_type IN ('interval', 'cron')`
  and status in `{scheduled, paused}`. If any dependency is an immediate/one-shot task,
  reject with 400: "Recurring template dependencies must be other scheduled templates, not one-shot tasks."
- Reject cross-template dependencies where the schedules are not aligned: if template A
  depends on template B but B is not due in the same tick, A's clone would get
  `depends_on=[B_template_id]` and block forever (B stays `scheduled`, never `done`).
  The simplest rule: **all dependency templates must share the same `schedule_expr` and
  `schedule_type` as the dependent.** Validate at create time; reject with 400 if schedules
  differ.

At activation time (when `_tick()` clones due templates):

- Clone all due templates first, tracking template_id → clone_id
- Then resolve dependencies: for each clone whose template had `depends_on`, map the
  dependency template IDs to the clone IDs created in this tick
- Set the clone's `depends_on` to the mapped clone IDs. All dependencies resolve to
  clones in the same batch — no stale references, no blocking.

This gives "depends on the current run of task B" — clean, predictable, no stale references.

**No static gate support.** Templates depend only on other templates with aligned
schedules. Static "done" gates are a Phase 2 feature requiring a separate policy
(dependency satisfication on pre-existing terminal tasks).

### 6. Overlap Policy

CODEIX FLAGGED: "Independent clones allow pileups if a task runs longer than its interval."

**Template-level policy: `allow_overlap` (Boolean, default `false`).**

When `allow_overlap=false` (default):

- Before cloning, check if any task with `scheduled_template_id == template.id`
  has status in `{queued, running, waiting_for_approval}`.
- If found, skip this activation. Log: "Skipped scheduled run — prior clone still active."
- The template's `next_run_at` still advances (we're skipping this interval, not retrying).

When `allow_overlap=true`:

- Clone unconditionally. Multiple concurrent runs from the same template are allowed.
- Appropriate for diagnostics/scanning tasks where concurrent runs don't interfere.

This is a simple DB query, no in-memory state needed.

### 7. Restart / Overdue Behavior

CODEIX FLAGGED: "Copy lessons from TaskScheduler regression test for restart double-firing
overdue tasks."

The existing `TaskScheduler.start()` (line 377-402 in `task_scheduler.py`) has a proven pattern:

1. On startup, query `ScheduledTask` with `status="active"` AND `next_run < now`
2. Advance their `next_run` to `now + 60s`
3. This prevents the in-memory `_executing` guard (which resets on restart) from
   re-dispatching the same overdue task on every poll

**The Agent Hub coordinator mirrors this exactly.** In `start_coordinator()` (or the
startup function that launches the coordinator loop), add a startup sweep:

```python
# On startup: advance overdue scheduled Agent Hub tasks by 60s
db = SessionLocal()
try:
    now = _utcnow()
    overdue = db.query(AgentTask).filter(
        AgentTask.status == "scheduled",
        AgentTask.next_run_at.isnot(None),
        AgentTask.next_run_at < now,
    ).all()
    if overdue:
        for t in overdue:
            t.next_run_at = now + timedelta(seconds=60)
        db.commit()
        logger.info("Pushed next_run_at forward for %d overdue scheduled tasks", len(overdue))
finally:
    db.close()
```

**Catch-up policy:** "Activate once, then advance from now." On restart, the overdue
sweep ensures at most one activation happens (the first tick after the 60s window).
Missed intervals are NOT backfilled — the template picks up from the next computed
time after the 60s offset. This matches the existing TaskScheduler behavior and avoids
flooding the queue with backlogged runs.

**Paused tasks with old `next_run_at`:** Not advanced. A paused task is not overdue
for execution — same behavior as `test_startup_does_not_advance_paused_tasks` in the
existing test suite. When the user resumes (paused → scheduled), `next_run_at` is
recomputed from `now` using `compute_next_run()`.

### 8. Schedule Expression Parser

Add one new function to `src/task_scheduler.py` alongside `compute_next_run()`:

```python
def compute_next_interval(expr: str, after: datetime = None) -> datetime | None:
    """Parse interval expressions and return next run as naive UTC.

    Supports both recurring ('every 2h') and relative-delay one-shot ('30m', '2h').
    The 'every ' prefix is optional; its presence signals recurrence (caller decides).
    """
    import re
    m = re.match(r'^(?:every\s+)?(\d+)\s*(h|m|d)$', expr.strip(), re.IGNORECASE)
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2).lower()
    if n <= 0:
        return None
    delta = {"h": timedelta(hours=n), "m": timedelta(minutes=n), "d": timedelta(days=n)}[unit]
    now = after or _utcnow()
    return now + delta
```

The Agent Hub coordinator calls this for `schedule_type="interval"`. For `cron` and
`once`, it calls `compute_next_run()` directly. For relative-delay one-shots (`30m`,
`2h`), the `compute_next_interval()` function handles those too (they're just
intervals without the `every` prefix).

**croniter is in requirements.txt (line 43).** No new dependency. The feature gates
gracefully if croniter is somehow unavailable (501 on cron-type schedules, intervals
still work).

### 9. Coordinator Changes (`src/agent_coordinator.py`)

A new method `_activate_due_scheduled()` called at the top of `_tick()`, **before**
the existing claim loop. Pseudocode:

```python
from src.task_scheduler import compute_next_run, compute_next_interval, _utcnow

async def _activate_due_scheduled(db):
    """Find scheduled tasks due for activation and transition/clone them."""
    now = _utcnow()
    due = db.query(AgentTask).filter(
        AgentTask.status == "scheduled",
        AgentTask.next_run_at <= now,
    ).order_by(AgentTask.next_run_at).all()

    if not due:
        return

    # Phase 1: Clone all recurring templates, track mapping
    clone_map = {}  # template_id → clone_id
    for template in due:
        if template.schedule_type in ("interval", "cron"):
            # Overlap check
            if not template.allow_overlap:
                active_clone = db.query(AgentTask).filter(
                    AgentTask.scheduled_template_id == template.id,
                    AgentTask.status.in_(["queued", "running", "waiting_for_approval"]),
                ).first()
                if active_clone:
                    # Skip — prior run still active. Advance next_run_at so we don't retry.
                    template.next_run_at = _compute_next_for_template(template, after=now)
                    continue

            # Clone
            clone = AgentTask(
                id=str(uuid.uuid4()),
                title=template.title,
                objective=template.objective,
                role=template.role,
                owner=template.owner,
                status="queued",
                priority=template.priority,
                sandbox_mode=template.sandbox_mode,
                tags=template.tags,
                chain_task_id=template.chain_task_id,
                scheduled_template_id=template.id,
                scheduled_run_at=template.next_run_at,
                # depends_on resolved in Phase 2
            )
            db.add(clone)
            db.flush()  # get clone.id
            _record_event(db, clone.id, "coordinator", "status_change",
                          summary=f"Auto-activated by schedule from '{template.title}'")
            clone_map[template.id] = clone.id

            # Advance template
            template.next_run_at = _compute_next_for_template(template, after=now)

        elif template.schedule_type == "once":
            # Transition directly
            template.status = "queued"
            template.next_run_at = None
            _record_event(db, template.id, "coordinator", "status_change",
                          summary="Scheduled task activated")

    # Phase 2: Resolve dependencies for clones
    for template_id, clone_id in list(clone_map.items()):
        template = next(t for t in due if t.id == template_id)
        if template.depends_on:
            mapped = []
            skip = False
            for dep_id in template.depends_on:
                if dep_id in clone_map:
                    mapped.append(clone_map[dep_id])
                else:
                    # Dependency template wasn't due in this tick. Since schedules
                    # must align (enforced at create), this means the dep was
                    # paused or deleted — it cannot resolve. Skip this clone
                    # entirely and advance the template's next_run_at so we
                    # don't retry the same dead dep on every tick.
                    skip = True
                    logger.warning(
                        "Skipping clone of template %s — dependency template %s not in due batch",
                        template_id, dep_id,
                    )
                    break
            if skip:
                clone_map.pop(template_id, None)
                continue
            clone = db.query(AgentTask).filter(AgentTask.id == clone_id).first()
            if clone:
                clone.depends_on = mapped

    db.commit()


def _compute_next_for_template(template, after=None):
    """Dispatch to the right compute function based on schedule_type."""
    if template.schedule_type == "cron":
        return compute_next_run("cron", None, cron_expression=template.schedule_expr, after=after)
    elif template.schedule_type == "interval":
        return compute_next_interval(template.schedule_expr, after=after)
    elif template.schedule_type == "once":
        return compute_next_run("once", None, scheduled_date=template.next_run_at)
    return None
```

### 10. Route Changes (`routes/agent_hub_routes.py`)

**TaskCreate schema** — add fields:
```python
schedule_type: Optional[str] = None   # 'once', 'interval', 'cron'
schedule_expr: Optional[str] = None   # '30m', 'every 2h', '0 9 * * *'
allow_overlap: bool = False
```

Validation on create:
- If `schedule_type` is set, `schedule_expr` is required (400 if missing)
- If `schedule_type="cron"`, validate via `croniter` at route level (422 on bad expression)
- If `schedule_type="interval"`, validate via regex (422 on bad format)
- If `schedule_type="once"`, `schedule_expr` must be a relative delay (`30m`, `2h`) or an
  ISO 8601 timestamp (`2026-06-15T02:00:00`). For ISO timestamps, parse with
  `datetime.fromisoformat()` before passing to `compute_next_run("once", ...,
  scheduled_date=parsed_dt)`. `compute_next_run()` does NOT parse ISO strings — it
  expects a pre-parsed `datetime` object in `scheduled_date`.
- If `schedule_type` is set, compute `next_run_at` and set `status="scheduled"`
- If `schedule_type` is null, status defaults to `draft` (current behavior)

**TaskUpdate schema** — add same fields (all Optional).

**List endpoint** — add `?scheduled=true` filter (shorthand for `?status=scheduled`).
Also support `?status=scheduled` and `?status=paused` directly (VALID_STATUSES covers them).

**Dependency validation** (`_validate_dependencies`) — when creating a scheduled template
with `depends_on`, validate that each dependency is itself a scheduled template (status
in `{scheduled, paused}`). Reject with 400 if any dependency is not a template.

**Template deletion** — When deleting a template, check for clones with `scheduled_template_id`
pointing to it and status in `{queued, running, waiting_for_approval}`.

- If **active** clones exist (non-terminal status): block deletion with 409:
  "Cannot delete template — N scheduled runs still active. Cancel them first."
- If only **historical** clones exist (status in `{done, cancelled, blocked}`):
  allow deletion. Clones retain their `scheduled_template_id` as lineage — it
  becomes a dangling reference (the template is gone, but the clone's history
  still says "created from template X"). The UI handles this gracefully: if
  `scheduled_template_id` doesn't resolve to a live task, show the ID as
  monospace text instead of a clickable link.

This avoids the cleanup trap where a template with years of historical runs
becomes impossible to delete.

### 11. UI Changes

**Status dropdown** (`agentHub.js:112`) — add two options:
```html
<option value="scheduled">Scheduled</option>
<option value="paused">Paused</option>
```

**New-task form** — schedule section (collapsed by default):
- Radio: "Now" | "Schedule once" | "Recurring"
- "Schedule once": date/time input or relative delay (`30m`, `2h`)
- "Recurring": interval picker (`every [N] [hours/days]`) + cron toggle
- Batch operations: select scheduled tasks → Cancel (sets `cancelled`), Pause, Resume

**Task detail view** — show schedule info when applicable:
- Next run time (with countdown)
- Schedule expression (human-readable)
- "Pause" / "Resume" button
- Lineage: "Template for [N] past runs" with link to filter by `scheduled_template_id`

**CSS:** No emoji. Scheduled = blue (#5b8abf) border-left indicator. Paused = gray (#888).
Overdue scheduled tasks (next_run_at < now) = amber warning.

### 12. Edge Cases & Guardrails

| Edge case | Behavior |
|---|---|
| Template deleted with active clones | Block (409). User cancels active clones first. |
| Template deleted with only historical clones | Allowed. Clones keep `scheduled_template_id` as dangling lineage. |
| Coordinator restart | Startup sweep advances overdue `next_run_at` by 60s. At most one activation. |
| Overlapping runs (`allow_overlap=false`) | Skip activation. Advance `next_run_at`. Log event. |
| Overlapping runs (`allow_overlap=true`) | Clone unconditionally. Concurrent runs allowed. |
| Template has `depends_on` non-template task | Rejected at create time (400). |
| Template `depends_on` another template with different schedule | Rejected at create time (400) — schedules must align. |
| Schedule expression invalid | Rejected at create/edit (422). |
| `croniter` not installed | Cron schedules return 501. Intervals + one-shots still work. |
| Mass activation (many due at once) | All processed in one tick before claim loop. Bounded by human-manageable template count. |
| Paused task with old `next_run_at` | Not advanced on startup. Recomputed on resume. |
| Clone inherits chain | `chain_task_id` copied. Chain activates normally after clone completes. |
| Batch retry on scheduled template | Creates one-shot clone, leaves template unchanged (preserves recurrence). |
| Batch cancel on scheduled template | Sets `cancelled`, clears `next_run_at`. |

### 13. Test Plan

`tests/test_scheduled_tasks.py` — new file, ~150-200 lines:

1. **One-shot activation** — create scheduled task 1s in future, advance clock, verify `scheduled` → `queued`
2. **Recurring clone creation** — create interval template, tick, verify clone exists with correct fields
3. **Recurring clone lineage** — verify `scheduled_template_id` and `scheduled_run_at` on clone
4. **Template advances next_run_at** — verify template's `next_run_at` updated after clone
5. **Cron parsing** — `0 9 * * *` → valid next run
6. **Interval parsing** — `every 2h` → valid next run, `every banana` → 422
7. **Invalid expression rejection** — 422 on bad cron, bad interval
8. **Overlap prevention** — template with `allow_overlap=false`, active clone → skip
9. **Overlap allowed** — template with `allow_overlap=true`, active clone → still clone
10. **Dependency mapping** — template A depends on template B, both due → clone B created, A's dependency set to B's clone ID
11. **Dependency rejection — non-template** — template depends on non-template task → 400
12. **Dependency rejection — misaligned schedule** — template A (every 2h) depends on B (every 4h) → 400
13. **Template deletion blocked — active clones** — template with queued/running clone → 409
14. **Template deletion allowed — historical clones only** — template with only done/cancelled clones → 200, clones keep dangling scheduled_template_id
15. **Pause/resume** — `scheduled` → `paused` → `scheduled` with recomputed `next_run_at`
16. **Restart overdue sweep** — overdue `scheduled` task gets `next_run_at` advanced by 60s (mirrors `test_restart_does_not_re_dispatch_overdue_task`)
17. **Paused not advanced on restart** — paused task with old `next_run_at` untouched (mirrors `test_startup_does_not_advance_paused_tasks`)
18. **Status transitions** — all valid/invalid transitions from `scheduled`/`paused` tested
19. **Batch cancel scheduled** — batch cancel on `scheduled` task → `cancelled`, `next_run_at` cleared
20. **Batch retry scheduled** — retry on scheduled template → one-shot clone created, template unchanged
21. **SSE serialization** — `_task_to_dict()` includes new fields
22. **List filter** — `?status=scheduled` and `?status=paused` work
23. **Clone does not inherit schedule** — clone has `schedule_type=null`

### 14. Files Touched

| File | Change | Est. Lines |
|---|---|---|
| `src/task_scheduler.py` | `compute_next_interval()` + export `_utcnow` if needed | ~25 |
| `core/database.py` | 6 new columns + migration + 2 indexes | ~45 |
| `src/agent_coordinator.py` | `_activate_due_scheduled()` + startup sweep + `_compute_next_for_template()` | ~100 |
| `routes/agent_hub_routes.py` | VALID_STATUSES, TRANSITIONS, schemas, validations, batch retry special case, template deletion guard | ~90 |
| `static/js/agentHub.js` | Status dropdown, schedule UI in new-task form, pause/resume, detail view, dangling lineage display | ~120 |
| `static/style.css` | Schedule picker, status badges (scheduled/paused), countdown | ~40 |
| `tests/test_scheduled_tasks.py` | New test file (~23 tests) | ~200 |
| `tests/test_agent_hub_events.py` | Add status transition tests for scheduled/paused (or keep in new file) | ~30 |

**Total: ~650 lines across 8 files.** No new infrastructure. No new dependency (croniter already present).

Pitfall note from odysseus-development convention: after *any* Python change, restart the
server — don't tell the user to do it. Kill the old uvicorn process and start a new one,
then verify with curl.

---

### What This Is NOT

- **Not a DAG orchestrator** — no workflow graphs. Use dependency gating for ordering.
- **Not durable execution** — at-most-once delivery. Same tier Hermes cron sits at.
- **Not a separate scheduler daemon** — coordinator loop IS the daemon.
- **Not routed through TaskScheduler's execution engine** — Agent Hub keeps its own
  dispatch (role resolution, dependency gating, SSE, approval, adapter ownership).
