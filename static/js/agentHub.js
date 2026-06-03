/**
 * Agent Hub — multi-agent cockpit for task management.
 *
 * Two-pane layout: task list (left) + selected task detail / timeline (right).
 * Registers with modalManager so the sidebar button toggles minimize/restore/close.
 *
 * Live updates via SSE (GET /api/agent-hub/stream) — replaces 5s polling.
 * Client-side task cache (_taskMap) enables instant filtering without re-fetch.
 * Separate list-level timer interval so row timers keep ticking even when no
 * task is selected (previously _stopTimer() on deselect froze list timers).
 *
 * Exports: { openAgentHub, closeAgentHub, isAgentHubOpen }
 */

import * as Modals from './modalManager.js';
import { makeWindowDraggable } from './windowDrag.js';

const API_BASE = window.location.origin;
const FALLBACK_POLL_MS = 10000;  // slow poll only while EventSource is errored

const MODAL_ID = 'agent-hub-modal';

let _open = false;
let _selectedTaskId = null;
let _taskMap = new Map();            // taskId → full task object
let _eventSource = null;             // SSE connection
let _fallbackPoll = null;            // polling fallback when SSE errors
let _listTimerInterval = null;       // 1s tick for all visible row timers
let _detailTimerSince = null;        // ISO timestamp for selected task's running timer

// ── Public API ────────────────────────────────────────────────────────────────

export function openAgentHub() {
  if (_open) return;
  if (Modals.isMinimized(MODAL_ID)) {
    Modals.restore(MODAL_ID);
    _open = true;
    return;
  }
  _open = true;
  const modal = _getModal();
  modal.classList.remove('hidden', 'modal-minimized');
  modal.style.display = 'flex';
  Modals.register(MODAL_ID, {
    railBtnId: 'rail-agent-hub',
    sidebarBtnId: 'tool-agent-hub-btn',
    closeFn: () => _teardown(),
    restoreFn: () => {},
  });
  _startSSE();
  _startListTimer();  // always tick — list timers need it even without a selected task
  _fetchAndRender();   // initial data load (SSE init may arrive first, but belt-and-suspenders)
}

export function closeAgentHub() {
  if (!_open && !Modals.isMinimized(MODAL_ID)) return;
  if (Modals.isRegistered(MODAL_ID)) {
    Modals.close(MODAL_ID);
  } else {
    _teardown();
  }
}

export function isAgentHubOpen() {
  if (Modals.isMinimized(MODAL_ID)) return false;
  return _open;
}

// ── Modal element ─────────────────────────────────────────────────────────────

function _getModal() {
  let modal = document.getElementById(MODAL_ID);
  if (modal) return modal;
  modal = document.createElement('div');
  modal.id = MODAL_ID;
  modal.className = 'modal hidden';
  modal.innerHTML = `
    <div class="modal-content ah-modal-content">
      <div class="modal-header ah-header" data-drag-handle>
        <span class="modal-title">Agent Hub</span>
        <div class="ah-header-status" id="ah-coordinator-status"></div>
        <div class="modal-header-actions">
          <button class="ah-bindings-btn" id="ah-bindings-btn" title="Role Bindings">⚙</button>
          <button class="ah-refresh-btn" title="Refresh">⟳</button>
          <button class="modal-minimize-btn">_</button>
          <button class="modal-close-btn close-btn">✕</button>
        </div>
      </div>
      <div class="ah-body">
        <div class="ah-left-pane" id="ah-task-list">
          <div class="ah-list-toolbar">
            <input type="text" class="ah-filter ah-search-input" id="ah-search-input" placeholder="Search tasks…">
            <button class="ah-btn ah-btn-primary" id="ah-new-task-btn">+ New Task</button>
            <select class="ah-filter" id="ah-status-filter">
              <option value="">All Statuses</option>
              <option value="draft">Draft</option>
              <option value="queued">Queued</option>
              <option value="running">Running</option>
              <option value="waiting_for_approval">Waiting for Approval</option>
              <option value="blocked">Blocked</option>
              <option value="done">Done</option>
              <option value="cancelled">Cancelled</option>
            </select>
            <select class="ah-filter" id="ah-owner-filter">
              <option value="">Any Owner</option>
              <option value="user">User</option>
              <option value="hermes">Hermes</option>
              <option value="codex">Codex</option>
              <option value="cursor">Cursor</option>
            </select>
          </div>
          <div class="ah-task-items" id="ah-task-items">
            <div class="ah-empty">Loading tasks…</div>
          </div>
        </div>
        <div class="ah-splitter" id="ah-splitter"></div>
        <div class="ah-right-pane" id="ah-task-detail">
          <div class="ah-empty ah-empty-detail">Select a task to view its timeline</div>
        </div>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  // Enable drag + resize
  const _content = modal.querySelector('.ah-modal-content');
  const _header = modal.querySelector('.ah-header');
  if (_content && _header) {
    makeWindowDraggable(modal, {
      content: _content,
      header: _header,
      minWidth: 480,
      minHeight: 300,
    });
  }

  // ── Event bindings ──
  modal.querySelector('.modal-close-btn').addEventListener('click', closeAgentHub);
  modal.querySelector('.modal-minimize-btn').addEventListener('click', () => {
    Modals.minimize(MODAL_ID);
    _open = false;
    _stopSSE();
    _stopListTimer();
  });
  modal.querySelector('.ah-refresh-btn').addEventListener('click', () => _fetchAndRender());
  modal.querySelector('#ah-bindings-btn')?.addEventListener('click', () => _showBindings());
  modal.querySelector('#ah-new-task-btn').addEventListener('click', () => _showNewTaskForm());
  modal.querySelector('#ah-status-filter').addEventListener('change', () => _renderFromCache());
  modal.querySelector('#ah-owner-filter').addEventListener('change', () => _renderFromCache());

  // Debounced keyword search
  const searchInput = modal.querySelector('#ah-search-input');
  let _searchTimer = null;
  searchInput.addEventListener('input', () => {
    clearTimeout(_searchTimer);
    _searchTimer = setTimeout(() => _renderFromCache(), 300);
  });

  // ── Splitter drag ──
  _wireSplitter(modal);

  return modal;
}

// ── SSE ───────────────────────────────────────────────────────────────────────

function _startSSE() {
  _stopSSE();
  try {
    _eventSource = new EventSource(`${API_BASE}/api/agent-hub/stream`);
  } catch (_) {
    _startFallbackPoll();
    return;
  }

  _eventSource.addEventListener('init', (e) => {
    try {
      const data = JSON.parse(e.data);
      _taskMap.clear();
      for (const t of (data.tasks || [])) {
        _taskMap.set(t.id, t);
      }
      _renderFromCache();
      _renderCoordinatorStatus();
      if (_selectedTaskId) {
        const cached = _taskMap.get(_selectedTaskId);
        if (cached) {
          _renderTaskDetail(cached);
          _updateDetailTimerState(cached);
        } else {
          _selectedTaskId = null;
          _renderEmptyDetail();
        }
      }
      // SSE is healthy — stop any fallback poll
      _stopFallbackPoll();
    } catch (_) {}
  });

  _eventSource.addEventListener('task_created', (e) => {
    try {
      const t = JSON.parse(e.data);
      _taskMap.set(t.id, t);
      _renderFromCache();
    } catch (_) {}
  });

  _eventSource.addEventListener('task_updated', (e) => {
    try {
      const t = JSON.parse(e.data);
      _taskMap.set(t.id, t);
      _renderFromCache();
      // If this is the selected task, update the detail view surgically
      if (_selectedTaskId === t.id) {
        _renderTaskDetail(t);
        _updateDetailTimerState(t);
      }
    } catch (_) {}
  });

  _eventSource.addEventListener('task_deleted', (e) => {
    try {
      const data = JSON.parse(e.data);
      _taskMap.delete(data.id);
      _renderFromCache();
      if (_selectedTaskId === data.id) {
        _selectedTaskId = null;
        _renderEmptyDetail();
        _detailTimerSince = null;
      }
    } catch (_) {}
  });

  _eventSource.addEventListener('event_created', (e) => {
    try {
      const t = JSON.parse(e.data);
      _taskMap.set(t.id, t);
      if (_selectedTaskId === t.id) {
        _renderTaskDetail(t);
      }
      // Re-render list for badge / timer updates
      _renderFromCache();
    } catch (_) {}
  });

  _eventSource.addEventListener('coordinator_status', (e) => {
    try {
      const data = JSON.parse(e.data);
      _renderCoordinatorStatusFromData(data);
    } catch (_) {}
  });

  _eventSource.onerror = () => {
    // EventSource auto-reconnects, but also start fallback polling
    _startFallbackPoll();
  };
}

function _stopSSE() {
  if (_eventSource) {
    _eventSource.close();
    _eventSource = null;
  }
  _stopFallbackPoll();
}

function _startFallbackPoll() {
  _stopFallbackPoll();
  _fallbackPoll = setInterval(() => _fetchAndRender(), FALLBACK_POLL_MS);
}

function _stopFallbackPoll() {
  if (_fallbackPoll) { clearInterval(_fallbackPoll); _fallbackPoll = null; }
}

// ── Data fetching (manual refresh + fallback) ─────────────────────────────────

async function _fetchTasks() {
  const statusEl = document.getElementById('ah-status-filter');
  const ownerEl = document.getElementById('ah-owner-filter');
  const searchEl = document.getElementById('ah-search-input');
  const params = new URLSearchParams();
  const status = statusEl?.value;
  const owner = ownerEl?.value;
  const search = searchEl?.value.trim();
  if (status) params.set('status', status);
  if (owner) params.set('owner', owner);
  if (search) params.set('q', search);
  const url = `${API_BASE}/api/agent-hub/tasks${params.toString() ? '?' + params : ''}`;
  try {
    const res = await fetch(url, { credentials: 'same-origin' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    // Sync cache
    _taskMap.clear();
    for (const t of (data.tasks || [])) {
      _taskMap.set(t.id, t);
    }
    return data.tasks || [];
  } catch (e) {
    console.warn('Agent Hub: fetch tasks failed', e);
    return [];
  }
}

async function _fetchTask(taskId) {
  try {
    const res = await fetch(`${API_BASE}/api/agent-hub/tasks/${taskId}`, { credentials: 'same-origin' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const task = await res.json();
    _taskMap.set(task.id, task);  // update cache
    return task;
  } catch (e) {
    console.warn('Agent Hub: fetch task failed', e);
    return null;
  }
}

async function _fetchStatus() {
  try {
    const res = await fetch(`${API_BASE}/api/agent-hub/status`, { credentials: 'same-origin' });
    if (res.ok) return await res.json();
  } catch (_) {}
  return null;
}

// ── Client-side filtering + rendering ─────────────────────────────────────────

async function _fetchAndRender() {
  const tasks = await _fetchTasks();
  _renderTaskList(tasks);
  _renderCoordinatorStatus();
  _updateBadge(tasks);
  if (_selectedTaskId) {
    const task = await _fetchTask(_selectedTaskId);
    if (task) {
      _renderTaskDetail(task);
      _updateDetailTimerState(task);
    } else { _selectedTaskId = null; _renderEmptyDetail(); }
  }
}

function _getFilteredTasks() {
  const statusEl = document.getElementById('ah-status-filter');
  const ownerEl = document.getElementById('ah-owner-filter');
  const searchEl = document.getElementById('ah-search-input');
  const status = statusEl?.value || '';
  const owner = ownerEl?.value || '';
  const search = (searchEl?.value || '').trim().toLowerCase();

  let tasks = Array.from(_taskMap.values());

  if (status) {
    tasks = tasks.filter(t => t.status === status);
  }
  if (owner) {
    tasks = tasks.filter(t => t.current_owner === owner);
  }
  if (search) {
    tasks = tasks.filter(t =>
      (t.title || '').toLowerCase().includes(search) ||
      (t.objective || '').toLowerCase().includes(search)
    );
  }

  // Sort by updated_at descending
  tasks.sort((a, b) => {
    const da = a.updated_at ? new Date(a.updated_at) : 0;
    const db = b.updated_at ? new Date(b.updated_at) : 0;
    return db - da;
  });

  return tasks;
}

function _renderFromCache() {
  const tasks = _getFilteredTasks();
  _renderTaskList(tasks);
  _updateBadge(tasks);
  _tickAllListTimers();  // populate fresh timer spans immediately
}

function _renderTaskList(tasks) {
  const container = document.getElementById('ah-task-items');
  if (!container) return;
  if (!tasks.length) {
    container.innerHTML = '<div class="ah-empty">No tasks yet. Click "+ New Task" to create one.</div>';
    return;
  }
  container.innerHTML = tasks.map(t => {
    const activeClass = t.id === _selectedTaskId ? 'ah-task-item--active' : '';
    const statusDot = _statusDot(t.status);
    const runningAttr = t.started_at
      ? `data-locked-at="${t.started_at}"` : '';
    const terminalStatuses = ['done', 'cancelled', 'blocked'];
    const doneAttr = terminalStatuses.includes(t.status) && t.started_at && t.updated_at
      ? `data-done-at="${t.updated_at}"` : '';
    return `
      <div class="ah-task-item ${activeClass}" data-task-id="${t.id}" data-status="${t.status}" ${runningAttr} ${doneAttr}>
        <div class="ah-task-item-header">
          ${statusDot}
          <span class="ah-task-title">${_esc(t.title)}</span>
          <button class="ah-task-delete-btn" data-delete-id="${t.id}" title="Delete task">×</button>
        </div>
        <div class="ah-task-item-meta">
          ${t.role ? `<span class="ah-task-role-badge">${_esc(t.role)}</span>` : ''}
          ${t.depends_on && t.depends_on.length ? `<span class="ah-task-dep-badge" title="Waiting on ${t.depends_on.length} dependencies">⏳${t.depends_on.length}</span>` : ''}
          <span class="ah-task-owner">${t.current_owner || 'unassigned'}</span>
          <span class="ah-task-status">${t.status}</span>
          <span class="ah-task-timer ${t.started_at ? '' : 'ah-task-timer--hidden'}"></span>
        </div>
      </div>
    `;
  }).join('');
  container.querySelectorAll('.ah-task-item').forEach(el => {
    el.addEventListener('click', () => _selectTask(el.dataset.taskId));
  });
  container.querySelectorAll('.ah-task-delete-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      _deleteTask(btn.dataset.deleteId);
    });
  });
}

function _renderTaskDetail(task) {
  const container = document.getElementById('ah-task-detail');
  if (!container) return;
  const events = task.events || [];

  // Check if there are pending actions from the last adapter message
  let pendingActions = [];
  for (let i = events.length - 1; i >= 0; i--) {
    const e = events[i];
    if (e.event_type === 'message' && e.metadata_json) {
      try {
        const meta = JSON.parse(e.metadata_json);
        if (meta.actions_pending && meta.actions && meta.actions.length) {
          pendingActions = meta.actions;
        }
      } catch (_) {}
      break;
    }
  }

  const timeline = events.map(e => {
    const actorClass = _actorClass(e.actor);
    const typeLabel = _eventTypeLabel(e.event_type);
    let metaBlock = '';
    if (e.metadata_json) {
      try {
        const meta = JSON.parse(e.metadata_json);
        if (meta.actions && meta.actions.length) {
          const actionList = meta.actions.map(a =>
            `<div class="ah-action-chip ah-action-chip--${a.type}">${_esc(a.label || a.type)}</div>`
          ).join('');
          const pendingBadge = meta.actions_pending
            ? '<span class="ah-action-pending">pending approval</span>'
            : '<span class="ah-action-done">executed</span>';
          metaBlock = `<div class="ah-event-actions">${pendingBadge}${actionList}</div>`;
        }
      } catch (_) {}
    }
    return `
      <div class="ah-event ah-event--${e.event_type}">
        <div class="ah-event-header">
          <span class="ah-event-actor ah-actor--${actorClass}">${_esc(e.actor)}</span>
          <span class="ah-event-type-label">${typeLabel}</span>
          <span class="ah-event-time">${_formatTime(e.created_at)}</span>
        </div>
        ${e.summary ? `<div class="ah-event-summary">${_esc(e.summary)}</div>` : ''}
        ${e.content ? `<div class="ah-event-content">${_esc(e.content)}</div>` : ''}
        ${metaBlock}
      </div>
    `;
  }).reverse().join('');

  const approvalCallout = task.status === 'waiting_for_approval'
    ? `<div class="ah-approval-callout">
         <div class="ah-approval-callout-body">
           <strong>Approval required</strong>
           <p>The adapter has proposed actions that need your approval before execution.</p>
           ${pendingActions.length ? `<div class="ah-approval-actions-preview">
             ${pendingActions.map(a => `<div class="ah-action-chip ah-action-chip--${a.type}">${_esc(a.label || a.command || a.path)}</div>`).join('')}
           </div>` : ''}
           <button class="ah-btn ah-btn-primary ah-approve-big-btn" id="ah-approve-btn">Approve and Execute</button>
         </div>
       </div>`
    : '';

  container.innerHTML = `
    <div class="ah-detail-header">
      <h3 class="ah-detail-title">${_esc(task.title)}</h3>
      <div class="ah-detail-id" title="Click to copy task ID">${task.id}</div>
      <div class="ah-detail-meta">
        <span class="ah-detail-status">${_statusDot(task.status)} ${task.status}</span>
        <span class="ah-detail-phase">${task.phase || 'no phase'}</span>
        ${task.role ? (task.current_owner ? `<span class="ah-detail-role">Role: <span class="ah-role-tag">${_esc(task.role)}</span> → <span class="ah-owner-tag">${task.current_owner}</span></span>` : `<span class="ah-detail-role">Role: <span class="ah-role-tag">${_esc(task.role)}</span> <span class="ah-role-pending">(pending dispatch)</span></span>`) : ''}
        <span class="ah-detail-owner"><span class="ah-owner-tag">${task.current_owner || 'unassigned'}</span></span>
        ${task.depends_on && task.depends_on.length ? `<span class="ah-detail-deps">Waiting on: ${task.depends_on.map(id => `<span class="ah-dep-tag" data-dep-id="${id}">${id.slice(0,8)}</span>`).join(', ')}</span>` : ''}
        ${task.created_by_task_id ? `<span class="ah-detail-parent">Created by: <span class="ah-parent-tag" data-parent-id="${task.created_by_task_id}">${task.created_by_task_id.slice(0,8)}</span></span>` : ''}
        ${task.approval_required ? '<span class="ah-detail-approval">Approval required</span>' : ''}
        ${task.locked_by ? `<span class="ah-detail-locked">Locked by ${_esc(task.locked_by)}</span>` : ''}
        ${task.attempt_count > 0 ? `<span class="ah-detail-attempts">Attempt ${task.attempt_count}</span>` : ''}
        <span class="ah-detail-sandbox ${task.sandbox_mode === 'danger-full-access' ? 'ah-detail-sandbox--danger' : ''}">Sandbox: ${_esc(task.sandbox_mode || 'workspace-write')}</span>
        <span class="ah-detail-timer" id="ah-running-timer" style="display:none"></span>
      </div>
      <div class="ah-detail-objective">${_esc(task.objective || 'No objective set.')}</div>
    </div>
    ${approvalCallout}
    <div class="ah-detail-actions">
      <select class="ah-action-select" id="ah-assign-select">
        <option value="">Assign to…</option>
        <option value="user">User</option>
        <option value="hermes" ${task.current_owner === 'hermes' ? 'selected' : ''}>Hermes</option>
        <option value="codex" ${task.current_owner === 'codex' ? 'selected' : ''}>Codex</option>
        <option value="cursor">Cursor</option>
      </select>
      ${task.status !== 'waiting_for_approval' ? `<button class="ah-btn ah-btn-small ah-btn-primary" id="ah-approve-btn" disabled>Approve</button>` : ''}
      <button class="ah-btn ah-btn-small ah-btn-danger" id="ah-cancel-btn">Cancel</button>
      <button class="ah-btn ah-btn-small ah-btn-export" id="ah-export-btn">Export</button>
    </div>
    <div class="ah-chain-row">
      <span class="ah-chain-direction">Triggers:</span>
      <span class="ah-chain-label" id="ah-chain-label"></span>
      <span class="ah-chain-sep">|</span>
      <span class="ah-chain-direction">Triggered by:</span>
      <span class="ah-chain-label" id="ah-chain-parent-label"></span>
    </div>
    <div class="ah-composer">
      <textarea class="ah-composer-input" id="ah-composer-input" placeholder="Add a comment or event…" rows="2"></textarea>
      <button class="ah-btn ah-btn-small" id="ah-add-event-btn">Add Event</button>
    </div>
    <div class="ah-timeline">
      <h4 class="ah-timeline-title">Timeline</h4>
      ${timeline || '<div class="ah-empty">No events yet.</div>'}
    </div>
  `;

  // Click-to-copy task ID
  const idEl = container.querySelector('.ah-detail-id');
  if (idEl) {
    idEl.addEventListener('click', () => {
      navigator.clipboard.writeText(task.id).then(() => {
        idEl.classList.add('ah-detail-id--copied');
        setTimeout(() => idEl.classList.remove('ah-detail-id--copied'), 1200);
      }).catch(() => {});
    });
  }

  // Bind detail actions
  const assignSelect = document.getElementById('ah-assign-select');
  if (assignSelect) {
    assignSelect.addEventListener('change', async () => {
      const owner = assignSelect.value;
      if (!owner) return;
      await _apiCall('POST', `/api/agent-hub/tasks/${task.id}/assign`, { current_owner: owner });
      _selectTask(task.id);
    });
  }

  const approveBtn = document.getElementById('ah-approve-btn');
  if (approveBtn && task.status === 'waiting_for_approval') {
    approveBtn.addEventListener('click', async () => {
      const result = await _apiCall('POST', `/api/agent-hub/tasks/${task.id}/approve`);
      _selectTask(task.id);
      if (result && result.action_results && result.action_results.length) {
        _showToast(`Executed ${result.action_results.length} action(s): ${
          result.action_results.map(r => (r.success ? 'OK' : 'FAIL') + ' ' + r.label).join(', ')
        }`);
      }
    });
  }

  const cancelBtn = document.getElementById('ah-cancel-btn');
  if (cancelBtn) {
    cancelBtn.addEventListener('click', async () => {
      await _apiCall('POST', `/api/agent-hub/tasks/${task.id}/transition`, { status: 'cancelled', force_cancel: true });
      _selectTask(task.id);
    });
  }

  // Export timeline as .md download
  const exportBtn = document.getElementById('ah-export-btn');
  if (exportBtn) {
    exportBtn.addEventListener('click', () => {
      const a = document.createElement('a');
      a.href = `${API_BASE}/api/agent-hub/tasks/${task.id}/export`;
      a.download = `task-${task.id.slice(0, 8)}.md`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    });
  }

  const addEventBtn = document.getElementById('ah-add-event-btn');
  if (addEventBtn) {
    addEventBtn.addEventListener('click', async () => {
      const input = document.getElementById('ah-composer-input');
      const content = input.value.trim();
      if (!content) return;
      await _apiCall('POST', `/api/agent-hub/tasks/${task.id}/events`, {
        actor: 'user',
        event_type: 'message',
        summary: content,
      });
      input.value = '';
      _selectTask(task.id);
    });
  }

  // Chain labels — show both directions
  if (task.chain_task_id) {
    _fetchTask(task.chain_task_id).then(chained => {
      const label = document.getElementById('ah-chain-label');
      if (label && chained) {
        label.innerHTML = `<a class="ah-chain-link" href="#">${_esc(chained.title || chained.id)}</a>`;
        label.querySelector('a').addEventListener('click', (e) => {
          e.preventDefault(); _selectTask(task.chain_task_id);
        });
      }
    });
  } else {
    const label = document.getElementById('ah-chain-label');
    if (label) label.textContent = '(none)';
  }

  // Find which task triggers THIS one (reverse lookup from cache)
  const parent = Array.from(_taskMap.values()).find(t => t.chain_task_id === task.id);
  const parentLabel = document.getElementById('ah-chain-parent-label');
  if (parentLabel && parent) {
    parentLabel.innerHTML = `<a class="ah-chain-link" href="#">${_esc(parent.title || parent.id)}</a>`;
    parentLabel.querySelector('a').addEventListener('click', (e) => {
      e.preventDefault(); _selectTask(parent.id);
    });
  } else if (parentLabel) {
    parentLabel.textContent = '(none)';
  }
}

function _renderEmptyDetail() {
  const container = document.getElementById('ah-task-detail');
  if (container) container.innerHTML = '<div class="ah-empty ah-empty-detail">Select a task to view its timeline</div>';
}

async function _renderCoordinatorStatus() {
  const status = await _fetchStatus();
  _renderCoordinatorStatusFromData(status);
}

function _renderCoordinatorStatusFromData(status) {
  const el = document.getElementById('ah-coordinator-status');
  if (!el) return;
  if (status && status.running) {
    el.innerHTML = '<span class="ah-status-live">● Live</span>';
    el.title = `Tasks: ${status.tasks_total || 0}, Last tick: ${status.last_tick || 'N/A'}`;
  } else {
    el.innerHTML = '<span class="ah-status-idle">○ Idle</span>';
    el.title = 'Coordinator not running';
  }
}

// ── New task inline form ──────────────────────────────────────────────────────

function _showNewTaskForm() {
  const container = document.getElementById('ah-task-detail');
  if (!container) return;
  _selectedTaskId = null;
  _detailTimerSince = null;
  container.innerHTML = `
    <div class="ah-new-task-form">
      <h3>New Task</h3>
      <div class="ah-templates">
        <button class="ah-template-btn" data-template="bug">Bug Fix</button>
        <button class="ah-template-btn" data-template="feature">Feature</button>
        <button class="ah-template-btn" data-template="review">Code Review</button>
        <button class="ah-template-btn ah-template-btn--clear" data-template="blank">Clear</button>
      </div>
      <input class="ah-input" id="ah-new-title" placeholder="Title" value="">
      <textarea class="ah-input ah-input-textarea" id="ah-new-objective" placeholder="Describe what you would like to do" rows="4"></textarea>
      <div class="ah-new-task-row">
        <select class="ah-input" id="ah-new-owner">
          <option value="">Assign to…</option>
          <option value="user">User</option>
          <option value="hermes">Hermes</option>
          <option value="codex">Codex</option>
        </select>
        <select class="ah-input" id="ah-new-phase">
          <option value="">Phase (optional)</option>
          <option value="planning">Planning</option>
          <option value="implementation">Implementation</option>
          <option value="review">Review</option>
          <option value="verification">Verification</option>
        </select>
      </div>
      <div class="ah-new-task-row">
        <select class="ah-input" id="ah-new-role">
          <option value="">Role (optional)</option>
          <option value="diagnoser">Diagnoser</option>
          <option value="implementer">Implementer</option>
          <option value="verifier">Verifier</option>
        </select>
      </div>
      <div class="ah-new-task-row">
        <input class="ah-input" id="ah-new-chain" placeholder="Triggered by task ID (optional)" value="" style="flex:1;">
      </div>
      <div class="ah-new-task-row ah-new-task-row--sandbox">
        <label class="ah-sandbox-label">Sandbox:</label>
        <select class="ah-input ah-input--sandbox" id="ah-new-sandbox">
          <option value="read-only">Read-only</option>
          <option value="workspace-write" selected>Workspace</option>
          <option value="danger-full-access">Full access</option>
        </select>
        <span class="ah-sandbox-warning" id="ah-sandbox-warning" style="display:none">Danger: full system access</span>
      </div>
      <div class="ah-new-task-actions">
        <button class="ah-btn ah-btn-primary" id="ah-create-btn">Create Task</button>
        <button class="ah-btn" id="ah-cancel-new-btn">Cancel</button>
      </div>
    </div>
  `;

  container.querySelectorAll('.ah-template-btn').forEach(btn => {
    btn.addEventListener('click', () => _applyTemplate(btn.dataset.template));
  });

  const sandboxSelect = document.getElementById('ah-new-sandbox');
  const sandboxWarning = document.getElementById('ah-sandbox-warning');
  if (sandboxSelect && sandboxWarning) {
    sandboxSelect.addEventListener('change', () => {
      sandboxWarning.style.display = sandboxSelect.value === 'danger-full-access' ? '' : 'none';
    });
  }

  document.getElementById('ah-create-btn').addEventListener('click', async () => {
    const title = document.getElementById('ah-new-title').value.trim() || 'Untitled Task';
    const objective = document.getElementById('ah-new-objective').value.trim();
    const owner = document.getElementById('ah-new-owner').value;
    const phase = document.getElementById('ah-new-phase').value;
    const chainId = document.getElementById('ah-new-chain').value.trim();
    const sandbox = document.getElementById('ah-new-sandbox').value;
    const role = document.getElementById('ah-new-role')?.value || undefined;
    const body = { title, objective, current_owner: owner || undefined, role, phase: phase || undefined, sandbox_mode: sandbox };
    // Auto-queue if assigned to a non-user agent OR a role is set
    if ((owner && owner !== 'user') || role) body.status = 'queued';
    const task = await _apiCall('POST', '/api/agent-hub/tasks', body);
    if (task) {
      if (chainId) {
        await _apiCall('PUT', `/api/agent-hub/tasks/${chainId}`, { chain_task_id: task.id });
      }
      _fetchAndRender(); _selectTask(task.id);
    }
  });
  document.getElementById('ah-cancel-new-btn').addEventListener('click', () => { _selectedTaskId = null; _renderEmptyDetail(); });
}

// ── Templates ─────────────────────────────────────────────────────────────────

const TASK_TEMPLATES = {
  bug: {
    title: 'Bug: ',
    objective: '## Steps to reproduce\n1. \n2. \n3. \n\n## Expected behavior\n\n\n## Actual behavior\n\n',
    phase: 'review',
  },
  feature: {
    title: 'Feature: ',
    objective: '## Description\n\n\n## Acceptance criteria\n- [ ] \n- [ ] \n\n## Files to modify\n- \n',
    phase: 'planning',
  },
  review: {
    title: 'Review: ',
    objective: '## What to review\n\n\n## Focus areas\n- \n- \n\n## Questions\n- \n',
    phase: 'review',
  },
  blank: {
    title: '',
    objective: '',
    phase: '',
  },
};

function _applyTemplate(name) {
  const tpl = TASK_TEMPLATES[name];
  if (!tpl) return;
  const titleEl = document.getElementById('ah-new-title');
  const objEl = document.getElementById('ah-new-objective');
  const phaseEl = document.getElementById('ah-new-phase');
  if (titleEl) titleEl.value = tpl.title;
  if (objEl) objEl.value = tpl.objective;
  if (phaseEl && tpl.phase) phaseEl.value = tpl.phase;
  document.querySelectorAll('.ah-template-btn').forEach(b => {
    b.classList.toggle('ah-template-btn--active', b.dataset.template === name);
  });
}

// ── Selection ─────────────────────────────────────────────────────────────────

async function _selectTask(taskId) {
  _selectedTaskId = taskId;
  const task = _taskMap.get(taskId) || await _fetchTask(taskId);
  if (!task) { _selectedTaskId = null; _renderEmptyDetail(); return; }
  _renderTaskDetail(task);
  _updateDetailTimerState(task);
  // Update list highlight
  document.querySelectorAll('.ah-task-item').forEach(el => {
    el.classList.toggle('ah-task-item--active', el.dataset.taskId === taskId);
  });
}

// ── Delete ───────────────────────────────────────────────────────────────────

async function _deleteTask(taskId) {
  await _apiCall('DELETE', `/api/agent-hub/tasks/${taskId}`);
  if (_selectedTaskId === taskId) { _selectedTaskId = null; _detailTimerSince = null; _renderEmptyDetail(); }
  _fetchAndRender();
}

// ── Timers (split: list-level always on, detail per-selection) ────────────────

function _startListTimer() {
  if (_listTimerInterval) return;
  _listTimerInterval = setInterval(_tickAllListTimers, 1000);
  _tickAllListTimers();
}

function _stopListTimer() {
  if (_listTimerInterval) { clearInterval(_listTimerInterval); _listTimerInterval = null; }
}

function _tickAllListTimers() {
  // Update every .ah-task-item[data-locked-at] row timer
  document.querySelectorAll('.ah-task-item[data-locked-at]').forEach(item => {
    const timerEl = item.querySelector('.ah-task-timer');
    if (!timerEl) return;
    const status = item.dataset.status;
    try {
      const started = new Date(item.dataset.lockedAt);
      const doneAt = item.dataset.doneAt ? new Date(item.dataset.doneAt) : null;
      const end = (status !== 'running' && doneAt) ? doneAt : Date.now();
      const elapsed = Math.max(0, Math.floor((end - started.getTime()) / 1000));
      const mins = Math.floor(elapsed / 60);
      const secs = elapsed % 60;
      timerEl.textContent = `${mins}:${String(secs).padStart(2, '0')}`;
      timerEl.classList.remove('ah-task-timer--hidden');
      timerEl.classList.remove('ah-task-timer--done', 'ah-task-timer--blocked', 'ah-task-timer--cancelled', 'ah-task-timer--running', 'ah-task-timer--waiting');
      if (status === 'running') {
        timerEl.classList.add('ah-task-timer--running');
      } else if (status === 'waiting_for_approval') {
        timerEl.classList.add('ah-task-timer--waiting');
      } else if (status === 'done') {
        timerEl.classList.add('ah-task-timer--done');
      } else if (status === 'blocked') {
        timerEl.classList.add('ah-task-timer--blocked');
      } else if (status === 'cancelled') {
        timerEl.classList.add('ah-task-timer--cancelled');
      }
    } catch (_) {
      timerEl.classList.add('ah-task-timer--hidden');
    }
  });

  // Detail timer
  _tickDetailTimer();
}

function _updateDetailTimerState(task) {
  if (task && task.status === 'running' && task.locked_at) {
    _detailTimerSince = task.locked_at;
  } else {
    _detailTimerSince = null;
  }
  _tickDetailTimer();
}

function _tickDetailTimer() {
  const el = document.getElementById('ah-running-timer');
  if (!el) return;
  if (_detailTimerSince) {
    try {
      const elapsed = Math.max(0, Math.floor((Date.now() - new Date(_detailTimerSince).getTime()) / 1000));
      const mins = Math.floor(elapsed / 60);
      const secs = elapsed % 60;
      el.textContent = `${mins}:${String(secs).padStart(2, '0')}`;
      el.style.display = '';
      el.classList.toggle('ah-timer-pulse', elapsed > 0);
    } catch (_) {
      el.style.display = 'none';
    }
  } else {
    el.style.display = 'none';
  }
}

// ── Badge ────────────────────────────────────────────────────────────────────

function _updateBadge(tasks) {
  const dot = document.getElementById('ah-notif-dot');
  if (!dot) return;
  const pending = tasks.filter(t =>
    t.status === 'queued' || t.status === 'waiting_for_approval'
  ).length;
  if (pending > 0) {
    dot.textContent = pending;
    dot.style.display = '';
  } else {
    dot.style.display = 'none';
  }
}

// ── Teardown ──────────────────────────────────────────────────────────────────

function _teardown() {
  _open = false;
  _selectedTaskId = null;
  _detailTimerSince = null;
  _stopSSE();
  _stopListTimer();
  const modal = document.getElementById(MODAL_ID);
  if (modal) modal.style.display = 'none';
}

// ── Role Bindings ─────────────────────────────────────────────────────────────

let _bindingsVisible = false;

async function _showBindings() {
  const container = document.getElementById('ah-task-detail');
  if (!container) return;

  if (_bindingsVisible) {
    _bindingsVisible = false;
    _renderEmptyDetail();
    return;
  }

  _bindingsVisible = true;
  _selectedTaskId = null;
  _detailTimerSince = null;

  const bindings = await _fetchBindings();

  container.innerHTML = `
    <div class="ah-new-task-form">
      <h3>Role Bindings</h3>
      <p class="ah-bindings-hint">Tasks target roles. The coordinator resolves roles to agents at dispatch time.</p>
      <div class="ah-bindings-list" id="ah-bindings-list">
        ${bindings.map(b => _renderBindingRow(b)).join('')}
      </div>
    </div>
  `;

  // Wire up dropdowns for changing bindings
  container.querySelectorAll('.ah-binding-select').forEach(select => {
    select.addEventListener('change', async () => {
      const role = select.dataset.role;
      const owner = select.dataset.owner || null;
      const adapter = select.value;
      await _updateBinding(role, adapter, owner);
      _showBindings(); // refresh
    });
  });
}

async function _fetchBindings() {
  try {
    const res = await fetch(`${API_BASE}/api/agent-hub/bindings`, { credentials: 'same-origin' });
    if (!res.ok) return [];
    const data = await res.json();
    return data.bindings || [];
  } catch (_) { return []; }
}

async function _updateBinding(role, adapter_name, owner) {
  await fetch(`${API_BASE}/api/agent-hub/bindings`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'same-origin',
    body: JSON.stringify({ role, adapter_name, owner }),
  });
}

function _renderBindingRow(b) {
  const ownerLabel = b.owner ? ` (${b.owner})` : ' (global)';
  return `
    <div class="ah-binding-row">
      <span class="ah-binding-role">${_esc(b.role)}${ownerLabel}</span>
      <span class="ah-binding-arrow">→</span>
      <select class="ah-binding-select" data-role="${b.role}" data-owner="${b.owner || ''}">
        <option value="hermes" ${b.adapter_name === 'hermes' ? 'selected' : ''}>Hermes</option>
        <option value="codex" ${b.adapter_name === 'codex' ? 'selected' : ''}>Codex</option>
        <option value="cursor" ${b.adapter_name === 'cursor' ? 'selected' : ''}>Cursor</option>
      </select>
    </div>
  `;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function _apiCall(method, path, body) {
  try {
    const opts = { method, headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin' };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(`${API_BASE}${path}`, opts);
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      console.warn(`Agent Hub: ${method} ${path} failed`, err);
      return null;
    }
    return await res.json();
  } catch (e) {
    console.warn(`Agent Hub: ${method} ${path} error`, e);
    return null;
  }
}

function _statusDot(status) {
  const colors = {
    draft: '#888', queued: '#5b8abf', running: '#f0a030',
    waiting_for_approval: '#e060a0', blocked: '#cc4444',
    done: '#44aa44', cancelled: '#888',
  };
  const color = colors[status] || '#888';
  return `<span class="ah-status-dot" style="background:${color}" title="${status}"></span>`;
}

function _formatTime(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    return d.toLocaleString();
  } catch (_) { return iso; }
}

function _esc(s) {
  if (!s) return '';
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

// ── Splitter ─────────────────────────────────────────────────────────────────

const SPLITTER_KEY = 'odysseus-ah-splitter';
const SPLITTER_MIN_LEFT = 160;
const SPLITTER_MIN_RIGHT = 200;

function _wireSplitter(modal) {
  const splitter = modal.querySelector('#ah-splitter');
  const left = modal.querySelector('#ah-task-list');
  if (!splitter || !left) return;

  try {
    const saved = localStorage.getItem(SPLITTER_KEY);
    if (saved) left.style.width = saved + 'px';
  } catch (_) {}

  let dragging = false;
  let startX = 0;
  let startW = 0;

  splitter.addEventListener('mousedown', (e) => {
    dragging = true;
    startX = e.clientX;
    startW = left.offsetWidth;
    splitter.classList.add('ah-splitter--active');
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    e.preventDefault();
  });

  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const dx = e.clientX - startX;
    let newW = startW + dx;
    const totalW = left.parentElement.offsetWidth;
    if (newW < SPLITTER_MIN_LEFT) newW = SPLITTER_MIN_LEFT;
    if (totalW - newW < SPLITTER_MIN_RIGHT) newW = totalW - SPLITTER_MIN_RIGHT;
    left.style.width = newW + 'px';
  });

  document.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    splitter.classList.remove('ah-splitter--active');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    try { localStorage.setItem(SPLITTER_KEY, left.offsetWidth); } catch (_) {}
  });
}

function _eventTypeLabel(eventType) {
  const labels = {
    message: '', status_change: 'status', approval: 'approved',
    error: 'error', lock: 'locked',
  };
  return labels[eventType] || eventType;
}

function _actorClass(actor) {
  const classes = {
    user: 'user', hermes: 'hermes', codex: 'codex',
    cursor: 'cursor', coordinator: 'system',
  };
  return classes[actor] || 'default';
}

function _showToast(msg) {
  const toast = document.createElement('div');
  toast.className = 'ah-toast';
  toast.textContent = msg;
  document.body.appendChild(toast);
  setTimeout(() => { toast.classList.add('ah-toast--visible'); }, 10);
  setTimeout(() => {
    toast.classList.remove('ah-toast--visible');
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}
