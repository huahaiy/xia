const storageKeys = {
  sessionId: 'xia.local-ui.session-id'
};
const state = {
  sessionId: sessionStorage.getItem(storageKeys.sessionId) || '',
  messages: [],
  pendingApproval: null,
  sending: false,
  approvalSubmitting: false,
  localDocs: [],
  activeLocalDocId: '',
  activeLocalDoc: null,
  localDocUploading: false,
  artifacts: [],
  activeArtifactId: '',
  activeArtifact: null,
  pendingLocalDocIds: [],
  pendingArtifactIds: [],
  scratchPads: [],
  activePadId: '',
  activePad: null,
  scratchDirty: false,
  scratchSaving: false,
  admin: {
    providers: [],
    conversationContext: null,
    memoryRetention: null,
    knowledgeDecay: null,
    localDocSummarization: null,
    databaseBackup: null,
    llmWorkloads: [],
    oauthProviderTemplates: [],
    oauthAccounts: [],
    services: [],
    sites: [],
    tools: [],
    skills: [],
    remoteBridge: null,
    remoteDevices: [],
    remoteEvents: [],
    remoteSnapshot: null
  },
  history: {
    sessions: [],
    schedules: [],
    activeSessionId: '',
    activeScheduleId: '',
    sessionMessages: [],
    scheduleRuns: []
  },
  knowledgeQuery: '',
  knowledgeNodes: [],
  activeKnowledgeNodeId: '',
  knowledgeFacts: [],
  knowledgeSearching: false,
  knowledgeLoadingFacts: false,
  knowledgeForgettingFactId: '',
  activeProviderId: '',
  activeOauthAccountId: '',
  activeServiceId: '',
  activeSiteId: '',
  providerSaving: false,
  contextSaving: false,
  retentionSaving: false,
  knowledgeDecaySaving: false,
  localDocSummarizationSaving: false,
  databaseBackupSaving: false,
  oauthSaving: false,
  serviceSaving: false,
  siteSaving: false,
  remoteBridgeSaving: false,
  remotePairing: false,
  localDocSummarizationStatus: 'Loading local document summarization settings...',
  contextStatus: 'Loading conversation context settings...',
  databaseBackupStatus: 'Loading database backup settings...',
  remoteBridgeStatus: 'Loading notification bridge settings...',
  remotePairStatus: 'Paste a pairing token from the mobile app to authorize a phone.',
  openclawImporting: false,
  openclawImportStatus: 'Import a ClawHub zip URL or a local OpenClaw skill path.',
  openclawImportReport: null,
  baseStatus: 'Ready',
  liveStatus: null,
  sendStartedAt: 0
};
const statusEl = document.getElementById('status');
const sessionLabelEl = document.getElementById('session-label');
const approvalPanelEl = document.getElementById('approval-panel');
const approvalToolEl = document.getElementById('approval-tool');
const approvalReasonEl = document.getElementById('approval-reason');
const approvalArgsEl = document.getElementById('approval-args');
const allowApprovalEl = document.getElementById('allow-approval');
const denyApprovalEl = document.getElementById('deny-approval');
const messagesEl = document.getElementById('messages');
const composerEl = document.getElementById('composer');
const sendEl = document.getElementById('send');
const clearInputEl = document.getElementById('clear-input');
const newChatEl = document.getElementById('new-chat');
const copyTranscriptEl = document.getElementById('copy-transcript');
const composerFormEl = document.getElementById('composer-form');
const scratchListEl = document.getElementById('scratch-list');
const scratchTitleEl = document.getElementById('scratch-title');
const scratchEditorEl = document.getElementById('scratch-editor');
const scratchStatusEl = document.getElementById('scratch-status');
const newScratchEl = document.getElementById('new-scratch');
const saveScratchEl = document.getElementById('save-scratch');
const deleteScratchEl = document.getElementById('delete-scratch');
const insertScratchEl = document.getElementById('insert-scratch');
const historySessionListEl = document.getElementById('history-session-list');
const historySessionStatusEl = document.getElementById('history-session-status');
const historySessionMessagesEl = document.getElementById('history-session-messages');
const historyScheduleListEl = document.getElementById('history-schedule-list');
const historyScheduleStatusEl = document.getElementById('history-schedule-status');
const historyScheduleRunsEl = document.getElementById('history-schedule-runs');
const refreshHistorySessionsEl = document.getElementById('refresh-history-sessions');
const refreshHistorySchedulesEl = document.getElementById('refresh-history-schedules');
const knowledgeQueryEl = document.getElementById('knowledge-query');
const searchKnowledgeEl = document.getElementById('search-knowledge');
const knowledgeNodeListEl = document.getElementById('knowledge-node-list');
const knowledgeFactListEl = document.getElementById('knowledge-fact-list');
const knowledgeStatusEl = document.getElementById('knowledge-status');
const providerListEl = document.getElementById('provider-list');
const providerIdEl = document.getElementById('provider-id');
const providerNameEl = document.getElementById('provider-name');
const providerBaseUrlEl = document.getElementById('provider-base-url');
const providerModelEl = document.getElementById('provider-model');
const providerWorkloadsEl = document.getElementById('provider-workloads');
const providerWorkloadsNoteEl = document.getElementById('provider-workloads-note');
const providerSystemPromptBudgetEl = document.getElementById('provider-system-prompt-budget');
const providerHistoryBudgetEl = document.getElementById('provider-history-budget');
const providerRateLimitEl = document.getElementById('provider-rate-limit-per-minute');
const providerVisionEl = document.getElementById('provider-vision');
const providerApiKeyEl = document.getElementById('provider-api-key');
const providerDefaultEl = document.getElementById('provider-default');
const providerStatusEl = document.getElementById('provider-status');
const newProviderEl = document.getElementById('new-provider');
const saveProviderEl = document.getElementById('save-provider');
const retentionFullResolutionDaysEl = document.getElementById('retention-full-resolution-days');
const retentionDecayHalfLifeDaysEl = document.getElementById('retention-decay-half-life-days');
const retentionRetainedCountEl = document.getElementById('retention-retained-count');
const retentionStatusEl = document.getElementById('retention-status');
const saveRetentionEl = document.getElementById('save-retention');
const contextRecentHistoryMessageLimitEl = document.getElementById('context-recent-history-message-limit');
const contextStatusEl = document.getElementById('context-status');
const saveContextEl = document.getElementById('save-context');
const knowledgeGracePeriodDaysEl = document.getElementById('knowledge-grace-period-days');
const knowledgeHalfLifeDaysEl = document.getElementById('knowledge-half-life-days');
const knowledgeMinConfidenceEl = document.getElementById('knowledge-min-confidence');
const knowledgeMaintenanceIntervalDaysEl = document.getElementById('knowledge-maintenance-interval-days');
const knowledgeArchiveAfterBottomDaysEl = document.getElementById('knowledge-archive-after-bottom-days');
const knowledgeDecayStatusEl = document.getElementById('knowledge-decay-status');
const saveKnowledgeDecayEl = document.getElementById('save-knowledge-decay');
const localDocModelSummariesEnabledEl = document.getElementById('local-doc-model-summaries-enabled');
const localDocModelSummaryBackendEl = document.getElementById('local-doc-model-summary-backend');
const localDocModelSummaryProviderIdEl = document.getElementById('local-doc-model-summary-provider-id');
const localDocChunkSummaryMaxTokensEl = document.getElementById('local-doc-chunk-summary-max-tokens');
const localDocDocSummaryMaxTokensEl = document.getElementById('local-doc-doc-summary-max-tokens');
const localDocSummarizationStatusEl = document.getElementById('local-doc-summarization-status');
const saveLocalDocSummarizationEl = document.getElementById('save-local-doc-summarization');
const databaseBackupEnabledEl = document.getElementById('database-backup-enabled');
const databaseBackupDirectoryEl = document.getElementById('database-backup-directory');
const databaseBackupIntervalHoursEl = document.getElementById('database-backup-interval-hours');
const databaseBackupRetainCountEl = document.getElementById('database-backup-retain-count');
const databaseBackupLastSuccessEl = document.getElementById('database-backup-last-success');
const databaseBackupLastArchiveEl = document.getElementById('database-backup-last-archive');
const databaseBackupStatusEl = document.getElementById('database-backup-status');
const saveDatabaseBackupEl = document.getElementById('save-database-backup');
const oauthAccountListEl = document.getElementById('oauth-account-list');
const oauthTemplateEl = document.getElementById('oauth-template');
const oauthTemplateNoteEl = document.getElementById('oauth-template-note');
const applyOauthTemplateEl = document.getElementById('apply-oauth-template');
const oauthAccountIdEl = document.getElementById('oauth-account-id');
const oauthAccountNameEl = document.getElementById('oauth-account-name');
const oauthAuthorizeUrlEl = document.getElementById('oauth-authorize-url');
const oauthTokenUrlEl = document.getElementById('oauth-token-url');
const oauthClientIdEl = document.getElementById('oauth-client-id');
const oauthClientSecretEl = document.getElementById('oauth-client-secret');
const oauthScopesEl = document.getElementById('oauth-scopes');
const oauthRedirectUriEl = document.getElementById('oauth-redirect-uri');
const oauthAuthParamsEl = document.getElementById('oauth-auth-params');
const oauthTokenParamsEl = document.getElementById('oauth-token-params');
const oauthAccountAutonomousApprovedEl = document.getElementById('oauth-account-autonomous-approved');
const oauthAccountStatusEl = document.getElementById('oauth-account-status');
const newOauthAccountEl = document.getElementById('new-oauth-account');
const saveOauthAccountEl = document.getElementById('save-oauth-account');
const oauthAccountCreateServiceEl = document.getElementById('oauth-account-create-service');
const connectOauthAccountEl = document.getElementById('connect-oauth-account');
const refreshOauthAccountEl = document.getElementById('refresh-oauth-account');
const deleteOauthAccountEl = document.getElementById('delete-oauth-account');
const serviceListEl = document.getElementById('service-list');
const serviceIdEl = document.getElementById('service-id');
const serviceNameEl = document.getElementById('service-name');
const serviceBaseUrlEl = document.getElementById('service-base-url');
const serviceAuthTypeEl = document.getElementById('service-auth-type');
const serviceAuthHeaderEl = document.getElementById('service-auth-header');
const serviceOauthAccountEl = document.getElementById('service-oauth-account');
const serviceRateLimitEl = document.getElementById('service-rate-limit');
const serviceAuthKeyEl = document.getElementById('service-auth-key');
const serviceAutonomousApprovedEl = document.getElementById('service-autonomous-approved');
const serviceEnabledEl = document.getElementById('service-enabled');
const serviceStatusEl = document.getElementById('service-status');
const newServiceEl = document.getElementById('new-service');
const saveServiceEl = document.getElementById('save-service');
const siteListEl = document.getElementById('site-list');
const siteIdEl = document.getElementById('site-id');
const siteNameEl = document.getElementById('site-name');
const siteLoginUrlEl = document.getElementById('site-login-url');
const siteUsernameFieldEl = document.getElementById('site-username-field');
const sitePasswordFieldEl = document.getElementById('site-password-field');
const siteUsernameEl = document.getElementById('site-username');
const sitePasswordEl = document.getElementById('site-password');
const siteFormSelectorEl = document.getElementById('site-form-selector');
const siteExtraFieldsEl = document.getElementById('site-extra-fields');
const siteAutonomousApprovedEl = document.getElementById('site-autonomous-approved');
const siteStatusEl = document.getElementById('site-status');
const newSiteEl = document.getElementById('new-site');
const saveSiteEl = document.getElementById('save-site');
const deleteSiteEl = document.getElementById('delete-site');
const remoteBridgeEnabledEl = document.getElementById('remote-bridge-enabled');
const remoteBridgeInstanceLabelEl = document.getElementById('remote-bridge-instance-label');
const remoteBridgeRelayUrlEl = document.getElementById('remote-bridge-relay-url');
const remoteBridgePublicKeyEl = document.getElementById('remote-bridge-public-key');
const remoteBridgeStatusEl = document.getElementById('remote-bridge-status');
const saveRemoteBridgeEl = document.getElementById('save-remote-bridge');
const remotePairingTokenEl = document.getElementById('remote-pairing-token');
const remotePairStatusEl = document.getElementById('remote-pair-status');
const pairRemoteDeviceEl = document.getElementById('pair-remote-device');
const remoteDeviceListEl = document.getElementById('remote-device-list');
const remoteSnapshotListEl = document.getElementById('remote-snapshot-list');
const remoteEventListEl = document.getElementById('remote-event-list');
const toolListEl = document.getElementById('tool-list');
const skillListEl = document.getElementById('skill-list');
const openclawImportSourceEl = document.getElementById('openclaw-import-source');
const openclawImportStrictEl = document.getElementById('openclaw-import-strict');
const openclawImportButtonEl = document.getElementById('openclaw-import-button');
const openclawImportStatusEl = document.getElementById('openclaw-import-status');
const openclawImportResultEl = document.getElementById('openclaw-import-result');
const fileInputEl = document.getElementById('file-input');
const uploadBtnEl = document.getElementById('upload-btn');
const localUploadBtnEl = document.getElementById('local-upload-btn');
const localDocListEl = document.getElementById('local-doc-list');
const localDocPreviewEl = document.getElementById('local-doc-preview');
const localDocStatusEl = document.getElementById('local-doc-status');
const localDocInsertEl = document.getElementById('local-doc-insert');
const localDocScratchEl = document.getElementById('local-doc-scratch');
const localDocDeleteEl = document.getElementById('local-doc-delete');
const artifactListEl = document.getElementById('artifact-list');
const artifactPreviewEl = document.getElementById('artifact-preview');
const artifactStatusEl = document.getElementById('artifact-status');
const artifactInsertEl = document.getElementById('artifact-insert');
const artifactScratchEl = document.getElementById('artifact-scratch');
const artifactDownloadEl = document.getElementById('artifact-download');
const artifactDeleteEl = document.getElementById('artifact-delete');
const tabLinks = document.querySelectorAll('.tab-link');
const tabPanels = document.querySelectorAll('.tab-panel');
const advancedToggleEl = document.getElementById('advanced-toggle');

function switchTab(tabId) {
  tabLinks.forEach((link) => {
    link.classList.toggle('active', link.dataset.tab === tabId);
  });
  tabPanels.forEach((panel) => {
    panel.classList.toggle('active', panel.id === tabId);
  });
}

tabLinks.forEach((link) => {
  link.addEventListener('click', () => switchTab(link.dataset.tab));
});

advancedToggleEl.addEventListener('change', () => {
  document.body.classList.toggle('advanced-mode', advancedToggleEl.checked);
});

uploadBtnEl.addEventListener('click', () => fileInputEl.click());
localUploadBtnEl.addEventListener('click', () => fileInputEl.click());

fileInputEl.addEventListener('change', async () => {
  try {
    await handleSelectedLocalFiles(Array.from(fileInputEl.files || []));
  } finally {
    fileInputEl.value = '';
  }
});

function persistSession() {
  if (state.sessionId) {
    sessionStorage.setItem(storageKeys.sessionId, state.sessionId);
  } else {
    sessionStorage.removeItem(storageKeys.sessionId);
  }
  updateSessionLabel();
}

function updateSessionLabel() {
  sessionLabelEl.textContent = state.sessionId
    ? 'Session ' + state.sessionId.slice(0, 8) + '...'
    : 'New local session';
}

function currentStatusText() {
  if (state.pendingApproval) {
    return 'Waiting for your approval...';
  }
  if (state.sending && state.liveStatus) {
    const s = state.liveStatus;
    const phase = s.phase;
    const tool = s.tool_name || s.tool_id;
    const elapsedSeconds = state.sendStartedAt
      ? Math.max(1, Math.floor((Date.now() - state.sendStartedAt) / 1000))
      : 0;
    
    switch (phase) {
      case 'working-memory': return 'Reading context...';
      case 'llm':
        if (s.partial_content) return s.partial_content;
        return elapsedSeconds ? `Thinking... ${elapsedSeconds}s` : 'Thinking...';
      case 'tool-plan':     return 'Planning next steps...';
      case 'tool':          return tool ? `Using ${tool}...` : 'Executing action...';
      case 'approval':      return 'Waiting for approval...';
      case 'finalizing':    return 'Writing response...';
      case 'complete':      return 'Ready';
      case 'error':         return s.message || 'Error occurred';
      default:              return s.message || 'Processing...';
    }
  }
  if (state.sending) {
    return 'Waiting for Xia...';
  }
  return state.baseStatus || 'Ready';
}

function syncStatus() {
  statusEl.textContent = currentStatusText();
}

function setStatus(text) {
  state.baseStatus = text || 'Ready';
  syncStatus();
}

function safeFetch(url, options) {
  const opts = Object.assign({}, options);
  opts.headers = Object.assign({ 'X-Requested-With': 'XMLHttpRequest' }, opts.headers || {});
  return fetch(url, opts);
}

function escapeHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

const _pending = {};
function dedup(key, fn) {
  if (_pending[key]) return _pending[key];
  const promise = fn().finally(() => { delete _pending[key]; });
  _pending[key] = promise;
  return promise;
}

function prettyJson(value) {
  try {
    return JSON.stringify(value == null ? {} : value, null, 2);
  } catch (_err) {
    return String(value);
  }
}

async function copyText(text, successLabel) {
  try {
    await navigator.clipboard.writeText(text);
    setStatus(successLabel || 'Copied');
  } catch (_err) {
    const fallback = document.createElement('textarea');
    fallback.value = text;
    fallback.setAttribute('readonly', 'readonly');
    fallback.style.position = 'fixed';
    fallback.style.opacity = '0';
    document.body.appendChild(fallback);
    fallback.select();
    document.execCommand('copy');
    document.body.removeChild(fallback);
    setStatus(successLabel || 'Copied');
  }
}

async function fetchJson(url, options) {
  const response = await safeFetch(url, options || {});
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || 'Request failed');
  }
  return data;
}

function firstNonEmpty(value, fallback) {
  return value && String(value).trim() ? String(value).trim() : (fallback || '');
}

function renderSelectableList(target, items, activeId, emptyText, titleFn, metaFn, onSelect) {
  target.replaceChildren();
  if (!items.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = emptyText;
    target.appendChild(empty);
    return;
  }
  items.forEach((item) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'admin-item' + (item.id === activeId ? ' active' : '');
    const title = document.createElement('div');
    title.className = 'admin-item-title';
    title.textContent = titleFn(item);
    const meta = document.createElement('div');
    meta.className = 'admin-item-meta';
    meta.textContent = metaFn(item);
    button.appendChild(title);
    button.appendChild(meta);
    button.addEventListener('click', () => {
      const result = onSelect(item);
      if (result && typeof result.catch === 'function') {
        result.catch((err) => console.error('Selection handler failed:', err));
      }
    });
    target.appendChild(button);
  });
}

function renderCapabilityList(target, items, emptyText, detailFn) {
  target.replaceChildren();
  if (!items.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = emptyText;
    target.appendChild(empty);
    return;
  }
  items.forEach((item) => {
    const card = document.createElement('div');
    card.className = 'capability-item';
    const title = document.createElement('div');
    title.className = 'capability-title';
    const name = document.createElement('span');
    name.textContent = firstNonEmpty(item.name, item.id);
    const badge = document.createElement('span');
    badge.className = 'badge' + (item.enabled ? '' : ' off');
    badge.textContent = item.enabled ? 'Enabled' : 'Disabled';
    title.appendChild(name);
    title.appendChild(badge);
    const meta = document.createElement('div');
    meta.className = 'capability-meta';
    meta.textContent = detailFn(item);
    card.appendChild(title);
    card.appendChild(meta);
    target.appendChild(card);
  });
}

function pluralize(count, singular, plural) {
  return count === 1 ? singular : (plural || (singular + 's'));
}

function skillMeta(skill) {
  const bits = [];
  if (skill.version) bits.push('version ' + skill.version);
  if (skill.imported_from_openclaw) bits.push('OpenClaw');
  if (skill.source_format) bits.push(skill.source_format);
  if (skill.source_name) bits.push(skill.source_name);
  else if (skill.source_url) bits.push(skill.source_url);
  else if (skill.source_path) bits.push(skill.source_path);
  if (Array.isArray(skill.import_warnings) && skill.import_warnings.length) {
    bits.push(skill.import_warnings.length + ' ' + pluralize(skill.import_warnings.length, 'warning'));
  }
  if (skill.description) bits.push(skill.description);
  return bits.join(' • ');
}

function formatOpenClawImportReport(report) {
  if (!report || !report.import) return '';
  const importReport = report.import;
  const skill = report.skill || {};
  const lines = [
    'Skill: ' + firstNonEmpty(skill.name, importReport.name || skill.id || importReport.skill_id || 'Unknown'),
    'ID: ' + firstNonEmpty(skill.id, importReport.skill_id || 'Unknown'),
    'Status: ' + firstNonEmpty(importReport.status, 'unknown'),
    'Source: ' + [importReport.source && importReport.source.format,
                  importReport.source && importReport.source.name,
                  importReport.source && (importReport.source.url || importReport.source.path)]
      .filter(Boolean)
      .join(' • ')
  ];

  if (Array.isArray(importReport.warnings) && importReport.warnings.length) {
    lines.push('');
    lines.push('Warnings:');
    importReport.warnings.forEach((warning) => lines.push('- ' + warning));
  }

  if (Array.isArray(importReport.tool_aliases) && importReport.tool_aliases.length) {
    lines.push('');
    lines.push('Tool aliases:');
    importReport.tool_aliases.forEach((alias) => {
      lines.push('- ' + firstNonEmpty(alias.from, alias.id || 'alias') + ' -> ' + firstNonEmpty(alias.to, 'unmapped'));
    });
  }

  if (Array.isArray(importReport.resources) && importReport.resources.length) {
    lines.push('');
    lines.push('Attached resources:');
    importReport.resources.forEach((resource) => {
      const size = typeof resource.size_bytes === 'number' ? (' (' + formatBytes(resource.size_bytes) + ')') : '';
      lines.push('- ' + firstNonEmpty(resource.path, 'resource') + size);
    });
  }

  return lines.filter((line, index, source) => line || (source[index - 1] && source[index - 1] !== '')).join('\n');
}

function renderOpenClawImport() {
  openclawImportStatusEl.textContent = state.openclawImportStatus;
  if (state.openclawImportReport) {
    openclawImportResultEl.hidden = false;
    openclawImportResultEl.textContent = formatOpenClawImportReport(state.openclawImportReport);
  } else {
    openclawImportResultEl.hidden = true;
    openclawImportResultEl.textContent = '';
  }
  updateAdminButtons();
}

function remoteBadge(label, off) {
  const badge = document.createElement('span');
  badge.className = 'badge' + (off ? ' off' : '');
  badge.textContent = label;
  return badge;
}

function renderRemoteDevices() {
  remoteDeviceListEl.replaceChildren();
  const devices = Array.isArray(state.admin.remoteDevices) ? state.admin.remoteDevices : [];
  if (!devices.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = 'No paired phones yet.';
    remoteDeviceListEl.appendChild(empty);
    return;
  }
  devices.forEach((device) => {
    const card = document.createElement('div');
    card.className = 'capability-item';

    const title = document.createElement('div');
    title.className = 'capability-title';
    const name = document.createElement('span');
    name.textContent = firstNonEmpty(device.name, device.id);
    title.appendChild(name);
    title.appendChild(remoteBadge(firstNonEmpty(device.status, 'unknown'), device.status === 'revoked'));

    const meta = document.createElement('div');
    meta.className = 'capability-meta';
    const metaBits = [];
    if (device.platform) metaBits.push(device.platform);
    if (Array.isArray(device.topics) && device.topics.length) metaBits.push('Topics: ' + device.topics.join(', '));
    meta.textContent = metaBits.join(' • ');

    const stamp = document.createElement('div');
    stamp.className = 'capability-meta';
    const stampBits = [];
    if (device.created_at) stampBits.push('Paired ' + formatDateTime(device.created_at));
    if (device.last_seen_at) stampBits.push('Last seen ' + formatDateTime(device.last_seen_at));
    stamp.textContent = stampBits.join(' • ');

    card.appendChild(title);
    if (meta.textContent) card.appendChild(meta);
    if (stamp.textContent) card.appendChild(stamp);

    if (device.status !== 'revoked') {
      const actions = document.createElement('div');
      actions.className = 'capability-actions';
      const revoke = document.createElement('button');
      revoke.type = 'button';
      revoke.className = 'secondary';
      revoke.textContent = 'Revoke';
      revoke.disabled = state.remotePairing || state.remoteBridgeSaving;
      revoke.addEventListener('click', () => revokeRemoteDevice(device));
      actions.appendChild(revoke);
      card.appendChild(actions);
    }

    remoteDeviceListEl.appendChild(card);
  });
}

function renderRemoteEvents() {
  remoteEventListEl.replaceChildren();
  const events = Array.isArray(state.admin.remoteEvents) ? state.admin.remoteEvents : [];
  if (!events.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = 'No bridge events recorded yet.';
    remoteEventListEl.appendChild(empty);
    return;
  }
  events.forEach((event) => {
    const card = document.createElement('div');
    card.className = 'capability-item';

    const title = document.createElement('div');
    title.className = 'capability-title';
    const name = document.createElement('span');
    name.textContent = firstNonEmpty(event.title, event.type);
    title.appendChild(name);
    title.appendChild(remoteBadge(firstNonEmpty(event.severity, 'info'), event.severity === 'info'));

    const meta = document.createElement('div');
    meta.className = 'capability-meta';
    meta.textContent = [event.topic, event.created_at ? formatDateTime(event.created_at) : '']
      .filter(Boolean)
      .join(' • ');

    card.appendChild(title);
    if (meta.textContent) card.appendChild(meta);
    if (event.detail) {
      const detail = document.createElement('div');
      detail.className = 'capability-meta';
      detail.textContent = event.detail;
      card.appendChild(detail);
    }
    remoteEventListEl.appendChild(card);
  });
}

function buildRemoteSnapshotCard(titleText, lines, badgeText, badgeOff) {
  const card = document.createElement('div');
  card.className = 'capability-item';

  const title = document.createElement('div');
  title.className = 'capability-title';
  const name = document.createElement('span');
  name.textContent = titleText;
  title.appendChild(name);
  if (badgeText) {
    title.appendChild(remoteBadge(badgeText, !!badgeOff));
  }
  card.appendChild(title);

  const validLines = lines.filter(Boolean);
  if (!validLines.length) {
    const empty = document.createElement('div');
    empty.className = 'capability-meta';
    empty.textContent = 'None';
    card.appendChild(empty);
    return card;
  }

  validLines.forEach((line) => {
    const meta = document.createElement('div');
    meta.className = 'capability-meta';
    meta.textContent = line;
    card.appendChild(meta);
  });

  return card;
}

function renderRemoteSnapshot() {
  remoteSnapshotListEl.replaceChildren();
  const snapshot = state.admin.remoteSnapshot || null;
  if (!snapshot) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = 'No snapshot available yet.';
    remoteSnapshotListEl.appendChild(empty);
    return;
  }

  const connectivity = snapshot.connectivity || {};
  const running = Array.isArray(snapshot.running) ? snapshot.running : [];
  const failures = Array.isArray(snapshot.recent_failures) ? snapshot.recent_failures : [];
  const successes = Array.isArray(snapshot.recent_successes) ? snapshot.recent_successes : [];
  const attention = Array.isArray(snapshot.attention) ? snapshot.attention : [];

  remoteSnapshotListEl.appendChild(buildRemoteSnapshotCard(
    'Connectivity',
    [
      snapshot.instance && snapshot.instance.label ? ('Instance: ' + snapshot.instance.label) : '',
      connectivity.relay_url ? ('Relay: ' + connectivity.relay_url) : 'Relay not configured',
      connectivity.last_seen_at ? ('Last seen: ' + formatDateTime(connectivity.last_seen_at)) : ''
    ],
    firstNonEmpty(connectivity.connection_state, 'disabled'),
    connectivity.connection_state !== 'connected'
  ));

  remoteSnapshotListEl.appendChild(buildRemoteSnapshotCard(
    'Attention',
    attention.map((item) => item.title + (item.detail ? ' - ' + item.detail : '')),
    attention.length ? String(attention.length) : 'clear',
    !attention.length
  ));

  remoteSnapshotListEl.appendChild(buildRemoteSnapshotCard(
    'Running Now',
    running.map((item) => [item.schedule_name || item.schedule_id, item.phase].filter(Boolean).join(' • ')),
    running.length ? String(running.length) : 'idle',
    !running.length
  ));

  remoteSnapshotListEl.appendChild(buildRemoteSnapshotCard(
    'Recent Failures',
    failures.map((item) => [item.schedule_name || item.schedule_id,
                            item.started_at ? formatDateTime(item.started_at) : '',
                            item.detail || '']
      .filter(Boolean)
      .join(' • ')),
    failures.length ? String(failures.length) : 'none',
    !failures.length
  ));

  remoteSnapshotListEl.appendChild(buildRemoteSnapshotCard(
    'Recent Successes',
    successes.map((item) => [item.schedule_name || item.schedule_id,
                             item.finished_at ? formatDateTime(item.finished_at) : (item.started_at ? formatDateTime(item.started_at) : ''),
                             item.detail || '']
      .filter(Boolean)
      .join(' • ')),
    successes.length ? String(successes.length) : 'none',
    !successes.length
  ));
}

function defaultRemoteBridgeStatus() {
  const bridge = state.admin.remoteBridge || {};
  const bits = [];
  bits.push(bridge.enabled ? 'Enabled' : 'Disabled');
  bits.push(bridge.keypair_ready ? 'Keys ready' : 'Keys missing');
  if (bridge.connection_state) bits.push('State: ' + bridge.connection_state);
  if (bridge.connected_at) bits.push('Connected ' + formatDateTime(bridge.connected_at));
  return bits.join(' • ');
}

function defaultRemotePairStatus() {
  const devices = Array.isArray(state.admin.remoteDevices) ? state.admin.remoteDevices : [];
  return devices.length
    ? ('Paired ' + devices.length + ' ' + pluralize(devices.length, 'device') + '.')
    : 'Paste a pairing token from the mobile app to authorize a phone.';
}

function renderRemoteBridge() {
  const bridge = state.admin.remoteBridge || {};
  remoteBridgeEnabledEl.checked = !!bridge.enabled;
  remoteBridgeInstanceLabelEl.value = bridge.instance_label || '';
  remoteBridgeRelayUrlEl.value = bridge.relay_url || '';
  remoteBridgePublicKeyEl.value = bridge.public_key || '';
  remoteBridgeStatusEl.textContent = state.remoteBridgeStatus || defaultRemoteBridgeStatus();
  remotePairStatusEl.textContent = state.remotePairStatus || defaultRemotePairStatus();
  renderRemoteDevices();
  renderRemoteEvents();
  renderRemoteSnapshot();
}

function providerMeta(provider) {
  const bits = [];
  if (provider.model) bits.push(provider.model);
  if (Array.isArray(provider.workloads) && provider.workloads.length) {
    bits.push('Workloads: ' + provider.workloads.join(', '));
  }
  if (provider.effective_rate_limit_per_minute) {
    bits.push('Rate ' + provider.effective_rate_limit_per_minute + '/min');
  }
  if (provider.health_status && provider.health_status !== 'healthy') {
    const label = provider.health_status === 'cooling-down'
      ? 'Cooling down'
      : 'Degraded';
    const suffix = provider.health_cooldown_ms
      ? ' (' + Math.ceil(provider.health_cooldown_ms / 1000) + 's)'
      : '';
    bits.push(label + suffix);
  }
  if (provider.vision) bits.push('Vision');
  if (provider.default) bits.push('Default');
  bits.push(provider.api_key_configured ? 'API key stored' : 'No API key');
  return bits.join(' • ');
}

function providerWorkloadIds() {
  return Array.isArray(state.admin.llmWorkloads)
    ? state.admin.llmWorkloads.map((entry) => entry.id).filter(Boolean)
    : [];
}

function renderProviderWorkloadNote() {
  const ids = providerWorkloadIds();
  providerWorkloadsNoteEl.textContent = ids.length
    ? 'Available workloads: ' + ids.join(', ')
    : '';
}

function parseProviderWorkloadsInput() {
  return providerWorkloadsEl.value
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);
}

function oauthAccountMeta(account) {
  const bits = [];
  bits.push(account.connected ? 'Connected' : 'Not connected');
  if (account.refresh_token_configured) bits.push('Refresh token stored');
  if (account.autonomous_approved) bits.push('Autonomous approved');
  if (account.expires_at) bits.push('Expires ' + formatStamp(account.expires_at));
  return bits.join(' • ');
}

function oauthTemplateMeta(template) {
  const bits = [];
  if (template.api_base_url) bits.push(template.api_base_url);
  if (template.description) bits.push(template.description);
  return bits.join(' • ');
}

function serviceMeta(service) {
  const bits = [];
  if (service.auth_type) bits.push(service.auth_type);
  if (service.effective_rate_limit_per_minute) bits.push('Rate ' + service.effective_rate_limit_per_minute + '/min');
  if (service.oauth_account_name) bits.push(service.oauth_account_name);
  if (service.auth_type === 'oauth-account') bits.push(service.oauth_account_connected ? 'OAuth connected' : 'OAuth not connected');
  if (service.autonomous_approved) bits.push('Autonomous approved');
  bits.push(service.enabled ? 'Enabled' : 'Disabled');
  bits.push(service.auth_type === 'oauth-account'
    ? (service.oauth_account ? 'Account linked' : 'No account linked')
    : (service.auth_key_configured ? 'Secret stored' : 'No secret'));
  return bits.join(' • ');
}

function siteMeta(site) {
  const bits = [];
  if (site.login_url) bits.push(site.login_url);
  if (site.autonomous_approved) bits.push('Autonomous approved');
  bits.push(site.username_configured ? 'Username stored' : 'No username');
  bits.push(site.password_configured ? 'Password stored' : 'No password');
  return bits.join(' • ');
}

function historySessionMeta(session) {
  const bits = [];
  if (session.channel) bits.push(session.channel);
  if (session.last_message_at) bits.push('Last ' + formatDateTime(session.last_message_at));
  else if (session.created_at) bits.push('Created ' + formatDateTime(session.created_at));
  bits.push((session.message_count || 0) + ' msgs');
  if (session.preview) bits.push(session.preview);
  return bits.join(' • ');
}

function historyScheduleMeta(schedule) {
  const bits = [];
  if (schedule.type) bits.push(schedule.type);
  bits.push(schedule.enabled ? 'Enabled' : 'Disabled');
  if (schedule.latest_status) bits.push('Last run ' + schedule.latest_status);
  if (schedule.last_run) bits.push('Ran ' + formatDateTime(schedule.last_run));
  else if (schedule.next_run) bits.push('Next ' + formatDateTime(schedule.next_run));
  if (schedule.latest_error) bits.push(schedule.latest_error);
  return bits.join(' • ');
}

function knowledgeNodeMeta(node) {
  return node.type ? String(node.type) : 'Entity';
}

function formatKnowledgeScore(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(2) : '0.00';
}

function renderKnowledgeNodeList() {
  renderSelectableList(
    knowledgeNodeListEl,
    state.knowledgeNodes,
    state.activeKnowledgeNodeId,
    state.knowledgeQuery
      ? 'No matching entities found.'
      : 'Search for a person, project, or topic to inspect stored facts.',
    (node) => firstNonEmpty(node.name, node.id),
    knowledgeNodeMeta,
    selectKnowledgeNode
  );
}

function buildKnowledgeFactEl(fact) {
  const card = document.createElement('article');
  card.className = 'history-run knowledge-fact';

  const head = document.createElement('div');
  head.className = 'knowledge-fact-head';

  const body = document.createElement('div');
  body.className = 'knowledge-fact-content';
  body.textContent = firstNonEmpty(fact.content, 'Untitled fact');
  head.appendChild(body);

  const actions = document.createElement('div');
  actions.className = 'actions';
  const forgetButton = document.createElement('button');
  forgetButton.type = 'button';
  forgetButton.className = 'secondary';
  forgetButton.textContent = state.knowledgeForgettingFactId === fact.id ? 'Forgetting...' : 'Forget';
  forgetButton.disabled = state.knowledgeForgettingFactId === fact.id || state.knowledgeLoadingFacts;
  forgetButton.addEventListener('click', () => forgetKnowledgeFact(fact));
  actions.appendChild(forgetButton);
  head.appendChild(actions);
  card.appendChild(head);

  const meta = document.createElement('div');
  meta.className = 'history-run-meta';
  meta.textContent = [
    'Confidence ' + formatKnowledgeScore(fact.confidence),
    'Utility ' + formatKnowledgeScore(fact.utility),
    fact.updated_at ? ('Updated ' + formatDateTime(fact.updated_at)) : ''
  ].filter(Boolean).join(' • ');
  card.appendChild(meta);

  return card;
}

function renderKnowledgeFacts() {
  knowledgeFactListEl.replaceChildren();
  if (!state.activeKnowledgeNodeId) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = state.knowledgeQuery
      ? 'Select a matching entity to inspect its stored facts.'
      : 'Select an entity to inspect its stored facts.';
    knowledgeFactListEl.appendChild(empty);
    return;
  }
  if (!state.knowledgeFacts.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = state.knowledgeLoadingFacts
      ? 'Loading facts...'
      : 'No facts stored for this entity.';
    knowledgeFactListEl.appendChild(empty);
    return;
  }
  state.knowledgeFacts.forEach((fact) => {
    knowledgeFactListEl.appendChild(buildKnowledgeFactEl(fact));
  });
}

function renderHistorySessionList() {
  renderSelectableList(
    historySessionListEl,
    state.history.sessions,
    state.history.activeSessionId,
    'No chat sessions recorded yet.',
    (session) => firstNonEmpty(session.preview, session.id),
    historySessionMeta,
    selectHistorySession
  );
}

function renderHistoryScheduleList() {
  renderSelectableList(
    historyScheduleListEl,
    state.history.schedules,
    state.history.activeScheduleId,
    'No schedules recorded yet.',
    (schedule) => firstNonEmpty(schedule.name, schedule.id),
    historyScheduleMeta,
    selectHistorySchedule
  );
}

function renderHistorySessionMessages() {
  historySessionMessagesEl.replaceChildren();
  if (!state.history.activeSessionId) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = 'Select a prior session to view its transcript.';
    historySessionMessagesEl.appendChild(empty);
    return;
  }
  if (!state.history.sessionMessages.length) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = 'This session has no user/assistant transcript yet.';
    historySessionMessagesEl.appendChild(empty);
    return;
  }
  state.history.sessionMessages.forEach((message) => {
    historySessionMessagesEl.appendChild(buildMessageEl(message));
  });
  historySessionMessagesEl.scrollTop = historySessionMessagesEl.scrollHeight;
}

function buildHistoryRunEl(run) {
  const card = document.createElement('article');
  card.className = 'history-run';

  const head = document.createElement('div');
  head.className = 'history-run-head';

  const titleWrap = document.createElement('div');
  const title = document.createElement('div');
  title.className = 'history-run-title';
  title.textContent = (run.status || 'unknown').toUpperCase();
  const meta = document.createElement('div');
  meta.className = 'history-run-meta';
  meta.textContent = [
    run.started_at ? ('Started ' + formatDateTime(run.started_at)) : '',
    run.finished_at ? ('Finished ' + formatDateTime(run.finished_at)) : ''
  ].filter(Boolean).join(' • ');
  titleWrap.appendChild(title);
  titleWrap.appendChild(meta);
  head.appendChild(titleWrap);
  card.appendChild(head);

  if (run.error) {
    const block = document.createElement('div');
    block.className = 'history-block';
    const label = document.createElement('div');
    label.className = 'history-block-label';
    label.textContent = 'Error';
    const body = document.createElement('pre');
    body.className = 'history-code';
    body.textContent = run.error;
    block.appendChild(label);
    block.appendChild(body);
    card.appendChild(block);
  }

  if (run.result) {
    const block = document.createElement('div');
    block.className = 'history-block';
    const label = document.createElement('div');
    label.className = 'history-block-label';
    label.textContent = 'Result';
    const body = document.createElement('pre');
    body.className = 'history-code';
    body.textContent = run.result;
    block.appendChild(label);
    block.appendChild(body);
    card.appendChild(block);
  }

  if (Array.isArray(run.actions) && run.actions.length) {
    const block = document.createElement('div');
    block.className = 'history-block';
    const label = document.createElement('div');
    label.className = 'history-block-label';
    label.textContent = 'Actions';
    const body = document.createElement('pre');
    body.className = 'history-code';
    body.textContent = prettyJson(run.actions);
    block.appendChild(label);
    block.appendChild(body);
    card.appendChild(block);
  }

  return card;
}

function renderHistoryScheduleRuns() {
  historyScheduleRunsEl.replaceChildren();
  if (!state.history.activeScheduleId) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = 'Select a schedule to inspect recent runs.';
    historyScheduleRunsEl.appendChild(empty);
    return;
  }
  if (!state.history.scheduleRuns.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = 'No runs recorded for this schedule yet.';
    historyScheduleRunsEl.appendChild(empty);
    return;
  }
  state.history.scheduleRuns.forEach((run) => {
    historyScheduleRunsEl.appendChild(buildHistoryRunEl(run));
  });
}

function updateAdminButtons() {
  knowledgeQueryEl.disabled = state.knowledgeSearching;
  searchKnowledgeEl.disabled = state.knowledgeSearching || !knowledgeQueryEl.value.trim();
  providerIdEl.disabled = state.providerSaving || !!state.activeProviderId;
  saveProviderEl.disabled = state.providerSaving;
  newProviderEl.disabled = state.providerSaving;
  contextRecentHistoryMessageLimitEl.disabled = state.contextSaving;
  saveContextEl.disabled = state.contextSaving;
  saveRetentionEl.disabled = state.retentionSaving;
  saveKnowledgeDecayEl.disabled = state.knowledgeDecaySaving;
  localDocModelSummariesEnabledEl.disabled = state.localDocSummarizationSaving;
  localDocModelSummaryBackendEl.disabled = state.localDocSummarizationSaving;
  localDocModelSummaryProviderIdEl.disabled = state.localDocSummarizationSaving
    || localDocModelSummaryBackendEl.value !== 'external';
  localDocChunkSummaryMaxTokensEl.disabled = state.localDocSummarizationSaving;
  localDocDocSummaryMaxTokensEl.disabled = state.localDocSummarizationSaving;
  saveLocalDocSummarizationEl.disabled = state.localDocSummarizationSaving;
  databaseBackupEnabledEl.disabled = state.databaseBackupSaving;
  databaseBackupDirectoryEl.disabled = state.databaseBackupSaving;
  databaseBackupIntervalHoursEl.disabled = state.databaseBackupSaving;
  databaseBackupRetainCountEl.disabled = state.databaseBackupSaving;
  saveDatabaseBackupEl.disabled = state.databaseBackupSaving;
  oauthAccountIdEl.disabled = state.oauthSaving || !!state.activeOauthAccountId;
  saveOauthAccountEl.disabled = state.oauthSaving;
  newOauthAccountEl.disabled = state.oauthSaving;
  oauthTemplateEl.disabled = state.oauthSaving;
  applyOauthTemplateEl.disabled = state.oauthSaving;
  oauthAccountCreateServiceEl.disabled = state.oauthSaving || !state.activeOauthAccountId;
  connectOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId;
  refreshOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId;
  deleteOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId;
  serviceIdEl.disabled = state.serviceSaving || !!state.activeServiceId;
  saveServiceEl.disabled = state.serviceSaving;
  newServiceEl.disabled = state.serviceSaving;
  siteIdEl.disabled = state.siteSaving || !!state.activeSiteId;
  saveSiteEl.disabled = state.siteSaving;
  newSiteEl.disabled = state.siteSaving;
  deleteSiteEl.disabled = state.siteSaving || !state.activeSiteId;
  remoteBridgeEnabledEl.disabled = state.remoteBridgeSaving;
  remoteBridgeInstanceLabelEl.disabled = state.remoteBridgeSaving;
  remoteBridgeRelayUrlEl.disabled = state.remoteBridgeSaving;
  remotePairingTokenEl.disabled = state.remotePairing;
  saveRemoteBridgeEl.disabled = state.remoteBridgeSaving;
  pairRemoteDeviceEl.disabled = state.remotePairing || !remotePairingTokenEl.value.trim();
  openclawImportSourceEl.disabled = state.openclawImporting;
  openclawImportStrictEl.disabled = state.openclawImporting;
  openclawImportButtonEl.disabled = state.openclawImporting || !openclawImportSourceEl.value.trim();
}

function renderProviderList() {
  renderSelectableList(
    providerListEl,
    state.admin.providers,
    state.activeProviderId,
    'No models configured yet. Add one so Xia can talk to an LLM.',
    (provider) => firstNonEmpty(provider.name, provider.id),
    providerMeta,
    selectProvider
  );
}

function renderOauthAccountList() {
  renderSelectableList(
    oauthAccountListEl,
    state.admin.oauthAccounts,
    state.activeOauthAccountId,
    'No connections configured yet. Add one for APIs that need a real OAuth flow.',
    (account) => firstNonEmpty(account.name, account.id),
    oauthAccountMeta,
    selectOauthAccount
  );
}

function renderOauthTemplateOptions() {
  const selected = oauthTemplateEl.value;
  oauthTemplateEl.replaceChildren();
  const blank = document.createElement('option');
  blank.value = '';
  blank.textContent = 'Custom';
  oauthTemplateEl.appendChild(blank);
  state.admin.oauthProviderTemplates.forEach((template) => {
    const option = document.createElement('option');
    option.value = template.id || '';
    option.textContent = firstNonEmpty(template.name, template.id);
    oauthTemplateEl.appendChild(option);
  });
  oauthTemplateEl.value = selected || '';
  const current = state.admin.oauthProviderTemplates.find((template) => template.id === oauthTemplateEl.value);
  oauthTemplateNoteEl.textContent = current
    ? ((current.description || 'OAuth provider preset.') + (current.notes ? ' ' + current.notes : ''))
    : 'Choose a preset for common providers, then fill in your client id and client secret.';
}

function renderOauthAccountOptions() {
  const previous = serviceOauthAccountEl.value;
  serviceOauthAccountEl.replaceChildren();
  const blank = document.createElement('option');
  blank.value = '';
  blank.textContent = 'None';
  serviceOauthAccountEl.appendChild(blank);
  state.admin.oauthAccounts.forEach((account) => {
    const option = document.createElement('option');
    option.value = account.id || '';
    option.textContent = firstNonEmpty(account.name, account.id)
      + (account.connected ? ' (connected)' : ' (not connected)');
    serviceOauthAccountEl.appendChild(option);
  });
  if (state.activeServiceId && state.admin.services.some((service) => service.id === state.activeServiceId)) {
    const current = state.admin.services.find((service) => service.id === state.activeServiceId);
    serviceOauthAccountEl.value = (current && current.oauth_account) || '';
  } else {
    serviceOauthAccountEl.value = previous || '';
  }
}

function renderServiceList() {
  renderSelectableList(
    serviceListEl,
    state.admin.services,
    state.activeServiceId,
    'No services configured yet.',
    (service) => firstNonEmpty(service.name, service.id),
    serviceMeta,
    selectService
  );
}

function renderSiteList() {
  renderSelectableList(
    siteListEl,
    state.admin.sites,
    state.activeSiteId,
    'No site logins configured yet.',
    (site) => firstNonEmpty(site.name, site.id),
    siteMeta,
    selectSite
  );
}

function renderCapabilities() {
  renderCapabilityList(
    toolListEl,
    state.admin.tools,
    'No tools installed.',
    (tool) => [tool.id, tool.approval ? ('approval: ' + tool.approval) : '', tool.description || '']
      .filter(Boolean)
      .join(' • ')
  );
  renderCapabilityList(
    skillListEl,
    state.admin.skills,
    'No skills installed.',
    skillMeta
  );
  renderRemoteBridge();
  renderOpenClawImport();
}

function resetProviderForm(statusText) {
  state.activeProviderId = '';
  providerIdEl.value = '';
  providerNameEl.value = '';
  providerBaseUrlEl.value = '';
  providerModelEl.value = '';
  providerWorkloadsEl.value = '';
  providerSystemPromptBudgetEl.value = '';
  providerHistoryBudgetEl.value = '';
  providerRateLimitEl.value = '';
  providerVisionEl.checked = false;
  providerApiKeyEl.value = '';
  providerDefaultEl.checked = !state.admin.providers.some((provider) => provider.default);
  providerStatusEl.textContent = statusText || 'Create a model or select an existing one.';
  renderProviderWorkloadNote();
  renderProviderList();
  updateAdminButtons();
}

function renderMemoryRetentionSettings() {
  const settings = state.admin.memoryRetention || {};
  retentionFullResolutionDaysEl.value = settings.full_resolution_days || '';
  retentionDecayHalfLifeDaysEl.value = settings.decay_half_life_days || '';
  retentionRetainedCountEl.value = settings.retained_count || '';
  retentionStatusEl.textContent = 'Tune how aggressively processed episodes are thinned after the full-resolution window.';
  updateAdminButtons();
}

function resetMemoryRetentionForm(statusText) {
  retentionFullResolutionDaysEl.value = '';
  retentionDecayHalfLifeDaysEl.value = '';
  retentionRetainedCountEl.value = '';
  retentionStatusEl.textContent = statusText || 'Configure retention for consolidated episodes.';
  updateAdminButtons();
}

function defaultConversationContextStatus() {
  const settings = state.admin.conversationContext || {};
  return 'Keep ' + firstNonEmpty(settings.recent_history_message_limit, 24) + ' recent messages verbatim before using recap history.';
}

function renderConversationContextSettings() {
  const settings = state.admin.conversationContext || {};
  contextRecentHistoryMessageLimitEl.value = settings.recent_history_message_limit || '';
  contextStatusEl.textContent = state.contextStatus || defaultConversationContextStatus();
  updateAdminButtons();
}

function resetConversationContextForm(statusText) {
  contextRecentHistoryMessageLimitEl.value = '';
  contextStatusEl.textContent = statusText || 'Configure how much recent chat stays verbatim in prompt context.';
  updateAdminButtons();
}

function renderKnowledgeDecaySettings() {
  const settings = state.admin.knowledgeDecay || {};
  knowledgeGracePeriodDaysEl.value = settings.grace_period_days || '';
  knowledgeHalfLifeDaysEl.value = settings.half_life_days || '';
  knowledgeMinConfidenceEl.value = settings.min_confidence ?? '';
  knowledgeMaintenanceIntervalDaysEl.value = settings.maintenance_interval_days || '';
  knowledgeArchiveAfterBottomDaysEl.value = settings.archive_after_bottom_days || '';
  knowledgeDecayStatusEl.textContent = 'Tune how quickly stale fact confidence fades after the grace period.';
  updateAdminButtons();
}

function defaultLocalDocSummarizationStatus() {
  const settings = state.admin.localDocSummarization || {};
  const bits = [];
  bits.push(settings.model_summaries_enabled ? 'Model summaries enabled' : 'Using extractive summaries');
  bits.push('Backend: ' + firstNonEmpty(settings.model_summary_backend, 'local'));
  if (settings.model_summary_backend === 'external') {
    bits.push('Provider: ' + firstNonEmpty(settings.model_summary_provider_id, 'default'));
  }
  return bits.join(' • ');
}

function renderLocalDocSummarizationProviderOptions() {
  const selected = localDocModelSummaryProviderIdEl.value;
  localDocModelSummaryProviderIdEl.replaceChildren();
  const blank = document.createElement('option');
  blank.value = '';
  blank.textContent = 'Default provider';
  localDocModelSummaryProviderIdEl.appendChild(blank);
  state.admin.providers.forEach((provider) => {
    const option = document.createElement('option');
    option.value = provider.id || '';
    option.textContent = firstNonEmpty(provider.name, provider.id);
    localDocModelSummaryProviderIdEl.appendChild(option);
  });
  localDocModelSummaryProviderIdEl.value = selected || '';
}

function renderLocalDocSummarizationSettings() {
  const settings = state.admin.localDocSummarization || {};
  localDocModelSummariesEnabledEl.checked = !!settings.model_summaries_enabled;
  localDocModelSummaryBackendEl.value = firstNonEmpty(settings.model_summary_backend, 'local');
  renderLocalDocSummarizationProviderOptions();
  localDocModelSummaryProviderIdEl.value = settings.model_summary_provider_id || '';
  localDocChunkSummaryMaxTokensEl.value = settings.chunk_summary_max_tokens || '';
  localDocDocSummaryMaxTokensEl.value = settings.doc_summary_max_tokens || '';
  localDocModelSummaryProviderIdEl.disabled = state.localDocSummarizationSaving
    || localDocModelSummaryBackendEl.value !== 'external';
  localDocSummarizationStatusEl.textContent = state.localDocSummarizationStatus || defaultLocalDocSummarizationStatus();
  updateAdminButtons();
}

function defaultDatabaseBackupStatus() {
  const settings = state.admin.databaseBackup || {};
  const bits = [];
  bits.push(settings.enabled ? 'Automatic backups enabled' : 'Automatic backups disabled');
  bits.push('Every ' + firstNonEmpty(settings.interval_hours, 24) + 'h');
  bits.push('Keep ' + firstNonEmpty(settings.retain_count, 7));
  if (settings.running) {
    bits.push('Running now');
  } else if (settings.last_success_at) {
    bits.push('Last success: ' + settings.last_success_at);
  } else if (settings.next_due_at) {
    bits.push('Next due: ' + settings.next_due_at);
  }
  if (settings.last_error) {
    bits.push('Last error: ' + settings.last_error);
  }
  return bits.join(' • ');
}

function renderDatabaseBackupSettings() {
  const settings = state.admin.databaseBackup || {};
  databaseBackupEnabledEl.checked = !!settings.enabled;
  databaseBackupDirectoryEl.value = settings.directory || '';
  databaseBackupIntervalHoursEl.value = settings.interval_hours || '';
  databaseBackupRetainCountEl.value = settings.retain_count || '';
  databaseBackupLastSuccessEl.value = settings.last_success_at || '';
  databaseBackupLastArchiveEl.value = settings.last_archive_path || '';
  databaseBackupStatusEl.textContent = state.databaseBackupStatus || defaultDatabaseBackupStatus();
  updateAdminButtons();
}

function resetOauthAccountForm(statusText) {
  state.activeOauthAccountId = '';
  oauthAccountIdEl.value = '';
  oauthAccountNameEl.value = '';
  oauthAuthorizeUrlEl.value = '';
  oauthTokenUrlEl.value = '';
  oauthClientIdEl.value = '';
  oauthClientSecretEl.value = '';
  oauthScopesEl.value = '';
  oauthRedirectUriEl.value = '';
  oauthAuthParamsEl.value = '';
  oauthTokenParamsEl.value = '';
  oauthAccountAutonomousApprovedEl.checked = true;
  oauthTemplateEl.value = '';
  renderOauthTemplateOptions();
  oauthAccountStatusEl.textContent = statusText || 'Create a connection or select an existing one.';
  renderOauthAccountList();
  updateAdminButtons();
}

function resetServiceForm(statusText) {
  state.activeServiceId = '';
  serviceIdEl.value = '';
  serviceNameEl.value = '';
  serviceBaseUrlEl.value = '';
  serviceAuthTypeEl.value = 'bearer';
  serviceAuthHeaderEl.value = '';
  serviceOauthAccountEl.value = '';
  serviceRateLimitEl.value = '';
  serviceAuthKeyEl.value = '';
  serviceAutonomousApprovedEl.checked = true;
  serviceEnabledEl.checked = true;
  serviceStatusEl.textContent = statusText || 'Create a service or select an existing one.';
  renderServiceList();
  renderOauthAccountOptions();
  updateAdminButtons();
}

function resetSiteForm(statusText) {
  state.activeSiteId = '';
  siteIdEl.value = '';
  siteNameEl.value = '';
  siteLoginUrlEl.value = '';
  siteUsernameFieldEl.value = 'username';
  sitePasswordFieldEl.value = 'password';
  siteUsernameEl.value = '';
  sitePasswordEl.value = '';
  siteFormSelectorEl.value = '';
  siteExtraFieldsEl.value = '';
  siteAutonomousApprovedEl.checked = true;
  siteStatusEl.textContent = statusText || 'Create a site login or select an existing one.';
  renderSiteList();
  updateAdminButtons();
}

function selectProvider(provider) {
  state.activeProviderId = provider.id || '';
  providerIdEl.value = provider.id || '';
  providerNameEl.value = provider.name || '';
  providerBaseUrlEl.value = provider.base_url || '';
  providerModelEl.value = provider.model || '';
  providerWorkloadsEl.value = Array.isArray(provider.workloads) ? provider.workloads.join(', ') : '';
  providerSystemPromptBudgetEl.value = provider.system_prompt_budget || '';
  providerHistoryBudgetEl.value = provider.history_budget || '';
  providerRateLimitEl.value = provider.rate_limit_per_minute || '';
  providerVisionEl.checked = !!provider.vision;
  providerApiKeyEl.value = '';
  providerDefaultEl.checked = !!provider.default;
  providerStatusEl.textContent = provider.api_key_configured
    ? 'API key stored. Enter a new one to replace it.'
    : 'No API key stored yet.';
  if (provider.effective_rate_limit_per_minute) {
    providerStatusEl.textContent += ' Rate limit: ' + provider.effective_rate_limit_per_minute + '/min.';
  }
  if (provider.health_status && provider.health_status !== 'healthy') {
    providerStatusEl.textContent += ' Current health: ' + provider.health_status + '.';
    if (provider.health_last_error) {
      providerStatusEl.textContent += ' Last error: ' + provider.health_last_error;
    }
  }
  renderProviderWorkloadNote();
  renderProviderList();
  updateAdminButtons();
}

function selectOauthAccount(account) {
  state.activeOauthAccountId = account.id || '';
  oauthAccountIdEl.value = account.id || '';
  oauthAccountNameEl.value = account.name || '';
  oauthAuthorizeUrlEl.value = account.authorize_url || '';
  oauthTokenUrlEl.value = account.token_url || '';
  oauthClientIdEl.value = account.client_id || '';
  oauthClientSecretEl.value = '';
  oauthScopesEl.value = account.scopes || '';
  oauthRedirectUriEl.value = account.redirect_uri || '';
  oauthAuthParamsEl.value = account.auth_params || '';
  oauthTokenParamsEl.value = account.token_params || '';
  oauthAccountAutonomousApprovedEl.checked = !!account.autonomous_approved;
  oauthTemplateEl.value = account.provider_template || '';
  renderOauthTemplateOptions();
  oauthAccountStatusEl.textContent = [
    account.client_secret_configured ? 'Secret stored.' : 'No secret stored.',
    account.connected ? 'Connected.' : 'Not connected.',
    account.refresh_token_configured ? 'Refresh token stored.' : ''
  ].filter(Boolean).join(' ');
  renderOauthAccountList();
  updateAdminButtons();
}

function selectService(service) {
  state.activeServiceId = service.id || '';
  serviceIdEl.value = service.id || '';
  serviceNameEl.value = service.name || '';
  serviceBaseUrlEl.value = service.base_url || '';
  serviceAuthTypeEl.value = service.auth_type || 'bearer';
  serviceAuthHeaderEl.value = service.auth_header || '';
  serviceOauthAccountEl.value = service.oauth_account || '';
  serviceRateLimitEl.value = service.rate_limit_per_minute || '';
  serviceAuthKeyEl.value = '';
  serviceAutonomousApprovedEl.checked = !!service.autonomous_approved;
  serviceEnabledEl.checked = !!service.enabled;
  serviceStatusEl.textContent = service.auth_type === 'oauth-account'
    ? ((service.oauth_account_name || 'No OAuth account') + (service.oauth_account_connected ? ' is connected.' : ' is not connected yet.'))
    : (service.auth_key_configured ? 'Secret stored.' : 'No secret stored.');
  if (service.effective_rate_limit_per_minute) {
    serviceStatusEl.textContent += ' Rate limit: ' + service.effective_rate_limit_per_minute + '/min.';
  }
  renderServiceList();
  renderOauthAccountOptions();
  updateAdminButtons();
}

function selectSite(site) {
  state.activeSiteId = site.id || '';
  siteIdEl.value = site.id || '';
  siteNameEl.value = site.name || '';
  siteLoginUrlEl.value = site.login_url || '';
  siteUsernameFieldEl.value = site.username_field || 'username';
  sitePasswordFieldEl.value = site.password_field || 'password';
  siteUsernameEl.value = '';
  sitePasswordEl.value = '';
  siteFormSelectorEl.value = '';
  siteExtraFieldsEl.value = '';
  siteAutonomousApprovedEl.checked = !!site.autonomous_approved;
  siteStatusEl.textContent = site.username_configured ? 'Credentials stored.' : 'No credentials stored.';
  renderSiteList();
  updateAdminButtons();
}

async function selectHistorySession(session) {
  const sessionId = (session && session.id) || '';
  state.history.activeSessionId = sessionId;
  state.history.sessionMessages = [];
  renderHistorySessionList();
  renderHistorySessionMessages();
  if (sessionId) {
    await loadHistorySessionMessages(sessionId);
  } else {
    historySessionStatusEl.textContent = 'No chat session selected.';
  }
}

async function loadHistorySessionMessages(sessionId) {
  if (!sessionId) {
    state.history.sessionMessages = [];
    renderHistorySessionMessages();
    historySessionStatusEl.textContent = 'No chat session selected.';
    return;
  }
  historySessionStatusEl.textContent = 'Loading transcript...';
  try {
    const data = await fetchJson('/sessions/' + encodeURIComponent(sessionId) + '/messages');
    if (state.history.activeSessionId !== sessionId) return;
    state.history.sessionMessages = Array.isArray(data.messages)
      ? data.messages.map(normalizeMessage).filter(Boolean)
      : [];
    renderHistorySessionMessages();
    historySessionStatusEl.textContent = state.history.sessionMessages.length
      ? ('Showing ' + state.history.sessionMessages.length + ' messages.')
      : 'This session has no user/assistant transcript yet.';
  } catch (err) {
    if (state.history.activeSessionId !== sessionId) return;
    state.history.sessionMessages = [];
    renderHistorySessionMessages();
    historySessionStatusEl.textContent = err.message || 'Failed to load transcript.';
  }
}

async function loadHistorySessions() {
  return dedup('loadHistorySessions', loadHistorySessionsImpl);
}
async function loadHistorySessionsImpl() {
  historySessionStatusEl.textContent = 'Loading sessions...';
  try {
    const data = await fetchJson('/history/sessions');
    const sessions = Array.isArray(data.sessions) ? data.sessions : [];
    state.history.sessions = sessions;
    const keepActive = sessions.some((session) => session.id === state.history.activeSessionId)
      ? state.history.activeSessionId
      : '';
    const preferCurrent = state.sessionId && sessions.some((session) => session.id === state.sessionId)
      ? state.sessionId
      : '';
    const nextId = keepActive || preferCurrent || ((sessions[0] && sessions[0].id) || '');
    state.history.activeSessionId = nextId;
    renderHistorySessionList();
    if (!nextId) {
      state.history.sessionMessages = [];
      renderHistorySessionMessages();
      historySessionStatusEl.textContent = 'No chat sessions recorded yet.';
      return;
    }
    historySessionStatusEl.textContent = sessions.length + (sessions.length === 1 ? ' session found.' : ' sessions found.');
    await loadHistorySessionMessages(nextId);
  } catch (err) {
    state.history.sessions = [];
    state.history.activeSessionId = '';
    state.history.sessionMessages = [];
    renderHistorySessionList();
    renderHistorySessionMessages();
    historySessionStatusEl.textContent = err.message || 'Failed to load sessions.';
  }
}

async function selectHistorySchedule(schedule) {
  const scheduleId = (schedule && schedule.id) || '';
  state.history.activeScheduleId = scheduleId;
  state.history.scheduleRuns = [];
  renderHistoryScheduleList();
  renderHistoryScheduleRuns();
  if (scheduleId) {
    await loadHistoryScheduleRuns(scheduleId);
  } else {
    historyScheduleStatusEl.textContent = 'No schedule selected.';
  }
}

async function loadHistoryScheduleRuns(scheduleId) {
  if (!scheduleId) {
    state.history.scheduleRuns = [];
    renderHistoryScheduleRuns();
    historyScheduleStatusEl.textContent = 'No schedule selected.';
    return;
  }
  historyScheduleStatusEl.textContent = 'Loading runs...';
  try {
    const data = await fetchJson('/history/schedules/' + encodeURIComponent(scheduleId) + '/runs');
    if (state.history.activeScheduleId !== scheduleId) return;
    state.history.scheduleRuns = Array.isArray(data.runs) ? data.runs : [];
    renderHistoryScheduleRuns();
    historyScheduleStatusEl.textContent = state.history.scheduleRuns.length
      ? ('Showing ' + state.history.scheduleRuns.length + ' recent runs.')
      : 'No runs recorded for this schedule yet.';
  } catch (err) {
    if (state.history.activeScheduleId !== scheduleId) return;
    state.history.scheduleRuns = [];
    renderHistoryScheduleRuns();
    historyScheduleStatusEl.textContent = err.message || 'Failed to load schedule runs.';
  }
}

async function loadHistorySchedules() {
  return dedup('loadHistorySchedules', loadHistorySchedulesImpl);
}
async function loadHistorySchedulesImpl() {
  historyScheduleStatusEl.textContent = 'Loading schedules...';
  try {
    const data = await fetchJson('/history/schedules');
    const schedules = Array.isArray(data.schedules) ? data.schedules : [];
    state.history.schedules = schedules;
    const nextId = schedules.some((schedule) => schedule.id === state.history.activeScheduleId)
      ? state.history.activeScheduleId
      : ((schedules[0] && schedules[0].id) || '');
    state.history.activeScheduleId = nextId;
    renderHistoryScheduleList();
    if (!nextId) {
      state.history.scheduleRuns = [];
      renderHistoryScheduleRuns();
      historyScheduleStatusEl.textContent = 'No schedules recorded yet.';
      return;
    }
    historyScheduleStatusEl.textContent = schedules.length + (schedules.length === 1 ? ' schedule found.' : ' schedules found.');
    await loadHistoryScheduleRuns(nextId);
  } catch (err) {
    state.history.schedules = [];
    state.history.activeScheduleId = '';
    state.history.scheduleRuns = [];
    renderHistoryScheduleList();
    renderHistoryScheduleRuns();
    historyScheduleStatusEl.textContent = err.message || 'Failed to load schedules.';
  }
}

async function selectKnowledgeNode(node) {
  const nodeId = (node && node.id) || '';
  state.activeKnowledgeNodeId = nodeId;
  state.knowledgeFacts = [];
  state.knowledgeLoadingFacts = false;
  renderKnowledgeNodeList();
  renderKnowledgeFacts();
  if (nodeId) {
    await loadKnowledgeFacts(nodeId);
  } else {
    knowledgeStatusEl.textContent = 'No knowledge entity selected.';
  }
}

async function searchKnowledgeNodes() {
  if (state.knowledgeSearching) return;
  const query = knowledgeQueryEl.value.trim();
  state.knowledgeQuery = query;
  state.knowledgeNodes = [];
  state.activeKnowledgeNodeId = '';
  state.knowledgeFacts = [];
  state.knowledgeLoadingFacts = false;
  renderKnowledgeNodeList();
  renderKnowledgeFacts();
  if (!query) {
    knowledgeStatusEl.textContent = 'Enter a person, project, or topic to search stored knowledge.';
    updateAdminButtons();
    return;
  }
  state.knowledgeSearching = true;
  updateAdminButtons();
  knowledgeStatusEl.textContent = 'Searching knowledge graph...';
  try {
    const data = await fetchJson('/knowledge/nodes?query=' + encodeURIComponent(query));
    if (state.knowledgeQuery !== query) return;
    state.knowledgeNodes = Array.isArray(data.nodes) ? data.nodes : [];
    const nextNodeId = (state.knowledgeNodes[0] && state.knowledgeNodes[0].id) || '';
    state.activeKnowledgeNodeId = nextNodeId;
    renderKnowledgeNodeList();
    renderKnowledgeFacts();
    if (!nextNodeId) {
      knowledgeStatusEl.textContent = 'No matching entities found.';
      return;
    }
    knowledgeStatusEl.textContent = state.knowledgeNodes.length
      + (state.knowledgeNodes.length === 1 ? ' matching entity found.' : ' matching entities found.');
    await loadKnowledgeFacts(nextNodeId);
  } catch (err) {
    if (state.knowledgeQuery !== query) return;
    state.knowledgeNodes = [];
    state.activeKnowledgeNodeId = '';
    state.knowledgeFacts = [];
    renderKnowledgeNodeList();
    renderKnowledgeFacts();
    knowledgeStatusEl.textContent = err.message || 'Failed to search knowledge graph.';
  } finally {
    if (state.knowledgeQuery === query) {
      state.knowledgeSearching = false;
      updateAdminButtons();
    }
  }
}

async function loadKnowledgeFacts(nodeId) {
  if (!nodeId) {
    state.knowledgeFacts = [];
    renderKnowledgeFacts();
    knowledgeStatusEl.textContent = 'No knowledge entity selected.';
    return;
  }
  const activeNode = state.knowledgeNodes.find((node) => node.id === nodeId);
  state.knowledgeLoadingFacts = true;
  renderKnowledgeFacts();
  knowledgeStatusEl.textContent = activeNode
    ? ('Loading facts for ' + firstNonEmpty(activeNode.name, activeNode.id) + '...')
    : 'Loading facts...';
  try {
    const data = await fetchJson('/knowledge/nodes/' + encodeURIComponent(nodeId) + '/facts');
    if (state.activeKnowledgeNodeId !== nodeId) return;
    state.knowledgeFacts = Array.isArray(data.facts) ? data.facts : [];
    renderKnowledgeFacts();
    const node = data.node || activeNode || {};
    const nodeName = firstNonEmpty(node.name, node.id);
    knowledgeStatusEl.textContent = state.knowledgeFacts.length
      ? ('Showing ' + state.knowledgeFacts.length + ' facts for ' + nodeName + '.')
      : ('No facts stored for ' + nodeName + '.');
  } catch (err) {
    if (state.activeKnowledgeNodeId !== nodeId) return;
    state.knowledgeFacts = [];
    renderKnowledgeFacts();
    knowledgeStatusEl.textContent = err.message || 'Failed to load knowledge facts.';
  } finally {
    if (state.activeKnowledgeNodeId === nodeId) {
      state.knowledgeLoadingFacts = false;
      renderKnowledgeFacts();
    }
  }
}

async function forgetKnowledgeFact(fact) {
  if (!fact || !fact.id || state.knowledgeForgettingFactId) return;
  if (!window.confirm('Forget this fact?\n\n' + firstNonEmpty(fact.content, 'Untitled fact'))) return;
  const activeNode = state.knowledgeNodes.find((node) => node.id === state.activeKnowledgeNodeId) || {};
  state.knowledgeForgettingFactId = fact.id;
  renderKnowledgeFacts();
  try {
    await fetchJson('/knowledge/facts/' + encodeURIComponent(fact.id), { method: 'DELETE' });
    state.knowledgeFacts = state.knowledgeFacts.filter((entry) => entry.id !== fact.id);
    renderKnowledgeFacts();
    const nodeName = firstNonEmpty(activeNode.name, activeNode.id);
    knowledgeStatusEl.textContent = state.knowledgeFacts.length
      ? ('Forgot 1 fact. Showing ' + state.knowledgeFacts.length + ' facts for ' + nodeName + '.')
      : ('Forgot 1 fact. No facts remain for ' + nodeName + '.');
  } catch (err) {
    knowledgeStatusEl.textContent = err.message || 'Failed to forget fact.';
  } finally {
    state.knowledgeForgettingFactId = '';
    renderKnowledgeFacts();
  }
}

async function loadAdminConfig() {
  return dedup('loadAdminConfig', loadAdminConfigImpl);
}
async function loadAdminConfigImpl() {
  try {
    const data = await fetchJson('/admin/config');
    state.admin.providers = Array.isArray(data.providers) ? data.providers : [];
    state.admin.conversationContext = data.conversation_context || null;
    state.admin.memoryRetention = data.memory_retention || null;
    state.admin.knowledgeDecay = data.knowledge_decay || null;
    state.admin.localDocSummarization = data.local_doc_summarization || null;
    state.admin.databaseBackup = data.database_backup || null;
    state.admin.llmWorkloads = Array.isArray(data.llm_workloads) ? data.llm_workloads : [];
    state.admin.oauthProviderTemplates = Array.isArray(data.oauth_provider_templates) ? data.oauth_provider_templates : [];
    state.admin.oauthAccounts = Array.isArray(data.oauth_accounts) ? data.oauth_accounts : [];
    state.admin.services = Array.isArray(data.services) ? data.services : [];
    state.admin.sites = Array.isArray(data.sites) ? data.sites : [];
    state.admin.tools = Array.isArray(data.tools) ? data.tools : [];
    state.admin.skills = Array.isArray(data.skills) ? data.skills : [];
    state.admin.remoteBridge = data.remote_bridge || null;
    state.admin.remoteDevices = Array.isArray(data.remote_devices) ? data.remote_devices : [];
    state.admin.remoteEvents = Array.isArray(data.remote_events) ? data.remote_events : [];
    state.admin.remoteSnapshot = data.remote_snapshot || null;
    state.contextStatus = defaultConversationContextStatus();
    state.localDocSummarizationStatus = defaultLocalDocSummarizationStatus();
    state.databaseBackupStatus = defaultDatabaseBackupStatus();
    state.remoteBridgeStatus = defaultRemoteBridgeStatus();
    state.remotePairStatus = defaultRemotePairStatus();
    const provider = state.admin.providers.find((entry) => entry.id === state.activeProviderId);
    const oauthAccount = state.admin.oauthAccounts.find((entry) => entry.id === state.activeOauthAccountId);
    const service = state.admin.services.find((entry) => entry.id === state.activeServiceId);
    const site = state.admin.sites.find((entry) => entry.id === state.activeSiteId);
    renderOauthTemplateOptions();
    renderOauthAccountOptions();
    renderProviderWorkloadNote();
    renderConversationContextSettings();
    renderMemoryRetentionSettings();
    renderKnowledgeDecaySettings();
    renderLocalDocSummarizationSettings();
    renderDatabaseBackupSettings();
    if (provider) {
      selectProvider(provider);
    } else if (!state.activeProviderId) {
      resetProviderForm('Create a model or select an existing one.');
    }
    if (oauthAccount) {
      selectOauthAccount(oauthAccount);
    } else if (!state.activeOauthAccountId) {
      resetOauthAccountForm('Create a connection or select an existing one.');
    }
    if (service) {
      selectService(service);
    } else if (!state.activeServiceId) {
      resetServiceForm('Create a service or select an existing one.');
    }
    if (site) {
      selectSite(site);
    } else if (!state.activeSiteId) {
      resetSiteForm('Create a site login or select an existing one.');
    }
    renderCapabilities();
  } catch (err) {
    console.error(err);
  }
}

async function saveRemoteBridge() {
  if (state.remoteBridgeSaving) return;
  state.remoteBridgeSaving = true;
  state.remoteBridgeStatus = 'Saving bridge settings...';
  renderRemoteBridge();
  updateAdminButtons();
  try {
    await fetchJson('/admin/remote-bridge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: remoteBridgeEnabledEl.checked,
        instance_label: remoteBridgeInstanceLabelEl.value,
        relay_url: remoteBridgeRelayUrlEl.value
      })
    });
    await loadAdminConfig();
    state.remoteBridgeStatus = 'Bridge settings saved.';
    setStatus('Notification bridge saved');
  } catch (err) {
    state.remoteBridgeStatus = err.message || 'Failed to save bridge settings.';
    setStatus('Failed to save notification bridge');
  } finally {
    state.remoteBridgeSaving = false;
    renderRemoteBridge();
    updateAdminButtons();
  }
}

async function pairRemoteDevice() {
  const pairingToken = remotePairingTokenEl.value.trim();
  if (!pairingToken || state.remotePairing) return;
  state.remotePairing = true;
  state.remotePairStatus = 'Pairing device...';
  renderRemoteBridge();
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/remote-bridge/pair', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pairing_token: pairingToken })
    });
    await loadAdminConfig();
    const name = firstNonEmpty(data.device && data.device.name, 'device');
    state.remotePairStatus = 'Paired ' + name + '.';
    remotePairingTokenEl.value = '';
    setStatus('Remote device paired');
  } catch (err) {
    state.remotePairStatus = err.message || 'Failed to pair device.';
    setStatus('Failed to pair remote device');
  } finally {
    state.remotePairing = false;
    renderRemoteBridge();
    updateAdminButtons();
  }
}

async function revokeRemoteDevice(device) {
  if (!device || !device.id || state.remotePairing) return;
  if (!window.confirm('Revoke this paired device?')) return;
  state.remotePairing = true;
  state.remotePairStatus = 'Revoking device...';
  renderRemoteBridge();
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/remote-bridge/devices/' + encodeURIComponent(device.id), {
      method: 'DELETE'
    });
    await loadAdminConfig();
    const name = firstNonEmpty(data.device && data.device.name, device.name || 'device');
    state.remotePairStatus = 'Revoked ' + name + '.';
    setStatus('Remote device revoked');
  } catch (err) {
    state.remotePairStatus = err.message || 'Failed to revoke device.';
    setStatus('Failed to revoke remote device');
  } finally {
    state.remotePairing = false;
    renderRemoteBridge();
    updateAdminButtons();
  }
}

async function importOpenClawSkill() {
  const source = openclawImportSourceEl.value.trim();
  if (!source || state.openclawImporting) return;
  state.openclawImporting = true;
  state.openclawImportStatus = 'Importing skill...';
  renderOpenClawImport();
  try {
    const data = await fetchJson('/admin/skills/import-openclaw', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source: source,
        strict: openclawImportStrictEl.checked
      })
    });
    state.openclawImportReport = {
      import: data.import || null,
      skill: data.skill || null
    };
    await loadAdminConfig();
    const importedName = firstNonEmpty(data.skill && data.skill.name, firstNonEmpty(data.import && data.import.name, 'skill'));
    const warningCount = Array.isArray(data.import && data.import.warnings) ? data.import.warnings.length : 0;
    state.openclawImportStatus = warningCount
      ? ('Imported ' + importedName + ' with ' + warningCount + ' ' + pluralize(warningCount, 'warning') + '.')
      : ('Imported ' + importedName + '.');
    openclawImportSourceEl.value = '';
    setStatus('Imported OpenClaw skill');
  } catch (err) {
    state.openclawImportReport = null;
    state.openclawImportStatus = err.message || 'Failed to import skill.';
    setStatus('Failed to import OpenClaw skill');
  } finally {
    state.openclawImporting = false;
    renderOpenClawImport();
  }
}

function applyOauthTemplate() {
  const templateId = oauthTemplateEl.value;
  const template = state.admin.oauthProviderTemplates.find((entry) => entry.id === templateId);
  if (!template) {
    oauthAccountStatusEl.textContent = 'Choose a preset first.';
    return;
  }
  if (state.activeOauthAccountId) {
    state.activeOauthAccountId = '';
  }
  renderOauthAccountList();
  oauthAccountIdEl.value = oauthAccountIdEl.value.trim() || template.id || '';
  oauthAccountNameEl.value = template.name || '';
  oauthAuthorizeUrlEl.value = template.authorize_url || '';
  oauthTokenUrlEl.value = template.token_url || '';
  oauthScopesEl.value = template.scopes || '';
  oauthRedirectUriEl.value = '';
  oauthAuthParamsEl.value = template.auth_params || '';
  oauthTokenParamsEl.value = template.token_params || '';
  oauthTemplateEl.value = template.id || '';
  oauthAccountStatusEl.textContent = 'Preset applied. Enter your Client ID and Secret.';
  updateAdminButtons();
  oauthClientIdEl.focus();
}

function createServiceFromOauthAccount() {
  const account = state.admin.oauthAccounts.find((entry) => entry.id === state.activeOauthAccountId);
  if (!account) {
    oauthAccountStatusEl.textContent = 'Select a connection first.';
    return;
  }
  const template = account.provider_template
    ? state.admin.oauthProviderTemplates.find((entry) => entry.id === account.provider_template)
    : null;
  state.activeServiceId = '';
  renderServiceList();
  renderOauthAccountOptions();
  serviceIdEl.value = (template && template.service_id) || account.id || '';
  serviceNameEl.value = (template && template.service_name) || (firstNonEmpty(account.name, account.id) + ' API');
  serviceBaseUrlEl.value = (template && template.api_base_url) || '';
  serviceAuthTypeEl.value = 'oauth-account';
  serviceAuthHeaderEl.value = '';
  serviceOauthAccountEl.value = account.id || '';
  serviceRateLimitEl.value = '';
  serviceAuthKeyEl.value = '';
  serviceAutonomousApprovedEl.checked = true;
  serviceEnabledEl.checked = true;
  serviceStatusEl.textContent = 'Service prefilled. Review and save it.';
  updateAdminButtons();
  serviceIdEl.focus();
}

async function saveProvider() {
  if (state.providerSaving) return;
  state.providerSaving = true;
  providerStatusEl.textContent = 'Saving...';
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/providers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: providerIdEl.value,
        name: providerNameEl.value,
        base_url: providerBaseUrlEl.value,
        model: providerModelEl.value,
        workloads: parseProviderWorkloadsInput(),
        system_prompt_budget: providerSystemPromptBudgetEl.value,
        history_budget: providerHistoryBudgetEl.value,
        rate_limit_per_minute: providerRateLimitEl.value,
        vision: providerVisionEl.checked,
        api_key: providerApiKeyEl.value,
        default: providerDefaultEl.checked
      })
    });
    const provider = data.provider || null;
    state.activeProviderId = provider && provider.id ? provider.id : state.activeProviderId;
    providerApiKeyEl.value = '';
    providerStatusEl.textContent = 'Model saved.';
    await loadAdminConfig();
    setStatus('Model saved');
  } catch (err) {
    providerStatusEl.textContent = err.message || 'Failed to save.';
  } finally {
    state.providerSaving = false;
    updateAdminButtons();
  }
}

async function saveMemoryRetention() {
  if (state.retentionSaving) return;
  state.retentionSaving = true;
  retentionStatusEl.textContent = 'Saving...';
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/memory-retention', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        full_resolution_days: retentionFullResolutionDaysEl.value,
        decay_half_life_days: retentionDecayHalfLifeDaysEl.value,
        retained_count: retentionRetainedCountEl.value
      })
    });
    state.admin.memoryRetention = data.memory_retention || state.admin.memoryRetention;
    renderMemoryRetentionSettings();
    retentionStatusEl.textContent = 'Retention settings saved.';
    setStatus('Retention settings saved');
  } catch (err) {
    retentionStatusEl.textContent = err.message || 'Failed to save.';
  } finally {
    state.retentionSaving = false;
    updateAdminButtons();
  }
}

async function saveConversationContext() {
  if (state.contextSaving) return;
  state.contextSaving = true;
  state.contextStatus = 'Saving...';
  renderConversationContextSettings();
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/context', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        recent_history_message_limit: contextRecentHistoryMessageLimitEl.value
      })
    });
    state.admin.conversationContext = data.conversation_context || state.admin.conversationContext;
    state.contextStatus = 'Conversation context settings saved.';
    renderConversationContextSettings();
    setStatus('Conversation context settings saved');
  } catch (err) {
    state.contextStatus = err.message || 'Failed to save conversation context settings.';
    renderConversationContextSettings();
  } finally {
    state.contextSaving = false;
    updateAdminButtons();
  }
}

async function saveKnowledgeDecay() {
  if (state.knowledgeDecaySaving) return;
  state.knowledgeDecaySaving = true;
  knowledgeDecayStatusEl.textContent = 'Saving...';
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/knowledge-decay', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        grace_period_days: knowledgeGracePeriodDaysEl.value,
        half_life_days: knowledgeHalfLifeDaysEl.value,
        min_confidence: knowledgeMinConfidenceEl.value,
        maintenance_interval_days: knowledgeMaintenanceIntervalDaysEl.value,
        archive_after_bottom_days: knowledgeArchiveAfterBottomDaysEl.value
      })
    });
    state.admin.knowledgeDecay = data.knowledge_decay || state.admin.knowledgeDecay;
    renderKnowledgeDecaySettings();
    knowledgeDecayStatusEl.textContent = 'Knowledge decay settings saved.';
    setStatus('Knowledge decay settings saved');
  } catch (err) {
    knowledgeDecayStatusEl.textContent = err.message || 'Failed to save.';
  } finally {
    state.knowledgeDecaySaving = false;
    updateAdminButtons();
  }
}

async function saveLocalDocSummarization() {
  if (state.localDocSummarizationSaving) return;
  state.localDocSummarizationSaving = true;
  state.localDocSummarizationStatus = 'Saving...';
  renderLocalDocSummarizationSettings();
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/local-doc-summarization', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model_summaries_enabled: localDocModelSummariesEnabledEl.checked,
        model_summary_backend: localDocModelSummaryBackendEl.value,
        model_summary_provider_id: localDocModelSummaryProviderIdEl.value,
        chunk_summary_max_tokens: localDocChunkSummaryMaxTokensEl.value,
        doc_summary_max_tokens: localDocDocSummaryMaxTokensEl.value
      })
    });
    state.admin.localDocSummarization = data.local_doc_summarization || state.admin.localDocSummarization;
    state.localDocSummarizationStatus = 'Local document summarization settings saved.';
    renderLocalDocSummarizationSettings();
    setStatus('Local document summarization settings saved');
  } catch (err) {
    state.localDocSummarizationStatus = err.message || 'Failed to save local document summarization settings.';
    renderLocalDocSummarizationSettings();
  } finally {
    state.localDocSummarizationSaving = false;
    updateAdminButtons();
  }
}

async function saveDatabaseBackup() {
  if (state.databaseBackupSaving) return;
  state.databaseBackupSaving = true;
  state.databaseBackupStatus = 'Saving...';
  renderDatabaseBackupSettings();
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/database-backup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: databaseBackupEnabledEl.checked,
        directory: databaseBackupDirectoryEl.value,
        interval_hours: databaseBackupIntervalHoursEl.value,
        retain_count: databaseBackupRetainCountEl.value
      })
    });
    state.admin.databaseBackup = data.database_backup || state.admin.databaseBackup;
    state.databaseBackupStatus = 'Database backup settings saved.';
    renderDatabaseBackupSettings();
    setStatus('Database backup settings saved');
  } catch (err) {
    state.databaseBackupStatus = err.message || 'Failed to save database backup settings.';
    renderDatabaseBackupSettings();
  } finally {
    state.databaseBackupSaving = false;
    updateAdminButtons();
  }
}

async function saveOauthAccount() {
  if (state.oauthSaving) return;
  state.oauthSaving = true;
  oauthAccountStatusEl.textContent = 'Saving...';
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/oauth-accounts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: oauthAccountIdEl.value,
        name: oauthAccountNameEl.value,
        authorize_url: oauthAuthorizeUrlEl.value,
        token_url: oauthTokenUrlEl.value,
        client_id: oauthClientIdEl.value,
        client_secret: oauthClientSecretEl.value,
        provider_template: oauthTemplateEl.value,
        scopes: oauthScopesEl.value,
        redirect_uri: oauthRedirectUriEl.value,
        auth_params: oauthAuthParamsEl.value,
        token_params: oauthTokenParamsEl.value,
        autonomous_approved: oauthAccountAutonomousApprovedEl.checked
      })
    });
    const account = data.oauth_account || null;
    state.activeOauthAccountId = account && account.id ? account.id : state.activeOauthAccountId;
    oauthClientSecretEl.value = '';
    oauthAccountStatusEl.textContent = 'Connection saved.';
    await loadAdminConfig();
    setStatus('Connection saved');
  } catch (err) {
    oauthAccountStatusEl.textContent = err.message || 'Failed to save.';
  } finally {
    state.oauthSaving = false;
    updateAdminButtons();
  }
}

async function connectOauthAccount() {
  if (!state.activeOauthAccountId || state.oauthSaving) {
    oauthAccountStatusEl.textContent = 'Save the details first, then connect.';
    return;
  }
  state.oauthSaving = true;
  oauthAccountStatusEl.textContent = 'Opening consent flow...';
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/oauth-accounts/' + encodeURIComponent(state.activeOauthAccountId) + '/connect', { method: 'POST' });
    const popup = window.open(data.authorization_url, 'xia-oauth-connect', 'popup,width=720,height=860');
    if (!popup) {
      oauthAccountStatusEl.textContent = 'Popup blocked. Allow popups and try again.';
    } else {
      oauthAccountStatusEl.textContent = 'Follow the instructions in the new window.';
      popup.focus();
    }
  } catch (err) {
    oauthAccountStatusEl.textContent = err.message || 'Failed to start flow.';
  } finally {
    state.oauthSaving = false;
    updateAdminButtons();
  }
}

async function refreshOauthAccount() {
  if (!state.activeOauthAccountId || state.oauthSaving) return;
  state.oauthSaving = true;
  oauthAccountStatusEl.textContent = 'Refreshing...';
  updateAdminButtons();
  try {
    await fetchJson('/admin/oauth-accounts/' + encodeURIComponent(state.activeOauthAccountId) + '/refresh', { method: 'POST' });
    oauthAccountStatusEl.textContent = 'Refreshed.';
    await loadAdminConfig();
    setStatus('Refreshed connection');
  } catch (err) {
    oauthAccountStatusEl.textContent = err.message || 'Failed to refresh.';
  } finally {
    state.oauthSaving = false;
    updateAdminButtons();
  }
}

async function deleteOauthAccount() {
  if (!state.activeOauthAccountId || state.oauthSaving) return;
  if (!window.confirm('Delete this connection?')) return;
  state.oauthSaving = true;
  oauthAccountStatusEl.textContent = 'Deleting...';
  updateAdminButtons();
  try {
    await fetchJson('/admin/oauth-accounts/' + encodeURIComponent(state.activeOauthAccountId), { method: 'DELETE' });
    state.activeOauthAccountId = '';
    resetOauthAccountForm('Deleted.');
    await loadAdminConfig();
    setStatus('Deleted connection');
  } catch (err) {
    oauthAccountStatusEl.textContent = err.message || 'Failed to delete.';
  } finally {
    state.oauthSaving = false;
    updateAdminButtons();
  }
}

async function saveService() {
  if (state.serviceSaving) return;
  state.serviceSaving = true;
  serviceStatusEl.textContent = 'Saving...';
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/services', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: serviceIdEl.value,
        name: serviceNameEl.value,
        base_url: serviceBaseUrlEl.value,
        auth_type: serviceAuthTypeEl.value,
        auth_header: serviceAuthHeaderEl.value,
        oauth_account: serviceOauthAccountEl.value,
        rate_limit_per_minute: serviceRateLimitEl.value,
        auth_key: serviceAuthKeyEl.value,
        autonomous_approved: serviceAutonomousApprovedEl.checked,
        enabled: serviceEnabledEl.checked
      })
    });
    const service = data.service || null;
    state.activeServiceId = service && service.id ? service.id : state.activeServiceId;
    serviceAuthKeyEl.value = '';
    serviceStatusEl.textContent = 'Service saved.';
    await loadAdminConfig();
    setStatus('Service saved');
  } catch (err) {
    serviceStatusEl.textContent = err.message || 'Failed to save.';
  } finally {
    state.serviceSaving = false;
    updateAdminButtons();
  }
}

async function saveSite() {
  if (state.siteSaving) return;
  state.siteSaving = true;
  siteStatusEl.textContent = 'Saving...';
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/sites', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: siteIdEl.value,
        name: siteNameEl.value,
        login_url: siteLoginUrlEl.value,
        username_field: siteUsernameFieldEl.value,
        password_field: sitePasswordFieldEl.value,
        username: siteUsernameEl.value,
        password: sitePasswordEl.value,
        form_selector: siteFormSelectorEl.value,
        extra_fields: siteExtraFieldsEl.value,
        autonomous_approved: siteAutonomousApprovedEl.checked
      })
    });
    const site = data.site || null;
    state.activeSiteId = site && site.id ? site.id : state.activeSiteId;
    siteUsernameEl.value = '';
    sitePasswordEl.value = '';
    siteStatusEl.textContent = 'Saved.';
    await loadAdminConfig();
    setStatus('Site login saved');
  } catch (err) {
    siteStatusEl.textContent = err.message || 'Failed to save.';
  } finally {
    state.siteSaving = false;
    updateAdminButtons();
  }
}

async function deleteSite() {
  if (!state.activeSiteId || state.siteSaving) return;
  if (!window.confirm('Delete this site login?')) return;
  state.siteSaving = true;
  siteStatusEl.textContent = 'Deleting...';
  updateAdminButtons();
  try {
    await fetchJson('/admin/sites/' + encodeURIComponent(state.activeSiteId), { method: 'DELETE' });
    state.activeSiteId = '';
    resetSiteForm('Deleted.');
    await loadAdminConfig();
    setStatus('Deleted site login');
  } catch (err) {
    siteStatusEl.textContent = err.message || 'Failed to delete.';
  } finally {
    state.siteSaving = false;
    updateAdminButtons();
  }
}

function renderApproval() {
  const approval = state.pendingApproval;
  approvalPanelEl.hidden = !approval;
  allowApprovalEl.disabled = !approval || state.approvalSubmitting;
  denyApprovalEl.disabled = !approval || state.approvalSubmitting;
  if (!approval) return;
  approvalToolEl.textContent = approval.tool_name || approval.tool_id || 'Privileged tool';
  approvalReasonEl.textContent = approval.reason || 'Needs confirmation.';
  approvalArgsEl.textContent = prettyJson(approval.arguments || {});
}

function formatDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleString([], {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit'
  });
}

function formatStamp(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
}

function normalizeMessage(message) {
  if (!message) return null;
  return Object.assign({}, message, {
    createdAt: message.createdAt || message.created_at || '',
    localDocs: Array.isArray(message.localDocs)
      ? message.localDocs
      : (Array.isArray(message.local_docs) ? message.local_docs : []),
    artifacts: Array.isArray(message.artifacts)
      ? message.artifacts
      : (Array.isArray(message.artifact_refs) ? message.artifact_refs : [])
  });
}

function buildMessageEl(message) {
  const card = document.createElement('article');
  card.className = 'message ' + (message.role === 'assistant' ? 'assistant' : message.role === 'user' ? 'user' : 'error');
  const head = document.createElement('div');
  head.className = 'message-head';
  const roleWrap = document.createElement('div');
  const role = document.createElement('div');
  role.className = 'message-role';
  role.textContent = message.role === 'assistant' ? 'Xia' : message.role === 'user' ? 'You' : 'Status';
  const meta = document.createElement('div');
  meta.className = 'message-meta';
  meta.textContent = formatStamp(message.createdAt);
  roleWrap.appendChild(role);
  roleWrap.appendChild(meta);
  head.appendChild(roleWrap);
  if (message.role !== 'user') {
    const copyButton = document.createElement('button');
    copyButton.type = 'button';
    copyButton.className = 'copy-link';
    copyButton.textContent = 'Copy';
    copyButton.addEventListener('click', () => copyText(message.content, 'Message copied'));
    head.appendChild(copyButton);
  }
  const body = document.createElement('div');
  body.className = 'message-body';
  if (message.role === 'assistant' && typeof marked !== 'undefined') {
    const raw = marked.parse(message.content);
    body.innerHTML = typeof DOMPurify !== 'undefined' ? DOMPurify.sanitize(raw) : escapeHtml(raw);
  } else {
    body.textContent = message.content;
  }
  card.appendChild(head);
  card.appendChild(body);
  if (Array.isArray(message.localDocs) && message.localDocs.length) {
    const refs = document.createElement('div');
    refs.className = 'message-meta';
    refs.textContent = 'Local docs: ' + message.localDocs
      .map((doc) => (doc && (doc.name || doc.id)) ? (doc.name || doc.id) : '')
      .filter(Boolean)
      .join(', ');
    card.appendChild(refs);
  }
  if (Array.isArray(message.artifacts) && message.artifacts.length) {
    const refs = document.createElement('div');
    refs.className = 'message-meta';
    refs.textContent = 'Artifacts: ' + message.artifacts
      .map((artifact) => {
        if (!artifact) return '';
        return artifact.title || artifact.name || artifact.id || '';
      })
      .filter(Boolean)
      .join(', ');
    card.appendChild(refs);
  }
  return card;
}

function addMessage(role, content, extra) {
  const message = Object.assign({
    role: role,
    content: content,
    createdAt: new Date().toISOString()
  }, extra || {});
  state.messages.push(message);
  if (state.messages.length === 1) {
    messagesEl.replaceChildren();
  }
  messagesEl.appendChild(buildMessageEl(message));
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function renderMessages() {
  messagesEl.replaceChildren();
  if (!state.messages.length) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = 'Start with a paste or a prompt.';
    messagesEl.appendChild(empty);
    return;
  }
  state.messages.forEach((message) => {
    messagesEl.appendChild(buildMessageEl(message));
  });
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function updateComposerState() {
  const empty = !composerEl.value.trim();
  if (empty) {
    state.pendingLocalDocIds = [];
    state.pendingArtifactIds = [];
  }
  sendEl.disabled = state.sending || empty;
  clearInputEl.disabled = state.sending || !composerEl.value.length;
}

function insertTextIntoComposer(text, statusText) {
  if (!text) return;
  const start = composerEl.selectionStart == null ? composerEl.value.length : composerEl.selectionStart;
  const end = composerEl.selectionEnd == null ? composerEl.value.length : composerEl.selectionEnd;
  const prefix = composerEl.value.slice(0, start);
  const suffix = composerEl.value.slice(end);
  const before = prefix && !prefix.endsWith('\n') ? '\n' : '';
  const after = suffix && !text.endsWith('\n') ? '\n' : '';
  composerEl.value = prefix + before + text + after + suffix;
  updateComposerState();
  composerEl.focus();
  if (statusText) setStatus(statusText);
}

function rememberPendingLocalDoc(docId) {
  if (!docId) return;
  if (!state.pendingLocalDocIds.includes(docId)) {
    state.pendingLocalDocIds.push(docId);
  }
}

function rememberPendingArtifact(artifactId) {
  if (!artifactId) return;
  if (!state.pendingArtifactIds.includes(artifactId)) {
    state.pendingArtifactIds.push(artifactId);
  }
}

function localDocTitle(doc) {
  return (doc && doc.name && doc.name.trim()) ? doc.name.trim() : 'Untitled upload';
}

function localDocMeta(doc) {
  const bits = [];
  if (doc.status === 'failed') bits.push('Failed');
  if (doc.media_type) bits.push(doc.media_type);
  if (typeof doc.size_bytes === 'number') bits.push(formatBytes(doc.size_bytes));
  if (doc.updated_at) bits.push('Updated ' + formatStamp(doc.updated_at));
  return bits.join(' • ');
}

function sortLocalDocs() {
  state.localDocs.sort((left, right) => {
    const a = Date.parse((left && left.updated_at) || '') || 0;
    const b = Date.parse((right && right.updated_at) || '') || 0;
    return b - a;
  });
}

function upsertLocalDocMeta(doc) {
  const meta = {
    id: doc.id,
    name: doc.name,
    media_type: doc.media_type,
    size_bytes: doc.size_bytes,
    status: doc.status,
    error: doc.error,
    preview: doc.preview,
    created_at: doc.created_at,
    updated_at: doc.updated_at
  };
  const index = state.localDocs.findIndex((entry) => entry.id === doc.id);
  if (index >= 0) state.localDocs[index] = meta;
  else state.localDocs.push(meta);
  sortLocalDocs();
}

function renderLocalDocList() {
  localDocListEl.replaceChildren();
  if (!state.localDocs.length) {
    const empty = document.createElement('div');
    empty.className = 'scratch-empty';
    empty.textContent = state.localDocUploading
      ? 'Uploading local documents...'
      : 'Choose a local file to make it available to Xia in this session.';
    localDocListEl.appendChild(empty);
    return;
  }
  state.localDocs.forEach((doc) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'admin-item' + (doc.id === state.activeLocalDocId ? ' active' : '');
    const title = document.createElement('div');
    title.className = 'admin-item-title';
    title.textContent = localDocTitle(doc);
    const meta = document.createElement('div');
    meta.className = 'admin-item-meta';
    meta.textContent = localDocMeta(doc);
    button.appendChild(title);
    button.appendChild(meta);
    button.addEventListener('click', () => loadLocalDoc(doc.id));
    localDocListEl.appendChild(button);
  });
}

function syncLocalDocPanel(statusText) {
  const doc = state.activeLocalDoc;
  localDocPreviewEl.value = doc ? (doc.text || doc.preview || '') : '';
  localDocStatusEl.textContent = statusText
    || (state.localDocUploading
      ? 'Uploading local documents...'
      : doc
        ? (doc.error || localDocMeta(doc) || 'Local document ready.')
        : 'No local document selected.');
  const hasDoc = !!doc;
  const usableDoc = hasDoc && doc.status !== 'failed';
  localDocInsertEl.disabled = !usableDoc || state.localDocUploading;
  localDocScratchEl.disabled = !usableDoc || state.localDocUploading;
  localDocDeleteEl.disabled = !hasDoc || state.localDocUploading;
  renderLocalDocList();
}

function resetLocalDocs(statusText) {
  state.localDocs = [];
  state.activeLocalDocId = '';
  state.activeLocalDoc = null;
  state.localDocUploading = false;
  syncLocalDocPanel(statusText || 'No local document selected.');
}

function artifactTitle(artifact) {
  return firstNonEmpty(artifact && artifact.title, firstNonEmpty(artifact && artifact.name, 'Untitled artifact'));
}

function artifactMeta(artifact) {
  const bits = [];
  if (artifact.kind) bits.push(String(artifact.kind).toUpperCase());
  if (artifact.media_type) bits.push(artifact.media_type);
  if (typeof artifact.size_bytes === 'number') bits.push(formatBytes(artifact.size_bytes));
  if (artifact.updated_at) bits.push('Updated ' + formatStamp(artifact.updated_at));
  return bits.join(' • ');
}

function sortArtifacts() {
  state.artifacts.sort((left, right) => {
    const a = Date.parse((left && left.updated_at) || '') || 0;
    const b = Date.parse((right && right.updated_at) || '') || 0;
    return b - a;
  });
}

function upsertArtifactMeta(artifact) {
  const meta = {
    id: artifact.id,
    name: artifact.name,
    title: artifact.title,
    kind: artifact.kind,
    media_type: artifact.media_type,
    size_bytes: artifact.size_bytes,
    compressed_size_bytes: artifact.compressed_size_bytes,
    status: artifact.status,
    error: artifact.error,
    has_blob: artifact.has_blob,
    text_available: artifact.text_available,
    preview: artifact.preview,
    created_at: artifact.created_at,
    updated_at: artifact.updated_at
  };
  const index = state.artifacts.findIndex((entry) => entry.id === artifact.id);
  if (index >= 0) state.artifacts[index] = meta;
  else state.artifacts.push(meta);
  sortArtifacts();
}

function renderArtifactList() {
  artifactListEl.replaceChildren();
  if (!state.artifacts.length) {
    const empty = document.createElement('div');
    empty.className = 'scratch-empty';
    empty.textContent = 'Artifacts created by Xia will appear here for this session.';
    artifactListEl.appendChild(empty);
    return;
  }
  state.artifacts.forEach((artifact) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'admin-item' + (artifact.id === state.activeArtifactId ? ' active' : '');
    const title = document.createElement('div');
    title.className = 'admin-item-title';
    title.textContent = artifactTitle(artifact);
    const meta = document.createElement('div');
    meta.className = 'admin-item-meta';
    meta.textContent = artifactMeta(artifact);
    button.appendChild(title);
    button.appendChild(meta);
    button.addEventListener('click', () => loadArtifact(artifact.id));
    artifactListEl.appendChild(button);
  });
}

function syncArtifactPanel(statusText) {
  const artifact = state.activeArtifact;
  artifactPreviewEl.value = artifact ? (artifact.text || artifact.preview || '') : '';
  artifactStatusEl.textContent = statusText
    || (artifact
      ? (artifact.error || artifactMeta(artifact) || 'Artifact ready.')
      : 'No artifact selected.');
  const hasArtifact = !!artifact;
  const canInsert = !!(artifact && artifact.text_available && artifact.text);
  const canCreateNote = !!(artifact && (artifact.text || artifact.preview));
  artifactInsertEl.disabled = !canInsert;
  artifactScratchEl.disabled = !canCreateNote;
  artifactDownloadEl.disabled = !hasArtifact;
  artifactDeleteEl.disabled = !hasArtifact;
  renderArtifactList();
}

function resetArtifacts(statusText) {
  state.artifacts = [];
  state.activeArtifactId = '';
  state.activeArtifact = null;
  syncArtifactPanel(statusText || 'No artifact selected.');
}

function formatBytes(bytes) {
  if (typeof bytes !== 'number' || !isFinite(bytes) || bytes < 0) return '';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < (1024 * 1024)) return (bytes / 1024).toFixed(1).replace(/\.0$/, '') + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1).replace(/\.0$/, '') + ' MB';
}

function selectedPreviewText() {
  const start = localDocPreviewEl.selectionStart;
  const end = localDocPreviewEl.selectionEnd;
  if (typeof start !== 'number' || typeof end !== 'number' || end <= start) return '';
  return localDocPreviewEl.value.slice(start, end).trim();
}

function selectedArtifactPreviewText() {
  const start = artifactPreviewEl.selectionStart;
  const end = artifactPreviewEl.selectionEnd;
  if (typeof start !== 'number' || typeof end !== 'number' || end <= start) return '';
  return artifactPreviewEl.value.slice(start, end).trim();
}

function padTitle(pad) {
  return (pad && pad.title && pad.title.trim()) ? pad.title.trim() : 'Untitled note';
}

function sortScratchPads() {
  state.scratchPads.sort((left, right) => {
    const a = Date.parse((left && left.updated_at) || '') || 0;
    const b = Date.parse((right && right.updated_at) || '') || 0;
    return b - a;
  });
}

function upsertScratchMeta(pad) {
  const meta = {
    id: pad.id, title: pad.title, mime: pad.mime, version: pad.version,
    created_at: pad.created_at, updated_at: pad.updated_at
  };
  const index = state.scratchPads.findIndex((entry) => entry.id === pad.id);
  if (index >= 0) state.scratchPads[index] = meta;
  else state.scratchPads.push(meta);
  sortScratchPads();
}

function renderScratchList() {
  scratchListEl.replaceChildren();
  if (!state.scratchPads.length) {
    const empty = document.createElement('div');
    empty.className = 'scratch-empty';
    empty.textContent = 'Create a note for drafts or intermediate results.';
    scratchListEl.appendChild(empty);
    return;
  }
  state.scratchPads.forEach((pad) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'scratch-item' + (pad.id === state.activePadId ? ' active' : '');
    const title = document.createElement('div');
    title.className = 'scratch-item-title';
    title.textContent = padTitle(pad);
    const meta = document.createElement('div');
    meta.className = 'scratch-item-meta';
    meta.textContent = 'Updated ' + formatStamp(pad.updated_at);
    button.appendChild(title);
    button.appendChild(meta);
    button.addEventListener('click', () => loadScratchPad(pad.id));
    scratchListEl.appendChild(button);
  });
}

function setScratchEditorEnabled(enabled) {
  scratchTitleEl.disabled = !enabled || state.scratchSaving;
  scratchEditorEl.disabled = !enabled || state.scratchSaving;
  saveScratchEl.disabled = !enabled || state.scratchSaving || !state.scratchDirty;
  deleteScratchEl.disabled = !enabled || state.scratchSaving;
  insertScratchEl.disabled = !enabled || !scratchEditorEl.value.length;
}

function syncScratchEditor(statusText) {
  if (state.activePad) {
    scratchTitleEl.value = state.activePad.title || '';
    scratchEditorEl.value = state.activePad.content || '';
    scratchStatusEl.textContent = statusText || (state.scratchSaving ? 'Saving...' : state.scratchDirty ? 'Unsaved changes.' : 'Saved.');
    setScratchEditorEnabled(true);
  } else {
    scratchTitleEl.value = '';
    scratchEditorEl.value = '';
    scratchStatusEl.textContent = statusText || 'No note selected.';
    setScratchEditorEnabled(false);
  }
  renderScratchList();
}

function discardScratchChanges() {
  if (!state.scratchDirty) return true;
  return window.confirm('Discard unsaved note changes?');
}

async function ensureSession() {
  if (state.sessionId) return state.sessionId;
  const response = await safeFetch('/sessions', { method: 'POST' });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'Failed to create session');
  state.sessionId = data.session_id || '';
  persistSession();
  return state.sessionId;
}

async function loadLocalDocs(options) {
  const keepActive = !options || options.keepActive !== false;
  if (!state.sessionId) {
    resetLocalDocs();
    return;
  }
  try {
    const data = await fetchJson('/sessions/' + encodeURIComponent(state.sessionId) + '/local-documents');
    state.localDocs = Array.isArray(data.documents) ? data.documents : [];
    sortLocalDocs();
    if (keepActive && state.activeLocalDocId && state.localDocs.some((doc) => doc.id === state.activeLocalDocId)) {
      renderLocalDocList();
      return;
    }
    if (state.localDocs.length) await loadLocalDoc(state.localDocs[0].id);
    else resetLocalDocs('No local document selected.');
  } catch (err) {
    syncLocalDocPanel(err.message || 'Failed to load local documents.');
  }
}

async function loadLocalDoc(docId) {
  if (!docId || !state.sessionId) return;
  try {
    const data = await fetchJson('/sessions/' + encodeURIComponent(state.sessionId) + '/local-documents/' + encodeURIComponent(docId));
    state.activeLocalDocId = docId;
    state.activeLocalDoc = data.document || null;
    if (state.activeLocalDoc) upsertLocalDocMeta(state.activeLocalDoc);
    syncLocalDocPanel();
  } catch (err) {
    syncLocalDocPanel(err.message || 'Failed to load local document.');
  }
}

async function loadArtifacts(options) {
  const keepActive = !options || options.keepActive !== false;
  if (!state.sessionId) {
    resetArtifacts();
    return;
  }
  try {
    const data = await fetchJson('/sessions/' + encodeURIComponent(state.sessionId) + '/artifacts');
    state.artifacts = Array.isArray(data.artifacts) ? data.artifacts : [];
    sortArtifacts();
    if (keepActive && state.activeArtifactId && state.artifacts.some((artifact) => artifact.id === state.activeArtifactId)) {
      renderArtifactList();
      return;
    }
    if (state.artifacts.length) await loadArtifact(state.artifacts[0].id);
    else resetArtifacts('No artifact selected.');
  } catch (err) {
    syncArtifactPanel(err.message || 'Failed to load artifacts.');
  }
}

async function loadArtifact(artifactId) {
  if (!artifactId || !state.sessionId) return;
  try {
    const data = await fetchJson('/sessions/' + encodeURIComponent(state.sessionId) + '/artifacts/' + encodeURIComponent(artifactId));
    state.activeArtifactId = artifactId;
    state.activeArtifact = data.artifact || null;
    if (state.activeArtifact) upsertArtifactMeta(state.activeArtifact);
    syncArtifactPanel();
  } catch (err) {
    syncArtifactPanel(err.message || 'Failed to load artifact.');
  }
}

async function handleSelectedLocalFiles(files) {
  if (!files.length) return;
  state.localDocUploading = true;
  let finalStatus = '';
  syncLocalDocPanel('Uploading local files...');
  try {
    await ensureSession();
    const formData = new FormData();
    files.forEach((file) => {
      formData.append('documents', file, file.name || 'upload');
    });
    const data = await fetchJson('/sessions/' + encodeURIComponent(state.sessionId) + '/local-documents', {
      method: 'POST',
      body: formData
    });
    const created = Array.isArray(data.documents) ? data.documents : [];
    const readyDocs = created.filter((doc) => doc && doc.status === 'ready');
    const failedDocs = created.filter((doc) => doc && doc.status === 'failed');
    const errors = Array.isArray(data.errors) ? data.errors : [];
    created.forEach(upsertLocalDocMeta);
    if (created.length) {
      const firstDoc = readyDocs[0] || failedDocs[0] || created[0];
      state.activeLocalDocId = firstDoc.id || '';
      await loadLocalDoc(state.activeLocalDocId);
      if (readyDocs.length && !failedDocs.length) {
        setStatus(readyDocs.length === 1 ? 'Local document uploaded' : (readyDocs.length + ' local documents uploaded'));
      } else if (!readyDocs.length && failedDocs.length) {
        setStatus(failedDocs.length === 1 ? 'Local document upload failed' : (failedDocs.length + ' local document uploads failed'));
      } else {
        setStatus(readyDocs.length + ' local documents uploaded, ' + failedDocs.length + ' failed');
      }
    } else {
      state.activeLocalDoc = null;
      state.activeLocalDocId = '';
    }
    if (errors.length) {
      finalStatus = created.length
        ? ('Some uploads failed: ' + errors.map((entry) => entry.name || entry.error).join(', '))
        : (errors[0].error || 'Failed to upload local documents');
      syncLocalDocPanel(finalStatus);
      setStatus(finalStatus);
    }
    await loadHistorySessions();
  } catch (err) {
    finalStatus = err.message || 'Failed to upload local documents.';
    syncLocalDocPanel(finalStatus);
    setStatus(finalStatus);
  } finally {
    state.localDocUploading = false;
    syncLocalDocPanel(finalStatus);
  }
}

async function createScratchPadFromContent(title, content) {
  if (!discardScratchChanges()) return;
  try {
    await ensureSession();
    const data = await fetchJson('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: title || 'Untitled note', content: content || '' })
    });
    state.activePad = data.pad || null;
    state.activePadId = state.activePad ? state.activePad.id : '';
    state.scratchDirty = false;
    if (state.activePad) upsertScratchMeta(state.activePad);
    syncScratchEditor(title ? 'Created note from local document.' : 'New note ready.');
    switchTab('notes-tab');
    scratchTitleEl.focus();
    scratchTitleEl.select();
  } catch (err) {
    scratchStatusEl.textContent = err.message || 'Failed to create.';
  }
}

async function loadScratchPads(options) {
  const keepActive = !options || options.keepActive !== false;
  if (!state.sessionId) {
    state.scratchPads = [];
    state.activePadId = '';
    state.activePad = null;
    state.scratchDirty = false;
    syncScratchEditor('No note selected.');
    return;
  }
  try {
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to load');
    state.scratchPads = Array.isArray(data.pads) ? data.pads : [];
    sortScratchPads();
    renderScratchList();
    if (keepActive && state.activePadId && state.scratchPads.some((pad) => pad.id === state.activePadId)) return;
    if (state.scratchPads.length) await loadScratchPad(state.scratchPads[0].id, true);
    else {
      state.activePadId = '';
      state.activePad = null;
      state.scratchDirty = false;
      syncScratchEditor('No note selected.');
    }
  } catch (err) {
    scratchStatusEl.textContent = err.message || 'Failed to load.';
  }
}

async function loadScratchPad(padId, bypassDirtyCheck) {
  if (!padId) return;
  if (!bypassDirtyCheck && !discardScratchChanges()) return;
  try {
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(padId));
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to load');
    state.activePadId = padId;
    state.activePad = data.pad || null;
    state.scratchDirty = false;
    if (state.activePad) upsertScratchMeta(state.activePad);
    syncScratchEditor();
  } catch (err) {
    scratchStatusEl.textContent = err.message || 'Failed to load.';
  }
}

async function createScratchPad() {
  await createScratchPadFromContent('Untitled note', '');
}

async function saveScratchPad() {
  if (!state.activePad || state.scratchSaving) return;
  state.scratchSaving = true;
  syncScratchEditor('Saving...');
  try {
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(state.activePad.id), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: scratchTitleEl.value,
        content: scratchEditorEl.value,
        expected_version: state.activePad.version
      })
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to save');
    state.activePad = data.pad || null;
    state.activePadId = state.activePad ? state.activePad.id : '';
    state.scratchDirty = false;
    if (state.activePad) upsertScratchMeta(state.activePad);
    syncScratchEditor('Saved.');
  } catch (err) {
    scratchStatusEl.textContent = err.message || 'Failed to save.';
  } finally {
    state.scratchSaving = false;
    syncScratchEditor();
  }
}

async function deleteScratchPad() {
  if (!state.activePad || state.scratchSaving) return;
  if (!window.confirm('Delete this note?')) return;
  try {
    const deletingId = state.activePad.id;
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(deletingId), { method: 'DELETE' });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to delete');
    state.scratchPads = state.scratchPads.filter((pad) => pad.id !== deletingId);
    state.activePad = null;
    state.activePadId = '';
    state.scratchDirty = false;
    if (state.scratchPads.length) await loadScratchPad(state.scratchPads[0].id, true);
    else syncScratchEditor('Deleted.');
  } catch (err) {
    scratchStatusEl.textContent = err.message || 'Failed to delete.';
  }
}

function insertScratchIntoComposer() {
  insertTextIntoComposer(scratchEditorEl.value, 'Note inserted into chat');
}

function trackScratchInput() {
  if (!state.activePad) return;
  state.activePad = Object.assign({}, state.activePad, {
    title: scratchTitleEl.value,
    content: scratchEditorEl.value
  });
  state.scratchDirty = true;
  syncScratchEditor('Unsaved changes.');
}

function localDocInsertText(doc) {
  if (!doc || !doc.text) return '';
  const selected = selectedPreviewText();
  const content = selected || doc.text;
  const label = selected ? 'Local Document Excerpt' : 'Local Document';
  return '--- ' + label + ': ' + localDocTitle(doc) + ' ---\n' + content + '\n';
}

function insertLocalDocIntoComposer() {
  if (!state.activeLocalDoc) return;
  insertTextIntoComposer(localDocInsertText(state.activeLocalDoc), 'Local document inserted into chat');
  rememberPendingLocalDoc(state.activeLocalDoc.id);
}

async function createScratchPadFromLocalDoc() {
  if (!state.activeLocalDoc || !state.sessionId) return;
  if (!discardScratchChanges()) return;
  try {
    const data = await fetchJson('/sessions/' + encodeURIComponent(state.sessionId)
      + '/local-documents/' + encodeURIComponent(state.activeLocalDoc.id)
      + '/scratch-pads', {
      method: 'POST'
    });
    state.activePad = data.pad || null;
    state.activePadId = state.activePad ? state.activePad.id : '';
    state.scratchDirty = false;
    if (state.activePad) upsertScratchMeta(state.activePad);
    syncScratchEditor('Created note from local document.');
    switchTab('notes-tab');
    scratchTitleEl.focus();
    scratchTitleEl.select();
    setStatus('Created note from local document');
  } catch (err) {
    scratchStatusEl.textContent = err.message || 'Failed to create.';
  }
}

async function deleteLocalDoc() {
  if (!state.activeLocalDoc || !state.sessionId || state.localDocUploading) return;
  if (!window.confirm('Delete this local document?')) return;
  const deletingId = state.activeLocalDoc.id;
  try {
    await fetchJson('/sessions/' + encodeURIComponent(state.sessionId) + '/local-documents/' + encodeURIComponent(deletingId), {
      method: 'DELETE'
    });
    state.localDocs = state.localDocs.filter((doc) => doc.id !== deletingId);
    state.activeLocalDoc = null;
    state.activeLocalDocId = '';
    if (state.localDocs.length) await loadLocalDoc(state.localDocs[0].id);
    else syncLocalDocPanel('Deleted.');
    setStatus('Local document deleted');
  } catch (err) {
    syncLocalDocPanel(err.message || 'Failed to delete local document.');
  }
}

function artifactInsertText(artifact) {
  if (!artifact || !artifact.text) return '';
  const selected = selectedArtifactPreviewText();
  const content = selected || artifact.text;
  const label = selected ? 'Artifact Excerpt' : 'Artifact';
  return '--- ' + label + ': ' + artifactTitle(artifact) + ' ---\n' + content + '\n';
}

function insertArtifactIntoComposer() {
  if (!state.activeArtifact) return;
  insertTextIntoComposer(artifactInsertText(state.activeArtifact), 'Artifact inserted into chat');
  rememberPendingArtifact(state.activeArtifact.id);
}

async function createScratchPadFromArtifact() {
  if (!state.activeArtifact || !state.sessionId) return;
  if (!discardScratchChanges()) return;
  try {
    const data = await fetchJson('/sessions/' + encodeURIComponent(state.sessionId)
      + '/artifacts/' + encodeURIComponent(state.activeArtifact.id)
      + '/scratch-pads', {
      method: 'POST'
    });
    state.activePad = data.pad || null;
    state.activePadId = state.activePad ? state.activePad.id : '';
    state.scratchDirty = false;
    if (state.activePad) upsertScratchMeta(state.activePad);
    syncScratchEditor('Created note from artifact.');
    switchTab('notes-tab');
    scratchTitleEl.focus();
    scratchTitleEl.select();
    setStatus('Created note from artifact');
  } catch (err) {
    scratchStatusEl.textContent = err.message || 'Failed to create.';
  }
}

function downloadFilenameFromHeaders(headers, fallback) {
  const disposition = headers.get('Content-Disposition') || headers.get('content-disposition') || '';
  const match = disposition.match(/filename=\"?([^\";]+)\"?/i);
  return (match && match[1]) ? match[1] : fallback;
}

async function downloadArtifact() {
  if (!state.activeArtifact || !state.sessionId) return;
  try {
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId)
      + '/artifacts/' + encodeURIComponent(state.activeArtifact.id)
      + '/download');
    if (!response.ok) {
      let message = 'Failed to download artifact';
      try {
        const data = await response.json();
        message = data.error || message;
      } catch (_err) {
        // Ignore non-JSON error bodies.
      }
      throw new Error(message);
    }
    const blob = await response.blob();
    const href = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = href;
    anchor.download = downloadFilenameFromHeaders(response.headers, state.activeArtifact.name || 'artifact');
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    URL.revokeObjectURL(href);
    setStatus('Artifact downloaded');
  } catch (err) {
    syncArtifactPanel(err.message || 'Failed to download artifact.');
  }
}

async function deleteArtifact() {
  if (!state.activeArtifact || !state.sessionId) return;
  if (!window.confirm('Delete this artifact?')) return;
  const deletingId = state.activeArtifact.id;
  try {
    await fetchJson('/sessions/' + encodeURIComponent(state.sessionId) + '/artifacts/' + encodeURIComponent(deletingId), {
      method: 'DELETE'
    });
    state.artifacts = state.artifacts.filter((artifact) => artifact.id !== deletingId);
    state.activeArtifact = null;
    state.activeArtifactId = '';
    if (state.artifacts.length) await loadArtifact(state.artifacts[0].id);
    else syncArtifactPanel('Deleted.');
    setStatus('Artifact deleted');
  } catch (err) {
    syncArtifactPanel(err.message || 'Failed to delete artifact.');
  }
}

let _pollFailures = 0;

async function pollApproval() {
  if (!state.sessionId) {
    state.pendingApproval = null;
    renderApproval();
    syncStatus();
    return;
  }
  try {
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId) + '/approval');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to load');
    state.pendingApproval = data.pending ? (data.approval || null) : null;
    _pollFailures = 0;
    renderApproval();
    syncStatus();
  } catch (err) {
    _pollFailures++;
    if (_pollFailures >= 5 && !state.sending) {
      setStatus('Connection lost — retrying...');
    }
  }
}

async function pollStatus() {
  if (!state.sessionId) {
    state.liveStatus = null;
    syncStatus();
    return;
  }
  try {
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId) + '/status');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to load');
    state.liveStatus = data.status || null;
    if (_pollFailures >= 5) {
      _pollFailures = 0;
      setStatus('Reconnected');
    }
    syncStatus();
  } catch (_err) {}
}

async function submitApproval(decision) {
  if (!state.sessionId || !state.pendingApproval || state.approvalSubmitting) return;
  state.approvalSubmitting = true;
  renderApproval();
  try {
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId) + '/approval', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        approval_id: state.pendingApproval.approval_id,
        decision: decision
      })
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to submit');
    state.pendingApproval = null;
    renderApproval();
    state.liveStatus = null;
    setStatus(decision === 'allow' ? 'Approved' : 'Denied');
  } catch (err) {
    setStatus(err.message || 'Failed to submit');
  } finally {
    state.approvalSubmitting = false;
    renderApproval();
  }
}

async function sendMessage(text, options) {
  const localDocIds = Array.isArray(options && options.localDocIds)
    ? options.localDocIds.filter(Boolean)
    : [];
  const localDocs = Array.isArray(options && options.localDocs)
    ? options.localDocs
    : [];
  const artifactIds = Array.isArray(options && options.artifactIds)
    ? options.artifactIds.filter(Boolean)
    : [];
  const artifacts = Array.isArray(options && options.artifacts)
    ? options.artifacts
    : [];
  state.sending = true;
  state.liveStatus = null;
  state.sendStartedAt = Date.now();
  updateComposerState();
  syncStatus();
  try {
    await ensureSession();
    const payload = { message: text };
    if (state.sessionId) payload.session_id = state.sessionId;
    if (localDocIds.length) payload.local_doc_ids = localDocIds;
    if (artifactIds.length) payload.artifact_ids = artifactIds;
    const responsePromise = safeFetch('/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    await pollStatus();
    const response = await responsePromise;
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Request failed');
    if (data.session_id) {
      state.sessionId = data.session_id;
      persistSession();
    }
    addMessage('assistant', data.content || '', { localDocs: localDocs, artifacts: artifacts });
    await loadHistorySessions();
    await loadArtifacts();
    state.liveStatus = null;
    setStatus('Ready');
  } catch (err) {
    addMessage('error', err.message || 'Request failed');
    state.liveStatus = null;
    setStatus('Failed');
  } finally {
    state.sending = false;
    state.sendStartedAt = 0;
    syncStatus();
    updateComposerState();
  }
}

async function loadSessionMessages() {
  if (!state.sessionId) {
    state.messages = [];
    renderMessages();
    return;
  }
  try {
    const response = await safeFetch('/sessions/' + encodeURIComponent(state.sessionId) + '/messages');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to load');
    state.messages = Array.isArray(data.messages)
      ? data.messages.map(normalizeMessage).filter(Boolean)
      : [];
    renderMessages();
  } catch (err) {
    state.messages = [];
    renderMessages();
    setStatus(err.message || 'Failed to load transcript');
  }
}

composerFormEl.addEventListener('submit', async (event) => {
  event.preventDefault();
  const text = composerEl.value.trim();
  if (!text || state.sending) return;
  const localDocIds = Array.from(new Set((state.pendingLocalDocIds || []).filter(Boolean)));
  const artifactIds = Array.from(new Set((state.pendingArtifactIds || []).filter(Boolean)));
  const localDocs = state.localDocs
    .filter((doc) => localDocIds.includes(doc.id))
    .map((doc) => ({ id: doc.id, name: doc.name, status: doc.status }));
  const artifacts = state.artifacts
    .filter((artifact) => artifactIds.includes(artifact.id))
    .map((artifact) => ({ id: artifact.id, name: artifact.name, title: artifact.title, status: artifact.status }));
  addMessage('user', text, { localDocs: localDocs, artifacts: artifacts });
  composerEl.value = '';
  state.pendingLocalDocIds = [];
  state.pendingArtifactIds = [];
  updateComposerState();
  await sendMessage(text, { localDocIds: localDocIds, localDocs: localDocs, artifactIds: artifactIds, artifacts: artifacts });
});

composerEl.addEventListener('keydown', (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
    event.preventDefault();
    composerFormEl.requestSubmit();
  }
});

composerEl.addEventListener('input', updateComposerState);

clearInputEl.addEventListener('click', () => {
  composerEl.value = '';
  state.pendingLocalDocIds = [];
  state.pendingArtifactIds = [];
  updateComposerState();
  composerEl.focus();
});

scratchTitleEl.addEventListener('input', trackScratchInput);
scratchEditorEl.addEventListener('input', trackScratchInput);

scratchTitleEl.addEventListener('keydown', (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {
    event.preventDefault();
    saveScratchPad();
  }
});

scratchEditorEl.addEventListener('keydown', (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {
    event.preventDefault();
    saveScratchPad();
  }
});

allowApprovalEl.addEventListener('click', () => submitApproval('allow'));
denyApprovalEl.addEventListener('click', () => submitApproval('deny'));

newScratchEl.addEventListener('click', () => createScratchPad());
saveScratchEl.addEventListener('click', () => saveScratchPad());
deleteScratchEl.addEventListener('click', () => deleteScratchPad());
insertScratchEl.addEventListener('click', () => insertScratchIntoComposer());
localDocInsertEl.addEventListener('click', () => insertLocalDocIntoComposer());
localDocScratchEl.addEventListener('click', () => createScratchPadFromLocalDoc());
localDocDeleteEl.addEventListener('click', () => deleteLocalDoc());
artifactInsertEl.addEventListener('click', () => insertArtifactIntoComposer());
artifactScratchEl.addEventListener('click', () => createScratchPadFromArtifact());
artifactDownloadEl.addEventListener('click', () => downloadArtifact());
artifactDeleteEl.addEventListener('click', () => deleteArtifact());
refreshHistorySessionsEl.addEventListener('click', () => loadHistorySessions());
refreshHistorySchedulesEl.addEventListener('click', () => loadHistorySchedules());
searchKnowledgeEl.addEventListener('click', () => searchKnowledgeNodes());
knowledgeQueryEl.addEventListener('input', () => updateAdminButtons());
knowledgeQueryEl.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    searchKnowledgeNodes();
  }
});

newProviderEl.addEventListener('click', () => {
  resetProviderForm('Create a model.');
  providerIdEl.focus();
});

saveProviderEl.addEventListener('click', () => saveProvider());
saveContextEl.addEventListener('click', () => saveConversationContext());
saveRetentionEl.addEventListener('click', () => saveMemoryRetention());
saveKnowledgeDecayEl.addEventListener('click', () => saveKnowledgeDecay());
saveLocalDocSummarizationEl.addEventListener('click', () => saveLocalDocSummarization());
localDocModelSummaryBackendEl.addEventListener('change', () => updateAdminButtons());
saveDatabaseBackupEl.addEventListener('click', () => saveDatabaseBackup());

newOauthAccountEl.addEventListener('click', () => {
  resetOauthAccountForm('Create a connection.');
  oauthAccountIdEl.focus();
});

oauthTemplateEl.addEventListener('change', () => renderOauthTemplateOptions());
applyOauthTemplateEl.addEventListener('click', () => applyOauthTemplate());
saveOauthAccountEl.addEventListener('click', () => saveOauthAccount());
oauthAccountCreateServiceEl.addEventListener('click', () => createServiceFromOauthAccount());
connectOauthAccountEl.addEventListener('click', () => connectOauthAccount());
refreshOauthAccountEl.addEventListener('click', () => refreshOauthAccount());
deleteOauthAccountEl.addEventListener('click', () => deleteOauthAccount());

newServiceEl.addEventListener('click', () => {
  resetServiceForm('Create a service.');
  serviceIdEl.focus();
});

saveServiceEl.addEventListener('click', () => saveService());

newSiteEl.addEventListener('click', () => {
  resetSiteForm('Create a site login.');
  siteIdEl.focus();
});

saveSiteEl.addEventListener('click', () => saveSite());
deleteSiteEl.addEventListener('click', () => deleteSite());
saveRemoteBridgeEl.addEventListener('click', () => saveRemoteBridge());
pairRemoteDeviceEl.addEventListener('click', () => pairRemoteDevice());
remotePairingTokenEl.addEventListener('input', () => updateAdminButtons());
remotePairingTokenEl.addEventListener('keydown', (event) => {
  if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
    event.preventDefault();
    pairRemoteDevice();
  }
});
openclawImportButtonEl.addEventListener('click', () => importOpenClawSkill());
openclawImportSourceEl.addEventListener('input', () => updateAdminButtons());
openclawImportSourceEl.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    importOpenClawSkill();
  }
});

newChatEl.addEventListener('click', () => {
  if (!discardScratchChanges()) return;
  state.sessionId = '';
  state.messages = [];
  state.pendingApproval = null;
  state.liveStatus = null;
  state.localDocs = [];
  state.activeLocalDocId = '';
  state.activeLocalDoc = null;
  state.localDocUploading = false;
  state.artifacts = [];
  state.activeArtifactId = '';
  state.activeArtifact = null;
  state.pendingLocalDocIds = [];
  state.pendingArtifactIds = [];
  state.scratchPads = [];
  state.activePadId = '';
  state.activePad = null;
  state.scratchDirty = false;
  persistSession();
  renderApproval();
  renderMessages();
  syncLocalDocPanel('No local document selected.');
  syncArtifactPanel('No artifact selected.');
  syncScratchEditor('No note selected.');
  loadHistorySessions();
  setStatus('Ready');
  composerEl.focus();
});

copyTranscriptEl.addEventListener('click', () => {
  const transcript = state.messages
    .map((message) => ((message.role === 'assistant' ? 'Xia' : message.role === 'user' ? 'You' : 'Status') + ':\n' + message.content))
    .join('\n\n');
  copyText(transcript, 'Transcript copied');
});

window.addEventListener('message', (event) => {
  if (!event || event.origin !== window.location.origin) return;
  const data = event.data || {};
  if (data.type === 'xia-oauth-complete') {
    loadAdminConfig();
    oauthAccountStatusEl.textContent = data.status === 'ok' ? 'Connected.' : 'Failed.';
    setStatus(data.status === 'ok' ? 'Connected' : 'Failed');
  }
});

persistSession();
renderApproval();
renderMessages();
syncLocalDocPanel('No local document selected.');
syncArtifactPanel('No artifact selected.');
syncScratchEditor('No note selected.');
renderKnowledgeNodeList();
renderKnowledgeFacts();
resetProviderForm('Loading...');
resetConversationContextForm('Loading...');
resetMemoryRetentionForm('Loading...');
renderLocalDocSummarizationSettings();
renderDatabaseBackupSettings();
resetOauthAccountForm('Loading...');
resetServiceForm('Loading...');
resetSiteForm('Loading...');
renderCapabilities();
updateAdminButtons();
updateComposerState();
composerEl.focus();
Promise.all([
  loadSessionMessages(),
  loadLocalDocs(),
  loadArtifacts(),
  loadScratchPads(),
  loadAdminConfig(),
  loadHistorySessions(),
  loadHistorySchedules()
]).catch(() => {
  setStatus('Some data failed to load — check your connection');
});
let _pollIntervalId = window.setInterval(() => {
  if (state.sessionId) {
    pollApproval();
    if (state.sending) pollStatus();
  }
}, 1000);

document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    if (_pollIntervalId) {
      window.clearInterval(_pollIntervalId);
      _pollIntervalId = null;
    }
  } else {
    if (!_pollIntervalId) {
      _pollIntervalId = window.setInterval(() => {
        if (state.sessionId) {
          pollApproval();
          if (state.sending) pollStatus();
        }
      }, 1000);
    }
    if (state.sessionId) {
      pollApproval();
      if (state.sending) pollStatus();
    }
  }
});
