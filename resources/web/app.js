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
    identity: {},
    instance: {},
    capabilities: {},
    storage: {},
    providers: [],
    llmProviderTemplates: [],
    conversationContext: null,
    memoryRetention: null,
    knowledgeDecay: null,
    localDocSummarization: null,
    localDocOcr: null,
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
  setupRequired: false,
  history: {
    sessions: [],
    schedules: [],
    activeSessionId: '',
    activeScheduleId: '',
    sessionMessages: [],
    scheduleRuns: []
  },
  llmCalls: [],
  activeLlmCallId: '',
  activeLlmCall: null,
  llmCallsLoading: false,
  knowledgeQuery: '',
  knowledgeNodes: [],
  activeKnowledgeNodeId: '',
  knowledgeFacts: [],
  knowledgeSearching: false,
  knowledgeLoadingFacts: false,
  knowledgeForgettingFactId: '',
  providerModels: [],
  providerModelsFetching: false,
  providerDraft: null,
  pendingProviderOauthFlow: null,
  pendingProviderBrowserSessionFlow: null,
  activeProviderId: '',
  activeOauthAccountId: '',
  activeServiceId: '',
  activeSiteId: '',
  providerSaving: false,
  providerAccountConnecting: false,
  contextSaving: false,
  retentionSaving: false,
  knowledgeDecaySaving: false,
  localDocSummarizationSaving: false,
  localDocOcrSaving: false,
  databaseBackupSaving: false,
  oauthSaving: false,
  serviceSaving: false,
  siteSaving: false,
  remoteBridgeSaving: false,
  remotePairing: false,
  localDocSummarizationStatus: 'Loading local document summarization settings...',
  localDocOcrStatus: 'Loading local OCR settings...',
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
const activityStatusEl = document.getElementById('activity-status');
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
const llmCallListEl = document.getElementById('llm-call-list');
const llmCallDetailEl = document.getElementById('llm-call-detail');
const llmCallStatusEl = document.getElementById('llm-call-status');
const refreshLlmCallsEl = document.getElementById('refresh-llm-calls');
const knowledgeQueryEl = document.getElementById('knowledge-query');
const searchKnowledgeEl = document.getElementById('search-knowledge');
const knowledgeNodeListEl = document.getElementById('knowledge-node-list');
const knowledgeFactListEl = document.getElementById('knowledge-fact-list');
const knowledgeStatusEl = document.getElementById('knowledge-status');
const heroNameEl = document.getElementById('hero-name');
const heroRoleEl = document.getElementById('hero-role');
const identityNameEl = document.getElementById('identity-name');
const identityRoleEl = document.getElementById('identity-role');
const identityDescriptionEl = document.getElementById('identity-description');
const identityControllerEnabledEl = document.getElementById('identity-controller-enabled');
const identityControllerNoteEl = document.getElementById('identity-controller-note');
const identityInstanceIdEl = document.getElementById('identity-instance-id');
const identityDbPathEl = document.getElementById('identity-db-path');
const identitySupportDirEl = document.getElementById('identity-support-dir');
const identityPersonalityEl = document.getElementById('identity-personality');
const identityGuidelinesEl = document.getElementById('identity-guidelines');
const identityStatusEl = document.getElementById('identity-status');
const saveIdentityEl = document.getElementById('save-identity');
const providerOnboardingEl = document.getElementById('provider-onboarding');
const providerTemplateListEl = document.getElementById('provider-template-list');
const providerOnboardingStatusEl = document.getElementById('provider-onboarding-status');
const providerCardEl = document.getElementById('provider-card');
const providerListEl = document.getElementById('provider-list');
const providerIdEl = document.getElementById('provider-id');
const providerNameEl = document.getElementById('provider-name');
const providerTemplateEl = document.getElementById('provider-template');
const providerTemplateNoteEl = document.getElementById('provider-template-note');
const providerBaseUrlEl = document.getElementById('provider-base-url');
const providerModelEl = document.getElementById('provider-model');
const providerModelListEl = document.getElementById('provider-model-list');
const fetchProviderModelsEl = document.getElementById('fetch-provider-models');
const providerAccessModeEl = document.getElementById('provider-access-mode');
const providerCredentialSourceEl = document.getElementById('provider-credential-source');
const providerOauthAccountEl = document.getElementById('provider-oauth-account');
const providerOauthAccountNoteEl = document.getElementById('provider-oauth-account-note');
const providerBrowserSessionEl = document.getElementById('provider-browser-session');
const providerBrowserSessionNoteEl = document.getElementById('provider-browser-session-note');
const providerConfigureOauthEl = document.getElementById('provider-configure-oauth');
const providerOpenAccountEl = document.getElementById('provider-open-account');
const providerOpenDocsEl = document.getElementById('provider-open-docs');
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
const deleteProviderEl = document.getElementById('delete-provider');
const retentionFullResolutionDaysEl = document.getElementById('retention-full-resolution-days');
const retentionDecayHalfLifeDaysEl = document.getElementById('retention-decay-half-life-days');
const retentionRetainedCountEl = document.getElementById('retention-retained-count');
const retentionStatusEl = document.getElementById('retention-status');
const saveRetentionEl = document.getElementById('save-retention');
const contextRecentHistoryMessageLimitEl = document.getElementById('context-recent-history-message-limit');
const contextStatusEl = document.getElementById('context-status');
const saveContextEl = document.getElementById('save-context');
const searchBackendEl = document.getElementById('search-backend');
const searchBraveApiKeyEl = document.getElementById('search-brave-api-key');
const searchSearxngUrlEl = document.getElementById('search-searxng-url');
const searchStatusEl = document.getElementById('search-status');
const saveSearchEl = document.getElementById('save-search');
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
const localDocOcrEnabledEl = document.getElementById('local-doc-ocr-enabled');
const localDocOcrModelBackendEl = document.getElementById('local-doc-ocr-model-backend');
const localDocOcrExternalProviderIdEl = document.getElementById('local-doc-ocr-external-provider-id');
const localDocOcrCommandEl = document.getElementById('local-doc-ocr-command');
const localDocOcrTimeoutMsEl = document.getElementById('local-doc-ocr-timeout-ms');
const localDocOcrModelPathEl = document.getElementById('local-doc-ocr-model-path');
const localDocOcrMmprojPathEl = document.getElementById('local-doc-ocr-mmproj-path');
const localDocOcrSpottingMmprojPathEl = document.getElementById('local-doc-ocr-spotting-mmproj-path');
const localDocOcrMaxTokensEl = document.getElementById('local-doc-ocr-max-tokens');
const localDocOcrStatusEl = document.getElementById('local-doc-ocr-status');
const saveLocalDocOcrEl = document.getElementById('save-local-doc-ocr');
const databaseBackupEnabledEl = document.getElementById('database-backup-enabled');
const databaseBackupDirectoryEl = document.getElementById('database-backup-directory');
const databaseBackupIntervalHoursEl = document.getElementById('database-backup-interval-hours');
const databaseBackupRetainCountEl = document.getElementById('database-backup-retain-count');
const databaseBackupLastSuccessEl = document.getElementById('database-backup-last-success');
const databaseBackupLastArchiveEl = document.getElementById('database-backup-last-archive');
const databaseBackupStatusEl = document.getElementById('database-backup-status');
const saveDatabaseBackupEl = document.getElementById('save-database-backup');
const oauthAccountCardEl = document.getElementById('oauth-account-card');
const oauthAccountListEl = document.getElementById('oauth-account-list');
const oauthTemplateEl = document.getElementById('oauth-template');
const oauthTemplateNoteEl = document.getElementById('oauth-template-note');
const applyOauthTemplateEl = document.getElementById('apply-oauth-template');
const oauthAccountIdEl = document.getElementById('oauth-account-id');
const oauthAccountNameEl = document.getElementById('oauth-account-name');
const oauthConnectionModeEl = document.getElementById('oauth-connection-mode');
const oauthAccessTokenFieldEl = document.getElementById('oauth-access-token-field');
const oauthAccessTokenEl = document.getElementById('oauth-access-token');
const oauthTokenTypeFieldEl = document.getElementById('oauth-token-type-field');
const oauthTokenTypeEl = document.getElementById('oauth-token-type');
const oauthExpiresAtFieldEl = document.getElementById('oauth-expires-at-field');
const oauthExpiresAtEl = document.getElementById('oauth-expires-at');
const oauthAuthorizeUrlFieldEl = document.getElementById('oauth-authorize-url-field');
const oauthAuthorizeUrlEl = document.getElementById('oauth-authorize-url');
const oauthTokenUrlFieldEl = document.getElementById('oauth-token-url-field');
const oauthTokenUrlEl = document.getElementById('oauth-token-url');
const oauthClientIdFieldEl = document.getElementById('oauth-client-id-field');
const oauthClientIdEl = document.getElementById('oauth-client-id');
const oauthClientSecretFieldEl = document.getElementById('oauth-client-secret-field');
const oauthClientSecretEl = document.getElementById('oauth-client-secret');
const oauthScopesFieldEl = document.getElementById('oauth-scopes-field');
const oauthScopesEl = document.getElementById('oauth-scopes');
const oauthRedirectUriFieldEl = document.getElementById('oauth-redirect-uri-field');
const oauthRedirectUriEl = document.getElementById('oauth-redirect-uri');
const oauthAuthParamsFieldEl = document.getElementById('oauth-auth-params-field');
const oauthAuthParamsEl = document.getElementById('oauth-auth-params');
const oauthTokenParamsFieldEl = document.getElementById('oauth-token-params-field');
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
const serviceAllowPrivateNetworkEl = document.getElementById('service-allow-private-network');
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

function currentActivityText() {
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
      case 'complete':      return '';
      case 'error':         return s.message || 'Error occurred';
      default:              return s.message || 'Processing...';
    }
  }
  if (state.sending) {
    return 'Waiting for response...';
  }
  return '';
}

function currentPillText() {
  if (state.pendingApproval) return 'Approval needed';
  if (state.sending) return 'Working...';
  return state.baseStatus || 'Ready';
}

function syncStatus() {
  statusEl.textContent = currentPillText();
  activityStatusEl.textContent = currentActivityText();
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

function providerTemplateById(templateId) {
  return (state.admin.llmProviderTemplates || []).find((template) => template.id === templateId) || null;
}

function providerAccessModeLabel(accessMode) {
  if (accessMode === 'local') return 'Local model';
  if (accessMode === 'account') return 'Web account session';
  if (accessMode === 'api') return 'API credential';
  return 'Not set';
}

function providerCredentialSourceLabel(credentialSource) {
  if (credentialSource === 'none') return 'No credential';
  if (credentialSource === 'api-key') return 'API key';
  if (credentialSource === 'oauth-account') return 'API sign-in';
  if (credentialSource === 'browser-session') return 'Web account session';
  return 'Not set';
}

function providerTemplateAccessModes(template) {
  const accessModes = Array.isArray(template && template.access_modes)
    ? template.access_modes.filter(Boolean)
    : [];
  if (accessModes.length) return accessModes;
  const authTypes = Array.isArray(template && template.auth_types) ? template.auth_types.filter(Boolean) : [];
  const inferred = [];
  if (authTypes.includes('oauth-account')) {
    inferred.push({
      id: 'api',
      label: 'API credential',
      credential_sources: ['oauth-account'],
      default: true
    });
  }
  if (authTypes.includes('browser-session')) {
    inferred.push({
      id: 'account',
      label: 'Web account session',
      credential_sources: ['browser-session'],
      default: !inferred.length
    });
  }
  if (authTypes.includes('api-key')) {
    inferred.push({
      id: 'api',
      label: 'API credential',
      credential_sources: ['api-key'],
      default: !inferred.length
    });
  }
  if (authTypes.includes('none')) {
    inferred.push({
      id: 'local',
      label: 'Local model',
      credential_sources: ['none'],
      default: !inferred.length
    });
  }
  return inferred.length
    ? inferred
    : [{
        id: 'api',
        label: 'API credential',
        credential_sources: ['api-key'],
        default: true
      }];
}

function providerTemplateAccessMode(template, accessMode) {
  return providerTemplateAccessModes(template).find((mode) => mode.id === accessMode) || null;
}

function providerCredentialSourcesForAccessMode(template, accessMode) {
  const mode = providerTemplateAccessMode(template, accessMode);
  const credentialSources = Array.isArray(mode && mode.credential_sources)
    ? mode.credential_sources.filter(Boolean)
    : [];
  return credentialSources.length ? credentialSources : ['api-key'];
}

function defaultProviderTemplateId() {
  const templates = Array.isArray(state.admin.llmProviderTemplates) ? state.admin.llmProviderTemplates : [];
  if (!templates.length) return '';
  return (templates.find((template) => defaultProviderAccessMode(template) === 'account')
    || templates.find((template) => template.id === 'openai')
    || templates.find((template) => template.id === 'ollama')
    || templates[0]).id || '';
}

function defaultProviderAccessMode(template) {
  const accessModes = providerTemplateAccessModes(template);
  const explicit = accessModes.find((mode) => !!mode.default);
  if (explicit && explicit.id) return explicit.id;
  return (accessModes.find((mode) => mode.id === 'account')
    || accessModes.find((mode) => mode.id === 'api')
    || accessModes[0]
    || {}).id || 'api';
}

function defaultProviderCredentialSource(template, accessMode) {
  const credentialSources = providerCredentialSourcesForAccessMode(template, accessMode);
  if (credentialSources.includes('browser-session')) return 'browser-session';
  if (credentialSources.includes('oauth-account')) return 'oauth-account';
  if (credentialSources.includes('api-key')) return 'api-key';
  if (credentialSources.includes('none')) return 'none';
  return credentialSources[0] || 'api-key';
}

function providerCredentialSource(provider) {
  if (provider && provider.credential_source) return provider.credential_source;
  if (provider && provider.auth_type) return provider.auth_type;
  if (provider && provider.browser_session) return 'browser-session';
  if (provider && provider.oauth_account) return 'oauth-account';
  if (provider && provider.api_key_configured) return 'api-key';
  return 'none';
}

function providerAccessMode(provider) {
  if (provider && provider.access_mode) return provider.access_mode;
  const credentialSource = providerCredentialSource(provider);
  if (credentialSource === 'browser-session') return 'account';
  if (credentialSource === 'oauth-account') return 'api';
  if (credentialSource === 'api-key') return 'api';
  if (provider && provider.template === 'ollama') return 'local';
  return 'api';
}

function providerMeta(provider) {
  const bits = [];
  const template = providerTemplateById(provider.template);
  const accessMode = providerAccessMode(provider);
  const credentialSource = providerCredentialSource(provider);
  if (template) bits.push(template.name);
  if (provider.model) bits.push(provider.model);
  if (Array.isArray(provider.workloads) && provider.workloads.length) {
    bits.push('Workloads: ' + provider.workloads.join(', '));
  }
  if (accessMode) bits.push(providerAccessModeLabel(accessMode));
  if (credentialSource) bits.push(providerCredentialSourceLabel(credentialSource));
  if (provider.browser_session_connected) {
    bits.push('Web account connected');
  } else if (credentialSource === 'browser-session') {
    bits.push(provider.browser_session ? 'Web account needs reconnect' : 'Web account not connected');
  }
  if (provider.oauth_account_name) {
    bits.push(provider.oauth_account_connected
      ? ('API sign-in: ' + provider.oauth_account_name)
      : ('API sign-in pending: ' + provider.oauth_account_name));
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
  if (credentialSource === 'api-key' || ((credentialSource !== 'oauth-account' && credentialSource !== 'browser-session') && provider.api_key_configured)) {
    bits.push(provider.api_key_configured ? 'API key stored' : 'No API key');
  }
  return bits.join(' • ');
}

function llmProviderTemplateMeta(template) {
  const bits = [];
  if (template.category) bits.push(template.category);
  if (template.base_url) bits.push(template.base_url);
  if (template.model_suggestion) bits.push('Suggested model: ' + template.model_suggestion);
  const defaultMode = defaultProviderAccessMode(template);
  if (defaultMode) bits.push('Default: ' + providerAccessModeLabel(defaultMode));
  return bits.join(' • ');
}

function providerSignInOptionLabel(option) {
  switch (option) {
    case 'google': return 'Google login';
    case 'github': return 'GitHub login';
    case 'apple': return 'Apple login';
    case 'microsoft': return 'Microsoft login';
    default: return option;
  }
}

function providerPrimaryAction(template, accessMode, credentialSource) {
  if (!template) return null;
  if (credentialSource === 'browser-session' && template.account_connector) {
    const pending = state.pendingProviderBrowserSessionFlow
      && state.pendingProviderBrowserSessionFlow.connectorId === template.account_connector;
    return {
      kind: 'browser-session',
      label: pending ? 'Finish Sign-In' : 'Start Sign-In',
      connectorId: template.account_connector
    };
  }
  if (credentialSource === 'oauth-account' || (accessMode === 'account' && !template.account_connector)) {
    return {
      kind: 'oauth',
      label: 'Set Up API Sign-In'
    };
  }
  if (credentialSource === 'api-key') {
    if (template.api_key_url) {
      return {
        kind: 'external',
        label: 'Open API Keys',
        url: template.api_key_url
      };
    }
    if (template.account_url) {
      return {
        kind: 'external',
        label: 'Open Provider Site',
        url: template.account_url
      };
    }
    if (template.docs_url) {
      return {
        kind: 'external',
        label: 'Open Setup Guide',
        url: template.docs_url
      };
    }
    return null;
  }
  if (credentialSource === 'none' || accessMode === 'local') {
    if (template.install_url) {
      return {
        kind: 'external',
        label: 'Open Local Setup',
        url: template.install_url
      };
    }
    if (template.docs_url) {
      return {
        kind: 'external',
        label: 'Open Setup Guide',
        url: template.docs_url
      };
    }
  }
  return null;
}

function providerActionByKind(template, kind) {
  if (!template) return null;
  switch (kind) {
    case 'account':
      return template.account_url
        ? { kind: 'external', label: 'Open Account', url: template.account_url }
        : null;
    case 'docs':
      return template.docs_url
        ? { kind: 'external', label: 'Open Docs', url: template.docs_url }
        : null;
    case 'install':
      return template.install_url
        ? { kind: 'external', label: 'Open Local Setup', url: template.install_url }
        : null;
    default:
      return null;
  }
}

function providerVisibleActions(template, accessMode, credentialSource) {
  const actions = [];
  const seen = new Set();
  const pushAction = (action) => {
    if (!action) return;
    const key = action.kind === 'external'
      ? ['external', action.url || ''].join('|')
      : [action.kind || '', action.label || ''].join('|');
    if (seen.has(key)) return;
    seen.add(key);
    actions.push(action);
  };

  pushAction(providerPrimaryAction(template, accessMode, credentialSource));
  if (credentialSource === 'none' || accessMode === 'local') {
    pushAction(providerActionByKind(template, 'docs'));
  } else {
    pushAction(providerActionByKind(template, 'account'));
    pushAction(providerActionByKind(template, 'docs'));
  }
  return actions;
}

function currentProviderPrimaryAction() {
  return providerPrimaryAction(providerTemplateById(providerTemplateEl.value),
    providerAccessModeEl.value,
    providerCredentialSourceEl.value);
}

async function openProviderPrimaryAction(action, options = {}) {
  if (!action) return;
  if (action.kind === 'oauth') {
    beginProviderOauthSetup();
    return;
  }
  if (action.kind === 'browser-session' && action.connectorId) {
    await beginProviderBrowserSessionSetup(action.connectorId, options);
    return;
  }
  if (action.kind === 'external' && action.url) {
    const popup = window.open(action.url, '_blank', 'noopener,noreferrer');
    if (popup && typeof popup.focus === 'function') {
      popup.focus();
    }
    if (options.statusEl) {
      options.statusEl.textContent = options.statusText || ('Opened ' + action.url);
    }
    if (options.globalStatus) {
      setStatus(options.globalStatus);
    }
  }
}

function providerTemplateNote(template) {
  if (!template) {
    return 'Choose a provider template to prefill common settings.';
  }
  const bits = [];
  if (template.description) bits.push(template.description);
  if (template.notes) bits.push(template.notes);
  return bits.join(' — ');
}

function renderProviderTemplateOptions() {
  const selected = providerTemplateEl.value || defaultProviderTemplateId();
  providerTemplateEl.replaceChildren();
  const blank = document.createElement('option');
  blank.value = '';
  blank.textContent = 'Choose a template';
  providerTemplateEl.appendChild(blank);
  (state.admin.llmProviderTemplates || []).forEach((template) => {
    const option = document.createElement('option');
    option.value = template.id || '';
    option.textContent = template.name || template.id || 'Template';
    providerTemplateEl.appendChild(option);
  });
  providerTemplateEl.value = selected || '';
  providerTemplateNoteEl.textContent = providerTemplateNote(providerTemplateById(providerTemplateEl.value));
}

function renderProviderAccessModeOptions() {
  const template = providerTemplateById(providerTemplateEl.value);
  const accessModes = providerTemplateAccessModes(template);
  const previous = providerAccessModeEl.value;
  providerAccessModeEl.replaceChildren();
  accessModes.forEach((accessMode) => {
    const option = document.createElement('option');
    option.value = accessMode.id;
    option.textContent = firstNonEmpty(accessMode.label, providerAccessModeLabel(accessMode.id));
    providerAccessModeEl.appendChild(option);
  });
  providerAccessModeEl.value = accessModes.some((mode) => mode.id === previous)
    ? previous
    : defaultProviderAccessMode(template);
}

function renderProviderCredentialSourceOptions() {
  const template = providerTemplateById(providerTemplateEl.value);
  const accessMode = providerAccessModeEl.value || defaultProviderAccessMode(template);
  const credentialSources = template
    ? providerCredentialSourcesForAccessMode(template, accessMode)
    : ['none', 'api-key', 'oauth-account', 'browser-session'];
  const previous = providerCredentialSourceEl.value;
  providerCredentialSourceEl.replaceChildren();
  credentialSources.forEach((credentialSource) => {
    const option = document.createElement('option');
    option.value = credentialSource;
    option.textContent = providerCredentialSourceLabel(credentialSource);
    providerCredentialSourceEl.appendChild(option);
  });
  providerCredentialSourceEl.value = credentialSources.includes(previous)
    ? previous
    : defaultProviderCredentialSource(template, accessMode);
}

function renderProviderOauthAccountOptions() {
  const previous = providerOauthAccountEl.value;
  providerOauthAccountEl.replaceChildren();
  const blank = document.createElement('option');
  blank.value = '';
  blank.textContent = 'Choose an API sign-in';
  providerOauthAccountEl.appendChild(blank);
  (state.admin.oauthAccounts || []).forEach((account) => {
    const option = document.createElement('option');
    option.value = account.id || '';
    option.textContent = firstNonEmpty(account.name, account.id)
      + (account.connected ? ' (connected)' : ' (not connected)');
    providerOauthAccountEl.appendChild(option);
  });
  providerOauthAccountEl.value = previous || '';
}

function providerOauthAccountStatusNote() {
  if (providerCredentialSourceEl.value !== 'oauth-account') {
    return '';
  }
  if (!(state.admin.oauthAccounts || []).length) {
    return 'Use Set Up API Sign-In to create a saved OAuth API sign-in, then link it here.';
  }
  if (!providerOauthAccountEl.value) {
    return 'Choose a connected API sign-in for this provider, or use Set Up API Sign-In to make one.';
  }
  const account = (state.admin.oauthAccounts || []).find((entry) => entry.id === providerOauthAccountEl.value);
  if (!account) {
    return 'Choose a connected API sign-in for this provider, or use Set Up API Sign-In to make one.';
  }
  return account.connected
    ? 'This provider will use the linked OAuth API credential.'
    : 'The linked OAuth API sign-in still needs to be connected.';
}

function providerBrowserSessionStatusNote() {
  if (providerCredentialSourceEl.value !== 'browser-session') {
    return '';
  }
  if (providerBrowserSessionEl.value) {
    return state.pendingProviderBrowserSessionFlow
      ? 'Finish sign-in after the provider page has loaded in the Xia-managed browser.'
      : 'A saved web account session is ready for this provider.';
  }
  return 'Use Start Sign-In to open a Xia-managed browser, complete the provider login there, then finish the connection.';
}

function syncProviderAuthInputs() {
  const accessMode = providerAccessModeEl.value;
  const credentialSource = providerCredentialSourceEl.value;
  const providerSaving = state.providerSaving;
  const template = providerTemplateById(providerTemplateEl.value);
  const actions = providerVisibleActions(template, accessMode, credentialSource);
  const primaryAction = actions[0] || null;
  const accountAction = actions.find((action) => action.label === 'Open Account') || null;
  const docsAction = actions.find((action) => action.label === 'Open Docs') || null;
  providerTemplateEl.disabled = providerSaving;
  providerAccessModeEl.disabled = providerSaving;
  providerCredentialSourceEl.disabled = providerSaving;
  providerOauthAccountEl.hidden = credentialSource !== 'oauth-account';
  providerOauthAccountEl.disabled = providerSaving || credentialSource !== 'oauth-account';
  providerApiKeyEl.disabled = providerSaving || credentialSource !== 'api-key';
  providerBrowserSessionNoteEl.hidden = credentialSource !== 'browser-session';
  providerConfigureOauthEl.hidden = !primaryAction;
  providerConfigureOauthEl.textContent = primaryAction ? primaryAction.label : 'Open Setup';
  providerConfigureOauthEl.disabled = providerSaving || state.oauthSaving || state.providerAccountConnecting || !primaryAction;
  providerOpenAccountEl.hidden = !accountAction;
  providerOpenAccountEl.disabled = providerSaving || state.oauthSaving || state.providerAccountConnecting || !accountAction;
  providerOpenDocsEl.hidden = !docsAction;
  providerOpenDocsEl.disabled = providerSaving || state.oauthSaving || state.providerAccountConnecting || !docsAction;
  providerOauthAccountNoteEl.textContent = providerOauthAccountStatusNote();
  providerBrowserSessionNoteEl.textContent = providerBrowserSessionStatusNote();
}

// ---------------------------------------------------------------------------
// Provider model autocomplete
// ---------------------------------------------------------------------------

function renderProviderModelList() {
  providerModelListEl.innerHTML = '';
  const query = providerModelEl.value.trim().toLowerCase();
  const models = state.providerModels;
  if (!models.length) {
    providerModelListEl.hidden = true;
    return;
  }
  const filtered = query
    ? models.filter(function (m) { return m.toLowerCase().includes(query); })
    : models;
  if (!filtered.length) {
    providerModelListEl.hidden = true;
    return;
  }
  var shown = filtered.slice(0, 80);
  shown.forEach(function (modelId) {
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'autocomplete-item';
    if (query) {
      var idx = modelId.toLowerCase().indexOf(query);
      if (idx >= 0) {
        btn.innerHTML = escapeHtml(modelId.substring(0, idx))
          + '<mark>' + escapeHtml(modelId.substring(idx, idx + query.length)) + '</mark>'
          + escapeHtml(modelId.substring(idx + query.length));
      } else {
        btn.textContent = modelId;
      }
    } else {
      btn.textContent = modelId;
    }
    btn.addEventListener('mousedown', function (e) {
      e.preventDefault();
      providerModelEl.value = modelId;
      providerModelListEl.hidden = true;
    });
    providerModelListEl.appendChild(btn);
  });
  if (filtered.length > 80) {
    var more = document.createElement('div');
    more.className = 'autocomplete-item';
    more.style.color = 'var(--muted)';
    more.style.fontStyle = 'italic';
    more.textContent = (filtered.length - 80) + ' more — keep typing to narrow';
    providerModelListEl.appendChild(more);
  }
  providerModelListEl.hidden = false;
}

function escapeHtml(text) {
  var d = document.createElement('div');
  d.appendChild(document.createTextNode(text));
  return d.innerHTML;
}

async function fetchProviderModels() {
  var baseUrl = providerBaseUrlEl.value.trim();
  var apiKey = providerApiKeyEl.value.trim();
  if (!baseUrl) {
    providerStatusEl.textContent = 'Enter a Base URL first.';
    return;
  }
  state.providerModelsFetching = true;
  fetchProviderModelsEl.disabled = true;
  providerStatusEl.textContent = 'Fetching models...';
  try {
    var data = await fetchJson('/admin/provider-models', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ base_url: baseUrl, api_key: apiKey || undefined })
    });
    state.providerModels = Array.isArray(data.models) ? data.models : [];
    providerStatusEl.textContent = state.providerModels.length
      ? state.providerModels.length + ' models available — type to filter.'
      : 'No models returned by this provider.';
    providerModelEl.focus();
    renderProviderModelList();
  } catch (err) {
    state.providerModels = [];
    providerStatusEl.textContent = err.message || 'Failed to fetch models.';
  } finally {
    state.providerModelsFetching = false;
    fetchProviderModelsEl.disabled = false;
  }
}

function applyProviderTemplate(templateId, options = {}) {
  const template = providerTemplateById(templateId);
  if (!template) return;
  const preserveId = Object.prototype.hasOwnProperty.call(options, 'preserveId')
    ? options.preserveId
    : !!state.activeProviderId;
  providerTemplateEl.value = template.id || '';
  if (!options.preserveName || !providerNameEl.value.trim()) {
    providerNameEl.value = template.name || '';
  }
  if (!options.preserveBaseUrl || !providerBaseUrlEl.value.trim()) {
    providerBaseUrlEl.value = template.base_url || '';
  }
  if (!options.preserveModel || !providerModelEl.value.trim()) {
    providerModelEl.value = template.model_suggestion || '';
  }
  if (!preserveId || !providerIdEl.value.trim()) {
    providerIdEl.value = template.id || '';
  }
  renderProviderTemplateOptions();
  renderProviderAccessModeOptions();
  if (!options.preserveAccessMode) {
    providerAccessModeEl.value = defaultProviderAccessMode(template);
  }
  renderProviderCredentialSourceOptions();
  if (!options.preserveCredentialSource) {
    providerCredentialSourceEl.value = defaultProviderCredentialSource(template, providerAccessModeEl.value);
    providerOauthAccountEl.value = '';
    providerBrowserSessionEl.value = '';
  }
  renderProviderOauthAccountOptions();
  providerStatusEl.textContent = options.statusText || ('Prefilled from ' + firstNonEmpty(template.name, template.id) + '.');
  providerOnboardingStatusEl.textContent = 'Selected ' + firstNonEmpty(template.name, template.id) + '. Review the model details below.';
  syncProviderAuthInputs();
  updateAdminButtons();
  if (options.focusTarget === 'api-key') {
    providerApiKeyEl.focus();
  } else if (options.focusTarget === 'oauth-account') {
    providerOauthAccountEl.focus();
  } else if (providerCredentialSourceEl.value === 'oauth-account') {
    providerOauthAccountEl.focus();
  } else if (providerModelEl.value) {
    providerModelEl.focus();
  } else {
    providerNameEl.focus();
  }
}

function buildProviderTemplateCard(template) {
  const card = document.createElement('article');
  card.className = 'template-card';

  const head = document.createElement('div');
  head.className = 'template-card-head';
  const title = document.createElement('div');
  title.className = 'template-card-title';
  title.textContent = firstNonEmpty(template.name, template.id);
  const meta = document.createElement('div');
  meta.className = 'template-card-meta';
  meta.textContent = [llmProviderTemplateMeta(template), template.description].filter(Boolean).join(' • ');
  head.appendChild(title);
  head.appendChild(meta);
  card.appendChild(head);

  const tags = document.createElement('div');
  tags.className = 'template-card-tags';
  providerTemplateAccessModes(template).forEach((accessMode) => {
    const badge = document.createElement('span');
    badge.className = 'template-badge';
    badge.textContent = firstNonEmpty(accessMode.label, providerAccessModeLabel(accessMode.id));
    tags.appendChild(badge);
  });
  (template.sign_in_options || []).forEach((option) => {
    const badge = document.createElement('span');
    badge.className = 'template-badge';
    badge.textContent = providerSignInOptionLabel(option);
    tags.appendChild(badge);
  });
  if (tags.childNodes.length) {
    card.appendChild(tags);
  }

  const note = document.createElement('div');
  note.className = 'template-card-meta';
  note.textContent = template.notes || 'Use this template to prefill the model settings.';
  card.appendChild(note);

  const actions = document.createElement('div');
  actions.className = 'actions';
  const useButton = document.createElement('button');
  useButton.type = 'button';
  useButton.className = 'secondary';
  useButton.textContent = 'Use Template';
  useButton.disabled = state.providerSaving;
  useButton.addEventListener('click', () => {
    switchTab('settings-tab');
    if (!state.activeProviderId) {
      resetProviderForm('Create a model from a template.');
    }
    applyProviderTemplate(template.id, {
      preserveId: false,
      preserveName: false,
      preserveBaseUrl: false,
      preserveModel: false,
      statusText: 'Template applied. Save the model when ready.'
    });
  });
  actions.appendChild(useButton);
  providerVisibleActions(template,
    defaultProviderAccessMode(template),
    defaultProviderCredentialSource(template, defaultProviderAccessMode(template))).forEach((action) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'secondary';
    button.textContent = action.label;
    button.disabled = state.providerSaving || state.oauthSaving;
    button.addEventListener('click', async () => {
      if (action.kind === 'oauth') {
        switchTab('settings-tab');
        if (!state.activeProviderId) {
          resetProviderForm('Create a model from a template.');
        }
        applyProviderTemplate(template.id, {
          preserveId: false,
          preserveName: false,
          preserveBaseUrl: false,
          preserveModel: false,
          statusText: 'Template applied. Finish setting up sign-in, then save the model.'
        });
      }
      await openProviderPrimaryAction(action, {
        statusEl: providerOnboardingStatusEl,
        statusText: 'Opened setup for ' + firstNonEmpty(template.name, template.id) + '.',
        globalStatus: 'Opened provider setup'
      });
    });
    actions.appendChild(button);
  });
  card.appendChild(actions);

  return card;
}

function renderProviderOnboarding() {
  const needsOnboarding = !!state.setupRequired && !state.admin.providers.length;
  providerOnboardingEl.hidden = !needsOnboarding;
  providerTemplateListEl.replaceChildren();
  if (!needsOnboarding) {
    return;
  }
  const templates = state.admin.llmProviderTemplates || [];
  if (!templates.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = 'No provider templates are available yet.';
    providerTemplateListEl.appendChild(empty);
    providerOnboardingStatusEl.textContent = 'Add a model manually below.';
    return;
  }
  templates.forEach((template) => {
    providerTemplateListEl.appendChild(buildProviderTemplateCard(template));
  });
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

function normalizeProviderIdSegment(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function ensureProviderId() {
  const current = providerIdEl.value.trim();
  const isEditing = !!state.activeProviderId;
  if (isEditing && current) return current;
  const templateId = normalizeProviderIdSegment(providerTemplateEl.value);
  const nameId = normalizeProviderIdSegment(providerNameEl.value);
  const modelId = normalizeProviderIdSegment(providerModelEl.value);
  const base = (nameId || templateId || 'provider') + (modelId ? '-' + modelId : '');
  const usedIds = new Set((state.admin.providers || []).map((provider) => provider.id).filter(Boolean));
  let candidate = base;
  let suffix = 2;
  while (usedIds.has(candidate)) {
    candidate = base + '-' + suffix;
    suffix += 1;
  }
  providerIdEl.value = candidate;
  return candidate;
}

function scrollCardIntoView(cardEl) {
  if (!cardEl || typeof cardEl.scrollIntoView !== 'function') return;
  cardEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function captureProviderDraft() {
  return {
    id: providerIdEl.value.trim(),
    name: providerNameEl.value,
    template: providerTemplateEl.value,
    baseUrl: providerBaseUrlEl.value,
    model: providerModelEl.value,
    accessMode: providerAccessModeEl.value,
    credentialSource: providerCredentialSourceEl.value,
    oauthAccount: providerOauthAccountEl.value,
    browserSession: providerBrowserSessionEl.value,
    workloads: parseProviderWorkloadsInput(),
    systemPromptBudget: providerSystemPromptBudgetEl.value,
    historyBudget: providerHistoryBudgetEl.value,
    rateLimitPerMinute: providerRateLimitEl.value,
    vision: providerVisionEl.checked,
    apiKey: providerApiKeyEl.value,
    default: providerDefaultEl.checked
  };
}

function restoreProviderDraft(draft, options = {}) {
  if (!draft) return false;
  state.activeProviderId = '';
  providerIdEl.value = draft.id || '';
  providerNameEl.value = draft.name || '';
  providerTemplateEl.value = draft.template || defaultProviderTemplateId();
  providerBaseUrlEl.value = draft.baseUrl || '';
  providerModelEl.value = draft.model || '';
  renderProviderTemplateOptions();
  providerTemplateEl.value = draft.template || providerTemplateEl.value || defaultProviderTemplateId();
  renderProviderAccessModeOptions();
  providerAccessModeEl.value = draft.accessMode || defaultProviderAccessMode(providerTemplateById(providerTemplateEl.value));
  renderProviderCredentialSourceOptions();
  providerCredentialSourceEl.value = draft.credentialSource
    || defaultProviderCredentialSource(providerTemplateById(providerTemplateEl.value), providerAccessModeEl.value);
  renderProviderOauthAccountOptions();
  providerOauthAccountEl.value = draft.oauthAccount || '';
  providerBrowserSessionEl.value = draft.browserSession || '';
  providerWorkloadsEl.value = Array.isArray(draft.workloads) ? draft.workloads.join(', ') : '';
  providerSystemPromptBudgetEl.value = draft.systemPromptBudget || '';
  providerHistoryBudgetEl.value = draft.historyBudget || '';
  providerRateLimitEl.value = draft.rateLimitPerMinute || '';
  providerVisionEl.checked = !!draft.vision;
  providerApiKeyEl.value = draft.apiKey || '';
  providerDefaultEl.checked = !!draft.default;
  providerStatusEl.textContent = options.statusText || 'Continue configuring the model.';
  renderProviderWorkloadNote();
  renderProviderList();
  syncProviderAuthInputs();
  updateAdminButtons();
  if (options.scroll) {
    scrollCardIntoView(providerCardEl);
  }
  return true;
}

function suggestedProviderOauthTemplateIds(template) {
  if (!template) return [];
  const explicit = Array.isArray(template.oauth_provider_templates)
    ? template.oauth_provider_templates.filter(Boolean)
    : [];
  if (explicit.length) return explicit;
  return state.admin.oauthProviderTemplates.some((entry) => entry.id === template.id)
    ? [template.id]
    : [];
}

function suggestedProviderOauthAccountId(draft) {
  const base = normalizeProviderIdSegment(draft.id || draft.name || draft.template || 'provider');
  return (base || 'provider') + '-oauth';
}

function syncPendingProviderOauthDraft(accountId) {
  if (!state.providerDraft) return;
  state.providerDraft = Object.assign({}, state.providerDraft, {
    accessMode: 'account',
    credentialSource: 'oauth-account',
    oauthAccount: accountId || '',
    browserSession: ''
  });
  if (state.pendingProviderOauthFlow) {
    state.pendingProviderOauthFlow = Object.assign({}, state.pendingProviderOauthFlow, {
      accountId: accountId || ''
    });
  }
}

function syncPendingProviderBrowserSessionDraft(sessionId, connectorId) {
  if (!state.providerDraft) return;
  state.providerDraft = Object.assign({}, state.providerDraft, {
    accessMode: 'account',
    credentialSource: 'browser-session',
    oauthAccount: '',
    browserSession: sessionId || ''
  });
  if (state.pendingProviderBrowserSessionFlow) {
    state.pendingProviderBrowserSessionFlow = Object.assign({}, state.pendingProviderBrowserSessionFlow, {
      connectorId: connectorId || state.pendingProviderBrowserSessionFlow.connectorId || '',
      sessionId: sessionId || ''
    });
  }
}

async function beginProviderBrowserSessionSetup(connectorId, options = {}) {
  const template = providerTemplateById(providerTemplateEl.value);
  const reconnecting = !!providerBrowserSessionEl.value && !state.pendingProviderBrowserSessionFlow;
  if (providerAccessModeEl.value !== 'account') {
    providerAccessModeEl.value = 'account';
    renderProviderCredentialSourceOptions();
  }
  if (providerCredentialSourceEl.value !== 'browser-session') {
    providerCredentialSourceEl.value = 'browser-session';
  }
  syncProviderAuthInputs();
  state.providerDraft = captureProviderDraft();
  if (!state.pendingProviderBrowserSessionFlow || reconnecting) {
    state.pendingProviderBrowserSessionFlow = {
      connectorId: connectorId || (template && template.account_connector) || '',
      sessionId: '',
      providerName: firstNonEmpty(providerNameEl.value, template && template.name, providerTemplateEl.value, 'provider')
    };
  }
  const pending = state.pendingProviderBrowserSessionFlow || {};

  state.providerAccountConnecting = true;
  providerBrowserSessionNoteEl.textContent = pending.sessionId
    ? 'Checking the local browser sign-in...'
    : 'Opening a Xia-managed browser...';
  updateAdminButtons();
  try {
    if (pending.sessionId) {
      const data = await fetchJson('/admin/provider-account-connectors/' + encodeURIComponent(pending.connectorId || connectorId) + '/complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          browser_session: pending.sessionId
        })
      });
      const connection = data.connection || {};
      providerBrowserSessionEl.value = connection.session_id || pending.sessionId || '';
      state.pendingProviderBrowserSessionFlow = null;
      syncPendingProviderBrowserSessionDraft(providerBrowserSessionEl.value, connectorId);
      providerStatusEl.textContent = 'Web account session is ready. Save the model when you are done.';
      providerBrowserSessionNoteEl.textContent = connection.message || 'Web account session is ready.';
      if (options.globalStatus) setStatus(options.globalStatus);
    } else {
      const data = await fetchJson('/admin/provider-account-connectors/' + encodeURIComponent(connectorId) + '/start', {
        method: 'POST'
      });
      const connection = data.connection || {};
      providerBrowserSessionEl.value = connection.session_id || '';
      state.pendingProviderBrowserSessionFlow = {
        connectorId: connection.connector || connectorId,
        sessionId: connection.session_id || '',
        providerName: pending.providerName || firstNonEmpty(providerNameEl.value, template && template.name, 'provider')
      };
      syncPendingProviderBrowserSessionDraft(providerBrowserSessionEl.value, connection.connector || connectorId);
      providerStatusEl.textContent = connection.message || 'Finish the provider sign-in in the browser window, then click Finish Sign-In.';
      providerBrowserSessionNoteEl.textContent = connection.message || 'Finish the provider sign-in in the browser window, then click Finish Sign-In.';
      if (options.globalStatus) setStatus(options.globalStatus);
    }
  } catch (err) {
    providerStatusEl.textContent = err.message || 'Failed to start account sign-in.';
    providerBrowserSessionNoteEl.textContent = err.message || 'Failed to start account sign-in.';
  } finally {
    state.providerAccountConnecting = false;
    syncProviderAuthInputs();
    updateAdminButtons();
  }
}

function beginProviderOauthSetup() {
  if (providerAccessModeEl.value !== 'account') {
    providerAccessModeEl.value = 'account';
    renderProviderCredentialSourceOptions();
  }
  if (providerCredentialSourceEl.value !== 'oauth-account') {
    providerCredentialSourceEl.value = 'oauth-account';
  }
  providerBrowserSessionEl.value = '';
  state.pendingProviderBrowserSessionFlow = null;
  syncProviderAuthInputs();
  const draft = captureProviderDraft();
  const template = providerTemplateById(draft.template);
  const linkedAccount = (state.admin.oauthAccounts || []).find((account) => account.id === draft.oauthAccount);
  state.providerDraft = draft;
  state.pendingProviderOauthFlow = {
    accountId: linkedAccount ? linkedAccount.id : '',
    providerName: firstNonEmpty(draft.name, template && template.name, draft.template, 'provider')
  };

  if (linkedAccount) {
    selectOauthAccount(linkedAccount);
    oauthAccountStatusEl.textContent = linkedAccount.connected
      ? 'Review or refresh this linked sign-in.'
      : 'Finish connecting this linked sign-in, then return to the model form.';
  } else {
    const oauthTemplateId = suggestedProviderOauthTemplateIds(template)[0] || '';
    resetOauthAccountForm('Configure a linked sign-in for ' + firstNonEmpty(draft.name, template && template.name, 'this provider') + '.');
    if (oauthTemplateId) {
      oauthTemplateEl.value = oauthTemplateId;
      applyOauthTemplate();
    }
    oauthAccountIdEl.value = oauthAccountIdEl.value.trim() || suggestedProviderOauthAccountId(draft);
    oauthAccountNameEl.value = firstNonEmpty(draft.name, template && template.name, 'Provider') + ' Sign-In';
    oauthAccountStatusEl.textContent = oauthTemplateId
      ? 'Preset applied. Enter the client credentials, save, then connect.'
      : 'Enter the provider-specific OAuth URLs and client credentials, save, then connect.';
    updateAdminButtons();
    if (oauthTemplateId) {
      oauthClientIdEl.focus();
    } else {
      oauthAuthorizeUrlEl.focus();
    }
  }

  scrollCardIntoView(oauthAccountCardEl);
}

function oauthAccountMeta(account) {
  const bits = [];
  if (account.connection_mode === 'manual-token') bits.push('Manual token');
  if (account.connection_mode === 'oauth-flow') bits.push('OAuth flow');
  bits.push(account.connected ? 'Connected' : 'Not connected');
  if (account.refresh_token_configured) bits.push('Refresh token stored');
  if (account.autonomous_approved) bits.push('Autonomous approved');
  if (account.expires_at) bits.push('Expires ' + formatStamp(account.expires_at));
  return bits.join(' • ');
}

function currentOauthConnectionMode() {
  return oauthConnectionModeEl.value === 'manual-token' ? 'manual-token' : 'oauth-flow';
}

function syncOauthAccountInputs() {
  const manualToken = currentOauthConnectionMode() === 'manual-token';
  oauthAccessTokenFieldEl.hidden = false;
  oauthTokenTypeFieldEl.hidden = false;
  oauthExpiresAtFieldEl.hidden = false;
  oauthAuthorizeUrlFieldEl.hidden = manualToken;
  oauthTokenUrlFieldEl.hidden = manualToken;
  oauthClientIdFieldEl.hidden = manualToken;
  oauthClientSecretFieldEl.hidden = manualToken;
  oauthScopesFieldEl.hidden = manualToken;
  oauthRedirectUriFieldEl.hidden = manualToken;
  oauthAuthParamsFieldEl.hidden = manualToken;
  oauthTokenParamsFieldEl.hidden = manualToken;
  oauthTemplateEl.disabled = state.oauthSaving || manualToken;
  applyOauthTemplateEl.disabled = state.oauthSaving || manualToken;
  if (manualToken) {
    oauthTemplateNoteEl.textContent = 'Store a durable provider token directly. Xia will use it as an API credential without an OAuth browser flow.';
  } else {
    const current = state.admin.oauthProviderTemplates.find((template) => template.id === oauthTemplateEl.value);
    oauthTemplateNoteEl.textContent = current
      ? ((current.description || 'OAuth provider preset.') + (current.notes ? ' ' + current.notes : ''))
      : 'Choose a preset for common providers, then fill in your client id and client secret.';
  }
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
  const activeOauthAccount = state.admin.oauthAccounts.find((entry) => entry.id === state.activeOauthAccountId) || null;
  const manualOauthToken = currentOauthConnectionMode() === 'manual-token';
  knowledgeQueryEl.disabled = state.knowledgeSearching;
  searchKnowledgeEl.disabled = state.knowledgeSearching || !knowledgeQueryEl.value.trim();
  providerIdEl.disabled = state.providerSaving || !!state.activeProviderId;
  providerTemplateEl.disabled = state.providerSaving;
  saveProviderEl.disabled = state.providerSaving;
  deleteProviderEl.disabled = state.providerSaving || !state.activeProviderId;
  newProviderEl.disabled = state.providerSaving;
  providerConfigureOauthEl.disabled = state.providerSaving || state.oauthSaving || !currentProviderPrimaryAction();
  providerOpenAccountEl.disabled = state.providerSaving || state.oauthSaving || !providerActionByKind(providerTemplateById(providerTemplateEl.value), 'account');
  providerOpenDocsEl.disabled = state.providerSaving || state.oauthSaving || !providerActionByKind(providerTemplateById(providerTemplateEl.value), 'docs');
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
  localDocOcrEnabledEl.disabled = state.localDocOcrSaving;
  localDocOcrModelBackendEl.disabled = state.localDocOcrSaving;
  localDocOcrExternalProviderIdEl.disabled = state.localDocOcrSaving
    || localDocOcrModelBackendEl.value !== 'external';
  localDocOcrCommandEl.disabled = state.localDocOcrSaving;
  localDocOcrTimeoutMsEl.disabled = state.localDocOcrSaving;
  localDocOcrModelPathEl.disabled = state.localDocOcrSaving;
  localDocOcrMmprojPathEl.disabled = state.localDocOcrSaving;
  localDocOcrSpottingMmprojPathEl.disabled = state.localDocOcrSaving;
  localDocOcrMaxTokensEl.disabled = state.localDocOcrSaving;
  saveLocalDocOcrEl.disabled = state.localDocOcrSaving;
  databaseBackupEnabledEl.disabled = state.databaseBackupSaving;
  databaseBackupDirectoryEl.disabled = state.databaseBackupSaving;
  databaseBackupIntervalHoursEl.disabled = state.databaseBackupSaving;
  databaseBackupRetainCountEl.disabled = state.databaseBackupSaving;
  saveDatabaseBackupEl.disabled = state.databaseBackupSaving;
  oauthAccountIdEl.disabled = state.oauthSaving || !!state.activeOauthAccountId;
  oauthAccountNameEl.disabled = state.oauthSaving;
  oauthConnectionModeEl.disabled = state.oauthSaving;
  oauthAccessTokenEl.disabled = state.oauthSaving;
  oauthTokenTypeEl.disabled = state.oauthSaving;
  oauthExpiresAtEl.disabled = state.oauthSaving;
  oauthAuthorizeUrlEl.disabled = state.oauthSaving || manualOauthToken;
  oauthTokenUrlEl.disabled = state.oauthSaving || manualOauthToken;
  oauthClientIdEl.disabled = state.oauthSaving || manualOauthToken;
  oauthClientSecretEl.disabled = state.oauthSaving || manualOauthToken;
  oauthScopesEl.disabled = state.oauthSaving || manualOauthToken;
  oauthRedirectUriEl.disabled = state.oauthSaving || manualOauthToken;
  oauthAuthParamsEl.disabled = state.oauthSaving || manualOauthToken;
  oauthTokenParamsEl.disabled = state.oauthSaving || manualOauthToken;
  saveOauthAccountEl.disabled = state.oauthSaving;
  newOauthAccountEl.disabled = state.oauthSaving;
  oauthAccountCreateServiceEl.disabled = state.oauthSaving || !state.activeOauthAccountId;
  connectOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId || manualOauthToken;
  refreshOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId || manualOauthToken || !(activeOauthAccount && activeOauthAccount.refresh_token_configured);
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
  syncOauthAccountInputs();
  syncProviderAuthInputs();
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
    'No connections configured yet. Add one for APIs that use OAuth or a durable manual token.',
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
  syncOauthAccountInputs();
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
  state.providerDraft = null;
  state.pendingProviderOauthFlow = null;
  state.pendingProviderBrowserSessionFlow = null;
  providerIdEl.value = '';
  providerNameEl.value = '';
  providerTemplateEl.value = defaultProviderTemplateId();
  providerBaseUrlEl.value = '';
  providerModelEl.value = '';
  providerAccessModeEl.value = '';
  providerCredentialSourceEl.value = '';
  providerOauthAccountEl.value = '';
  providerBrowserSessionEl.value = '';
  providerWorkloadsEl.value = '';
  providerSystemPromptBudgetEl.value = '';
  providerHistoryBudgetEl.value = '';
  providerRateLimitEl.value = '';
  providerVisionEl.checked = false;
  providerApiKeyEl.value = '';
  providerDefaultEl.checked = !state.admin.providers.some((provider) => provider.default);
  renderProviderTemplateOptions();
  renderProviderAccessModeOptions();
  renderProviderCredentialSourceOptions();
  renderProviderOauthAccountOptions();
  const templateId = providerTemplateEl.value;
  if (templateId) {
    applyProviderTemplate(templateId, {
      preserveId: false,
      preserveName: false,
      preserveBaseUrl: false,
      preserveModel: false,
      statusText: statusText || 'Choose a model template, review the fields, then save it.'
    });
  } else {
    providerStatusEl.textContent = statusText || 'Create a model or select an existing one.';
  }
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

function renderWebSearchSettings() {
  const settings = state.admin.webSearch || {};
  searchBackendEl.value = settings.backend || '';
  searchBraveApiKeyEl.value = settings.brave_api_key || '';
  searchSearxngUrlEl.value = settings.searxng_url || '';
}

async function saveWebSearch() {
  searchStatusEl.textContent = 'Saving...';
  try {
    const data = await fetchJson('/admin/web-search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        backend: searchBackendEl.value,
        brave_api_key: searchBraveApiKeyEl.value.trim(),
        searxng_url: searchSearxngUrlEl.value.trim()
      })
    });
    state.admin.webSearch = data.web_search || {};
    searchStatusEl.textContent = 'Saved.';
    setStatus('Search settings saved');
  } catch (err) {
    searchStatusEl.textContent = err.message || 'Failed to save.';
  }
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

function defaultLocalDocOcrStatus() {
  const settings = state.admin.localDocOcr || {};
  const bits = [];
  bits.push(settings.enabled ? 'Local OCR enabled' : 'Local OCR disabled');
  bits.push('Model backend: ' + firstNonEmpty(settings.model_backend, 'local'));
  if (settings.model_backend === 'external') {
    bits.push('Provider: ' + firstNonEmpty(settings.resolved_external_provider_id, settings.external_provider_id, 'default'));
    if (settings.external_provider_vision) bits.push('Vision-capable');
  } else {
    bits.push('Runtime: ' + firstNonEmpty(settings.backend, 'llama.cpp-cli'));
    if (settings.command) bits.push('Command: ' + settings.command);
    if (settings.managed_install) bits.push('Managed model install available');
    if (settings.resolved_model_path) bits.push('Model: ' + settings.resolved_model_path);
    if (settings.resolved_mmproj_path) bits.push('mmproj: ' + settings.resolved_mmproj_path);
  }
  if (settings.configured) bits.push('Configured');
  else bits.push(settings.model_backend === 'external'
    ? 'Needs a vision-capable external provider'
    : 'Needs a llama.cpp command plus managed or explicit OCR assets');
  if (settings.default_mode) bits.push('Default mode: ' + settings.default_mode);
  return bits.join(' • ');
}

function renderLocalDocOcrProviderOptions() {
  const selected = localDocOcrExternalProviderIdEl.value;
  localDocOcrExternalProviderIdEl.replaceChildren();
  const blank = document.createElement('option');
  blank.value = '';
  blank.textContent = 'Default provider';
  localDocOcrExternalProviderIdEl.appendChild(blank);
  state.admin.providers
    .filter((provider) => !!provider.vision)
    .forEach((provider) => {
      const option = document.createElement('option');
      option.value = provider.id || '';
      option.textContent = firstNonEmpty(provider.name, provider.id);
      localDocOcrExternalProviderIdEl.appendChild(option);
    });
  localDocOcrExternalProviderIdEl.value = selected || '';
}

function renderLocalDocOcrSettings() {
  const settings = state.admin.localDocOcr || {};
  localDocOcrEnabledEl.checked = !!settings.enabled;
  localDocOcrModelBackendEl.value = firstNonEmpty(settings.model_backend, 'local');
  renderLocalDocOcrProviderOptions();
  localDocOcrExternalProviderIdEl.value = settings.external_provider_id || '';
  localDocOcrCommandEl.value = settings.command || '';
  localDocOcrTimeoutMsEl.value = settings.timeout_ms || '';
  localDocOcrModelPathEl.value = settings.model_path || '';
  localDocOcrMmprojPathEl.value = settings.mmproj_path || '';
  localDocOcrSpottingMmprojPathEl.value = settings.spotting_mmproj_path || '';
  localDocOcrMaxTokensEl.value = settings.max_tokens || '';
  localDocOcrExternalProviderIdEl.disabled = state.localDocOcrSaving
    || localDocOcrModelBackendEl.value !== 'external';
  localDocOcrStatusEl.textContent = state.localDocOcrStatus || defaultLocalDocOcrStatus();
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
  oauthConnectionModeEl.value = 'oauth-flow';
  oauthAccessTokenEl.value = '';
  oauthTokenTypeEl.value = 'Bearer';
  oauthExpiresAtEl.value = '';
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
  serviceAllowPrivateNetworkEl.checked = false;
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
  state.providerDraft = null;
  state.pendingProviderOauthFlow = null;
  state.pendingProviderBrowserSessionFlow = null;
  providerIdEl.value = provider.id || '';
  providerNameEl.value = provider.name || '';
  providerTemplateEl.value = provider.template || '';
  providerBaseUrlEl.value = provider.base_url || '';
  providerModelEl.value = provider.model || '';
  renderProviderTemplateOptions();
  renderProviderAccessModeOptions();
  providerAccessModeEl.value = provider.access_mode || providerAccessMode(provider);
  renderProviderCredentialSourceOptions();
  providerCredentialSourceEl.value = provider.credential_source || providerCredentialSource(provider);
  renderProviderOauthAccountOptions();
  providerOauthAccountEl.value = provider.oauth_account || '';
  providerBrowserSessionEl.value = provider.browser_session || '';
  providerWorkloadsEl.value = Array.isArray(provider.workloads) ? provider.workloads.join(', ') : '';
  providerSystemPromptBudgetEl.value = provider.system_prompt_budget || '';
  providerHistoryBudgetEl.value = provider.history_budget || '';
  providerRateLimitEl.value = provider.rate_limit_per_minute || '';
  providerVisionEl.checked = !!provider.vision;
  providerApiKeyEl.value = '';
  providerDefaultEl.checked = !!provider.default;
  if (providerCredentialSource(provider) === 'oauth-account') {
    providerStatusEl.textContent = provider.oauth_account_connected
      ? 'API sign-in connected.'
      : 'API sign-in still needs to be connected.';
  } else if (providerCredentialSource(provider) === 'browser-session') {
    providerStatusEl.textContent = provider.browser_session_connected
      ? 'Web account session is ready.'
      : 'Web account session needs to be reconnected.';
  } else if (providerCredentialSource(provider) === 'api-key') {
    providerStatusEl.textContent = provider.api_key_configured
      ? 'API key stored. Enter a new one to replace it.'
      : 'No API key stored yet.';
  } else {
    providerStatusEl.textContent = 'No credential required.';
  }
  if (provider.effective_rate_limit_per_minute) {
    providerStatusEl.textContent += ' Rate limit: ' + provider.effective_rate_limit_per_minute + '/min.';
  }
  if (provider.health_status && provider.health_status !== 'healthy') {
    providerStatusEl.textContent += ' Current health: ' + provider.health_status + '.';
    if (provider.health_last_error) {
      providerStatusEl.textContent += ' Last error: ' + provider.health_last_error;
    }
  }
  if (providerCredentialSource(provider) === 'oauth-account' && provider.oauth_account_name) {
    providerStatusEl.textContent += ' API sign-in: ' + provider.oauth_account_name + '.';
  }
  renderProviderWorkloadNote();
  renderProviderList();
  updateAdminButtons();
}

function selectOauthAccount(account) {
  state.activeOauthAccountId = account.id || '';
  oauthAccountIdEl.value = account.id || '';
  oauthAccountNameEl.value = account.name || '';
  oauthConnectionModeEl.value = account.connection_mode || 'oauth-flow';
  oauthAccessTokenEl.value = '';
  oauthTokenTypeEl.value = account.token_type || 'Bearer';
  oauthExpiresAtEl.value = account.expires_at || '';
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
    account.connection_mode === 'manual-token' ? 'Manual token connection.' : (account.client_secret_configured ? 'Secret stored.' : 'No secret stored.'),
    account.access_token_configured ? 'Access token stored.' : 'No access token stored.',
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
  serviceAllowPrivateNetworkEl.checked = !!service.allow_private_network;
  serviceAutonomousApprovedEl.checked = !!service.autonomous_approved;
  serviceEnabledEl.checked = !!service.enabled;
  serviceStatusEl.textContent = service.auth_type === 'oauth-account'
    ? ((service.oauth_account_name || 'No OAuth account') + (service.oauth_account_connected ? ' is connected.' : ' is not connected yet.'))
    : (service.auth_key_configured ? 'Secret stored.' : 'No secret stored.');
  if (service.effective_rate_limit_per_minute) {
    serviceStatusEl.textContent += ' Rate limit: ' + service.effective_rate_limit_per_minute + '/min.';
  }
  if (service.allow_private_network) {
    serviceStatusEl.textContent += ' Private-network access enabled.';
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

// ---------------------------------------------------------------------------
// LLM Call Log
// ---------------------------------------------------------------------------

function renderLlmCallList() {
  llmCallListEl.innerHTML = '';
  if (!state.llmCalls.length) {
    llmCallListEl.innerHTML = '<div class="admin-list-empty">No LLM calls recorded yet.</div>';
    return;
  }
  state.llmCalls.forEach(function (call) {
    const button = document.createElement('button');
    button.className = 'admin-item' + (call.id === state.activeLlmCallId ? ' active' : '');
    const title = document.createElement('div');
    title.className = 'admin-item-title';
    title.textContent = (call.provider_id || '?') + ' / ' + (call.model || '?');
    const meta = document.createElement('div');
    meta.className = 'admin-item-meta';
    const parts = [];
    if (call.workload) parts.push(call.workload);
    if (call.status) parts.push(call.status);
    if (call.duration_ms != null) parts.push(call.duration_ms + 'ms');
    if (call.prompt_tokens != null || call.completion_tokens != null)
      parts.push((call.prompt_tokens || 0) + '→' + (call.completion_tokens || 0) + ' tok');
    if (call.created_at) parts.push(formatStamp(call.created_at));
    meta.textContent = parts.join(' · ');
    button.appendChild(title);
    button.appendChild(meta);
    button.addEventListener('click', function () { loadLlmCallDetail(call.id); });
    llmCallListEl.appendChild(button);
  });
}

function renderLlmCallDetail() {
  llmCallDetailEl.innerHTML = '';
  const call = state.activeLlmCall;
  if (!call) {
    llmCallDetailEl.innerHTML = '<div class="admin-list-empty">Select a call to inspect its prompt and response.</div>';
    return;
  }
  // Meta row
  const metaDiv = document.createElement('div');
  metaDiv.className = 'llm-call-meta';
  const metaParts = [call.provider_id, call.model, call.workload, call.status,
    call.duration_ms != null ? call.duration_ms + 'ms' : null,
    (call.prompt_tokens != null || call.completion_tokens != null)
      ? (call.prompt_tokens || 0) + '→' + (call.completion_tokens || 0) + ' tok' : null,
    call.created_at ? formatStamp(call.created_at) : null].filter(Boolean);
  metaDiv.textContent = metaParts.join(' · ');
  llmCallDetailEl.appendChild(metaDiv);

  if (call.error) {
    const errLabel = document.createElement('div');
    errLabel.className = 'history-block-label';
    errLabel.textContent = 'Error';
    llmCallDetailEl.appendChild(errLabel);
    const errPre = document.createElement('pre');
    errPre.textContent = call.error;
    llmCallDetailEl.appendChild(errPre);
  }
  // Messages
  if (call.messages) {
    const msgLabel = document.createElement('div');
    msgLabel.className = 'history-block-label';
    msgLabel.textContent = 'Messages (prompt)';
    llmCallDetailEl.appendChild(msgLabel);
    const msgPre = document.createElement('pre');
    try { msgPre.textContent = JSON.stringify(JSON.parse(call.messages), null, 2); }
    catch (_) { msgPre.textContent = call.messages; }
    llmCallDetailEl.appendChild(msgPre);
  }
  // Tools
  if (call.tools) {
    const toolLabel = document.createElement('div');
    toolLabel.className = 'history-block-label';
    toolLabel.textContent = 'Tools';
    llmCallDetailEl.appendChild(toolLabel);
    const toolPre = document.createElement('pre');
    try { toolPre.textContent = JSON.stringify(JSON.parse(call.tools), null, 2); }
    catch (_) { toolPre.textContent = call.tools; }
    llmCallDetailEl.appendChild(toolPre);
  }
  // Response
  if (call.response) {
    const respLabel = document.createElement('div');
    respLabel.className = 'history-block-label';
    respLabel.textContent = 'Response';
    llmCallDetailEl.appendChild(respLabel);
    const respPre = document.createElement('pre');
    try { respPre.textContent = JSON.stringify(JSON.parse(call.response), null, 2); }
    catch (_) { respPre.textContent = call.response; }
    llmCallDetailEl.appendChild(respPre);
  }
}

async function loadLlmCalls() {
  state.llmCallsLoading = true;
  llmCallStatusEl.textContent = 'Loading LLM call log...';
  try {
    const data = await fetchJson('/llm-calls?limit=100');
    state.llmCalls = Array.isArray(data.calls) ? data.calls : [];
    renderLlmCallList();
    if (!state.llmCalls.length) {
      llmCallStatusEl.textContent = 'No LLM calls recorded yet.';
    } else {
      llmCallStatusEl.textContent = state.llmCalls.length + (state.llmCalls.length === 1 ? ' call.' : ' calls.');
      if (!state.activeLlmCallId && state.llmCalls.length) {
        await loadLlmCallDetail(state.llmCalls[0].id);
      }
    }
  } catch (err) {
    state.llmCalls = [];
    renderLlmCallList();
    llmCallStatusEl.textContent = err.message || 'Failed to load LLM call log.';
  } finally {
    state.llmCallsLoading = false;
  }
}

async function loadLlmCallDetail(callId) {
  state.activeLlmCallId = callId;
  state.activeLlmCall = null;
  renderLlmCallList();
  renderLlmCallDetail();
  if (!callId) return;
  try {
    const data = await fetchJson('/llm-calls/' + encodeURIComponent(callId));
    if (state.activeLlmCallId !== callId) return;
    state.activeLlmCall = data.call || null;
    renderLlmCallDetail();
  } catch (err) {
    llmCallStatusEl.textContent = err.message || 'Failed to load call detail.';
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

function renderIdentitySettings() {
  const identity = state.admin.identity || {};
  const instance = state.admin.instance || {};
  const capabilities = state.admin.capabilities || {};
  const storage = state.admin.storage || {};
  identityNameEl.value = identity.name || '';
  identityRoleEl.value = identity.role || '';
  identityDescriptionEl.value = identity.description || '';
  identityControllerEnabledEl.checked = !!capabilities.instance_management_configured;
  identityInstanceIdEl.value = instance.id || '';
  identityDbPathEl.value = storage.db_path || '';
  identitySupportDirEl.value = storage.support_dir || '';
  identityPersonalityEl.value = identity.personality || '';
  identityGuidelinesEl.value = identity.guidelines || '';
  if (capabilities.instance_management_enabled) {
    identityControllerNoteEl.textContent = 'Controller mode is active. This Xia can start and stop managed child Xia instances.';
  } else {
    identityControllerNoteEl.textContent = 'Enable controller mode to let this Xia start and stop managed child Xia instances.';
  }
  heroNameEl.textContent = identity.name || 'Xia';
  document.title = identity.name || 'Xia';
}

async function saveIdentity() {
  identityStatusEl.textContent = 'Saving...';
  try {
    const data = await fetchJson('/admin/identity', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: identityNameEl.value.trim(),
        role: identityRoleEl.value.trim(),
        description: identityDescriptionEl.value.trim(),
        controller_enabled: !!identityControllerEnabledEl.checked,
        personality: identityPersonalityEl.value.trim(),
        guidelines: identityGuidelinesEl.value.trim()
      })
    });
    state.admin.identity = data.identity || {};
    state.admin.capabilities = data.capabilities || state.admin.capabilities || {};
    renderIdentitySettings();
    identityStatusEl.textContent = 'Saved.';
  } catch (err) {
    identityStatusEl.textContent = err.message || 'Failed to save.';
  }
}

async function loadAdminConfig() {
  return dedup('loadAdminConfig', loadAdminConfigImpl);
}
async function loadAdminConfigImpl() {
  try {
    const data = await fetchJson('/admin/config');
    state.setupRequired = !!data.setup_required;
    state.admin.identity = data.identity || {};
    state.admin.instance = data.instance || {};
    state.admin.capabilities = data.capabilities || {};
    state.admin.storage = data.storage || {};
    state.admin.providers = Array.isArray(data.providers) ? data.providers : [];
    state.admin.llmProviderTemplates = Array.isArray(data.llm_provider_templates) ? data.llm_provider_templates : [];
    state.admin.conversationContext = data.conversation_context || null;
    state.admin.webSearch = data.web_search || {};
    state.admin.memoryRetention = data.memory_retention || null;
    state.admin.knowledgeDecay = data.knowledge_decay || null;
    state.admin.localDocSummarization = data.local_doc_summarization || null;
    state.admin.localDocOcr = data.local_doc_ocr || null;
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
    state.localDocOcrStatus = defaultLocalDocOcrStatus();
    state.databaseBackupStatus = defaultDatabaseBackupStatus();
    state.remoteBridgeStatus = defaultRemoteBridgeStatus();
    state.remotePairStatus = defaultRemotePairStatus();
    const provider = state.admin.providers.find((entry) => entry.id === state.activeProviderId);
    const oauthAccount = state.admin.oauthAccounts.find((entry) => entry.id === state.activeOauthAccountId);
    const service = state.admin.services.find((entry) => entry.id === state.activeServiceId);
    const site = state.admin.sites.find((entry) => entry.id === state.activeSiteId);
    renderOauthTemplateOptions();
    renderOauthAccountOptions();
    renderIdentitySettings();
    renderProviderTemplateOptions();
    renderProviderAccessModeOptions();
    renderProviderCredentialSourceOptions();
    renderProviderOauthAccountOptions();
    renderProviderWorkloadNote();
    renderConversationContextSettings();
    renderWebSearchSettings();
    renderMemoryRetentionSettings();
    renderKnowledgeDecaySettings();
    renderLocalDocSummarizationSettings();
    renderLocalDocOcrSettings();
    renderDatabaseBackupSettings();
    if (state.providerDraft) {
      restoreProviderDraft(state.providerDraft);
    } else if (provider) {
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
    renderProviderOnboarding();
    renderCapabilities();
    if (state.setupRequired && !state.admin.providers.length) {
      switchTab('settings-tab');
      setStatus('Connect a model to start chatting');
    }
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
  oauthConnectionModeEl.value = 'oauth-flow';
  oauthAccessTokenEl.value = '';
  oauthTokenTypeEl.value = 'Bearer';
  oauthExpiresAtEl.value = '';
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
  syncOauthAccountInputs();
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

async function deleteProvider() {
  if (state.providerSaving || !state.activeProviderId) return;
  if (!window.confirm('Delete this model provider?')) return;
  var deletingId = state.activeProviderId;
  state.providerSaving = true;
  providerStatusEl.textContent = 'Deleting...';
  updateAdminButtons();
  try {
    await fetchJson('/admin/providers', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: deletingId })
    });
    state.admin.providers = state.admin.providers.filter(function (p) { return p.id !== deletingId; });
    state.providerDraft = null;
    state.providerModels = [];
    providerModelListEl.hidden = true;
    state.providerSaving = false;
    resetProviderForm('Provider deleted.');
    renderProviderOnboarding();
    updateAdminButtons();
  } catch (err) {
    providerStatusEl.textContent = err.message || 'Failed to delete provider.';
    state.providerSaving = false;
    updateAdminButtons();
  }
}

async function saveProvider() {
  if (state.providerSaving) return;
  state.providerSaving = true;
  providerStatusEl.textContent = 'Saving...';
  updateAdminButtons();
  try {
    const providerId = ensureProviderId();
    const data = await fetchJson('/admin/providers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: providerId,
        name: providerNameEl.value,
        template: providerTemplateEl.value,
        base_url: providerBaseUrlEl.value,
        model: providerModelEl.value,
        access_mode: providerAccessModeEl.value,
        credential_source: providerCredentialSourceEl.value,
        oauth_account: providerOauthAccountEl.value,
        browser_session: providerBrowserSessionEl.value,
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
    state.providerDraft = null;
    state.pendingProviderOauthFlow = null;
    state.pendingProviderBrowserSessionFlow = null;
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

async function saveLocalDocOcr() {
  if (state.localDocOcrSaving) return;
  state.localDocOcrSaving = true;
  state.localDocOcrStatus = 'Saving...';
  renderLocalDocOcrSettings();
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/local-doc-ocr', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: localDocOcrEnabledEl.checked,
        model_backend: localDocOcrModelBackendEl.value,
        external_provider_id: localDocOcrExternalProviderIdEl.value,
        command: localDocOcrCommandEl.value,
        model_path: localDocOcrModelPathEl.value,
        mmproj_path: localDocOcrMmprojPathEl.value,
        spotting_mmproj_path: localDocOcrSpottingMmprojPathEl.value,
        timeout_ms: localDocOcrTimeoutMsEl.value,
        max_tokens: localDocOcrMaxTokensEl.value
      })
    });
    state.admin.localDocOcr = data.local_doc_ocr || state.admin.localDocOcr;
    state.localDocOcrStatus = 'Local OCR settings saved.';
    renderLocalDocOcrSettings();
    setStatus('Local OCR settings saved');
  } catch (err) {
    state.localDocOcrStatus = err.message || 'Failed to save local OCR settings.';
    renderLocalDocOcrSettings();
  } finally {
    state.localDocOcrSaving = false;
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
  if (state.pendingProviderOauthFlow && state.providerDraft) {
    state.providerDraft = captureProviderDraft();
  }
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
        connection_mode: oauthConnectionModeEl.value,
        access_token: oauthAccessTokenEl.value,
        token_type: oauthTokenTypeEl.value,
        expires_at: oauthExpiresAtEl.value,
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
    if (account && state.pendingProviderOauthFlow) {
      syncPendingProviderOauthDraft(account.id || '');
    }
    oauthAccessTokenEl.value = '';
    oauthClientSecretEl.value = '';
    oauthAccountStatusEl.textContent = 'Connection saved.';
    await loadAdminConfig();
    if (account && state.pendingProviderOauthFlow) {
      restoreProviderDraft(state.providerDraft, {
        statusText: 'API sign-in saved. Connect it, then save the model.'
      });
    }
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
  if (currentOauthConnectionMode() === 'manual-token') {
    oauthAccountStatusEl.textContent = 'Manual token connections do not use Connect Now.';
    return;
  }
  if (state.pendingProviderOauthFlow && state.providerDraft) {
    state.providerDraft = captureProviderDraft();
    state.pendingProviderOauthFlow = Object.assign({}, state.pendingProviderOauthFlow, {
      accountId: state.activeOauthAccountId
    });
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
  const account = state.admin.oauthAccounts.find((entry) => entry.id === state.activeOauthAccountId);
  if (currentOauthConnectionMode() === 'manual-token') {
    oauthAccountStatusEl.textContent = 'Manual token connections do not use Refresh.';
    return;
  }
  if (account && !account.refresh_token_configured) {
    oauthAccountStatusEl.textContent = 'This connection has no refresh token.';
    return;
  }
  if (state.pendingProviderOauthFlow && state.providerDraft) {
    state.providerDraft = captureProviderDraft();
  }
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
  if (state.pendingProviderOauthFlow && state.providerDraft) {
    state.providerDraft = captureProviderDraft();
  }
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
        allow_private_network: serviceAllowPrivateNetworkEl.checked,
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
    switchTab('chat-tab');
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
    switchTab('chat-tab');
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
    switchTab('chat-tab');
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

scratchTitleEl.parentElement.addEventListener('click', () => { if (scratchTitleEl.disabled && !state.activePad) createScratchPad(); });
scratchEditorEl.parentElement.addEventListener('click', () => { if (scratchEditorEl.disabled && !state.activePad) createScratchPad(); });
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
refreshLlmCallsEl.addEventListener('click', () => loadLlmCalls());
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
  providerNameEl.focus();
});

providerTemplateEl.addEventListener('change', () => {
  state.providerModels = [];
  providerModelListEl.hidden = true;
  renderProviderTemplateOptions();
  renderProviderAccessModeOptions();
  renderProviderCredentialSourceOptions();
  renderProviderOauthAccountOptions();
  const template = providerTemplateById(providerTemplateEl.value);
  if (template) {
    applyProviderTemplate(template.id, {
      preserveId: false,
      preserveName: false,
      preserveBaseUrl: false,
      preserveModel: false,
      preserveAccessMode: false,
      preserveCredentialSource: false,
      statusText: 'Template applied. Review the details, then save the model.'
    });
  } else {
    syncProviderAuthInputs();
    updateAdminButtons();
  }
});
providerModelEl.addEventListener('input', () => {
  if (state.providerModels.length) renderProviderModelList();
});
providerModelEl.addEventListener('focus', () => {
  if (state.providerModels.length) renderProviderModelList();
});
providerModelEl.addEventListener('blur', () => {
  setTimeout(() => { providerModelListEl.hidden = true; }, 150);
});
fetchProviderModelsEl.addEventListener('click', () => fetchProviderModels());
providerAccessModeEl.addEventListener('change', () => {
  if (providerAccessModeEl.value !== 'account') {
    state.pendingProviderOauthFlow = null;
    state.pendingProviderBrowserSessionFlow = null;
    providerBrowserSessionEl.value = '';
  }
  renderProviderCredentialSourceOptions();
  syncProviderAuthInputs();
  updateAdminButtons();
});
providerCredentialSourceEl.addEventListener('change', () => {
  if (providerCredentialSourceEl.value !== 'oauth-account') {
    state.pendingProviderOauthFlow = null;
  }
  if (providerCredentialSourceEl.value !== 'browser-session') {
    state.pendingProviderBrowserSessionFlow = null;
    providerBrowserSessionEl.value = '';
  }
  syncProviderAuthInputs();
  updateAdminButtons();
});
providerOauthAccountEl.addEventListener('change', () => {
  if (state.pendingProviderOauthFlow && providerOauthAccountEl.value) {
    syncPendingProviderOauthDraft(providerOauthAccountEl.value);
  }
  syncProviderAuthInputs();
  updateAdminButtons();
});
providerConfigureOauthEl.addEventListener('click', async () => {
  const action = currentProviderPrimaryAction();
  await openProviderPrimaryAction(action, {
    statusEl: providerStatusEl,
    statusText: action && action.kind === 'external'
      ? 'Opened setup in a new tab.'
      : undefined,
    globalStatus: action && action.kind === 'external' ? 'Opened provider setup' : undefined
  });
});
providerOpenAccountEl.addEventListener('click', async () => {
  await openProviderPrimaryAction(providerActionByKind(providerTemplateById(providerTemplateEl.value), 'account'), {
    statusEl: providerStatusEl,
    statusText: 'Opened provider account in a new tab.',
    globalStatus: 'Opened provider account'
  });
});
providerOpenDocsEl.addEventListener('click', async () => {
  await openProviderPrimaryAction(providerActionByKind(providerTemplateById(providerTemplateEl.value), 'docs'), {
    statusEl: providerStatusEl,
    statusText: 'Opened provider docs in a new tab.',
    globalStatus: 'Opened provider docs'
  });
});
saveIdentityEl.addEventListener('click', () => saveIdentity());
saveProviderEl.addEventListener('click', () => saveProvider());
deleteProviderEl.addEventListener('click', () => deleteProvider());
saveContextEl.addEventListener('click', () => saveConversationContext());
saveSearchEl.addEventListener('click', () => saveWebSearch());
saveRetentionEl.addEventListener('click', () => saveMemoryRetention());
saveKnowledgeDecayEl.addEventListener('click', () => saveKnowledgeDecay());
saveLocalDocSummarizationEl.addEventListener('click', () => saveLocalDocSummarization());
localDocModelSummaryBackendEl.addEventListener('change', () => updateAdminButtons());
saveLocalDocOcrEl.addEventListener('click', () => saveLocalDocOcr());
localDocOcrModelBackendEl.addEventListener('change', () => renderLocalDocOcrSettings());
saveDatabaseBackupEl.addEventListener('click', () => saveDatabaseBackup());

newOauthAccountEl.addEventListener('click', () => {
  resetOauthAccountForm('Create a connection.');
  oauthAccountIdEl.focus();
});

oauthConnectionModeEl.addEventListener('change', () => {
  syncOauthAccountInputs();
  updateAdminButtons();
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
    const completedPendingFlow = state.pendingProviderOauthFlow
      && data.account_id
      && state.pendingProviderOauthFlow.accountId === data.account_id;
    loadAdminConfig().then(() => {
      if (completedPendingFlow && state.providerDraft) {
        restoreProviderDraft(state.providerDraft, {
          statusText: data.status === 'ok'
            ? 'API sign-in connected. Save the model to finish.'
            : 'API sign-in failed. Check the connection details and try again.',
          scroll: true
        });
        if (data.status === 'ok') {
          state.pendingProviderOauthFlow = null;
        }
      }
    });
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
renderLocalDocOcrSettings();
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
  loadHistorySchedules(),
  loadLlmCalls()
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
