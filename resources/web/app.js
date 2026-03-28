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
  sharedWorkspaceItems: [],
  sharedWorkspaceStatus: 'Loading...',
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
    managedInstances: [],
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
  providerModelMetadataLoading: false,
  providerModelMetadataModelId: '',
  providerModelCapabilities: {},
  providerModelVision: false,
  providerModelVisionModelId: '',
  providerDraft: null,
  workloadRoutingDraft: null,
  pendingProviderOauthFlow: null,
  pendingProviderBrowserSessionFlow: null,
  activeProviderId: '',
  activeOauthAccountId: '',
  activeServiceId: '',
  activeSiteId: '',
  activeSkillId: '',
  providerSaving: false,
  workloadRoutingSaving: false,
  providerAccountConnecting: false,
  contextSaving: false,
  retentionSaving: false,
  knowledgeDecaySaving: false,
  localDocSummarizationSaving: false,
  localDocSummarizationDraftMode: '',
  localDocSummarizationDraftBackend: 'local',
  localDocOcrSaving: false,
  localDocOcrDraftMode: '',
  localDocOcrDraftBackend: 'local',
  databaseBackupSaving: false,
  oauthSaving: false,
  serviceSaving: false,
  siteSaving: false,
  skillSaving: false,
  managedInstanceStoppingId: '',
  remoteBridgeSaving: false,
  remotePairing: false,
  localDocSummarizationStatus: 'Loading document summary settings...',
  localDocOcrStatus: 'Loading OCR settings...',
  contextStatus: 'Loading conversation context settings...',
  workloadRoutingStatus: 'Loading workload routing...',
  databaseBackupStatus: 'Loading database backup settings...',
  remoteBridgeStatus: 'Loading notification bridge settings...',
  remotePairStatus: 'Paste a pairing token from the mobile app to authorize a phone.',
  littleXiaStatus: 'Loading child Xia instances...',
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
const providerOnboardingModalEl = document.getElementById('provider-onboarding-modal');
const providerOnboardingModalBodyEl = document.getElementById('provider-onboarding-modal-body');
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
const providerCountLabelEl = document.getElementById('provider-count-label');
const providerDetailHeaderEl = document.getElementById('provider-detail-header');
const providerIdEl = document.getElementById('provider-id');
const providerTemplateEl = document.getElementById('provider-template');
const providerTemplateNoteEl = document.getElementById('provider-template-note');
const providerBaseUrlFieldEl = document.getElementById('provider-base-url-field');
const providerBaseUrlEl = document.getElementById('provider-base-url');
const providerModelEl = document.getElementById('provider-model');
const providerModelListEl = document.getElementById('provider-model-list');
const fetchProviderModelsEl = document.getElementById('fetch-provider-models');
const providerModelCapabilityNoteEl = document.getElementById('provider-model-capability-note');
const providerAccessModeEl = document.getElementById('provider-access-mode');
const providerCredentialSourceEl = document.getElementById('provider-credential-source');
const providerOauthAccountEl = document.getElementById('provider-oauth-account');
const providerOauthAccountNoteEl = document.getElementById('provider-oauth-account-note');
const providerBrowserSessionEl = document.getElementById('provider-browser-session');
const providerBrowserSessionNoteEl = document.getElementById('provider-browser-session-note');
const providerConfigureOauthEl = document.getElementById('provider-configure-oauth');
const providerOpenAccountEl = document.getElementById('provider-open-account');
const providerOpenDocsEl = document.getElementById('provider-open-docs');
const providerSystemPromptBudgetEl = document.getElementById('provider-system-prompt-budget');
const providerHistoryBudgetEl = document.getElementById('provider-history-budget');
const providerRateLimitEl = document.getElementById('provider-rate-limit-per-minute');
const providerApiKeyEl = document.getElementById('provider-api-key');
const providerApiKeyNoteEl = document.getElementById('provider-api-key-note');
const providerDefaultEl = document.getElementById('provider-default');
const providerStatusEl = document.getElementById('provider-status');
const saveProviderEl = document.getElementById('save-provider');
const deleteProviderEl = document.getElementById('delete-provider');
const workloadRoutingStatusEl = document.getElementById('workload-routing-status');
const saveWorkloadRoutingEl = document.getElementById('save-workload-routing');
const retentionFullResolutionDaysEl = document.getElementById('retention-full-resolution-days');
const retentionDecayHalfLifeDaysEl = document.getElementById('retention-decay-half-life-days');
const retentionRetainedCountEl = document.getElementById('retention-retained-count');
const retentionStatusEl = document.getElementById('retention-status');
const saveRetentionEl = document.getElementById('save-retention');
const contextRecentHistoryMessageLimitEl = document.getElementById('context-recent-history-message-limit');
const contextHistoryBudgetEl = document.getElementById('context-history-budget');
const contextStatusEl = document.getElementById('context-status');
const saveContextEl = document.getElementById('save-context');
const searchBackendEl = document.getElementById('search-backend');
const searchBraveApiKeyEl = document.getElementById('search-brave-api-key');
const searchBraveApiKeyFieldEl = document.getElementById('search-brave-api-key-field')
  || (searchBraveApiKeyEl ? searchBraveApiKeyEl.closest('.field') : null);
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
const localDocModelSummaryBackendEl = document.getElementById('local-doc-model-summary-backend');
const localDocModelSummaryProviderFieldEl = document.getElementById('local-doc-model-summary-provider-field');
const localDocModelSummaryProviderIdEl = document.getElementById('local-doc-model-summary-provider-id');
const localDocChunkSummaryMaxTokensEl = document.getElementById('local-doc-chunk-summary-max-tokens');
const localDocDocSummaryMaxTokensEl = document.getElementById('local-doc-doc-summary-max-tokens');
const localDocSummarizationStatusEl = document.getElementById('local-doc-summarization-status');
const saveLocalDocSummarizationEl = document.getElementById('save-local-doc-summarization');
const localDocOcrModelBackendEl = document.getElementById('local-doc-ocr-model-backend');
const localDocOcrLocalNoteFieldEl = document.getElementById('local-doc-ocr-local-note-field');
const localDocOcrExternalProviderFieldEl = document.getElementById('local-doc-ocr-external-provider-field');
const localDocOcrExternalProviderIdEl = document.getElementById('local-doc-ocr-external-provider-id');
const localDocOcrTimeoutMsEl = document.getElementById('local-doc-ocr-timeout-ms');
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
const saveOauthAccountEl = document.getElementById('save-oauth-account');
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
const skillIdEl = document.getElementById('skill-id');
const skillNameEl = document.getElementById('skill-name');
const skillDescriptionEl = document.getElementById('skill-description');
const skillTagsEl = document.getElementById('skill-tags');
const skillEnabledEl = document.getElementById('skill-enabled');
const skillContentEl = document.getElementById('skill-content');
const skillStatusEl = document.getElementById('skill-status');
const saveSkillEl = document.getElementById('save-skill');
const deleteSkillEl = document.getElementById('delete-skill');
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
const sharedWorkspaceListEl = document.getElementById('shared-workspace-list');
const littleXiaListEl = document.getElementById('little-xia-list');
const sidebarAccordionEls = Array.from(document.querySelectorAll('.sidebar-accordion'));
const tabLinks = document.querySelectorAll('.tab-link');
const tabPanels = document.querySelectorAll('.tab-panel');
const advancedToggleEl = document.getElementById('advanced-toggle');
const providerOnboardingPlaceholderEl = document.createComment('provider-onboarding-placeholder');
if (providerOnboardingEl && providerOnboardingEl.parentNode) {
  providerOnboardingEl.parentNode.insertBefore(providerOnboardingPlaceholderEl, providerOnboardingEl);
}
const providerCardPlaceholderEl = document.createComment('provider-card-placeholder');
if (providerCardEl && providerCardEl.parentNode) {
  providerCardEl.parentNode.insertBefore(providerCardPlaceholderEl, providerCardEl);
}

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

sidebarAccordionEls.forEach((accordion) => {
  accordion.addEventListener('toggle', () => {
    if (!accordion.open) return;
    sidebarAccordionEls.forEach((other) => {
    if (other !== accordion) other.open = false;
    });
    if (accordion.id === 'shared-workspace-accordion') {
      loadSharedWorkspaceItems().catch(() => {});
    }
    if (accordion.id === 'little-xia-accordion') {
      loadManagedInstances().catch(() => {});
    }
  });
});

advancedToggleEl.addEventListener('change', () => {
  document.body.classList.toggle('advanced-mode', advancedToggleEl.checked);
  // If current section is advanced-only and we turned off advanced, switch to identity
  if (!advancedToggleEl.checked) {
    const activeNav = document.querySelector('.settings-nav-item.active');
    if (activeNav && activeNav.classList.contains('advanced-only')) {
      switchSettingsSection('identity');
    }
  }
});

// Section navigation (shared by Settings and History tabs)
function createSectionSwitcher(containerId) {
  const container = document.getElementById(containerId);
  const navItems = container.querySelectorAll('.settings-nav-item');
  const cards = container.querySelectorAll('.settings-content > .admin-card');

  function switchSection(sectionId) {
    navItems.forEach(item => item.classList.toggle('active', item.dataset.section === sectionId));
    cards.forEach(card => card.classList.toggle('active-section', card.dataset.settingsSection === sectionId));
  }

  navItems.forEach(item => item.addEventListener('click', () => switchSection(item.dataset.section)));
  return switchSection;
}

const switchSettingsSection = createSectionSwitcher('settings-tab');
switchSettingsSection('identity');

const switchHistorySection = createSectionSwitcher('history-tab');
switchHistorySection('chat-history');

function restoreNodeAfterPlaceholder(node, placeholder) {
  if (!node || !placeholder || !placeholder.parentNode) return;
  const parent = placeholder.parentNode;
  const next = placeholder.nextSibling;
  if (next) {
    parent.insertBefore(node, next);
  } else {
    parent.appendChild(node);
  }
}

function syncProviderOnboardingModal() {
  const shouldOpen = !!state.setupRequired && !state.admin.providers.length;
  if (!providerOnboardingModalEl || !providerOnboardingModalBodyEl || !providerOnboardingEl || !providerCardEl) {
    return;
  }
  if (shouldOpen) {
    providerOnboardingModalBodyEl.replaceChildren(providerOnboardingEl, providerCardEl);
    providerOnboardingModalEl.hidden = false;
    providerOnboardingModalEl.setAttribute('aria-hidden', 'false');
    document.body.classList.add('modal-open');
    return;
  }
  restoreNodeAfterPlaceholder(providerOnboardingEl, providerOnboardingPlaceholderEl);
  restoreNodeAfterPlaceholder(providerCardEl, providerCardPlaceholderEl);
  providerOnboardingModalEl.hidden = true;
  providerOnboardingModalEl.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('modal-open');
}

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

function isClosedSessionResponse(response, data) {
  if (!response || ![404, 409].includes(response.status)) return false;
  const message = firstNonEmpty(data && data.error, '').toLowerCase();
  return message.includes('session');
}

function resetCurrentSession(statusText) {
  state.sessionId = '';
  state.messages = [];
  state.pendingApproval = null;
  state.sending = false;
  state.approvalSubmitting = false;
  state.liveStatus = null;
  state.sendStartedAt = 0;
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
  _pollFailures = 0;
  persistSession();
  renderApproval();
  renderMessages();
  syncLocalDocPanel('No local document selected.');
  syncArtifactPanel('No artifact selected.');
  syncScratchEditor('No note selected.');
  updateComposerState();
  loadHistorySessions().catch(() => {});
  setStatus(statusText || 'Session closed. Start a new session.');
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
  return state.baseStatus && state.baseStatus !== 'Ready'
    ? state.baseStatus
    : 'Ready for input.';
}

function currentPillText() {
  if (state.pendingApproval) return 'Approval needed';
  if (state.sending) return 'Working...';
  return state.baseStatus || 'Ready';
}

function statusToneFromText(text) {
  const value = String(text || '').trim().toLowerCase();
  if (!value || value === 'ready') return 'ready';
  if (value.includes('approval')) return 'approval';
  if (value.includes('failed') || value.includes('error')) return 'error';
  if (value.includes('lost') || value.includes('retry') || value.includes('denied')) return 'warning';
  if (value.includes('saved')
      || value.includes('connected')
      || value.includes('reconnected')
      || value.includes('approved')
      || value.includes('deleted')
      || value.includes('copied')
      || value.includes('opened')
      || value.includes('uploaded')
      || value.includes('imported')
      || value.includes('downloaded')
      || value.includes('created')
      || value.includes('paired')
      || value.includes('revoked')
      || value.includes('refreshed')) {
    return 'success';
  }
  return 'neutral';
}

function currentPillTone() {
  if (state.pendingApproval) return 'approval';
  if (state.sending) return 'working';
  return statusToneFromText(state.baseStatus);
}

function currentActivityTone() {
  if (state.pendingApproval) return 'approval';
  if (state.sending && state.liveStatus) {
    switch (state.liveStatus.phase) {
      case 'approval': return 'approval';
      case 'error': return 'error';
      case 'llm':
      case 'tool':
      case 'tool-plan':
      case 'working-memory':
      case 'finalizing':
        return 'working';
      default:
        return 'neutral';
    }
  }
  if (state.sending) return 'working';
  if (!state.baseStatus || state.baseStatus === 'Ready') return 'ready';
  return statusToneFromText(state.baseStatus);
}

function syncStatus() {
  const pillText = currentPillText();
  const activityText = currentActivityText();
  statusEl.textContent = pillText;
  statusEl.dataset.tone = currentPillTone();
  if (state.liveStatus && state.liveStatus.phase) {
    statusEl.dataset.phase = state.liveStatus.phase;
  } else {
    delete statusEl.dataset.phase;
  }
  activityStatusEl.textContent = activityText;
  activityStatusEl.dataset.tone = currentActivityTone();
}

function setStatus(text) {
  state.baseStatus = text || 'Ready';
  syncStatus();
}

async function isLocalSessionSecretResponse(response) {
  if (!response || response.status !== 401) return false;
  const contentType = firstNonEmpty(response.headers && response.headers.get('content-type'), '').toLowerCase();
  if (!contentType.includes('application/json')) return false;
  try {
    const data = await response.clone().json();
    const message = firstNonEmpty(data && data.error, '').toLowerCase();
    return message.includes('local session secret');
  } catch (_err) {
    return false;
  }
}

async function refreshLocalSessionCookie() {
  return dedup('local-session-cookie', async () => {
    const response = await fetch('/local-session', {
      headers: { 'X-Requested-With': 'XMLHttpRequest' }
    });
    if (!response.ok) {
      throw new Error('Failed to refresh local session');
    }
    return response;
  });
}

async function safeFetch(url, options, retryingLocalSession) {
  const opts = Object.assign({}, options);
  opts.headers = Object.assign({ 'X-Requested-With': 'XMLHttpRequest' }, opts.headers || {});
  const response = await fetch(url, opts);
  if (!retryingLocalSession && await isLocalSessionSecretResponse(response)) {
    await refreshLocalSessionCookie();
    return safeFetch(url, options, true);
  }
  return response;
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
    throw new Error(normalizeServerErrorMessage(data.error || 'Request failed'));
  }
  return data;
}

function normalizeServerErrorMessage(message) {
  const text = firstNonEmpty(message, 'Request failed');
  if (text.includes('Database not connected. Call (xia.db/connect!) first.')) {
    return 'Database became unavailable unexpectedly; check server logs.';
  }
  return text;
}

function firstNonEmpty(value, fallback) {
  return value && String(value).trim() ? String(value).trim() : (fallback || '');
}

function providerDisplayName(provider) {
  const name = firstNonEmpty(provider.name, provider.id);
  const model = provider.model || '';
  if (!model) return name;
  if (name.includes(model) || name.includes(model.split('/').pop())) return name;
  const shortModel = model.includes('/') ? model.split('/').pop() : model;
  return name + ' — ' + shortModel;
}

function providerModelPlaceholder(template) {
  const suggestion = firstNonEmpty(template && template.model_suggestion);
  return suggestion
    ? ('Type to search models, e.g. ' + suggestion)
    : 'Type to search models';
}

function syncProviderModelPlaceholder(template) {
  providerModelEl.placeholder = providerModelPlaceholder(
    template || providerTemplateById(providerTemplateEl.value)
  );
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

let infoTooltipEl = null;
let activeInfoHintEl = null;

function ensureInfoTooltip() {
  if (infoTooltipEl) return infoTooltipEl;
  const tooltip = document.createElement('div');
  tooltip.className = 'floating-info-tooltip';
  tooltip.hidden = true;
  document.body.appendChild(tooltip);
  infoTooltipEl = tooltip;
  return tooltip;
}

function positionInfoTooltip(anchor) {
  if (!infoTooltipEl || !anchor || infoTooltipEl.hidden) return;
  const margin = 12;
  const gap = 10;
  const rect = anchor.getBoundingClientRect();
  const maxWidth = Math.min(320, Math.max(180, window.innerWidth - (margin * 2)));
  infoTooltipEl.style.maxWidth = maxWidth + 'px';
  infoTooltipEl.style.left = '0px';
  infoTooltipEl.style.top = '0px';
  const tooltipRect = infoTooltipEl.getBoundingClientRect();
  let left = rect.left + (rect.width / 2) - (tooltipRect.width / 2);
  left = Math.max(margin, Math.min(left, window.innerWidth - tooltipRect.width - margin));
  let top = rect.bottom + gap;
  if (top + tooltipRect.height + margin > window.innerHeight) {
    top = Math.max(margin, rect.top - tooltipRect.height - gap);
  }
  infoTooltipEl.style.left = Math.round(left) + 'px';
  infoTooltipEl.style.top = Math.round(top) + 'px';
}

function showInfoTooltip(anchor) {
  const text = anchor && anchor.dataset ? anchor.dataset.tooltip : '';
  if (!text) return;
  const tooltip = ensureInfoTooltip();
  activeInfoHintEl = anchor;
  tooltip.textContent = text;
  tooltip.hidden = false;
  positionInfoTooltip(anchor);
}

function hideInfoTooltip(anchor) {
  if (anchor && activeInfoHintEl && anchor !== activeInfoHintEl) return;
  activeInfoHintEl = null;
  if (infoTooltipEl) infoTooltipEl.hidden = true;
}

function bindInfoHintEvents(hint) {
  if (!hint || hint.dataset.infoHintBound === 'true') return hint;
  hint.addEventListener('mouseenter', () => showInfoTooltip(hint));
  hint.addEventListener('mouseleave', () => hideInfoTooltip(hint));
  hint.addEventListener('focus', () => showInfoTooltip(hint));
  hint.addEventListener('blur', () => hideInfoTooltip(hint));
  hint.dataset.infoHintBound = 'true';
  return hint;
}

function buildInfoHint(text) {
  if (!text) return null;
  const hint = document.createElement('span');
  hint.className = 'info-hint';
  hint.textContent = '?';
  hint.tabIndex = 0;
  hint.dataset.tooltip = text;
  hint.setAttribute('role', 'img');
  hint.setAttribute('aria-label', text);
  return bindInfoHintEvents(hint);
}

function bindStaticInfoHints() {
  document.querySelectorAll('.info-hint[data-tooltip]').forEach((hint) => {
    bindInfoHintEvents(hint);
  });
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

function inferredSkillIdBase() {
  return normalizeIdSegment(skillNameEl.value) || 'skill';
}

function ensureSkillId() {
  const current = skillIdEl.value.trim();
  if (current) return current;
  const base = inferredSkillIdBase();
  const usedIds = new Set((state.admin.skills || []).map((skill) => skill.id).filter(Boolean));
  let candidate = base;
  let suffix = 2;
  while (usedIds.has(candidate)) {
    candidate = base + '-' + suffix;
    suffix += 1;
  }
  skillIdEl.value = candidate;
  return candidate;
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
  if (!bridge.enabled) return 'Disabled.';
  if (!bridge.keypair_ready) return 'Keys missing.';
  if (bridge.connection_state === 'connected') return 'Connected.';
  return 'Enabled.';
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
  const supportedCredentialSources = credentialSources.filter((source) =>
    source === 'none' || source === 'api-key' || source === 'oauth-account');
  return supportedCredentialSources.length ? supportedCredentialSources : ['api-key'];
}

function defaultProviderTemplateId() {
  const templates = Array.isArray(state.admin.llmProviderTemplates) ? state.admin.llmProviderTemplates : [];
  if (!templates.length) return '';
  return (templates.find((template) => template.id === 'openrouter')
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
  if (credentialSources.includes('oauth-account')) return 'oauth-account';
  if (credentialSources.includes('api-key')) return 'api-key';
  if (credentialSources.includes('none')) return 'none';
  return credentialSources[0] || 'api-key';
}

function providerCredentialSource(provider) {
  if (provider && (provider.credential_source === 'none'
    || provider.credential_source === 'api-key'
    || provider.credential_source === 'oauth-account')) {
    return provider.credential_source;
  }
  if (provider && (provider.auth_type === 'none'
    || provider.auth_type === 'api-key'
    || provider.auth_type === 'oauth-account')) {
    return provider.auth_type;
  }
  if (provider && provider.oauth_account) return 'oauth-account';
  if (provider && provider.api_key_configured) return 'api-key';
  if (provider && provider.browser_session) return 'api-key';
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

function currentProviderRecord() {
  return (state.admin.providers || []).find((provider) => provider.id === state.activeProviderId) || null;
}

const storedApiKeyMask = '__xia_stored_api_key__';

function normalizeBaseUrl(value) {
  return String(value || '').trim().replace(/\/+$/, '');
}

function findReusableApiKeyProvider(options = {}) {
  const templateId = firstNonEmpty(options.templateId, providerTemplateEl.value);
  const baseUrl = normalizeBaseUrl(Object.prototype.hasOwnProperty.call(options, 'baseUrl')
    ? options.baseUrl
    : providerBaseUrlEl.value);
  const excludeProviderId = Object.prototype.hasOwnProperty.call(options, 'excludeProviderId')
    ? options.excludeProviderId
    : '';
  const currentProvider = currentProviderRecord();
  if (currentProvider
      && (!excludeProviderId || currentProvider.id !== excludeProviderId)
      && providerCredentialSource(currentProvider) === 'api-key'
      && currentProvider.api_key_configured) {
    return currentProvider;
  }
  const providers = (state.admin.providers || []).filter((provider) =>
    provider
    && provider.id
    && provider.id !== excludeProviderId
    && providerCredentialSource(provider) === 'api-key'
    && provider.api_key_configured);
  if (templateId) {
    const templateMatch = providers.find((provider) => provider.template === templateId);
    if (templateMatch) return templateMatch;
  }
  if (baseUrl) {
    const baseUrlMatch = providers.find((provider) => normalizeBaseUrl(provider.base_url) === baseUrl);
    if (baseUrlMatch) return baseUrlMatch;
  }
  return null;
}

function clearStoredProviderApiKey(options = {}) {
  if (providerApiKeyEl.dataset.reuseStoredKey === 'true'
      && (options.clearValue || providerApiKeyEl.value === storedApiKeyMask)) {
    providerApiKeyEl.value = '';
  }
  delete providerApiKeyEl.dataset.reuseStoredKey;
  delete providerApiKeyEl.dataset.reuseProviderId;
  delete providerApiKeyEl.dataset.reuseProviderLabel;
  providerApiKeyNoteEl.textContent = '';
  providerApiKeyNoteEl.hidden = true;
}

function applyStoredProviderApiKey(provider) {
  if (!provider || !provider.id) {
    clearStoredProviderApiKey({ clearValue: true });
    return;
  }
  providerApiKeyEl.value = storedApiKeyMask;
  providerApiKeyEl.dataset.reuseStoredKey = 'true';
  providerApiKeyEl.dataset.reuseProviderId = provider.id;
  providerApiKeyEl.dataset.reuseProviderLabel = providerDisplayName(provider);
  providerApiKeyNoteEl.textContent = 'Using the stored API key from '
    + providerDisplayName(provider)
    + '. Enter a new key to override it.';
  providerApiKeyNoteEl.hidden = false;
}

function syncProviderApiKeyReuse() {
  const credentialSource = providerCredentialSourceEl.value;
  if (credentialSource !== 'api-key') {
    clearStoredProviderApiKey({ clearValue: true });
    return;
  }
  if (providerApiKeyEl.dataset.reuseStoredKey === 'true'
      && providerApiKeyEl.value !== storedApiKeyMask) {
    clearStoredProviderApiKey();
  }
  if (providerApiKeyEl.value.trim() && providerApiKeyEl.dataset.reuseStoredKey !== 'true') {
    providerApiKeyNoteEl.textContent = '';
    providerApiKeyNoteEl.hidden = true;
    return;
  }
  const reusableProvider = findReusableApiKeyProvider();
  if (reusableProvider) {
    applyStoredProviderApiKey(reusableProvider);
  } else {
    clearStoredProviderApiKey({ clearValue: true });
  }
}

function providerApiKeyValueForRequests() {
  if (providerApiKeyEl.dataset.reuseStoredKey === 'true') return '';
  return providerApiKeyEl.value.trim();
}

function providerApiKeyReuseProviderId() {
  return providerApiKeyEl.dataset.reuseStoredKey === 'true'
    ? firstNonEmpty(providerApiKeyEl.dataset.reuseProviderId)
    : '';
}

function providerRequestContextProviderId() {
  return state.activeProviderId || providerApiKeyReuseProviderId() || undefined;
}

function providerModelFetchCredentialState() {
  const credentialSource = providerCredentialSourceEl.value;
  const provider = currentProviderRecord();
  if (credentialSource === 'none' || credentialSource === 'local' || !credentialSource) {
    return { enabled: true, reason: '' };
  }
  if (credentialSource === 'api-key') {
    if (providerApiKeyEl.dataset.reuseStoredKey === 'true' || providerApiKeyEl.value.trim()) {
      return { enabled: true, reason: '' };
    }
    if (provider && providerCredentialSource(provider) === 'api-key' && provider.api_key_configured) {
      return { enabled: true, reason: '' };
    }
    return {
      enabled: false,
      reason: 'Enter an API key before fetching models.'
    };
  }
  if (credentialSource === 'oauth-account') {
    const accountId = providerOauthAccountEl.value.trim()
      || ((provider && providerCredentialSource(provider) === 'oauth-account' && provider.oauth_account) || '');
    const account = (state.admin.oauthAccounts || []).find((entry) => entry.id === accountId) || null;
    if (account && account.connected) {
      return { enabled: true, reason: '' };
    }
    if (accountId) {
      return {
        enabled: false,
        reason: 'Connect the selected API sign-in before fetching models.'
      };
    }
    if ((state.admin.oauthAccounts || []).length) {
      return {
        enabled: false,
        reason: 'Choose a connected API sign-in before fetching models.'
      };
    }
    return {
      enabled: false,
      reason: 'Set up an API sign-in before fetching models.'
    };
  }
  if (credentialSource === 'browser-session') {
    return {
      enabled: false,
      reason: 'Fetching models requires an API credential for this provider.'
    };
  }
  return { enabled: true, reason: '' };
}

function providerMeta(provider) {
  const bits = [];
  const template = providerTemplateById(provider.template);
  const accessMode = providerAccessMode(provider);
  const credentialSource = providerCredentialSource(provider);
  if (template) bits.push(template.name);
  if (provider.model) bits.push(provider.model);
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
  if (provider.system_prompt_budget || provider.history_budget) {
    const budgetParts = [];
    if (provider.system_prompt_budget) {
      budgetParts.push('system ' + formatTokenCountFull(provider.system_prompt_budget));
    }
    if (provider.history_budget) {
      budgetParts.push('history ' + formatTokenCountFull(provider.history_budget));
    }
    if (budgetParts.length) {
      bits.push('Budgets: ' + budgetParts.join(', '));
    }
  }
  if (provider.default) bits.push('Default');
  if (credentialSource === 'api-key' || ((credentialSource !== 'oauth-account' && credentialSource !== 'browser-session') && provider.api_key_configured)) {
    bits.push(provider.api_key_configured ? 'API key stored' : 'No API key');
  }
  return bits.join(' • ');
}

function llmProviderTemplateMeta(template) {
  if (template.category) return template.category;
  return '';
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

function oauthAccountSupportsConnectNow(account) {
  if (!account) return false;
  if ((account.connection_mode || 'oauth-flow') === 'manual-token') return false;
  return !!String(account.authorize_url || '').trim()
    && !!String(account.token_url || '').trim()
    && !!String(account.client_id || '').trim()
    && !!account.client_secret_configured;
}

function selectedProviderOauthAccount() {
  const accounts = state.admin.oauthAccounts || [];
  if (!accounts.length) return null;
  const selectedId = String(providerOauthAccountEl.value || '').trim();
  if (selectedId) {
    return accounts.find((entry) => (entry.id || '') === selectedId) || null;
  }
  return accounts.length === 1 ? (accounts[0] || null) : null;
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
    const linkedAccount = selectedProviderOauthAccount();
    if (linkedAccount && !linkedAccount.connected && oauthAccountSupportsConnectNow(linkedAccount)) {
      return {
        kind: 'oauth-connect',
        label: 'Connect API Sign-In',
        accountId: linkedAccount.id || ''
      };
    }
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
  if (action.kind === 'oauth-connect') {
    await beginProviderOauthSetup({
      quickConnect: true,
      accountId: action.accountId || ''
    });
    return;
  }
  if (action.kind === 'oauth') {
    await beginProviderOauthSetup();
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
  if (!template) return '';
  if (!String(template.base_url || '').trim()) {
    return 'Enter a compatible Base URL below.';
  }
  return '';
}

function providerTemplateTooltip(template) {
  if (!template) return '';
  const bits = [];
  if (template.description) bits.push(template.description);
  if (template.notes) bits.push(template.notes);
  return bits.join(' — ');
}

function renderProviderTemplateOptions() {
  const selected = providerTemplateEl.value || defaultProviderTemplateId();
  providerTemplateEl.replaceChildren();
  (state.admin.llmProviderTemplates || []).forEach((template) => {
    const option = document.createElement('option');
    option.value = template.id || '';
    option.textContent = template.name || template.id || 'Template';
    providerTemplateEl.appendChild(option);
  });
  const fallback = defaultProviderTemplateId()
    || ((state.admin.llmProviderTemplates || [])[0] || {}).id
    || '';
  providerTemplateEl.value = providerTemplateById(selected) ? selected : fallback;
  const selectedTemplate = providerTemplateById(providerTemplateEl.value);
  providerTemplateNoteEl.textContent = providerTemplateNote(selectedTemplate);
  providerTemplateEl.title = providerTemplateTooltip(selectedTemplate);
  syncProviderModelFetchUi();
}

function providerTemplateNeedsManualBaseUrl(template) {
  return !String(template && template.base_url || '').trim();
}

function providerModelFetchState() {
  const template = providerTemplateById(providerTemplateEl.value);
  const baseUrl = providerBaseUrlEl.value.trim();
  if (!baseUrl) {
    return {
      enabled: false,
      reason: 'Enter a compatible Base URL before fetching models.'
    };
  }
  const credentialState = providerModelFetchCredentialState();
  if (!credentialState.enabled) {
    return credentialState;
  }
  return { enabled: true, reason: '' };
}

function syncProviderModelFetchUi() {
  const template = providerTemplateById(providerTemplateEl.value);
  providerBaseUrlFieldEl.classList.toggle('force-visible', providerTemplateNeedsManualBaseUrl(template));
  const fetchState = providerModelFetchState();
  fetchProviderModelsEl.disabled = state.providerSaving || state.providerModelsFetching || !fetchState.enabled;
  fetchProviderModelsEl.title = fetchState.enabled ? '' : fetchState.reason;
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
    : ['none', 'api-key', 'oauth-account'];
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
  const accounts = state.admin.oauthAccounts || [];
  const previous = providerOauthAccountEl.value;
  providerOauthAccountEl.replaceChildren();
  accounts.forEach((account) => {
    const option = document.createElement('option');
    option.value = account.id || '';
    option.textContent = firstNonEmpty(account.name, account.id)
      + (account.connected ? ' (connected)' : ' (not connected)');
    providerOauthAccountEl.appendChild(option);
  });
  providerOauthAccountEl.value = accounts.some((account) => (account.id || '') === previous)
    ? previous
    : ((accounts[0] && accounts[0].id) || '');
}

function syncProviderOauthAccountSelection() {
  if (providerCredentialSourceEl.value !== 'oauth-account') {
    return;
  }
  const accounts = state.admin.oauthAccounts || [];
  if (!accounts.length) {
    return;
  }
  if (!providerOauthAccountEl.value
      || !accounts.some((account) => (account.id || '') === providerOauthAccountEl.value)) {
    providerOauthAccountEl.value = (accounts[0] && accounts[0].id) || '';
  }
}

function providerOauthAccountStatusNote() {
  if (providerCredentialSourceEl.value !== 'oauth-account') {
    return '';
  }
  const accounts = state.admin.oauthAccounts || [];
  if (!accounts.length) {
    return 'No API sign-in saved yet. Use Set Up API Sign-In to make one.';
  }
  if (accounts.length === 1 && accounts[0] && accounts[0].connected) {
    return '';
  }
  if (!providerOauthAccountEl.value) {
    return 'Choose a connected API sign-in for this provider, or use Set Up API Sign-In to make one.';
  }
  const account = accounts.find((entry) => entry.id === providerOauthAccountEl.value);
  if (!account) {
    return 'Choose a connected API sign-in for this provider, or use Set Up API Sign-In to make one.';
  }
  if (account.connected) {
    return 'This provider will use the linked OAuth API credential.';
  }
  if (oauthAccountSupportsConnectNow(account)) {
    return 'Click Connect API Sign-In to complete OAuth and link this provider.';
  }
  return 'The linked OAuth API sign-in is missing OAuth client details. Use Set Up API Sign-In first.';
}

function providerPrimaryActionTooltip(primaryAction) {
  if (providerCredentialSourceEl.value !== 'oauth-account') {
    return '';
  }
  if (primaryAction && primaryAction.label === 'Connect API Sign-In') {
    return 'Starts OAuth immediately using the selected saved API sign-in.';
  }
  if (!primaryAction || primaryAction.label !== 'Set Up API Sign-In') {
    return '';
  }
  if ((state.admin.oauthAccounts || []).length) {
    return '';
  }
  return 'Use Set Up API Sign-In to create a saved OAuth API sign-in, then link it here.';
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
  const oauthAccounts = state.admin.oauthAccounts || [];
  const hideOauthAccountPicker = credentialSource !== 'oauth-account' || oauthAccounts.length === 1;
  const actions = providerVisibleActions(template, accessMode, credentialSource);
  const primaryAction = actions[0] || null;
  const accountAction = actions.find((action) => action.label === 'Open Account') || null;
  const docsAction = actions.find((action) => action.label === 'Open Docs') || null;
  syncProviderOauthAccountSelection();
  providerTemplateEl.disabled = providerSaving;
  providerAccessModeEl.disabled = providerSaving;
  providerCredentialSourceEl.disabled = providerSaving;
  providerOauthAccountEl.hidden = hideOauthAccountPicker;
  providerOauthAccountEl.disabled = providerSaving || hideOauthAccountPicker;
  providerApiKeyEl.disabled = providerSaving || credentialSource !== 'api-key';
  providerBrowserSessionNoteEl.hidden = credentialSource !== 'browser-session';
  providerConfigureOauthEl.hidden = !primaryAction;
  providerConfigureOauthEl.textContent = primaryAction ? primaryAction.label : 'Open Setup';
  providerConfigureOauthEl.title = providerPrimaryActionTooltip(primaryAction);
  providerConfigureOauthEl.href = primaryAction && primaryAction.kind === 'external' && primaryAction.url
    ? primaryAction.url
    : '#';
  providerConfigureOauthEl.target = primaryAction && primaryAction.kind === 'external' ? '_blank' : '';
  providerConfigureOauthEl.rel = primaryAction && primaryAction.kind === 'external'
    ? 'noopener noreferrer'
    : '';
  providerConfigureOauthEl.setAttribute('aria-disabled',
    (providerSaving || state.oauthSaving || state.providerAccountConnecting || !primaryAction) ? 'true' : 'false');
  providerOpenAccountEl.hidden = !accountAction;
  providerOpenAccountEl.href = accountAction ? accountAction.url : '';
  providerOpenDocsEl.hidden = !docsAction;
  providerOpenDocsEl.href = docsAction ? docsAction.url : '';
  providerOauthAccountNoteEl.textContent = providerOauthAccountStatusNote();
  providerOauthAccountNoteEl.hidden = !providerOauthAccountNoteEl.textContent;
  providerBrowserSessionNoteEl.textContent = providerBrowserSessionStatusNote();
  syncProviderApiKeyReuse();
}

// ---------------------------------------------------------------------------
// Provider model autocomplete
// ---------------------------------------------------------------------------

function providerModelCapabilityKey(baseUrl, modelId) {
  return String(baseUrl || '').trim() + '::' + String(modelId || '').trim();
}

function parsePositiveInteger(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  const rounded = Math.round(number);
  return rounded > 0 ? rounded : null;
}

function formatTokenCountCompact(value) {
  const count = parsePositiveInteger(value);
  if (!count) return '';
  if (count >= 1000000) {
    const millions = count / 1000000;
    return (Number.isInteger(millions) ? String(millions) : String(Math.round(millions * 10) / 10)) + 'M';
  }
  if (count >= 1000) {
    const thousands = count / 1000;
    return (Number.isInteger(thousands) ? String(thousands) : String(Math.round(thousands * 10) / 10)) + 'K';
  }
  return String(count);
}

function formatTokenCountFull(value) {
  const count = parsePositiveInteger(value);
  if (!count) return '';
  return count.toLocaleString();
}

function cachedProviderModelCapability(modelId) {
  const normalizedModelId = String(modelId || '').trim();
  const baseUrl = providerBaseUrlEl.value.trim();
  if (!normalizedModelId || !baseUrl) return null;
  return state.providerModelCapabilities[providerModelCapabilityKey(baseUrl, normalizedModelId)] || null;
}

function setProviderModelCapability(modelId, capability = {}) {
  const normalizedModelId = (modelId || '').trim();
  const baseUrl = providerBaseUrlEl.value.trim();
  const normalizedCapability = {
    vision: !!capability.vision
  };
  const contextWindow = parsePositiveInteger(capability.contextWindow);
  const recommendedSystemPromptBudget = parsePositiveInteger(capability.recommendedSystemPromptBudget);
  const recommendedHistoryBudget = parsePositiveInteger(capability.recommendedHistoryBudget);
  const recommendedInputBudgetCap = parsePositiveInteger(capability.recommendedInputBudgetCap);
  if (contextWindow) {
    normalizedCapability.contextWindow = contextWindow;
  }
  if (capability.contextWindowSource) {
    normalizedCapability.contextWindowSource = String(capability.contextWindowSource);
  }
  if (recommendedSystemPromptBudget) {
    normalizedCapability.recommendedSystemPromptBudget = recommendedSystemPromptBudget;
  }
  if (recommendedHistoryBudget) {
    normalizedCapability.recommendedHistoryBudget = recommendedHistoryBudget;
  }
  if (recommendedInputBudgetCap) {
    normalizedCapability.recommendedInputBudgetCap = recommendedInputBudgetCap;
  }
  state.providerModelVisionModelId = normalizedModelId;
  state.providerModelVision = normalizedCapability.vision;
  if (normalizedModelId && baseUrl) {
    state.providerModelCapabilities[providerModelCapabilityKey(baseUrl, normalizedModelId)] = normalizedCapability;
  }
  renderProviderModelCapabilityNote();
  if (state.providerModels.length && !providerModelListEl.hidden) {
    renderProviderModelList();
  }
  return normalizedCapability;
}

function setProviderModelVision(modelId, vision, metadata = {}) {
  return setProviderModelCapability(modelId, Object.assign({}, metadata, { vision: !!vision }));
}

function currentProviderModelCapability() {
  const modelId = providerModelEl.value.trim();
  if (!modelId) return null;
  const cached = cachedProviderModelCapability(modelId);
  if (cached) return cached;
  if (modelId === state.providerModelVisionModelId) {
    return { vision: !!state.providerModelVision };
  }
  return null;
}

function clearProviderBudgetAutoFlags() {
  delete providerSystemPromptBudgetEl.dataset.autoBudget;
  delete providerHistoryBudgetEl.dataset.autoBudget;
}

function setProviderBudgetAutoFlags(options = {}) {
  if (options.system) {
    providerSystemPromptBudgetEl.dataset.autoBudget = 'metadata';
  }
  if (options.history) {
    providerHistoryBudgetEl.dataset.autoBudget = 'metadata';
  }
}

function maybeApplyProviderBudgetsFromMetadata(capability) {
  const result = { systemApplied: false, historyApplied: false };
  if (!capability) return result;
  const recommendedSystem = parsePositiveInteger(capability.recommendedSystemPromptBudget);
  const recommendedHistory = parsePositiveInteger(capability.recommendedHistoryBudget);
  if (!recommendedSystem || !recommendedHistory) return result;
  const canApplySystem = !providerSystemPromptBudgetEl.value.trim()
    || providerSystemPromptBudgetEl.dataset.autoBudget === 'metadata';
  const canApplyHistory = !providerHistoryBudgetEl.value.trim()
    || providerHistoryBudgetEl.dataset.autoBudget === 'metadata';
  if (canApplySystem) {
    providerSystemPromptBudgetEl.value = String(recommendedSystem);
    result.systemApplied = true;
  }
  if (canApplyHistory) {
    providerHistoryBudgetEl.value = String(recommendedHistory);
    result.historyApplied = true;
  }
  if (result.systemApplied || result.historyApplied) {
    setProviderBudgetAutoFlags({
      system: result.systemApplied,
      history: result.historyApplied
    });
  }
  return result;
}

function currentProviderVision() {
  const capability = currentProviderModelCapability();
  return !!(capability && capability.vision);
}

function currentProviderModelVisionKnown() {
  return !!currentProviderModelCapability();
}

function renderProviderModelCapabilityNote() {
  const modelId = providerModelEl.value.trim();
  if (state.providerModelMetadataLoading
      && modelId
      && modelId === state.providerModelMetadataModelId) {
    providerModelCapabilityNoteEl.textContent = 'Image input: checking...';
    return;
  }
  if (!currentProviderModelVisionKnown()) {
    providerModelCapabilityNoteEl.textContent = '';
    return;
  }
  const capability = currentProviderModelCapability() || { vision: false };
  const bits = [];
  bits.push(capability.vision ? 'Image input: supported.' : 'Image input: not supported.');
  if (capability.contextWindow) {
    bits.push('Context window: ' + formatTokenCountFull(capability.contextWindow) + ' tokens.');
  }
  if (capability.recommendedSystemPromptBudget && capability.recommendedHistoryBudget) {
    bits.push('Suggested budgets: '
      + formatTokenCountCompact(capability.recommendedSystemPromptBudget)
      + ' system / '
      + formatTokenCountCompact(capability.recommendedHistoryBudget)
      + ' history.');
  }
  providerModelCapabilityNoteEl.textContent = bits.join(' ');
}

function providerModelBadgeStates(modelId) {
  const normalizedModelId = String(modelId || '').trim();
  if (state.providerModelMetadataLoading
      && normalizedModelId
      && normalizedModelId === state.providerModelMetadataModelId) {
    return [{ label: 'Checking...', kind: 'checking' }];
  }
  const cached = cachedProviderModelCapability(normalizedModelId);
  if (!cached) return [];
  const badges = [];
  if (cached.contextWindow) {
    badges.push({ label: formatTokenCountCompact(cached.contextWindow) + ' ctx', kind: 'context' });
  }
  badges.push(cached.vision
    ? { label: 'Image', kind: 'vision' }
    : { label: 'Text', kind: 'text' });
  return badges;
}

function clearProviderModelVisionIfModelChanged() {
  const modelId = providerModelEl.value.trim();
  if (!modelId || modelId !== state.providerModelVisionModelId) {
    setProviderModelVision('', false);
  }
}

function providerModelWasFetched(modelId) {
  return !!modelId && state.providerModels.includes(modelId);
}

async function fetchProviderModelMetadata(modelId) {
  const normalizedModelId = (modelId || '').trim();
  const baseUrl = providerBaseUrlEl.value.trim();
  const apiKey = providerApiKeyValueForRequests();
  const providerId = providerRequestContextProviderId();
  if (!baseUrl || !normalizedModelId) return;
  state.providerModelMetadataLoading = true;
  state.providerModelMetadataModelId = normalizedModelId;
  renderProviderModelCapabilityNote();
  if (state.providerModels.length && !providerModelListEl.hidden) {
    renderProviderModelList();
  }
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/provider-model-metadata', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider_id: providerId,
        base_url: baseUrl,
        api_key: apiKey || undefined,
        model: normalizedModelId
      })
    });
    if (providerModelEl.value.trim() !== normalizedModelId) return;
    const model = data.model || {};
    const capability = setProviderModelVision(normalizedModelId, !!model.vision, {
      contextWindow: model.context_window,
      contextWindowSource: model.context_window_source,
      recommendedSystemPromptBudget: model.recommended_system_prompt_budget,
      recommendedHistoryBudget: model.recommended_history_budget,
      recommendedInputBudgetCap: model.recommended_input_budget_cap
    });
    const appliedBudgets = maybeApplyProviderBudgetsFromMetadata(capability);
    const statusBits = [];
    statusBits.push(capability.vision
      ? 'Selected model supports image input.'
      : 'Selected model appears text-only.');
    if (capability.contextWindow) {
      statusBits.push('Context window: ' + formatTokenCountFull(capability.contextWindow) + ' tokens.');
    }
    if (appliedBudgets.systemApplied || appliedBudgets.historyApplied) {
      statusBits.push('Budgets auto-set from model metadata.');
    }
    providerStatusEl.textContent = statusBits.join(' ');
  } catch (err) {
    if (providerModelEl.value.trim() !== normalizedModelId) return;
    setProviderModelVision('', false);
    providerStatusEl.textContent = err.message || 'Failed to inspect the selected model.';
  } finally {
    state.providerModelMetadataLoading = false;
    state.providerModelMetadataModelId = '';
    renderProviderModelCapabilityNote();
    if (state.providerModels.length && !providerModelListEl.hidden) {
      renderProviderModelList();
    }
    updateAdminButtons();
  }
}

function maybeFetchProviderModelMetadata() {
  const modelId = providerModelEl.value.trim();
  if (state.providerModelMetadataLoading) return;
  if (!providerModelWasFetched(modelId)) return;
  const cached = cachedProviderModelCapability(modelId);
  if (cached) {
    setProviderModelVision(modelId, !!cached.vision, {
      contextWindow: cached.contextWindow,
      contextWindowSource: cached.contextWindowSource,
      recommendedSystemPromptBudget: cached.recommendedSystemPromptBudget,
      recommendedHistoryBudget: cached.recommendedHistoryBudget,
      recommendedInputBudgetCap: cached.recommendedInputBudgetCap
    });
    maybeApplyProviderBudgetsFromMetadata(cached);
    return;
  }
  if (modelId === state.providerModelVisionModelId) return;
  fetchProviderModelMetadata(modelId);
}

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
    var label = document.createElement('span');
    label.className = 'autocomplete-item-label';
    if (query) {
      var idx = modelId.toLowerCase().indexOf(query);
      if (idx >= 0) {
        label.innerHTML = escapeHtml(modelId.substring(0, idx))
          + '<mark>' + escapeHtml(modelId.substring(idx, idx + query.length)) + '</mark>'
          + escapeHtml(modelId.substring(idx + query.length));
      } else {
        label.textContent = modelId;
      }
    } else {
      label.textContent = modelId;
    }
    btn.appendChild(label);
    var badgeStates = providerModelBadgeStates(modelId);
    badgeStates.forEach(function (badgeState) {
      var badge = document.createElement('span');
      badge.className = 'autocomplete-item-badge autocomplete-item-badge-' + badgeState.kind;
      badge.textContent = badgeState.label;
      btn.appendChild(badge);
    });
    btn.addEventListener('mousedown', function (e) {
      e.preventDefault();
      providerModelEl.value = modelId;
      providerModelListEl.hidden = true;
      clearProviderModelVisionIfModelChanged();
      fetchProviderModelMetadata(modelId);
    });
    providerModelListEl.appendChild(btn);
  });
  if (filtered.length > 80) {
    var more = document.createElement('div');
    more.className = 'autocomplete-item autocomplete-item-note';
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
  var apiKey = providerApiKeyValueForRequests();
  var providerId = providerRequestContextProviderId();
  var fetchState = providerModelFetchState();
  if (!fetchState.enabled) {
    providerStatusEl.textContent = fetchState.reason;
    return;
  }
  state.providerModelsFetching = true;
  syncProviderModelFetchUi();
  providerStatusEl.textContent = 'Fetching models...';
  try {
    var data = await fetchJson('/admin/provider-models', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider_id: providerId,
        base_url: baseUrl,
        api_key: apiKey || undefined
      })
    });
    state.providerModels = Array.isArray(data.models) ? data.models : [];
    providerStatusEl.textContent = state.providerModels.length
      ? state.providerModels.length + ' models available — type to filter.'
      : 'No models returned by this provider.';
    providerModelEl.focus();
    renderProviderModelList();
    maybeFetchProviderModelMetadata();
  } catch (err) {
    state.providerModels = [];
    providerStatusEl.textContent = err.message || 'Failed to fetch models.';
  } finally {
    state.providerModelsFetching = false;
    syncProviderModelFetchUi();
  }
}

function applyProviderTemplate(templateId, options = {}) {
  const template = providerTemplateById(templateId);
  if (!template) return;
  const preserveId = Object.prototype.hasOwnProperty.call(options, 'preserveId')
    ? options.preserveId
    : !!state.activeProviderId;
  providerTemplateEl.value = template.id || '';
  if (!options.preserveBaseUrl || !providerBaseUrlEl.value.trim()) {
    providerBaseUrlEl.value = template.base_url || '';
  }
  if (!options.preserveModel || !providerModelEl.value.trim()) {
    providerModelEl.value = '';
  }
  syncProviderModelPlaceholder(template);
  clearProviderModelVisionIfModelChanged();
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
  providerStatusEl.textContent = options.statusText || ('Template applied. Choose a model for ' + firstNonEmpty(template.name, template.id) + '.');
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
    providerBaseUrlEl.focus();
  }
}

function buildProviderTemplateCard(template) {
  const card = document.createElement('article');
  card.className = 'template-card';

  const tooltip = [template.description, template.notes].filter(Boolean).join(' — ');
  if (tooltip) card.title = tooltip;

  const head = document.createElement('div');
  head.className = 'template-card-head';
  const title = document.createElement('div');
  title.className = 'template-card-title';
  title.textContent = firstNonEmpty(template.name, template.id);
  head.appendChild(title);
  card.appendChild(head);

  const tags = document.createElement('div');
  tags.className = 'template-card-tags';
  providerTemplateAccessModes(template).forEach((accessMode) => {
    const badge = document.createElement('span');
    badge.className = 'template-badge';
    badge.textContent = firstNonEmpty(accessMode.label, providerAccessModeLabel(accessMode.id));
    if (accessMode.description) badge.title = accessMode.description;
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

  const actions = document.createElement('div');
  actions.className = 'actions';
  const useButton = document.createElement('button');
  useButton.type = 'button';
  useButton.className = 'secondary';
  useButton.textContent = 'Use Template';
  useButton.disabled = state.providerSaving;
  useButton.addEventListener('click', () => {
    if (!state.activeProviderId) {
      resetProviderForm('Create a model from a template.');
    }
    applyProviderTemplate(template.id, {
      preserveId: false,
      preserveBaseUrl: false,
      preserveModel: false,
      statusText: 'Template applied. Save the model when ready.'
    });
    providerCardEl.scrollIntoView({ block: 'start', behavior: 'smooth' });
  });
  actions.appendChild(useButton);
  providerVisibleActions(template,
    defaultProviderAccessMode(template),
    defaultProviderCredentialSource(template, defaultProviderAccessMode(template))).forEach((action) => {
    if (action.kind === 'external' && action.url) {
      const link = document.createElement('a');
      link.className = 'secondary-link';
      link.href = action.url;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = action.label;
      actions.appendChild(link);
    } else {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'secondary';
      button.textContent = action.label;
      button.disabled = state.providerSaving || state.oauthSaving;
      button.addEventListener('click', async () => {
        if (action.kind === 'oauth') {
          if (!state.activeProviderId) {
            resetProviderForm('Create a model from a template.');
          }
          applyProviderTemplate(template.id, {
            preserveId: false,
            preserveBaseUrl: false,
            preserveModel: false,
            statusText: 'Template applied. Finish setting up sign-in, then save the model.'
          });
          providerCardEl.scrollIntoView({ block: 'start', behavior: 'smooth' });
        }
        await openProviderPrimaryAction(action, {
          statusEl: providerOnboardingStatusEl,
          statusText: 'Opened setup for ' + firstNonEmpty(template.name, template.id) + '.',
          globalStatus: 'Opened provider setup'
        });
      });
      actions.appendChild(button);
    }
  });
  card.appendChild(actions);

  return card;
}

function renderProviderOnboarding() {
  const needsOnboarding = !!state.setupRequired && !state.admin.providers.length;
  providerOnboardingEl.hidden = !needsOnboarding;
  providerTemplateListEl.replaceChildren();
  if (!needsOnboarding) {
    syncProviderOnboardingModal();
    return;
  }
  const templates = state.admin.llmProviderTemplates || [];
  if (!templates.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = 'No provider templates are available yet.';
    providerTemplateListEl.appendChild(empty);
    providerOnboardingStatusEl.textContent = 'Add a model manually below.';
    syncProviderOnboardingModal();
    return;
  }
  templates.forEach((template) => {
    providerTemplateListEl.appendChild(buildProviderTemplateCard(template));
  });
  syncProviderOnboardingModal();
}

function sortedUniqueStrings(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).filter(Boolean))).sort();
}

function arraysEqual(left, right) {
  if (left.length !== right.length) return false;
  return left.every((value, index) => value === right[index]);
}

function buildWorkloadRoutingDraft(providers = state.admin.providers || []) {
  const draft = {};
  (state.admin.llmWorkloads || []).forEach((workload) => {
    if (workload && workload.id) draft[workload.id] = [];
  });
  providers.forEach((provider) => {
    sortedUniqueStrings(provider.workloads).forEach((workloadId) => {
      if (!draft[workloadId]) draft[workloadId] = [];
      draft[workloadId].push(provider.id);
    });
  });
  Object.keys(draft).forEach((workloadId) => {
    draft[workloadId] = sortedUniqueStrings(draft[workloadId]);
  });
  return draft;
}

function currentWorkloadRoutingDraft() {
  return state.workloadRoutingDraft || buildWorkloadRoutingDraft();
}

function parseWorkloadRoutingInput() {
  const draft = buildWorkloadRoutingDraft([]);
  document.querySelectorAll('.workload-route-provider-check:checked').forEach((input) => {
    const workloadId = input.dataset.workloadId;
    const providerId = input.dataset.providerId;
    if (!workloadId || !providerId) return;
    if (!draft[workloadId]) draft[workloadId] = [];
    draft[workloadId].push(providerId);
  });
  Object.keys(draft).forEach((workloadId) => {
    draft[workloadId] = sortedUniqueStrings(draft[workloadId]);
  });
  return draft;
}

function providerWorkloadsFromDraft(providerId, draft = currentWorkloadRoutingDraft()) {
  return Object.keys(draft)
    .filter((workloadId) => Array.isArray(draft[workloadId]) && draft[workloadId].includes(providerId))
    .sort();
}

function workloadRoutingHasChanges(draft = currentWorkloadRoutingDraft()) {
  return (state.admin.providers || []).some((provider) => {
    const current = sortedUniqueStrings(provider.workloads);
    const next = providerWorkloadsFromDraft(provider.id, draft);
    return !arraysEqual(current, next);
  });
}

function handleWorkloadRoutingInputChange() {
  state.workloadRoutingDraft = parseWorkloadRoutingInput();
  state.workloadRoutingStatus = workloadRoutingHasChanges(state.workloadRoutingDraft)
    ? 'Routing changes pending. Save to apply them.'
    : 'Routing matches the saved configuration.';
  renderWorkloadRouting();
  updateAdminButtons();
}

async function saveWorkloadRouting() {
  if (state.workloadRoutingSaving) return;
  const draft = currentWorkloadRoutingDraft();
  const updates = (state.admin.providers || []).map((provider) => {
    const workloads = providerWorkloadsFromDraft(provider.id, draft);
    if (arraysEqual(sortedUniqueStrings(provider.workloads), workloads)) return null;
    const credentialSource = providerCredentialSource(provider);
    return {
      id: provider.id,
      name: provider.name || provider.id,
      template: provider.template || '',
      base_url: provider.base_url,
      model: provider.model,
      access_mode: provider.access_mode || providerAccessMode(provider),
      credential_source: credentialSource,
      oauth_account: credentialSource === 'oauth-account'
        ? (provider.oauth_account || '')
        : '',
      workloads,
      system_prompt_budget: provider.system_prompt_budget || '',
      history_budget: provider.history_budget || '',
      rate_limit_per_minute: provider.rate_limit_per_minute || '',
      vision: !!provider.vision,
      allow_private_network: !!provider.allow_private_network
    };
  }).filter(Boolean);

  if (!updates.length) {
    state.workloadRoutingStatus = 'Routing matches the saved configuration.';
    workloadRoutingStatusEl.textContent = state.workloadRoutingStatus;
    updateAdminButtons();
    return;
  }

  state.workloadRoutingSaving = true;
  state.workloadRoutingStatus = 'Saving...';
  renderWorkloadRouting();
  updateAdminButtons();
  try {
    for (const update of updates) {
      await fetchJson('/admin/providers', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(update)
      });
    }
    await loadAdminConfig();
    state.workloadRoutingStatus = 'Workload routing saved.';
    workloadRoutingStatusEl.textContent = state.workloadRoutingStatus;
    setStatus('Workload routing saved');
  } catch (err) {
    state.workloadRoutingStatus = err.message || 'Failed to save workload routing.';
    workloadRoutingStatusEl.textContent = state.workloadRoutingStatus;
  } finally {
    state.workloadRoutingSaving = false;
    renderWorkloadRouting();
    updateAdminButtons();
  }
}

function renderWorkloadRouting() {
  const container = document.getElementById('workload-routing-list');
  if (!container) return;
  container.innerHTML = '';
  const workloads = state.admin.llmWorkloads || [];
  const providers = state.admin.providers || [];
  const draft = currentWorkloadRoutingDraft();
  const defaultProvider = providers.find((provider) => provider.default) || null;

  workloadRoutingStatusEl.textContent = state.workloadRoutingStatus || '';

  workloads.forEach(wl => {
    const assignedProviderIds = sortedUniqueStrings(draft[wl.id]);
    const assigned = providers.filter((provider) => assignedProviderIds.includes(provider.id));

    const row = document.createElement('div');
    row.className = 'workload-route';

    const header = document.createElement('div');
    header.className = 'workload-route-header';

    const name = document.createElement('div');
    name.className = 'workload-route-name';
    const nameText = document.createElement('span');
    nameText.className = 'workload-route-name-text';
    nameText.textContent = wl.label;
    name.appendChild(nameText);
    const hint = buildInfoHint(wl.description);
    if (hint) name.appendChild(hint);

    const count = document.createElement('div');
    count.className = 'workload-route-count';
    count.textContent = assigned.length
      ? assigned.length + ' provider' + (assigned.length > 1 ? 's' : '') + ' (round-robin)'
      : (defaultProvider ? 'Falls back to default provider' : 'No providers assigned');

    header.appendChild(name);
    header.appendChild(count);
    row.appendChild(header);

    if (assigned.length) {
      const list = document.createElement('div');
      list.className = 'workload-route-providers';
      assigned.forEach(p => {
        const tag = document.createElement('span');
        tag.className = 'workload-provider-tag';
        tag.textContent = providerDisplayName(p);
        if (p.model) tag.title = p.model;
        list.appendChild(tag);
      });
      row.appendChild(list);
    } else if (defaultProvider) {
      const list = document.createElement('div');
      list.className = 'workload-route-providers';

      const tag = document.createElement('span');
      tag.className = 'workload-provider-tag';
      tag.textContent = providerDisplayName(defaultProvider);
      tag.title = 'Default provider';
      list.appendChild(tag);
      row.appendChild(list);

      const empty = document.createElement('div');
      empty.className = 'workload-route-empty';
      empty.textContent = 'No dedicated provider assigned; requests use the default model.';
      row.appendChild(empty);
    } else {
      const empty = document.createElement('div');
      empty.className = 'workload-route-empty';
      empty.textContent = 'Add models in AI Models first.';
      row.appendChild(empty);
    }

    if (providers.length) {
      const controls = document.createElement('div');
      controls.className = 'workload-route-controls';
      providers.forEach((provider) => {
        const option = document.createElement('label');
        option.className = 'workload-route-option';

        const input = document.createElement('input');
        input.type = 'checkbox';
        input.className = 'workload-route-provider-check';
        input.checked = assignedProviderIds.includes(provider.id);
        input.disabled = state.workloadRoutingSaving;
        input.dataset.workloadId = wl.id;
        input.dataset.providerId = provider.id;
        input.addEventListener('change', handleWorkloadRoutingInputChange);
        option.appendChild(input);

        const text = document.createElement('span');
        text.className = 'check-label';
        const labelText = document.createElement('span');
        labelText.className = 'check-label-text';
        labelText.textContent = providerDisplayName(provider);
        text.appendChild(labelText);
        if (provider.default) {
          const badge = document.createElement('span');
          badge.className = 'admin-item-capability-badge';
          badge.textContent = 'Default';
          text.appendChild(badge);
        }
        option.appendChild(text);
        controls.appendChild(option);
      });
      row.appendChild(controls);
    }

    container.appendChild(row);
  });
}

function normalizeIdSegment(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function computedProviderName(options = {}) {
  const templateId = Object.prototype.hasOwnProperty.call(options, 'templateId')
    ? options.templateId
    : providerTemplateEl.value;
  const modelValue = Object.prototype.hasOwnProperty.call(options, 'model')
    ? options.model
    : providerModelEl.value;
  const template = providerTemplateById(templateId);
  const templateName = firstNonEmpty(template && template.name, templateId, 'Model');
  const model = String(modelValue || '').trim();
  if (!model) return templateName;
  const shortModel = model.includes('/') ? model.split('/').pop() : model;
  return templateName + ' — ' + shortModel;
}

function ensureProviderId() {
  const current = providerIdEl.value.trim();
  const isEditing = !!state.activeProviderId;
  if (isEditing && current) return current;
  const templateId = normalizeIdSegment(providerTemplateEl.value);
  const modelId = normalizeIdSegment(providerModelEl.value);
  const base = (templateId || 'provider') + (modelId ? '-' + modelId : '');
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

function ensureProviderName() {
  return computedProviderName();
}

function inferredSiteIdBase() {
  const explicitName = normalizeIdSegment(siteNameEl.value);
  if (explicitName) return explicitName;
  try {
    const loginUrl = siteLoginUrlEl.value.trim();
    if (!loginUrl) return 'site';
    const url = new URL(loginUrl);
    const host = normalizeIdSegment(url.hostname.replace(/^www\./, ''));
    const path = normalizeIdSegment(url.pathname);
    return [host, path].filter(Boolean).join('-') || host || 'site';
  } catch (_) {
    return normalizeIdSegment(siteLoginUrlEl.value) || 'site';
  }
}

function ensureSiteId() {
  const current = siteIdEl.value.trim();
  const isEditing = !!state.activeSiteId;
  if (isEditing && current) return current;
  const base = inferredSiteIdBase();
  const usedIds = new Set((state.admin.sites || []).map((site) => site.id).filter(Boolean));
  let candidate = base;
  let suffix = 2;
  while (usedIds.has(candidate)) {
    candidate = base + '-' + suffix;
    suffix += 1;
  }
  siteIdEl.value = candidate;
  return candidate;
}

function scrollCardIntoView(cardEl) {
  if (!cardEl || typeof cardEl.scrollIntoView !== 'function') return;
  cardEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function captureProviderDraft() {
  return {
    id: providerIdEl.value.trim(),
    name: ensureProviderName(),
    template: providerTemplateEl.value,
    baseUrl: providerBaseUrlEl.value,
    model: providerModelEl.value,
    accessMode: providerAccessModeEl.value,
    credentialSource: providerCredentialSourceEl.value,
    oauthAccount: providerOauthAccountEl.value,
    browserSession: providerBrowserSessionEl.value,
    systemPromptBudget: providerSystemPromptBudgetEl.value,
    historyBudget: providerHistoryBudgetEl.value,
    systemPromptBudgetAuto: providerSystemPromptBudgetEl.dataset.autoBudget === 'metadata',
    historyBudgetAuto: providerHistoryBudgetEl.dataset.autoBudget === 'metadata',
    rateLimitPerMinute: providerRateLimitEl.value,
    vision: currentProviderVision(),
    apiKey: providerApiKeyEl.dataset.reuseStoredKey === 'true' ? '' : providerApiKeyEl.value,
    default: providerDefaultEl.checked
  };
}

function restoreProviderDraft(draft, options = {}) {
  if (!draft) return false;
  state.activeProviderId = '';
  providerDetailHeaderEl.textContent = draft.model || draft.id || 'New model';
  providerIdEl.value = draft.id || '';
  providerTemplateEl.value = draft.template || defaultProviderTemplateId();
  providerBaseUrlEl.value = draft.baseUrl || '';
  providerModelEl.value = draft.model || '';
  renderProviderTemplateOptions();
  providerTemplateEl.value = draft.template || providerTemplateEl.value || defaultProviderTemplateId();
  syncProviderModelPlaceholder(providerTemplateById(providerTemplateEl.value));
  renderProviderAccessModeOptions();
  providerAccessModeEl.value = draft.accessMode || defaultProviderAccessMode(providerTemplateById(providerTemplateEl.value));
  renderProviderCredentialSourceOptions();
  providerCredentialSourceEl.value = draft.credentialSource
    || defaultProviderCredentialSource(providerTemplateById(providerTemplateEl.value), providerAccessModeEl.value);
  renderProviderOauthAccountOptions();
  providerOauthAccountEl.value = draft.oauthAccount || '';
  providerBrowserSessionEl.value = draft.browserSession || '';
  providerSystemPromptBudgetEl.value = draft.systemPromptBudget || '';
  providerHistoryBudgetEl.value = draft.historyBudget || '';
  clearProviderBudgetAutoFlags();
  setProviderBudgetAutoFlags({
    system: !!draft.systemPromptBudgetAuto,
    history: !!draft.historyBudgetAuto
  });
  providerRateLimitEl.value = draft.rateLimitPerMinute || '';
  setProviderModelVision(draft.model || '', !!draft.vision);
  providerApiKeyEl.value = draft.apiKey || '';
  providerDefaultEl.checked = !!draft.default;
  providerStatusEl.textContent = options.statusText || 'Continue configuring the model.';
  renderProviderList();
  renderWorkloadRouting();
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
  const base = normalizeIdSegment(draft.id || draft.name || draft.template || 'provider');
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
      providerName: computedProviderName({ templateId: providerTemplateEl.value })
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
        providerName: pending.providerName || computedProviderName({ templateId: providerTemplateEl.value })
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

async function beginProviderOauthSetup(options = {}) {
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
  const requestedAccountId = String(options.accountId || '').trim();
  const linkedAccountId = requestedAccountId || String(draft.oauthAccount || '').trim();
  const linkedAccount = (state.admin.oauthAccounts || []).find((account) => (account.id || '') === linkedAccountId);
  state.providerDraft = draft;
  state.pendingProviderOauthFlow = {
    accountId: linkedAccount ? linkedAccount.id : '',
    providerName: firstNonEmpty(draft.name, template && template.name, draft.template, 'provider')
  };

  if (linkedAccount) {
    selectOauthAccount(linkedAccount);
    if (options.quickConnect && !linkedAccount.connected && oauthAccountSupportsConnectNow(linkedAccount)) {
      providerStatusEl.textContent = 'Opening OAuth sign-in...';
      await connectOauthAccount();
      return;
    }
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
  syncProviderModelFetchUi();
  saveProviderEl.disabled = state.providerSaving;
  deleteProviderEl.disabled = state.providerSaving || !state.activeProviderId;
  saveWorkloadRoutingEl.disabled = state.workloadRoutingSaving
    || !state.admin.providers.length
    || !workloadRoutingHasChanges();
  providerConfigureOauthEl.setAttribute('aria-disabled',
    (state.providerSaving || state.oauthSaving || !currentProviderPrimaryAction()) ? 'true' : 'false');
  contextRecentHistoryMessageLimitEl.disabled = state.contextSaving;
  contextHistoryBudgetEl.disabled = state.contextSaving;
  saveContextEl.disabled = state.contextSaving;
  saveRetentionEl.disabled = state.retentionSaving;
  saveKnowledgeDecayEl.disabled = state.knowledgeDecaySaving;
  localDocModelSummaryBackendEl.disabled = state.localDocSummarizationSaving;
  localDocModelSummaryProviderIdEl.disabled = state.localDocSummarizationSaving
    || localDocModelSummaryBackendEl.value !== 'external';
  localDocChunkSummaryMaxTokensEl.disabled = state.localDocSummarizationSaving;
  localDocDocSummaryMaxTokensEl.disabled = state.localDocSummarizationSaving;
  saveLocalDocSummarizationEl.disabled = state.localDocSummarizationSaving;
  localDocOcrModelBackendEl.disabled = state.localDocOcrSaving;
  localDocOcrExternalProviderIdEl.disabled = state.localDocOcrSaving
    || localDocOcrModelBackendEl.value !== 'external'
    || !localDocOcrExternalProviderIdEl.options.length;
  localDocOcrTimeoutMsEl.disabled = state.localDocOcrSaving;
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
  connectOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId || manualOauthToken;
  refreshOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId || manualOauthToken || !(activeOauthAccount && activeOauthAccount.refresh_token_configured);
  deleteOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId;
  serviceIdEl.disabled = state.serviceSaving || !!state.activeServiceId;
  saveServiceEl.disabled = state.serviceSaving;
  newServiceEl.disabled = state.serviceSaving;
  siteIdEl.disabled = state.siteSaving || !!state.activeSiteId;
  saveSiteEl.disabled = state.siteSaving;
  deleteSiteEl.disabled = state.siteSaving || !state.activeSiteId;
  skillIdEl.disabled = state.skillSaving || !!state.activeSkillId;
  skillNameEl.disabled = state.skillSaving;
  skillDescriptionEl.disabled = state.skillSaving;
  skillTagsEl.disabled = state.skillSaving;
  skillEnabledEl.disabled = state.skillSaving;
  skillContentEl.disabled = state.skillSaving;
  saveSkillEl.disabled = state.skillSaving;
  deleteSkillEl.disabled = state.skillSaving || !state.activeSkillId;
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
  const providerCount = state.admin.providers.length;
  providerCountLabelEl.textContent = '(' + providerCount + ' '
    + pluralize(providerCount, 'model') + ')';
  providerListEl.replaceChildren();
  if (!state.admin.providers.length) {
    const empty = document.createElement('div');
    empty.className = 'admin-list-empty';
    empty.textContent = 'No models configured yet. Add one so Xia can talk to an LLM.';
    providerListEl.appendChild(empty);
    return;
  }
  state.admin.providers.forEach((provider) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'admin-item' + (provider.id === state.activeProviderId ? ' active' : '');

    const titleRow = document.createElement('div');
    titleRow.className = 'admin-item-title-row';

    const title = document.createElement('div');
    title.className = 'admin-item-title';
    title.textContent = providerDisplayName(provider);
    titleRow.appendChild(title);

    if (provider.vision) {
      const badges = document.createElement('div');
      badges.className = 'admin-item-badges';
      const badge = document.createElement('span');
      badge.className = 'admin-item-capability-badge';
      badge.textContent = 'Image input';
      badges.appendChild(badge);
      titleRow.appendChild(badges);
    }

    const meta = document.createElement('div');
    meta.className = 'admin-item-meta';
    meta.textContent = providerMeta(provider);

    button.appendChild(titleRow);
    button.appendChild(meta);
    button.addEventListener('click', () => {
      const result = selectProvider(provider);
      if (result && typeof result.catch === 'function') {
        result.catch((err) => console.error('Selection handler failed:', err));
      }
    });
    providerListEl.appendChild(button);
  });
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
    (site) => {
      if ((site && site.id || '') === state.activeSiteId) {
        resetSiteForm('Create a site login or select an existing one.');
        return;
      }
      return selectSite(site);
    }
  );
}

function renderToolList() {
  renderCapabilityList(
    toolListEl,
    state.admin.tools,
    'No tools installed.',
    (tool) => [tool.id, tool.approval ? ('approval: ' + tool.approval) : '', tool.description || '']
      .filter(Boolean)
      .join(' • ')
  );
}

function renderSkillList() {
  renderSelectableList(
    skillListEl,
    state.admin.skills,
    state.activeSkillId,
    'No skills installed.',
    (skill) => firstNonEmpty(skill.name, skill.id),
    skillMeta,
    (skill) => {
      if ((skill && skill.id || '') === state.activeSkillId) {
        resetSkillForm('Create a skill or select an existing one.');
        return;
      }
      return loadSkill(skill.id);
    }
  );
}

function renderCapabilities() {
  renderToolList();
  renderSkillList();
  renderRemoteBridge();
  renderOpenClawImport();
}

function resetProviderForm(statusText) {
  state.activeProviderId = '';
  state.providerDraft = null;
  state.pendingProviderOauthFlow = null;
  state.pendingProviderBrowserSessionFlow = null;
  providerDetailHeaderEl.textContent = 'New model';
  providerIdEl.value = '';
  providerTemplateEl.value = defaultProviderTemplateId();
  providerBaseUrlEl.value = '';
  providerModelEl.value = '';
  providerAccessModeEl.value = '';
  providerCredentialSourceEl.value = '';
  providerOauthAccountEl.value = '';
  providerBrowserSessionEl.value = '';
  providerSystemPromptBudgetEl.value = '';
  providerHistoryBudgetEl.value = '';
  clearProviderBudgetAutoFlags();
  providerRateLimitEl.value = '';
  setProviderModelVision('', false);
  providerApiKeyEl.value = '';
  providerDefaultEl.checked = !state.admin.providers.some((provider) => provider.default);
  renderProviderTemplateOptions();
  syncProviderModelPlaceholder(providerTemplateById(providerTemplateEl.value));
  renderProviderAccessModeOptions();
  renderProviderCredentialSourceOptions();
  renderProviderOauthAccountOptions();
  const templateId = providerTemplateEl.value;
  if (templateId) {
    applyProviderTemplate(templateId, {
      preserveId: false,
      preserveBaseUrl: false,
      preserveModel: false,
      statusText: statusText || 'Choose a model template, review the fields, then save it.'
    });
  } else {
    providerStatusEl.textContent = statusText || 'Create a model or select an existing one.';
  }
  renderProviderList();
  renderWorkloadRouting();
  updateAdminButtons();
}

function renderMemoryRetentionSettings() {
  const settings = state.admin.memoryRetention || {};
  retentionFullResolutionDaysEl.value = settings.full_resolution_days || '';
  retentionDecayHalfLifeDaysEl.value = settings.decay_half_life_days || '';
  retentionRetainedCountEl.value = settings.retained_count || '';
  retentionStatusEl.textContent = '';
  updateAdminButtons();
}

function resetMemoryRetentionForm(statusText) {
  retentionFullResolutionDaysEl.value = '';
  retentionDecayHalfLifeDaysEl.value = '';
  retentionRetainedCountEl.value = '';
  retentionStatusEl.textContent = statusText || '';
  updateAdminButtons();
}

function defaultConversationContextStatus() {
  return '';
}

function syncWebSearchInputs() {
  const backend = searchBackendEl.value || '';
  if (searchBraveApiKeyFieldEl) {
    searchBraveApiKeyFieldEl.hidden = backend !== 'brave-json';
  }
}

function renderWebSearchSettings() {
  const settings = state.admin.webSearch || {};
  searchBackendEl.value = settings.backend || '';
  searchBraveApiKeyEl.value = settings.brave_api_key || '';
  searchSearxngUrlEl.value = settings.searxng_url || '';
  syncWebSearchInputs();
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
  contextHistoryBudgetEl.value = settings.history_budget || '';
  contextStatusEl.textContent = state.contextStatus || defaultConversationContextStatus();
  updateAdminButtons();
}

function resetConversationContextForm(statusText) {
  contextRecentHistoryMessageLimitEl.value = '';
  contextHistoryBudgetEl.value = '';
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
  knowledgeDecayStatusEl.textContent = '';
  updateAdminButtons();
}

function defaultLocalDocSummarizationStatus() {
  const settings = state.admin.localDocSummarization || {};
  if (!settings.model_summaries_enabled) return 'Using extractive default.';
  return settings.model_summary_backend === 'external'
    ? 'Using model-generated summaries from an external provider.'
    : 'Using model-generated summaries locally.';
}

function renderLocalDocSummarizationProviderOptions() {
  const settings = state.admin.localDocSummarization || {};
  const selected = firstNonEmpty(localDocModelSummaryProviderIdEl.value,
    settings.model_summary_provider_id,
    '');
  localDocModelSummaryProviderIdEl.replaceChildren();
  const blank = document.createElement('option');
  blank.value = '';
  blank.textContent = 'Default provider';
  localDocModelSummaryProviderIdEl.appendChild(blank);
  state.admin.providers.forEach((provider) => {
    const option = document.createElement('option');
    option.value = provider.id || '';
    option.textContent = providerDisplayName(provider);
    localDocModelSummaryProviderIdEl.appendChild(option);
  });
  localDocModelSummaryProviderIdEl.value = selected || '';
}

function renderLocalDocSummarizationSettings() {
  const settings = state.admin.localDocSummarization || {};
  const summaryMode = firstNonEmpty(state.localDocSummarizationDraftMode,
    settings.model_summaries_enabled ? settings.model_summary_backend : 'default',
    'default');
  const isExternal = summaryMode === 'external';
  localDocModelSummaryBackendEl.value = summaryMode;
  renderLocalDocSummarizationProviderOptions();
  localDocChunkSummaryMaxTokensEl.value = settings.chunk_summary_max_tokens || '';
  localDocDocSummaryMaxTokensEl.value = settings.doc_summary_max_tokens || '';
  localDocModelSummaryProviderFieldEl.hidden = !isExternal;
  localDocModelSummaryProviderIdEl.disabled = state.localDocSummarizationSaving
    || !isExternal;
  localDocSummarizationStatusEl.textContent = state.localDocSummarizationStatus || defaultLocalDocSummarizationStatus();
  updateAdminButtons();
}

function defaultLocalDocOcrStatus() {
  const settings = state.admin.localDocOcr || {};
  if (!settings.enabled) return 'Disabled.';
  if (settings.model_backend === 'external') {
    return settings.configured
      ? 'Enabled.'
      : 'Select a vision-capable external provider.';
  }
  return 'Enabled. Xia will use managed PaddleOCR models.';
}

function renderLocalDocOcrProviderOptions() {
  const settings = state.admin.localDocOcr || {};
  const selected = firstNonEmpty(localDocOcrExternalProviderIdEl.value,
    settings.external_provider_id,
    '');
  const visionProviders = state.admin.providers.filter((provider) => !!provider.vision);
  const selectedProvider = selected
    ? state.admin.providers.find((provider) => (provider.id || '') === selected)
    : null;
  const providers = selectedProvider
      && !visionProviders.some((provider) => (provider.id || '') === selected)
    ? visionProviders.concat([selectedProvider])
    : visionProviders;
  localDocOcrExternalProviderIdEl.replaceChildren();
  providers.forEach((provider) => {
    const option = document.createElement('option');
    option.value = provider.id || '';
    option.textContent = providerDisplayName(provider);
    localDocOcrExternalProviderIdEl.appendChild(option);
  });
  if (selected && providers.some((provider) => (provider.id || '') === selected)) {
    localDocOcrExternalProviderIdEl.value = selected;
  } else {
    localDocOcrExternalProviderIdEl.selectedIndex = -1;
  }
}

function renderLocalDocOcrSettings() {
  const settings = state.admin.localDocOcr || {};
  const modelBackend = firstNonEmpty(state.localDocOcrDraftMode,
    settings.enabled ? settings.model_backend : 'disabled',
    'disabled');
  const isExternal = modelBackend === 'external';
  const isLocal = modelBackend === 'local';
  localDocOcrModelBackendEl.value = modelBackend;
  renderLocalDocOcrProviderOptions();
  if (settings.external_provider_id
      && Array.from(localDocOcrExternalProviderIdEl.options)
        .some((option) => option.value === settings.external_provider_id)) {
    localDocOcrExternalProviderIdEl.value = settings.external_provider_id;
  }
  localDocOcrTimeoutMsEl.value = settings.timeout_ms || '';
  localDocOcrMaxTokensEl.value = settings.max_tokens || '';
  localDocOcrLocalNoteFieldEl.hidden = !isLocal;
  localDocOcrExternalProviderFieldEl.hidden = !isExternal;
  localDocOcrExternalProviderIdEl.disabled = state.localDocOcrSaving
    || !isExternal
    || !localDocOcrExternalProviderIdEl.options.length;
  localDocOcrStatusEl.textContent = state.localDocOcrStatus || defaultLocalDocOcrStatus();
  updateAdminButtons();
}

function defaultDatabaseBackupStatus() {
  const settings = state.admin.databaseBackup || {};
  if (settings.last_error) return 'Last backup failed: ' + settings.last_error;
  if (settings.running) return 'Running now...';
  if (!settings.enabled) return 'Disabled.';
  return 'Enabled.';
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

function resetSkillForm(statusText) {
  state.activeSkillId = '';
  skillIdEl.value = '';
  skillNameEl.value = '';
  skillDescriptionEl.value = '';
  skillTagsEl.value = '';
  skillEnabledEl.checked = true;
  skillContentEl.value = '';
  skillStatusEl.textContent = statusText || 'Create a skill or select an existing one.';
  renderSkillList();
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
  providerDetailHeaderEl.textContent = providerDisplayName(provider);
  providerIdEl.value = provider.id || '';
  providerTemplateEl.value = provider.template || '';
  providerBaseUrlEl.value = provider.base_url || '';
  providerModelEl.value = provider.model || '';
  renderProviderTemplateOptions();
  syncProviderModelPlaceholder(providerTemplateById(providerTemplateEl.value));
  renderProviderAccessModeOptions();
  providerAccessModeEl.value = provider.access_mode || providerAccessMode(provider);
  renderProviderCredentialSourceOptions();
  providerCredentialSourceEl.value = provider.credential_source || providerCredentialSource(provider);
  renderProviderOauthAccountOptions();
  providerOauthAccountEl.value = provider.oauth_account || '';
  providerBrowserSessionEl.value = provider.browser_session || '';
  providerSystemPromptBudgetEl.value = provider.system_prompt_budget || '';
  providerHistoryBudgetEl.value = provider.history_budget || '';
  clearProviderBudgetAutoFlags();
  providerRateLimitEl.value = provider.rate_limit_per_minute || '';
  setProviderModelVision(provider.model || '', !!provider.vision);
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
  renderProviderList();
  renderWorkloadRouting();
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
  siteFormSelectorEl.value = site.form_selector || '';
  siteExtraFieldsEl.value = site.extra_fields || '';
  siteAutonomousApprovedEl.checked = !!site.autonomous_approved;
  siteStatusEl.textContent = site.username_configured ? 'Credentials stored.' : 'No credentials stored.';
  renderSiteList();
  updateAdminButtons();
}

async function loadSkill(skillId) {
  if (!skillId) return;
  state.activeSkillId = skillId;
  skillStatusEl.textContent = 'Loading skill...';
  renderSkillList();
  updateAdminButtons();
  try {
    const data = await fetchJson('/admin/skills/' + encodeURIComponent(skillId));
    const skill = data.skill || {};
    if (state.activeSkillId !== skillId) return;
    skillIdEl.value = skill.id || '';
    skillNameEl.value = skill.name || '';
    skillDescriptionEl.value = skill.description || '';
    skillTagsEl.value = Array.isArray(skill.tags) ? skill.tags.join(', ') : '';
    skillEnabledEl.checked = skill.enabled !== false;
    skillContentEl.value = skill.content || '';
    skillStatusEl.textContent = 'Loaded.';
    renderSkillList();
  } catch (err) {
    if (state.activeSkillId === skillId) {
      resetSkillForm(err.message || 'Failed to load skill.');
    }
  } finally {
    updateAdminButtons();
  }
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
  if (Array.isArray(call.related_messages) && call.related_messages.length) {
    const relatedLabel = document.createElement('div');
    relatedLabel.className = 'history-block-label';
    relatedLabel.textContent = 'Related transcript messages';
    llmCallDetailEl.appendChild(relatedLabel);
    const relatedPre = document.createElement('pre');
    relatedPre.textContent = call.related_messages
      .map(function (message) {
        const bits = [message.role];
        if (message.provider_id) bits.push(message.provider_id);
        if (message.model) bits.push(message.model);
        if (message.workload) bits.push(message.workload);
        if (message.created_at) bits.push(formatDateTime(message.created_at));
        return bits.join(' · ') + ' · ' + (message.id || '');
      })
      .join('\n');
    llmCallDetailEl.appendChild(relatedPre);
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
  heroNameEl.textContent = identity.name || 'Xia';
  heroRoleEl.textContent = identity.role || 'Personal Assistant';
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
    state.localDocSummarizationDraftBackend = state.admin.localDocSummarization && state.admin.localDocSummarization.model_summary_backend
      ? state.admin.localDocSummarization.model_summary_backend
      : 'local';
    state.localDocSummarizationDraftMode = state.admin.localDocSummarization
      && state.admin.localDocSummarization.model_summaries_enabled
      ? state.localDocSummarizationDraftBackend
      : 'default';
    state.admin.localDocOcr = data.local_doc_ocr || null;
    state.localDocOcrDraftBackend = state.admin.localDocOcr && state.admin.localDocOcr.model_backend
      ? state.admin.localDocOcr.model_backend
      : 'local';
    state.localDocOcrDraftMode = state.admin.localDocOcr && state.admin.localDocOcr.enabled
      ? state.localDocOcrDraftBackend
      : 'disabled';
    state.admin.databaseBackup = data.database_backup || null;
    state.admin.llmWorkloads = Array.isArray(data.llm_workloads) ? data.llm_workloads : [];
    state.admin.oauthProviderTemplates = Array.isArray(data.oauth_provider_templates) ? data.oauth_provider_templates : [];
    state.admin.oauthAccounts = Array.isArray(data.oauth_accounts) ? data.oauth_accounts : [];
    state.admin.services = Array.isArray(data.services) ? data.services : [];
    state.admin.sites = Array.isArray(data.sites) ? data.sites : [];
    state.admin.tools = Array.isArray(data.tools) ? data.tools : [];
    state.admin.skills = Array.isArray(data.skills) ? data.skills : [];
    state.admin.managedInstances = Array.isArray(data.managed_instances) ? data.managed_instances : [];
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
    state.littleXiaStatus = state.admin.managedInstances.length
      ? ''
      : (state.admin.capabilities && state.admin.capabilities.instance_management_configured === false
        ? 'Controller mode is off.'
        : 'No child Xia instances.');
    state.workloadRoutingDraft = buildWorkloadRoutingDraft(state.admin.providers);
    state.workloadRoutingStatus = state.admin.providers.length
      ? 'Assign which models handle each workload here.'
      : 'Add a model in AI Models to start routing workloads.';
    const provider = state.admin.providers.find((entry) => entry.id === state.activeProviderId);
    const oauthAccount = state.admin.oauthAccounts.find((entry) => entry.id === state.activeOauthAccountId);
    const service = state.admin.services.find((entry) => entry.id === state.activeServiceId);
    const site = state.admin.sites.find((entry) => entry.id === state.activeSiteId);
    const skill = state.admin.skills.find((entry) => entry.id === state.activeSkillId);
    renderOauthTemplateOptions();
    renderOauthAccountOptions();
    renderIdentitySettings();
    renderLittleXiaList();
    renderProviderTemplateOptions();
    renderProviderAccessModeOptions();
    renderProviderCredentialSourceOptions();
    renderProviderOauthAccountOptions();
    renderWorkloadRouting();
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
    if (skill) {
      await loadSkill(skill.id);
    } else if (!state.activeSkillId) {
      resetSkillForm('Create a skill or select an existing one.');
    }
    renderProviderOnboarding();
    renderCapabilities();
    if (state.setupRequired && !state.admin.providers.length) {
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
    state.activeSkillId = firstNonEmpty(data.skill && data.skill.id, state.activeSkillId);
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
    const providerName = ensureProviderName();
    const template = providerTemplateById(providerTemplateEl.value);
    const rawAccessMode = providerAccessModeEl.value;
    const accessMode = (rawAccessMode === 'api' || rawAccessMode === 'local' || rawAccessMode === 'account')
      ? rawAccessMode
      : defaultProviderAccessMode(template);
    const rawCredentialSource = providerCredentialSourceEl.value;
    const credentialSource = (rawCredentialSource === 'none'
      || rawCredentialSource === 'api-key'
      || rawCredentialSource === 'oauth-account')
      ? rawCredentialSource
      : defaultProviderCredentialSource(template, accessMode);
    const data = await fetchJson('/admin/providers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: providerId,
        name: providerName,
        template: providerTemplateEl.value,
        base_url: providerBaseUrlEl.value,
        model: providerModelEl.value,
        access_mode: accessMode,
        credential_source: credentialSource,
        oauth_account: credentialSource === 'oauth-account'
          ? providerOauthAccountEl.value
          : '',
        reuse_api_key_provider_id: credentialSource === 'api-key'
          ? (providerApiKeyReuseProviderId() || undefined)
          : undefined,
        system_prompt_budget: providerSystemPromptBudgetEl.value,
        history_budget: providerHistoryBudgetEl.value,
        rate_limit_per_minute: providerRateLimitEl.value,
        vision: currentProviderVision(),
        api_key: providerApiKeyValueForRequests() || undefined,
        default: providerDefaultEl.checked
      })
    });
    state.providerDraft = null;
    state.pendingProviderOauthFlow = null;
    state.pendingProviderBrowserSessionFlow = null;
    providerApiKeyEl.value = '';
    state.activeProviderId = '';
    await loadAdminConfig();
    resetProviderForm('Model saved. Add another or select one from the list.');
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
        recent_history_message_limit: contextRecentHistoryMessageLimitEl.value,
        history_budget: contextHistoryBudgetEl.value
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
    const summaryMode = localDocModelSummaryBackendEl.value || 'default';
    const summaryBackend = summaryMode === 'default'
      ? firstNonEmpty(state.localDocSummarizationDraftBackend,
          state.admin.localDocSummarization && state.admin.localDocSummarization.model_summary_backend,
          'local')
      : summaryMode;
    const data = await fetchJson('/admin/local-doc-summarization', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model_summaries_enabled: summaryMode !== 'default',
        model_summary_backend: summaryBackend,
        model_summary_provider_id: summaryBackend === 'external'
          ? localDocModelSummaryProviderIdEl.value
          : '',
        chunk_summary_max_tokens: localDocChunkSummaryMaxTokensEl.value,
        doc_summary_max_tokens: localDocDocSummaryMaxTokensEl.value
      })
    });
    state.admin.localDocSummarization = data.local_doc_summarization || state.admin.localDocSummarization;
    state.localDocSummarizationDraftBackend = state.admin.localDocSummarization
      && state.admin.localDocSummarization.model_summary_backend
      ? state.admin.localDocSummarization.model_summary_backend
      : 'local';
    state.localDocSummarizationDraftMode = state.admin.localDocSummarization
      && state.admin.localDocSummarization.model_summaries_enabled
      ? state.localDocSummarizationDraftBackend
      : 'default';
    state.localDocSummarizationStatus = 'Document summary settings saved.';
    renderLocalDocSummarizationSettings();
    setStatus('Document summary settings saved');
  } catch (err) {
    state.localDocSummarizationStatus = err.message || 'Failed to save document summary settings.';
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
    const ocrMode = localDocOcrModelBackendEl.value || 'disabled';
    const ocrBackend = ocrMode === 'disabled'
      ? firstNonEmpty(state.localDocOcrDraftBackend,
          state.admin.localDocOcr && state.admin.localDocOcr.model_backend,
          'local')
      : ocrMode;
    const data = await fetchJson('/admin/local-doc-ocr', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: ocrMode !== 'disabled',
        model_backend: ocrBackend,
        external_provider_id: ocrBackend === 'external'
          ? localDocOcrExternalProviderIdEl.value
          : '',
        timeout_ms: localDocOcrTimeoutMsEl.value,
        max_tokens: localDocOcrMaxTokensEl.value
      })
    });
    state.admin.localDocOcr = data.local_doc_ocr || state.admin.localDocOcr;
    state.localDocOcrDraftBackend = state.admin.localDocOcr && state.admin.localDocOcr.model_backend
      ? state.admin.localDocOcr.model_backend
      : 'local';
    state.localDocOcrDraftMode = state.admin.localDocOcr && state.admin.localDocOcr.enabled
      ? state.localDocOcrDraftBackend
      : 'disabled';
    state.localDocOcrStatus = 'OCR settings saved.';
    renderLocalDocOcrSettings();
    setStatus('OCR settings saved');
  } catch (err) {
    state.localDocOcrStatus = err.message || 'Failed to save OCR settings.';
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
    const creatingSite = !state.activeSiteId;
    const siteId = ensureSiteId();
    const data = await fetchJson('/admin/sites', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: siteId,
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
    await loadAdminConfig();
    if (creatingSite) {
      resetSiteForm('Site login saved. Add another or select an existing one.');
    } else {
      siteStatusEl.textContent = 'Saved.';
    }
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

async function saveSkill() {
  if (state.skillSaving) return;
  state.skillSaving = true;
  skillStatusEl.textContent = 'Saving...';
  updateAdminButtons();
  try {
    const skillId = ensureSkillId();
    const data = await fetchJson('/admin/skills', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: skillId,
        name: skillNameEl.value,
        description: skillDescriptionEl.value,
        tags: skillTagsEl.value,
        enabled: skillEnabledEl.checked,
        content: skillContentEl.value
      })
    });
    const skill = data.skill || null;
    state.activeSkillId = skill && skill.id ? skill.id : skillId;
    await loadAdminConfig();
    skillStatusEl.textContent = 'Saved.';
    setStatus('Skill saved');
  } catch (err) {
    skillStatusEl.textContent = err.message || 'Failed to save.';
  } finally {
    state.skillSaving = false;
    updateAdminButtons();
  }
}

async function deleteSkill() {
  if (!state.activeSkillId || state.skillSaving) return;
  if (!window.confirm('Delete this skill?')) return;
  state.skillSaving = true;
  skillStatusEl.textContent = 'Deleting...';
  updateAdminButtons();
  try {
    await fetchJson('/admin/skills/' + encodeURIComponent(state.activeSkillId), {
      method: 'DELETE'
    });
    state.activeSkillId = '';
    resetSkillForm('Deleted.');
    await loadAdminConfig();
    setStatus('Deleted skill');
  } catch (err) {
    skillStatusEl.textContent = err.message || 'Failed to delete.';
  } finally {
    state.skillSaving = false;
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
    id: message.id || '',
    createdAt: message.createdAt || message.created_at || '',
    llmCallId: message.llmCallId || message.llm_call_id || '',
    providerId: message.providerId || message.provider_id || '',
    model: message.model || '',
    workload: message.workload || '',
    toolCalls: Array.isArray(message.toolCalls)
      ? message.toolCalls
      : (Array.isArray(message.tool_calls) ? message.tool_calls : []),
    localDocs: Array.isArray(message.localDocs)
      ? message.localDocs
      : (Array.isArray(message.local_docs) ? message.local_docs : []),
    artifacts: Array.isArray(message.artifacts)
      ? message.artifacts
      : (Array.isArray(message.artifact_refs) ? message.artifact_refs : [])
  });
}

function messageProvenanceText(message) {
  const bits = [];
  if (message.createdAt) bits.push(formatStamp(message.createdAt));
  if (message.role === 'assistant') {
    if (message.providerId) bits.push(message.providerId);
    if (message.model) bits.push(message.model);
    if (message.workload) bits.push(message.workload);
  }
  return bits.join(' · ');
}

function messageToolPlanText(message) {
  const toolCalls = Array.isArray(message.toolCalls) ? message.toolCalls : [];
  if (!toolCalls.length) return '';
  return 'Tool plan: ' + toolCalls
    .map((toolCall) => toolCall && (toolCall.name || toolCall.id))
    .filter(Boolean)
    .join(', ');
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
  meta.textContent = messageProvenanceText(message);
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
  const toolPlan = messageToolPlanText(message);
  if (toolPlan) {
    const refs = document.createElement('div');
    refs.className = 'message-meta';
    refs.textContent = toolPlan;
    card.appendChild(refs);
  }
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
      ? 'Uploading...'
      : 'No local documents.';
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
    empty.textContent = 'No artifacts.';
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

function sharedWorkspaceItemLabel(value) {
  return firstNonEmpty(value, '').replace(/-/g, ' ');
}

function sharedWorkspaceItemTitle(item) {
  return firstNonEmpty(item && item.name,
    firstNonEmpty(item && item.title,
      firstNonEmpty(item && item.id, 'Untitled file')));
}

function sharedWorkspaceItemMeta(item) {
  const bits = [];
  if (item && item.title && item.title !== item.name) bits.push(item.title);
  if (item && item.source_type) bits.push(sharedWorkspaceItemLabel(item.source_type));
  if (item && item.media_type) bits.push(item.media_type);
  if (item && typeof item.size_bytes === 'number') bits.push(formatBytes(item.size_bytes));
  if (item && item.created_at) bits.push('Added ' + formatDateTime(item.created_at));
  return bits.join(' • ');
}

function renderSharedWorkspaceList() {
  if (!sharedWorkspaceListEl) return;
  sharedWorkspaceListEl.replaceChildren();
  if (!state.sharedWorkspaceItems.length) {
    const empty = document.createElement('div');
    empty.className = 'scratch-empty';
    empty.textContent = firstNonEmpty(state.sharedWorkspaceStatus, 'No shared files.');
    sharedWorkspaceListEl.appendChild(empty);
    return;
  }
  state.sharedWorkspaceItems.forEach((item) => {
    const link = document.createElement('a');
    link.className = 'shared-workspace-link';
    link.href = firstNonEmpty(item.download_url,
      item && item.id ? ('/workspace/items/' + encodeURIComponent(item.id) + '/download') : '#');
    link.textContent = '';
    link.setAttribute('download', item && item.name ? item.name : '');
    const title = document.createElement('div');
    title.className = 'admin-item-title';
    title.textContent = sharedWorkspaceItemTitle(item);
    const meta = document.createElement('div');
    meta.className = 'admin-item-meta';
    meta.textContent = sharedWorkspaceItemMeta(item);
    link.appendChild(title);
    if (meta.textContent) link.appendChild(meta);
    sharedWorkspaceListEl.appendChild(link);
  });
}

function littleXiaMeta(instance) {
  const bits = [];
  if (instance && instance.base_url) bits.push(instance.base_url);
  if (instance && instance.template_instance) bits.push('Template ' + instance.template_instance);
  if (instance && instance.pid) bits.push('PID ' + instance.pid);
  if (instance && instance.started_at) bits.push('Started ' + formatDateTime(instance.started_at));
  if (instance && instance.exited_at) bits.push('Exited ' + formatDateTime(instance.exited_at));
  if (instance && typeof instance.exit_code === 'number') bits.push('Exit ' + instance.exit_code);
  if (instance && instance.attached === false) bits.push('Detached from controller process');
  return bits.join(' • ');
}

function renderLittleXiaList() {
  if (!littleXiaListEl) return;
  littleXiaListEl.replaceChildren();
  const instances = Array.isArray(state.admin.managedInstances) ? state.admin.managedInstances : [];
  if (!instances.length) {
    const empty = document.createElement('div');
    empty.className = 'scratch-empty';
    if (state.admin.capabilities && state.admin.capabilities.instance_management_configured === false) {
      empty.textContent = 'Controller mode is off.';
    } else {
      empty.textContent = firstNonEmpty(state.littleXiaStatus, 'No child Xia instances.');
    }
    littleXiaListEl.appendChild(empty);
    return;
  }
  instances.forEach((instance) => {
    const card = document.createElement('div');
    card.className = 'admin-item little-xia-item';

    const titleRow = document.createElement('div');
    titleRow.className = 'admin-item-title-row';

    const title = document.createElement('div');
    title.className = 'admin-item-title';
    title.textContent = firstNonEmpty(instance.service_name, instance.instance_id, 'Little Xia');
    titleRow.appendChild(title);

    const badges = document.createElement('div');
    badges.className = 'admin-item-badges';
    const stateBadge = document.createElement('span');
    stateBadge.className = 'admin-item-capability-badge';
    stateBadge.textContent = firstNonEmpty(instance.state, 'unknown');
    badges.appendChild(stateBadge);
    titleRow.appendChild(badges);

    const meta = document.createElement('div');
    meta.className = 'admin-item-meta';
    meta.textContent = littleXiaMeta(instance);

    const actions = document.createElement('div');
    actions.className = 'little-xia-actions';

    if (instance && instance.base_url) {
      const open = document.createElement('a');
      open.className = 'secondary-link';
      open.href = instance.base_url;
      open.target = '_blank';
      open.rel = 'noopener noreferrer';
      open.textContent = 'Open';
      actions.appendChild(open);
    }

    const stop = document.createElement('button');
    stop.type = 'button';
    stop.className = 'secondary';
    stop.textContent = state.managedInstanceStoppingId === instance.instance_id ? 'Stopping...' : 'Stop';
    stop.disabled = !instance.instance_id || state.managedInstanceStoppingId === instance.instance_id;
    stop.addEventListener('click', () => stopManagedInstance(instance));
    actions.appendChild(stop);

    card.appendChild(titleRow);
    if (meta.textContent) card.appendChild(meta);
    card.appendChild(actions);
    littleXiaListEl.appendChild(card);
  });
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
    empty.textContent = 'No notes.';
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

async function loadSharedWorkspaceItems() {
  state.sharedWorkspaceStatus = 'Loading...';
  renderSharedWorkspaceList();
  try {
    const data = await fetchJson('/workspace/items');
    state.sharedWorkspaceItems = Array.isArray(data.items) ? data.items : [];
    state.sharedWorkspaceStatus = state.sharedWorkspaceItems.length
      ? ''
      : 'No shared files.';
  } catch (err) {
    state.sharedWorkspaceItems = [];
    state.sharedWorkspaceStatus = err.message || 'Failed to load shared workspace.';
  }
  renderSharedWorkspaceList();
}

async function loadManagedInstances() {
  state.littleXiaStatus = 'Loading child Xia instances...';
  renderLittleXiaList();
  try {
    const data = await fetchJson('/admin/managed-instances');
    state.admin.managedInstances = Array.isArray(data.instances) ? data.instances : [];
    state.littleXiaStatus = state.admin.managedInstances.length
      ? ''
      : 'No child Xia instances.';
  } catch (err) {
    state.admin.managedInstances = [];
    state.littleXiaStatus = err.message || 'Failed to load child Xia instances.';
  }
  renderLittleXiaList();
}

async function stopManagedInstance(instance) {
  if (!instance || !instance.instance_id || state.managedInstanceStoppingId) return;
  const label = firstNonEmpty(instance.service_name, instance.instance_id);
  if (!window.confirm('Stop ' + label + '?')) return;
  state.managedInstanceStoppingId = instance.instance_id;
  renderLittleXiaList();
  try {
    const data = await fetchJson('/admin/managed-instances/'
      + encodeURIComponent(instance.instance_id)
      + '/stop', {
      method: 'POST'
    });
    const updated = data.instance || null;
    if (updated && updated.instance_id) {
      state.admin.managedInstances = (state.admin.managedInstances || []).map((entry) =>
        entry.instance_id === updated.instance_id ? updated : entry);
    } else {
      await loadManagedInstances();
    }
    state.littleXiaStatus = state.admin.managedInstances.length ? '' : 'No child Xia instances.';
    renderLittleXiaList();
    setStatus('Stopped ' + label);
  } catch (err) {
    state.littleXiaStatus = err.message || 'Failed to stop child Xia instance.';
    renderLittleXiaList();
    setStatus('Failed');
  } finally {
    state.managedInstanceStoppingId = '';
    renderLittleXiaList();
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
    if (!response.ok) {
      if (isClosedSessionResponse(response, data)) {
        resetCurrentSession(response.status === 409
          ? 'Session closed. Start a new session.'
          : 'Session expired. Start a new session.');
        return;
      }
      throw new Error(data.error || 'Failed to load');
    }
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
    if (!response.ok) {
      if (isClosedSessionResponse(response, data)) {
        resetCurrentSession(response.status === 409
          ? 'Session closed. Start a new session.'
          : 'Session expired. Start a new session.');
        return;
      }
      throw new Error(data.error || 'Failed to load');
    }
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
    if (!response.ok) {
      if (isClosedSessionResponse(response, data)) {
        resetCurrentSession(response.status === 409
          ? 'Session closed. Start a new session.'
          : 'Session expired. Start a new session.');
        return;
      }
      throw new Error(data.error || 'Request failed');
    }
    if (data.session_id) {
      state.sessionId = data.session_id;
      persistSession();
    }
    const assistantMessage = normalizeMessage(data.message || {
      role: 'assistant',
      content: data.content || '',
      local_docs: localDocs,
      artifacts: artifacts
    });
    addMessage('assistant', assistantMessage.content || '', assistantMessage);
    await loadHistorySessions();
    await loadArtifacts();
    await loadSharedWorkspaceItems();
    await loadManagedInstances();
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


providerTemplateEl.addEventListener('change', () => {
  state.providerModels = [];
  setProviderModelVision('', false);
  providerModelListEl.hidden = true;
  renderProviderTemplateOptions();
  renderProviderAccessModeOptions();
  renderProviderCredentialSourceOptions();
  renderProviderOauthAccountOptions();
  const template = providerTemplateById(providerTemplateEl.value);
  if (template) {
    applyProviderTemplate(template.id, {
      preserveId: false,
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
providerBaseUrlEl.addEventListener('input', () => {
  state.providerModels = [];
  setProviderModelVision('', false);
  providerModelListEl.hidden = true;
  syncProviderModelFetchUi();
});
providerModelEl.addEventListener('input', () => {
  clearProviderModelVisionIfModelChanged();
  if (state.providerModels.length) renderProviderModelList();
});
providerModelEl.addEventListener('focus', () => {
  if (state.providerModels.length) renderProviderModelList();
});
providerModelEl.addEventListener('change', () => {
  clearProviderModelVisionIfModelChanged();
  maybeFetchProviderModelMetadata();
});
providerModelEl.addEventListener('blur', () => {
  maybeFetchProviderModelMetadata();
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
providerApiKeyEl.addEventListener('focus', () => {
  if (providerApiKeyEl.dataset.reuseStoredKey === 'true') {
    providerApiKeyEl.select();
  }
});
providerApiKeyEl.addEventListener('input', () => {
  if (providerApiKeyEl.dataset.reuseStoredKey === 'true'
      && providerApiKeyEl.value !== storedApiKeyMask) {
    clearStoredProviderApiKey();
  }
  syncProviderModelFetchUi();
});
providerSystemPromptBudgetEl.addEventListener('input', () => {
  delete providerSystemPromptBudgetEl.dataset.autoBudget;
});
providerHistoryBudgetEl.addEventListener('input', () => {
  delete providerHistoryBudgetEl.dataset.autoBudget;
});
providerConfigureOauthEl.addEventListener('click', async (event) => {
  const action = currentProviderPrimaryAction();
  const disabled = providerConfigureOauthEl.getAttribute('aria-disabled') === 'true';
  if (disabled || !action) {
    event.preventDefault();
    return;
  }
  if (action.kind === 'external') {
    providerStatusEl.textContent = 'Opened setup in a new tab.';
    setStatus('Opened provider setup');
    return;
  }
  event.preventDefault();
  await openProviderPrimaryAction(action, {
    statusEl: providerStatusEl
  });
});
saveIdentityEl.addEventListener('click', () => saveIdentity());
saveProviderEl.addEventListener('click', () => saveProvider());
deleteProviderEl.addEventListener('click', () => deleteProvider());
saveWorkloadRoutingEl.addEventListener('click', () => saveWorkloadRouting());
saveContextEl.addEventListener('click', () => saveConversationContext());
saveSearchEl.addEventListener('click', () => saveWebSearch());
searchBackendEl.addEventListener('change', () => syncWebSearchInputs());
saveRetentionEl.addEventListener('click', () => saveMemoryRetention());
saveKnowledgeDecayEl.addEventListener('click', () => saveKnowledgeDecay());
saveLocalDocSummarizationEl.addEventListener('click', () => saveLocalDocSummarization());
localDocModelSummaryBackendEl.addEventListener('change', () => {
  state.localDocSummarizationDraftMode = localDocModelSummaryBackendEl.value || 'default';
  if (state.localDocSummarizationDraftMode === 'local' || state.localDocSummarizationDraftMode === 'external') {
    state.localDocSummarizationDraftBackend = state.localDocSummarizationDraftMode;
  }
  localDocModelSummaryProviderFieldEl.hidden = state.localDocSummarizationDraftMode !== 'external';
  updateAdminButtons();
});
saveLocalDocOcrEl.addEventListener('click', () => saveLocalDocOcr());
localDocOcrModelBackendEl.addEventListener('change', () => {
  state.localDocOcrDraftMode = localDocOcrModelBackendEl.value || 'disabled';
  if (state.localDocOcrDraftMode === 'local' || state.localDocOcrDraftMode === 'external') {
    state.localDocOcrDraftBackend = state.localDocOcrDraftMode;
  }
  renderLocalDocOcrSettings();
});
saveDatabaseBackupEl.addEventListener('click', () => saveDatabaseBackup());

oauthConnectionModeEl.addEventListener('change', () => {
  syncOauthAccountInputs();
  updateAdminButtons();
});
oauthTemplateEl.addEventListener('change', () => {
  if (oauthTemplateEl.value) {
    applyOauthTemplate();
  } else {
    renderOauthTemplateOptions();
    updateAdminButtons();
  }
});
saveOauthAccountEl.addEventListener('click', () => saveOauthAccount());
connectOauthAccountEl.addEventListener('click', () => connectOauthAccount());
refreshOauthAccountEl.addEventListener('click', () => refreshOauthAccount());
deleteOauthAccountEl.addEventListener('click', () => deleteOauthAccount());

newServiceEl.addEventListener('click', () => {
  resetServiceForm('Create a service.');
  serviceIdEl.focus();
});

saveServiceEl.addEventListener('click', () => saveService());

saveSiteEl.addEventListener('click', () => saveSite());
deleteSiteEl.addEventListener('click', () => deleteSite());
saveSkillEl.addEventListener('click', () => saveSkill());
deleteSkillEl.addEventListener('click', () => deleteSkill());
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
  resetCurrentSession('Ready');
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
syncWebSearchInputs();
renderLocalDocSummarizationSettings();
renderLocalDocOcrSettings();
renderDatabaseBackupSettings();
resetOauthAccountForm('Loading...');
resetServiceForm('Loading...');
resetSiteForm('Loading...');
resetSkillForm('Loading...');
renderCapabilities();
updateAdminButtons();
updateComposerState();
renderSharedWorkspaceList();
renderLittleXiaList();
bindStaticInfoHints();
composerEl.focus();
Promise.all([
  loadSessionMessages(),
  loadLocalDocs(),
  loadSharedWorkspaceItems(),
  loadManagedInstances(),
  loadArtifacts(),
  loadScratchPads(),
  loadAdminConfig(),
  loadHistorySessions(),
  loadHistorySchedules(),
  loadLlmCalls()
]).catch(() => {
  setStatus('Some data failed to load — check your connection');
});
if (state.sessionId) {
  pollApproval();
  if (state.sending) pollStatus();
}
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

window.addEventListener('resize', () => {
  if (activeInfoHintEl) positionInfoTooltip(activeInfoHintEl);
});

window.addEventListener('scroll', () => {
  if (activeInfoHintEl) positionInfoTooltip(activeInfoHintEl);
}, true);
