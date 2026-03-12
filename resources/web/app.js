const storageKeys = {
  sessionId: 'xia.local-ui.session-id'
};
const state = {
  sessionId: sessionStorage.getItem(storageKeys.sessionId) || '',
  messages: [],
  pendingApproval: null,
  sending: false,
  approvalSubmitting: false,
  scratchPads: [],
  activePadId: '',
  activePad: null,
  scratchDirty: false,
  scratchSaving: false,
  admin: {
    providers: [],
    llmWorkloads: [],
    oauthProviderTemplates: [],
    oauthAccounts: [],
    services: [],
    sites: [],
    tools: [],
    skills: []
  },
  activeProviderId: '',
  activeOauthAccountId: '',
  activeServiceId: '',
  activeSiteId: '',
  providerSaving: false,
  oauthSaving: false,
  serviceSaving: false,
  siteSaving: false,
  baseStatus: 'Ready',
  liveStatus: null
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
const providerListEl = document.getElementById('provider-list');
const providerIdEl = document.getElementById('provider-id');
const providerNameEl = document.getElementById('provider-name');
const providerBaseUrlEl = document.getElementById('provider-base-url');
const providerModelEl = document.getElementById('provider-model');
const providerWorkloadsEl = document.getElementById('provider-workloads');
const providerWorkloadsNoteEl = document.getElementById('provider-workloads-note');
const providerSystemPromptBudgetEl = document.getElementById('provider-system-prompt-budget');
const providerHistoryBudgetEl = document.getElementById('provider-history-budget');
const providerApiKeyEl = document.getElementById('provider-api-key');
const providerDefaultEl = document.getElementById('provider-default');
const providerStatusEl = document.getElementById('provider-status');
const newProviderEl = document.getElementById('new-provider');
const saveProviderEl = document.getElementById('save-provider');
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
const siteStatusEl = document.getElementById('site-status');
const newSiteEl = document.getElementById('new-site');
const saveSiteEl = document.getElementById('save-site');
const deleteSiteEl = document.getElementById('delete-site');
const toolListEl = document.getElementById('tool-list');
const skillListEl = document.getElementById('skill-list');
const fileInputEl = document.getElementById('file-input');
const uploadBtnEl = document.getElementById('upload-btn');
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

fileInputEl.addEventListener('change', async () => {
  const files = Array.from(fileInputEl.files);
  if (!files.length) return;

  const contents = await Promise.all(files.map((file) => {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = (e) => resolve(`--- File: ${file.name} ---\n${e.target.result}\n`);
      reader.onerror = () => resolve(`--- File: ${file.name} (Error reading file) ---\n`);
      reader.readAsText(file);
    });
  }));

  const text = contents.join('\n');
  const start = composerEl.selectionStart == null ? composerEl.value.length : composerEl.selectionStart;
  const end = composerEl.selectionEnd == null ? composerEl.value.length : composerEl.selectionEnd;
  const prefix = composerEl.value.slice(0, start);
  const suffix = composerEl.value.slice(end);
  const before = prefix && !prefix.endsWith('\n') ? '\n' : '';
  const after = suffix && !text.endsWith('\n') ? '\n' : '';
  
  composerEl.value = prefix + before + text + after + suffix;
  updateComposerState();
  composerEl.focus();
  fileInputEl.value = ''; // Reset for next selection
  setStatus('Files uploaded into chat composer');
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
    
    switch (phase) {
      case 'working-memory': return 'Reading context...';
      case 'llm':            return 'Thinking...';
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
  const response = await fetch(url, options || {});
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
  target.innerHTML = '';
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
    button.addEventListener('click', () => onSelect(item));
    target.appendChild(button);
  });
}

function renderCapabilityList(target, items, emptyText, detailFn) {
  target.innerHTML = '';
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

function providerMeta(provider) {
  const bits = [];
  if (provider.model) bits.push(provider.model);
  if (Array.isArray(provider.workloads) && provider.workloads.length) {
    bits.push('Workloads: ' + provider.workloads.join(', '));
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
  bits.push(service.enabled ? 'Enabled' : 'Disabled');
  bits.push(service.auth_type === 'oauth-account'
    ? (service.oauth_account ? 'Account linked' : 'No account linked')
    : (service.auth_key_configured ? 'Secret stored' : 'No secret'));
  return bits.join(' • ');
}

function siteMeta(site) {
  const bits = [];
  if (site.login_url) bits.push(site.login_url);
  bits.push(site.username_configured ? 'Username stored' : 'No username');
  bits.push(site.password_configured ? 'Password stored' : 'No password');
  return bits.join(' • ');
}

function updateAdminButtons() {
  providerIdEl.disabled = state.providerSaving || !!state.activeProviderId;
  saveProviderEl.disabled = state.providerSaving;
  newProviderEl.disabled = state.providerSaving;
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
  oauthTemplateEl.innerHTML = '';
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
  serviceOauthAccountEl.innerHTML = '';
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
    (skill) => [skill.id, skill.version ? ('version ' + skill.version) : '', skill.description || '']
      .filter(Boolean)
      .join(' • ')
  );
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
  providerApiKeyEl.value = '';
  providerDefaultEl.checked = !state.admin.providers.some((provider) => provider.default);
  providerStatusEl.textContent = statusText || 'Create a model or select an existing one.';
  renderProviderWorkloadNote();
  renderProviderList();
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
  providerApiKeyEl.value = '';
  providerDefaultEl.checked = !!provider.default;
  providerStatusEl.textContent = provider.api_key_configured
    ? 'API key stored. Enter a new one to replace it.'
    : 'No API key stored yet.';
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
  siteStatusEl.textContent = site.username_configured ? 'Credentials stored.' : 'No credentials stored.';
  renderSiteList();
  updateAdminButtons();
}

async function loadAdminConfig() {
  try {
    const data = await fetchJson('/admin/config');
    state.admin.providers = Array.isArray(data.providers) ? data.providers : [];
    state.admin.llmWorkloads = Array.isArray(data.llm_workloads) ? data.llm_workloads : [];
    state.admin.oauthProviderTemplates = Array.isArray(data.oauth_provider_templates) ? data.oauth_provider_templates : [];
    state.admin.oauthAccounts = Array.isArray(data.oauth_accounts) ? data.oauth_accounts : [];
    state.admin.services = Array.isArray(data.services) ? data.services : [];
    state.admin.sites = Array.isArray(data.sites) ? data.sites : [];
    state.admin.tools = Array.isArray(data.tools) ? data.tools : [];
    state.admin.skills = Array.isArray(data.skills) ? data.skills : [];
    const provider = state.admin.providers.find((entry) => entry.id === state.activeProviderId);
    const oauthAccount = state.admin.oauthAccounts.find((entry) => entry.id === state.activeOauthAccountId);
    const service = state.admin.services.find((entry) => entry.id === state.activeServiceId);
    const site = state.admin.sites.find((entry) => entry.id === state.activeSiteId);
    renderOauthTemplateOptions();
    renderOauthAccountOptions();
    renderProviderWorkloadNote();
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
        token_params: oauthTokenParamsEl.value
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
        extra_fields: siteExtraFieldsEl.value
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

function formatStamp(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
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
    body.innerHTML = typeof DOMPurify !== 'undefined' ? DOMPurify.sanitize(raw) : raw.replace(/</g, '&lt;');
  } else {
    body.textContent = message.content;
  }
  card.appendChild(head);
  card.appendChild(body);
  return card;
}

function addMessage(role, content) {
  const message = { role: role, content: content, createdAt: new Date().toISOString() };
  state.messages.push(message);
  if (state.messages.length === 1) {
    messagesEl.innerHTML = '';
  }
  messagesEl.appendChild(buildMessageEl(message));
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function renderMessages() {
  messagesEl.innerHTML = '';
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
  sendEl.disabled = state.sending || empty;
  clearInputEl.disabled = state.sending || !composerEl.value.length;
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
  scratchListEl.innerHTML = '';
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
  const response = await fetch('/sessions', { method: 'POST' });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'Failed to create session');
  state.sessionId = data.session_id || '';
  persistSession();
  return state.sessionId;
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
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads');
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
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(padId));
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
  if (!discardScratchChanges()) return;
  try {
    await ensureSession();
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: 'Untitled note', content: '' })
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to create');
    state.activePad = data.pad || null;
    state.activePadId = state.activePad ? state.activePad.id : '';
    state.scratchDirty = false;
    if (state.activePad) upsertScratchMeta(state.activePad);
    syncScratchEditor('New note ready.');
    scratchTitleEl.focus();
    scratchTitleEl.select();
  } catch (err) {
    scratchStatusEl.textContent = err.message || 'Failed to create.';
  }
}

async function saveScratchPad() {
  if (!state.activePad || state.scratchSaving) return;
  state.scratchSaving = true;
  syncScratchEditor('Saving...');
  try {
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(state.activePad.id), {
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
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(deletingId), { method: 'DELETE' });
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
  const text = scratchEditorEl.value;
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
  setStatus('Note inserted into chat');
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

async function pollApproval() {
  if (!state.sessionId) {
    state.pendingApproval = null;
    renderApproval();
    syncStatus();
    return;
  }
  try {
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/approval');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to load');
    state.pendingApproval = data.pending ? (data.approval || null) : null;
    renderApproval();
    syncStatus();
  } catch (_err) {}
}

async function pollStatus() {
  if (!state.sessionId) {
    state.liveStatus = null;
    syncStatus();
    return;
  }
  try {
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/status');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to load');
    state.liveStatus = data.status || null;
    syncStatus();
  } catch (_err) {}
}

async function submitApproval(decision) {
  if (!state.sessionId || !state.pendingApproval || state.approvalSubmitting) return;
  state.approvalSubmitting = true;
  renderApproval();
  try {
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/approval', {
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

async function sendMessage(text) {
  state.sending = true;
  state.liveStatus = null;
  updateComposerState();
  syncStatus();
  try {
    await ensureSession();
    const payload = { message: text };
    if (state.sessionId) payload.session_id = state.sessionId;
    const responsePromise = fetch('/chat', {
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
    addMessage('assistant', data.content || '');
    state.liveStatus = null;
    setStatus('Ready');
  } catch (err) {
    addMessage('error', err.message || 'Request failed');
    state.liveStatus = null;
    setStatus('Failed');
  } finally {
    state.sending = false;
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
    const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/messages');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Failed to load');
    state.messages = Array.isArray(data.messages) ? data.messages : [];
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
  addMessage('user', text);
  composerEl.value = '';
  updateComposerState();
  await sendMessage(text);
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

newProviderEl.addEventListener('click', () => {
  resetProviderForm('Create a model.');
  providerIdEl.focus();
});

saveProviderEl.addEventListener('click', () => saveProvider());

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

newChatEl.addEventListener('click', () => {
  if (!discardScratchChanges()) return;
  state.sessionId = '';
  state.messages = [];
  state.pendingApproval = null;
  state.liveStatus = null;
  state.scratchPads = [];
  state.activePadId = '';
  state.activePad = null;
  state.scratchDirty = false;
  persistSession();
  renderApproval();
  renderMessages();
  syncScratchEditor('No note selected.');
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
syncScratchEditor('No note selected.');
resetProviderForm('Loading...');
resetOauthAccountForm('Loading...');
resetServiceForm('Loading...');
resetSiteForm('Loading...');
renderCapabilities();
updateAdminButtons();
updateComposerState();
composerEl.focus();
loadSessionMessages();
loadScratchPads();
loadAdminConfig();
window.setInterval(() => {
  if (state.sessionId) {
    pollApproval();
    if (state.sending) pollStatus();
  }
}, 1000);
