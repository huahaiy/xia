(function () {
  'use strict';

  var result = document.getElementById('oauth-callback-result');
  if (!result) return;

  var accountId = result.dataset.accountId || null;
  try {
    if (window.opener && window.opener !== window) {
      window.opener.postMessage({
        type: 'xia-oauth-complete',
        status: result.dataset.status,
        account_id: accountId
      }, window.location.origin);
    }
  } catch (_error) {
    // The page remains useful even when the opener is unavailable.
  }

  window.setTimeout(function () {
    try {
      window.close();
    } catch (_error) {
      // Some browsers do not allow scripts to close manually opened tabs.
    }
  }, 1200);
})();
