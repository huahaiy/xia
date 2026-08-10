(function () {
  'use strict';

  var currentVersion = null;

  async function poll() {
    try {
      var response = await fetch('/__dev/web-reload', { cache: 'no-store' });
      if (!response.ok) return;

      var payload = await response.json();
      if (!payload || !payload.version) return;

      if (currentVersion !== null && payload.version !== currentVersion) {
        window.location.reload();
        return;
      }
      currentVersion = payload.version;
    } catch (_error) {
      // The development server may be restarting; the next poll retries.
    }
  }

  poll();
  window.setInterval(function () {
    if (document.visibilityState !== 'hidden') poll();
  }, 1000);
})();
