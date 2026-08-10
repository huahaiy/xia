(function () {
  'use strict';

  try {
    var xiaTheme = localStorage.getItem('xia.local-ui.theme');
    if (xiaTheme !== 'light' && xiaTheme !== 'dark') {
      xiaTheme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    document.documentElement.dataset.theme = xiaTheme;
  } catch (_error) {
    // Theme selection is best-effort; the stylesheet default remains usable.
  }
})();
