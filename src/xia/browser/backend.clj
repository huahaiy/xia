(ns xia.browser.backend
  "Browser backend protocol used by xia.browser to dispatch session operations.")

(defprotocol BrowserBackend
  (backend-id [this]
    "Return the backend keyword, such as :playwright.")
  (runtime-status* [this]
    "Return a status map describing whether the backend is ready.")
  (bootstrap-runtime!* [this opts]
    "Prepare the backend runtime and return an updated status map.")
  (install-browser-deps!* [this opts]
    "Install or preview browser system dependencies for this backend.")
  (open-session* [this url opts]
    "Open a new browser session. Backends may honor :js, :storage-state, and backend-specific options such as :headless or :channel in opts.")
  (navigate* [this session-id url]
    "Navigate an existing session to a new URL.")
  (click* [this session-id selector]
    "Click an element in an existing session.")
  (fill-selector* [this session-id selector value opts]
    "Fill an element matching a CSS selector.")
  (fill-form* [this session-id fields opts]
    "Fill and optionally submit a form in an existing session.")
  (read-page* [this session-id]
    "Read the current page in an existing session.")
  (query-elements* [this session-id opts]
    "Inspect the current page in an existing session and return paginated DOM elements.")
  (screenshot* [this session-id opts]
    "Capture a screenshot in an existing session.")
  (wait-for-page* [this session-id opts]
    "Wait for the current page to settle or satisfy a condition.")
  (release-session* [this session-id]
    "Release a live session while preserving its resumable snapshot when possible.")
  (close-session* [this session-id]
    "Close a single session.")
  (release-all-sessions!* [this]
    "Release all live sessions while preserving resumable snapshots when possible.")
  (close-all-sessions!* [this]
    "Close all sessions for this backend.")
  (list-sessions* [this]
    "List sessions known to this backend."))
