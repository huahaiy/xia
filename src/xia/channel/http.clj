(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [clojure.string :as str]
            [org.httpkit.server :as http]
            [charred.api :as json]
            [clojure.tools.logging :as log]
            [xia.db :as db]
            [xia.agent :as agent]
            [xia.working-memory :as wm]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private server-atom (atom nil))
(defonce ^:private ws-sessions (atom {})) ; channel → session-id

;; ---------------------------------------------------------------------------
;; Local web UI
;; ---------------------------------------------------------------------------

(def ^:private local-ui-html
  (str/join
    "\n"
    ["<!DOCTYPE html>"
     "<html lang=\"en\">"
     "<head>"
     "  <meta charset=\"utf-8\">"
     "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
     "  <title>Xia Local Interface</title>"
     "  <style>"
     "    :root {"
     "      --paper: #f5efe3;"
     "      --paper-strong: #fff9ef;"
     "      --ink: #172119;"
     "      --muted: #5b6658;"
     "      --accent: #b24c32;"
     "      --accent-dark: #8f351c;"
     "      --panel: rgba(255, 249, 239, 0.84);"
     "      --panel-strong: rgba(255, 252, 246, 0.95);"
     "      --line: rgba(23, 33, 25, 0.12);"
     "      --shadow: 0 20px 50px rgba(23, 33, 25, 0.12);"
     "    }"
     "    * { box-sizing: border-box; }"
     "    html, body { height: 100%; }"
     "    body {"
     "      margin: 0;"
     "      font-family: \"Avenir Next\", \"Segoe UI\", sans-serif;"
     "      color: var(--ink);"
     "      background:"
     "        radial-gradient(circle at top left, rgba(178, 76, 50, 0.18), transparent 32%),"
     "        radial-gradient(circle at bottom right, rgba(70, 110, 84, 0.2), transparent 28%),"
     "        linear-gradient(135deg, #eee4d3 0%, #f9f3e8 48%, #efe6d7 100%);"
     "    }"
     "    body::before {"
     "      content: \"\";"
     "      position: fixed;"
     "      inset: 0;"
     "      pointer-events: none;"
     "      background-image: linear-gradient(rgba(23, 33, 25, 0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(23, 33, 25, 0.03) 1px, transparent 1px);"
     "      background-size: 28px 28px;"
     "      mask-image: radial-gradient(circle at center, black 55%, transparent 100%);"
     "    }"
     "    .shell {"
     "      position: relative;"
     "      z-index: 1;"
     "      min-height: 100%;"
     "      padding: 24px;"
     "    }"
     "    .frame {"
     "      max-width: 1280px;"
     "      margin: 0 auto;"
     "      display: grid;"
     "      gap: 18px;"
     "    }"
     "    .hero {"
     "      display: grid;"
     "      gap: 16px;"
     "      grid-template-columns: minmax(0, 1.6fr) minmax(280px, 0.9fr);"
     "      align-items: start;"
     "    }"
     "    .hero-copy, .hero-meta, .panel {"
     "      border: 1px solid var(--line);"
     "      background: var(--panel);"
     "      backdrop-filter: blur(18px);"
     "      border-radius: 24px;"
     "      box-shadow: var(--shadow);"
     "    }"
     "    .hero-copy { padding: 26px 28px; }"
     "    .hero-meta { padding: 22px; display: grid; gap: 14px; }"
     "    .eyebrow {"
     "      margin: 0 0 10px;"
     "      color: var(--accent-dark);"
     "      font-size: 0.8rem;"
     "      font-weight: 700;"
     "      letter-spacing: 0.14em;"
     "      text-transform: uppercase;"
     "    }"
     "    h1 {"
     "      margin: 0;"
     "      font-family: \"Iowan Old Style\", \"Palatino Linotype\", serif;"
     "      font-size: clamp(2.5rem, 5vw, 4.4rem);"
     "      line-height: 0.95;"
     "      letter-spacing: -0.04em;"
     "    }"
     "    .lead {"
     "      margin: 14px 0 0;"
     "      max-width: 46rem;"
     "      color: var(--muted);"
     "      font-size: 1.02rem;"
     "      line-height: 1.65;"
     "    }"
     "    .meta-row { display: grid; gap: 6px; }"
     "    .meta-label {"
     "      margin: 0;"
     "      color: var(--muted);"
     "      font-size: 0.78rem;"
     "      font-weight: 700;"
     "      letter-spacing: 0.08em;"
     "      text-transform: uppercase;"
     "    }"
     "    .meta-value { margin: 0; font-size: 0.96rem; line-height: 1.45; }"
     "    .status-pill {"
     "      display: inline-flex;"
     "      align-items: center;"
     "      gap: 8px;"
     "      width: fit-content;"
     "      padding: 10px 14px;"
     "      border-radius: 999px;"
     "      background: rgba(255, 255, 255, 0.55);"
     "      font-size: 0.92rem;"
     "      font-weight: 600;"
     "    }"
     "    .status-pill::before {"
     "      content: \"\";"
     "      width: 10px;"
     "      height: 10px;"
     "      border-radius: 50%;"
     "      background: #4c7a5e;"
     "      box-shadow: 0 0 0 4px rgba(76, 122, 94, 0.15);"
     "    }"
     "    .workspace {"
     "      display: grid;"
     "      gap: 18px;"
     "      grid-template-columns: minmax(0, 1.4fr) minmax(320px, 0.9fr);"
     "      min-height: 62vh;"
     "    }"
     "    .panel {"
     "      padding: 20px;"
     "      display: flex;"
     "      flex-direction: column;"
     "      min-height: 0;"
     "    }"
     "    .panel-header {"
     "      display: flex;"
     "      justify-content: space-between;"
     "      gap: 12px;"
     "      align-items: start;"
     "      margin-bottom: 16px;"
     "    }"
     "    .panel-title {"
     "      margin: 0;"
     "      font-family: \"Iowan Old Style\", \"Palatino Linotype\", serif;"
     "      font-size: 1.55rem;"
     "    }"
     "    .panel-note {"
     "      margin: 4px 0 0;"
     "      color: var(--muted);"
     "      font-size: 0.94rem;"
     "      line-height: 1.45;"
     "    }"
     "    .actions { display: flex; gap: 10px; flex-wrap: wrap; }"
     "    button {"
     "      appearance: none;"
     "      border: 0;"
     "      border-radius: 999px;"
     "      padding: 11px 16px;"
     "      font: inherit;"
     "      font-weight: 650;"
     "      cursor: pointer;"
     "      transition: transform 120ms ease, background 120ms ease, opacity 120ms ease;"
     "    }"
     "    button:hover:not(:disabled) { transform: translateY(-1px); }"
     "    button:disabled { opacity: 0.55; cursor: wait; }"
     "    .primary { background: var(--accent); color: white; }"
     "    .primary:hover:not(:disabled) { background: var(--accent-dark); }"
     "    .secondary {"
     "      background: rgba(255, 255, 255, 0.7);"
     "      color: var(--ink);"
     "      border: 1px solid var(--line);"
     "    }"
     "    .messages {"
     "      flex: 1 1 auto;"
     "      min-height: 0;"
     "      overflow: auto;"
     "      display: grid;"
     "      gap: 12px;"
     "      padding-right: 4px;"
     "    }"
     "    .empty {"
     "      padding: 22px;"
     "      border-radius: 20px;"
     "      border: 1px dashed rgba(23, 33, 25, 0.22);"
     "      background: rgba(255, 255, 255, 0.46);"
     "      color: var(--muted);"
     "      line-height: 1.6;"
     "    }"
     "    .message {"
     "      padding: 16px 16px 14px;"
     "      border-radius: 20px;"
     "      border: 1px solid var(--line);"
     "      background: var(--panel-strong);"
     "    }"
     "    .message.user {"
     "      background: rgba(237, 230, 214, 0.96);"
     "      border-color: rgba(178, 76, 50, 0.16);"
     "    }"
     "    .message.assistant {"
     "      background: rgba(250, 248, 242, 0.98);"
     "    }"
     "    .message.error {"
     "      background: rgba(255, 238, 233, 0.95);"
     "      border-color: rgba(178, 76, 50, 0.26);"
     "    }"
     "    .message-head {"
     "      display: flex;"
     "      justify-content: space-between;"
     "      align-items: center;"
     "      gap: 12px;"
     "      margin-bottom: 10px;"
     "    }"
     "    .message-role {"
     "      font-size: 0.78rem;"
     "      font-weight: 800;"
     "      letter-spacing: 0.12em;"
     "      text-transform: uppercase;"
     "      color: var(--accent-dark);"
     "    }"
     "    .message-meta {"
     "      font-size: 0.8rem;"
     "      color: var(--muted);"
     "    }"
     "    .copy-link {"
     "      padding: 6px 10px;"
     "      background: transparent;"
     "      border: 1px solid var(--line);"
     "      font-size: 0.82rem;"
     "    }"
     "    .message-body {"
     "      margin: 0;"
     "      white-space: pre-wrap;"
     "      word-break: break-word;"
     "      font-family: \"SFMono-Regular\", Consolas, \"Liberation Mono\", monospace;"
     "      font-size: 0.95rem;"
     "      line-height: 1.6;"
     "    }"
     "    .composer { display: grid; gap: 14px; }"
     "    textarea {"
     "      width: 100%;"
     "      min-height: 340px;"
     "      resize: vertical;"
     "      border-radius: 22px;"
     "      border: 1px solid rgba(23, 33, 25, 0.15);"
     "      background: rgba(255, 252, 246, 0.92);"
     "      padding: 18px 18px 20px;"
     "      color: var(--ink);"
     "      font: inherit;"
     "      line-height: 1.6;"
     "      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.6);"
     "    }"
     "    textarea:focus {"
     "      outline: 2px solid rgba(178, 76, 50, 0.26);"
     "      outline-offset: 2px;"
     "    }"
     "    .composer-foot {"
     "      display: flex;"
     "      justify-content: space-between;"
     "      gap: 12px;"
     "      align-items: center;"
     "      flex-wrap: wrap;"
     "    }"
     "    .hint { color: var(--muted); font-size: 0.9rem; line-height: 1.4; }"
     "    @media (max-width: 980px) {"
     "      .hero, .workspace { grid-template-columns: 1fr; }"
     "      .shell { padding: 16px; }"
     "      textarea { min-height: 240px; }"
     "    }"
     "  </style>"
     "</head>"
     "<body>"
     "  <div class=\"shell\">"
     "    <div class=\"frame\">"
     "      <section class=\"hero\">"
     "        <div class=\"hero-copy\">"
     "          <p class=\"eyebrow\">Local bridge</p>"
     "          <h1>Xia</h1>"
     "          <p class=\"lead\">Xia stays online-first and does not read your local files directly. Use this page to paste local notes, logs, snippets, or document excerpts into a chat that is easy to read, review, and copy back out.</p>"
     "        </div>"
     "        <aside class=\"hero-meta\">"
     "          <div class=\"status-pill\" id=\"status\">Ready</div>"
     "          <div class=\"meta-row\">"
     "            <p class=\"meta-label\">Input model</p>"
     "            <p class=\"meta-value\">Multiline paste box for prompts, copied files, stack traces, and arbitrary text.</p>"
     "          </div>"
     "          <div class=\"meta-row\">"
     "            <p class=\"meta-label\">Output model</p>"
     "            <p class=\"meta-value\">Copy-friendly responses with preserved whitespace and per-message copy buttons.</p>"
     "          </div>"
     "          <div class=\"meta-row\">"
     "            <p class=\"meta-label\">Session</p>"
     "            <p class=\"meta-value\" id=\"session-label\">New local session</p>"
     "          </div>"
     "        </aside>"
     "      </section>"
     "      <main class=\"workspace\">"
     "        <section class=\"panel\">"
     "          <div class=\"panel-header\">"
     "            <div>"
     "              <h2 class=\"panel-title\">Transcript</h2>"
     "              <p class=\"panel-note\">Responses stay in plain text formatting so you can copy them directly into other tools.</p>"
     "            </div>"
     "            <div class=\"actions\">"
     "              <button class=\"secondary\" id=\"copy-transcript\" type=\"button\">Copy transcript</button>"
     "            </div>"
     "          </div>"
     "          <div class=\"messages\" id=\"messages\"></div>"
     "        </section>"
     "        <section class=\"panel\">"
     "          <div class=\"panel-header\">"
     "            <div>"
     "              <h2 class=\"panel-title\">Paste Input</h2>"
     "              <p class=\"panel-note\">Anything Xia cannot read from disk has to come through here. Paste it as-is, then add the instruction you want.</p>"
     "            </div>"
     "            <div class=\"actions\">"
     "              <button class=\"secondary\" id=\"new-chat\" type=\"button\">New chat</button>"
     "            </div>"
     "          </div>"
     "          <form class=\"composer\" id=\"composer-form\">"
     "            <textarea id=\"composer\" name=\"message\" spellcheck=\"false\" placeholder=\"Paste prompts, local file excerpts, copied pages, logs, or notes here. Cmd/Ctrl+Enter sends.\"></textarea>"
     "            <div class=\"composer-foot\">"
     "              <div class=\"hint\">Use Shift+Enter for new lines. Use Cmd/Ctrl+Enter to send.</div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"clear-input\" type=\"button\">Clear</button>"
     "                <button class=\"primary\" id=\"send\" type=\"submit\">Send to Xia</button>"
     "              </div>"
     "            </div>"
     "          </form>"
     "        </section>"
     "      </main>"
     "    </div>"
     "  </div>"
     "  <script>"
     "    const storageKeys = {"
     "      sessionId: 'xia.local-ui.session-id',"
     "      messages: 'xia.local-ui.messages'"
     "    };"
     "    const state = {"
     "      sessionId: localStorage.getItem(storageKeys.sessionId) || '',"
     "      messages: [],"
     "      sending: false"
     "    };"
     "    const statusEl = document.getElementById('status');"
     "    const sessionLabelEl = document.getElementById('session-label');"
     "    const messagesEl = document.getElementById('messages');"
     "    const composerEl = document.getElementById('composer');"
     "    const sendEl = document.getElementById('send');"
     "    const clearInputEl = document.getElementById('clear-input');"
     "    const newChatEl = document.getElementById('new-chat');"
     "    const copyTranscriptEl = document.getElementById('copy-transcript');"
     "    const composerFormEl = document.getElementById('composer-form');"
     ""
     "    function loadMessages() {"
     "      try {"
     "        const raw = localStorage.getItem(storageKeys.messages);"
     "        const parsed = raw ? JSON.parse(raw) : [];"
     "        state.messages = Array.isArray(parsed) ? parsed : [];"
     "      } catch (_err) {"
     "        state.messages = [];"
     "      }"
     "    }"
     ""
     "    function persistState() {"
     "      if (state.sessionId) {"
     "        localStorage.setItem(storageKeys.sessionId, state.sessionId);"
     "      } else {"
     "        localStorage.removeItem(storageKeys.sessionId);"
     "      }"
     "      localStorage.setItem(storageKeys.messages, JSON.stringify(state.messages));"
     "      updateSessionLabel();"
     "    }"
     ""
     "    function updateSessionLabel() {"
     "      sessionLabelEl.textContent = state.sessionId"
     "        ? 'Session ' + state.sessionId.slice(0, 8) + '...'"
     "        : 'New local session';"
     "    }"
     ""
     "    function setStatus(text) {"
     "      statusEl.textContent = text;"
     "    }"
     ""
     "    async function copyText(text, successLabel) {"
     "      try {"
     "        await navigator.clipboard.writeText(text);"
     "        setStatus(successLabel || 'Copied');"
     "      } catch (_err) {"
     "        const fallback = document.createElement('textarea');"
     "        fallback.value = text;"
     "        fallback.setAttribute('readonly', 'readonly');"
     "        fallback.style.position = 'fixed';"
     "        fallback.style.opacity = '0';"
     "        document.body.appendChild(fallback);"
     "        fallback.select();"
     "        document.execCommand('copy');"
     "        document.body.removeChild(fallback);"
     "        setStatus(successLabel || 'Copied');"
     "      }"
     "    }"
     ""
     "    function addMessage(role, content) {"
     "      state.messages.push({"
     "        role: role,"
     "        content: content,"
     "        createdAt: new Date().toISOString()"
     "      });"
     "      persistState();"
     "      renderMessages();"
     "    }"
     ""
     "    function formatStamp(value) {"
     "      const date = new Date(value);"
     "      if (Number.isNaN(date.getTime())) {"
     "        return '';"
     "      }"
     "      return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });"
     "    }"
     ""
     "    function renderMessages() {"
     "      messagesEl.innerHTML = '';"
     "      if (!state.messages.length) {"
     "        const empty = document.createElement('div');"
     "        empty.className = 'empty';"
     "        empty.textContent = 'Start with a paste or a prompt. This interface is here to bridge the gap when Xia needs local material that cannot be read directly from disk.';"
     "        messagesEl.appendChild(empty);"
     "        return;"
     "      }"
     "      state.messages.forEach((message) => {"
     "        const card = document.createElement('article');"
     "        card.className = 'message ' + (message.role === 'assistant' ? 'assistant' : message.role === 'user' ? 'user' : 'error');"
     ""
     "        const head = document.createElement('div');"
     "        head.className = 'message-head';"
     ""
     "        const roleWrap = document.createElement('div');"
     "        const role = document.createElement('div');"
     "        role.className = 'message-role';"
     "        role.textContent = message.role === 'assistant' ? 'Xia' : message.role === 'user' ? 'You' : 'Status';"
     "        const meta = document.createElement('div');"
     "        meta.className = 'message-meta';"
     "        meta.textContent = formatStamp(message.createdAt);"
     "        roleWrap.appendChild(role);"
     "        roleWrap.appendChild(meta);"
     "        head.appendChild(roleWrap);"
     ""
     "        if (message.role !== 'user') {"
     "          const copyButton = document.createElement('button');"
     "          copyButton.type = 'button';"
     "          copyButton.className = 'copy-link';"
     "          copyButton.textContent = 'Copy';"
     "          copyButton.addEventListener('click', () => copyText(message.content, 'Message copied'));"
     "          head.appendChild(copyButton);"
     "        }"
     ""
     "        const body = document.createElement('pre');"
     "        body.className = 'message-body';"
     "        body.textContent = message.content;"
     ""
     "        card.appendChild(head);"
     "        card.appendChild(body);"
     "        messagesEl.appendChild(card);"
     "      });"
     "      messagesEl.scrollTop = messagesEl.scrollHeight;"
     "    }"
     ""
     "    function updateComposerState() {"
     "      const empty = !composerEl.value.trim();"
     "      sendEl.disabled = state.sending || empty;"
     "      clearInputEl.disabled = state.sending || !composerEl.value.length;"
     "    }"
     ""
     "    async function sendMessage(text) {"
     "      state.sending = true;"
     "      updateComposerState();"
     "      setStatus('Waiting for Xia');"
     "      try {"
     "        const payload = { message: text };"
     "        if (state.sessionId) {"
     "          payload.session_id = state.sessionId;"
     "        }"
     "        const response = await fetch('/chat', {"
     "          method: 'POST',"
     "          headers: { 'Content-Type': 'application/json' },"
     "          body: JSON.stringify(payload)"
     "        });"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Request failed');"
     "        }"
     "        if (data.session_id) {"
     "          state.sessionId = data.session_id;"
     "        }"
     "        addMessage('assistant', data.content || '');"
     "        setStatus('Ready');"
     "      } catch (err) {"
     "        addMessage('error', err.message || 'Request failed');"
     "        setStatus('Request failed');"
     "      } finally {"
     "        state.sending = false;"
     "        persistState();"
     "        updateComposerState();"
     "      }"
     "    }"
     ""
     "    composerFormEl.addEventListener('submit', async (event) => {"
     "      event.preventDefault();"
     "      const text = composerEl.value.trim();"
     "      if (!text || state.sending) {"
     "        return;"
     "      }"
     "      addMessage('user', text);"
     "      composerEl.value = '';"
     "      updateComposerState();"
     "      await sendMessage(text);"
     "    });"
     ""
     "    composerEl.addEventListener('keydown', (event) => {"
     "      if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {"
     "        event.preventDefault();"
     "        composerFormEl.requestSubmit();"
     "      }"
     "    });"
     ""
     "    composerEl.addEventListener('input', updateComposerState);"
     ""
     "    clearInputEl.addEventListener('click', () => {"
     "      composerEl.value = '';"
     "      updateComposerState();"
     "      composerEl.focus();"
     "    });"
     ""
     "    newChatEl.addEventListener('click', () => {"
     "      state.sessionId = '';"
     "      state.messages = [];"
     "      persistState();"
     "      renderMessages();"
     "      setStatus('Ready');"
     "      composerEl.focus();"
     "    });"
     ""
     "    copyTranscriptEl.addEventListener('click', () => {"
     "      const transcript = state.messages"
     "        .map((message) => ((message.role === 'assistant' ? 'Xia' : message.role === 'user' ? 'You' : 'Status') + ':\\n' + message.content))"
     "        .join('\\n\\n');"
     "      copyText(transcript, 'Transcript copied');"
     "    });"
     ""
     "    loadMessages();"
     "    persistState();"
     "    renderMessages();"
     "    updateComposerState();"
     "    composerEl.focus();"
     "  </script>"
     "</body>"
     "</html>"]))

;; ---------------------------------------------------------------------------
;; WebSocket handler
;; ---------------------------------------------------------------------------

(defn- ws-handler [req]
  (http/as-channel req
    {:on-open
     (fn [ch]
       (let [sid (db/create-session! :websocket)]
         (swap! ws-sessions assoc ch sid)
         (wm/create-wm! sid)
         (wm/warm-start!)
         (log/info "WebSocket connected, session:" sid)
         (http/send! ch (json/write-json-str {:type "connected" :session-id (str sid)}))))

     :on-receive
     (fn [ch msg]
       (let [sid (get @ws-sessions ch)]
         (try
           (let [data     (json/read-json msg)
                 text     (get data "message" (get data "content" msg))
                 response (agent/process-message sid text :channel :websocket)]
             (http/send! ch (json/write-json-str {:type    "message"
                                                   :role    "assistant"
                                                   :content response})))
           (catch Exception e
             (log/error e "WebSocket message error")
             (http/send! ch (json/write-json-str {:type  "error"
                                                   :error (.getMessage e)}))))))

     :on-close
     (fn [ch _status]
       (wm/snapshot!)
       (wm/clear-wm!)
       (swap! ws-sessions dissoc ch)
       (log/info "WebSocket disconnected"))}))

;; ---------------------------------------------------------------------------
;; REST endpoints
;; ---------------------------------------------------------------------------

(defn- read-body [req]
  (when-let [body (:body req)]
    (json/read-json (slurp body))))

(defn- json-response [status body]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/write-json-str body)})

(defn- html-response [body]
  {:status  200
   :headers {"Content-Type" "text/html; charset=utf-8"}
   :body    body})

(defn- handle-chat [req]
  (let [data       (read-body req)
        message    (get data "message")
        session-id (get data "session_id")]
    (if-not message
      (json-response 400 {:error "missing 'message' field"})
      (let [sid      (if session-id
                       (java.util.UUID/fromString session-id)
                       (db/create-session! :http))
            _        (do (wm/create-wm! sid) (wm/warm-start!))
            response (agent/process-message sid message :channel :http)]
        (json-response 200 {:session_id (str sid)
                            :role       "assistant"
                            :content    response})))))

(defn- handle-skills [_req]
  (json-response 200 {:skills (mapv (fn [s]
                                      {:id          (name (:skill/id s))
                                       :name        (:skill/name s)
                                       :description (:skill/description s)
                                       :type        (name (:skill/type s))
                                       :enabled     (:skill/enabled? s)})
                                    (db/list-skills))}))

(defn- handle-health [_req]
  (json-response 200 {:status "ok" :version "0.1.0"}))

(defn- handle-home [_req]
  (html-response local-ui-html))

;; ---------------------------------------------------------------------------
;; Router
;; ---------------------------------------------------------------------------

(defn- router [req]
  (let [uri    (:uri req)
        method (:request-method req)]
    (cond
      (and (= method :get) (= uri "/"))
      (handle-home req)

      ;; WebSocket upgrade
      (and (= uri "/ws") (http/websocket-handshake-check req))
      (ws-handler req)

      ;; REST
      (and (= method :post) (= uri "/chat"))
      (handle-chat req)

      (and (= method :get) (= uri "/skills"))
      (handle-skills req)

      (and (= method :get) (= uri "/health"))
      (handle-health req)

      :else
      (json-response 404 {:error "not found"}))))

;; ---------------------------------------------------------------------------
;; Server lifecycle
;; ---------------------------------------------------------------------------

(defn start!
  "Start the HTTP/WebSocket server on the given port."
  [port]
  (when @server-atom
    (log/warn "Server already running"))
  (let [s (http/run-server router {:port port})]
    (reset! server-atom s)
    (log/info "HTTP/WebSocket server started on port" port)
    s))

(defn stop! []
  (when-let [s @server-atom]
    (s) ; http-kit stop fn
    (reset! server-atom nil)
    (log/info "Server stopped")))
