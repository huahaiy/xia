(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [clojure.string :as str]
            [org.httpkit.server :as http]
            [charred.api :as json]
            [clojure.tools.logging :as log]
            [xia.scratch :as scratch]
            [xia.db :as db]
            [xia.agent :as agent]
            [xia.prompt :as prompt]
            [xia.working-memory :as wm])
  (:import [java.security SecureRandom]
           [java.util Base64]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private server-atom (atom nil))
(defonce ^:private ws-sessions (atom {})) ; channel → session-id
(defonce ^:private pending-approvals (atom {})) ; session-id string → approval map

(def ^:private local-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})
(def ^:private approval-timeout-ms (* 5 60 1000))
(def ^:private local-session-cookie-name "xia-local-session")
(defonce ^:private local-session-secret
  (delay
    (let [bytes (byte-array 32)
          _     (.nextBytes (SecureRandom.) bytes)]
      (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bytes))))

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
     "    .side-stack {"
     "      display: grid;"
     "      gap: 18px;"
     "      min-height: 0;"
     "    }"
     "    .panel {"
      "      padding: 20px;"
      "      display: flex;"
      "      flex-direction: column;"
      "      min-height: 0;"
     "    }"
     "    .approval-panel {"
     "      border-color: rgba(178, 76, 50, 0.22);"
     "      background: rgba(255, 246, 240, 0.9);"
     "    }"
     "    .approval-panel[hidden] { display: none; }"
     "    .approval-summary {"
     "      display: grid;"
     "      gap: 10px;"
     "      margin-bottom: 16px;"
     "    }"
     "    .approval-reason {"
     "      margin: 0;"
     "      color: var(--accent-dark);"
     "      font-size: 0.95rem;"
     "      line-height: 1.5;"
     "    }"
     "    .approval-args {"
     "      margin: 0;"
     "      padding: 14px;"
     "      border-radius: 18px;"
     "      border: 1px solid rgba(23, 33, 25, 0.12);"
     "      background: rgba(255, 255, 255, 0.72);"
     "      white-space: pre-wrap;"
     "      word-break: break-word;"
     "      font-family: \"SFMono-Regular\", Consolas, \"Liberation Mono\", monospace;"
     "      font-size: 0.9rem;"
     "      line-height: 1.55;"
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
     "    .scratch-workspace { display: grid; gap: 14px; min-height: 0; }"
     "    .scratch-list {"
     "      display: grid;"
     "      gap: 10px;"
     "      max-height: 180px;"
     "      overflow: auto;"
     "      padding-right: 4px;"
     "    }"
     "    .scratch-empty {"
     "      padding: 16px;"
     "      border-radius: 16px;"
     "      border: 1px dashed rgba(23, 33, 25, 0.2);"
     "      color: var(--muted);"
     "      background: rgba(255, 255, 255, 0.42);"
     "      line-height: 1.5;"
     "    }"
     "    .scratch-item {"
     "      width: 100%;"
     "      border-radius: 18px;"
     "      border: 1px solid rgba(23, 33, 25, 0.12);"
     "      background: rgba(255, 255, 255, 0.68);"
     "      padding: 12px 14px;"
     "      text-align: left;"
     "      display: grid;"
     "      gap: 4px;"
     "    }"
     "    .scratch-item.active {"
     "      border-color: rgba(178, 76, 50, 0.28);"
     "      background: rgba(255, 245, 240, 0.92);"
     "    }"
     "    .scratch-item-title {"
     "      font-size: 0.95rem;"
     "      font-weight: 700;"
     "      color: var(--ink);"
     "    }"
     "    .scratch-item-meta {"
     "      color: var(--muted);"
     "      font-size: 0.82rem;"
     "    }"
     "    .field { display: grid; gap: 6px; }"
     "    .field-label {"
     "      color: var(--muted);"
     "      font-size: 0.78rem;"
     "      font-weight: 700;"
     "      letter-spacing: 0.08em;"
     "      text-transform: uppercase;"
     "    }"
     "    .text-input {"
     "      width: 100%;"
     "      border-radius: 16px;"
     "      border: 1px solid rgba(23, 33, 25, 0.15);"
     "      background: rgba(255, 252, 246, 0.92);"
     "      padding: 14px 16px;"
     "      color: var(--ink);"
     "      font: inherit;"
     "      line-height: 1.4;"
     "      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.6);"
     "    }"
     "    .text-input:focus {"
     "      outline: 2px solid rgba(178, 76, 50, 0.26);"
     "      outline-offset: 2px;"
     "    }"
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
     "    .scratch-editor {"
     "      min-height: 220px;"
     "      font-family: \"SFMono-Regular\", Consolas, \"Liberation Mono\", monospace;"
     "      font-size: 0.93rem;"
     "    }"
     "    .scratch-status {"
     "      color: var(--muted);"
     "      font-size: 0.86rem;"
     "      line-height: 1.4;"
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
     "      <section class=\"panel approval-panel\" id=\"approval-panel\" hidden>"
     "        <div class=\"panel-header\">"
     "          <div>"
     "            <h2 class=\"panel-title\">Approval Required</h2>"
     "            <p class=\"panel-note\">Privileged tools need a local confirmation before Xia can execute them.</p>"
     "          </div>"
     "          <div class=\"actions\">"
     "            <button class=\"secondary\" id=\"deny-approval\" type=\"button\">Deny</button>"
     "            <button class=\"primary\" id=\"allow-approval\" type=\"button\">Allow for session</button>"
     "          </div>"
     "        </div>"
     "        <div class=\"approval-summary\">"
     "          <div class=\"meta-row\">"
     "            <p class=\"meta-label\">Tool</p>"
     "            <p class=\"meta-value\" id=\"approval-tool\">Waiting</p>"
     "          </div>"
     "          <div class=\"meta-row\">"
     "            <p class=\"meta-label\">Reason</p>"
     "            <p class=\"approval-reason\" id=\"approval-reason\">The current tool needs confirmation.</p>"
     "          </div>"
     "        </div>"
     "        <pre class=\"approval-args\" id=\"approval-args\">{}</pre>"
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
     "        <div class=\"side-stack\">"
     "          <section class=\"panel\">"
     "            <div class=\"panel-header\">"
     "              <div>"
     "                <h2 class=\"panel-title\">Paste Input</h2>"
     "                <p class=\"panel-note\">Anything Xia cannot read from disk has to come through here. Paste it as-is, then add the instruction you want.</p>"
     "              </div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"new-chat\" type=\"button\">New chat</button>"
     "              </div>"
     "            </div>"
     "            <form class=\"composer\" id=\"composer-form\">"
     "              <textarea id=\"composer\" name=\"message\" spellcheck=\"false\" placeholder=\"Paste prompts, local file excerpts, copied pages, logs, or notes here. Cmd/Ctrl+Enter sends.\"></textarea>"
     "              <div class=\"composer-foot\">"
     "                <div class=\"hint\">Use Shift+Enter for new lines. Use Cmd/Ctrl+Enter to send.</div>"
     "                <div class=\"actions\">"
     "                  <button class=\"secondary\" id=\"clear-input\" type=\"button\">Clear</button>"
     "                  <button class=\"primary\" id=\"send\" type=\"submit\">Send to Xia</button>"
     "                </div>"
     "              </div>"
     "            </form>"
     "          </section>"
     "          <section class=\"panel\">"
     "            <div class=\"panel-header\">"
     "              <div>"
     "                <h2 class=\"panel-title\">Scratch Pads</h2>"
     "                <p class=\"panel-note\">Persistent notes stored inside Xia's DB for drafts, extracted facts, and intermediate results.</p>"
     "              </div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"new-scratch\" type=\"button\">New pad</button>"
     "              </div>"
     "            </div>"
     "            <div class=\"scratch-workspace\">"
     "              <div class=\"scratch-list\" id=\"scratch-list\"></div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"scratch-title\">Title</label>"
     "                <input class=\"text-input\" id=\"scratch-title\" type=\"text\" value=\"\" placeholder=\"Untitled scratch pad\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"scratch-editor\">Content</label>"
     "                <textarea class=\"scratch-editor\" id=\"scratch-editor\" spellcheck=\"false\" placeholder=\"Use this pad for intermediate notes, structured drafts, or copied local text you want to refine before sending.\"></textarea>"
     "              </div>"
     "              <div class=\"composer-foot\">"
     "                <div class=\"scratch-status\" id=\"scratch-status\">No scratch pad selected.</div>"
     "                <div class=\"actions\">"
     "                  <button class=\"secondary\" id=\"insert-scratch\" type=\"button\">Insert into chat</button>"
     "                  <button class=\"secondary\" id=\"delete-scratch\" type=\"button\">Delete</button>"
     "                  <button class=\"primary\" id=\"save-scratch\" type=\"button\">Save pad</button>"
     "                </div>"
     "              </div>"
     "            </div>"
     "          </section>"
     "        </div>"
     "      </main>"
     "    </div>"
     "  </div>"
     "  <script>"
     "    const storageKeys = {"
     "      sessionId: 'xia.local-ui.session-id'"
     "    };"
     "    const state = {"
     "      sessionId: sessionStorage.getItem(storageKeys.sessionId) || '',"
     "      messages: [],"
     "      pendingApproval: null,"
     "      sending: false,"
     "      approvalSubmitting: false,"
     "      scratchPads: [],"
     "      activePadId: '',"
     "      activePad: null,"
     "      scratchDirty: false,"
     "      scratchSaving: false"
     "    };"
     "    const statusEl = document.getElementById('status');"
     "    const sessionLabelEl = document.getElementById('session-label');"
     "    const approvalPanelEl = document.getElementById('approval-panel');"
     "    const approvalToolEl = document.getElementById('approval-tool');"
     "    const approvalReasonEl = document.getElementById('approval-reason');"
     "    const approvalArgsEl = document.getElementById('approval-args');"
     "    const allowApprovalEl = document.getElementById('allow-approval');"
     "    const denyApprovalEl = document.getElementById('deny-approval');"
     "    const messagesEl = document.getElementById('messages');"
     "    const composerEl = document.getElementById('composer');"
     "    const sendEl = document.getElementById('send');"
     "    const clearInputEl = document.getElementById('clear-input');"
     "    const newChatEl = document.getElementById('new-chat');"
     "    const copyTranscriptEl = document.getElementById('copy-transcript');"
     "    const composerFormEl = document.getElementById('composer-form');"
     "    const scratchListEl = document.getElementById('scratch-list');"
     "    const scratchTitleEl = document.getElementById('scratch-title');"
     "    const scratchEditorEl = document.getElementById('scratch-editor');"
     "    const scratchStatusEl = document.getElementById('scratch-status');"
     "    const newScratchEl = document.getElementById('new-scratch');"
     "    const saveScratchEl = document.getElementById('save-scratch');"
     "    const deleteScratchEl = document.getElementById('delete-scratch');"
     "    const insertScratchEl = document.getElementById('insert-scratch');"
     ""
     "    function persistSession() {"
     "      if (state.sessionId) {"
     "        sessionStorage.setItem(storageKeys.sessionId, state.sessionId);"
     "      } else {"
     "        sessionStorage.removeItem(storageKeys.sessionId);"
     "      }"
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
     "    function prettyJson(value) {"
     "      try {"
     "        return JSON.stringify(value == null ? {} : value, null, 2);"
     "      } catch (_err) {"
     "        return String(value);"
     "      }"
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
     "    function renderApproval() {"
     "      const approval = state.pendingApproval;"
     "      approvalPanelEl.hidden = !approval;"
     "      allowApprovalEl.disabled = !approval || state.approvalSubmitting;"
     "      denyApprovalEl.disabled = !approval || state.approvalSubmitting;"
     "      if (!approval) {"
     "        approvalToolEl.textContent = 'Waiting';"
     "        approvalReasonEl.textContent = 'The current tool needs confirmation.';"
     "        approvalArgsEl.textContent = '{}';"
     "        return;"
     "      }"
     "      approvalToolEl.textContent = approval.tool_name || approval.tool_id || 'Privileged tool';"
     "      approvalReasonEl.textContent = approval.reason || 'This action can use stored credentials or cause live side effects.';"
     "      approvalArgsEl.textContent = prettyJson(approval.arguments || {});"
     "    }"
     ""
     "    function addMessage(role, content) {"
     "      state.messages.push({"
     "        role: role,"
     "        content: content,"
     "        createdAt: new Date().toISOString()"
     "      });"
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
     "        const head = document.createElement('div');"
     "        head.className = 'message-head';"
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
     "        if (message.role !== 'user') {"
     "          const copyButton = document.createElement('button');"
     "          copyButton.type = 'button';"
     "          copyButton.className = 'copy-link';"
     "          copyButton.textContent = 'Copy';"
     "          copyButton.addEventListener('click', () => copyText(message.content, 'Message copied'));"
     "          head.appendChild(copyButton);"
     "        }"
     "        const body = document.createElement('pre');"
     "        body.className = 'message-body';"
     "        body.textContent = message.content;"
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
     "    function padTitle(pad) {"
     "      return (pad && pad.title && pad.title.trim()) ? pad.title.trim() : 'Untitled scratch pad';"
     "    }"
     ""
     "    function sortScratchPads() {"
     "      state.scratchPads.sort((left, right) => {"
     "        const a = Date.parse((left && left.updated_at) || '') || 0;"
     "        const b = Date.parse((right && right.updated_at) || '') || 0;"
     "        return b - a;"
     "      });"
     "    }"
     ""
     "    function upsertScratchMeta(pad) {"
     "      const meta = {"
     "        id: pad.id,"
     "        title: pad.title,"
     "        mime: pad.mime,"
     "        version: pad.version,"
     "        created_at: pad.created_at,"
     "        updated_at: pad.updated_at"
     "      };"
     "      const index = state.scratchPads.findIndex((entry) => entry.id === pad.id);"
     "      if (index >= 0) {"
     "        state.scratchPads[index] = meta;"
     "      } else {"
     "        state.scratchPads.push(meta);"
     "      }"
     "      sortScratchPads();"
     "    }"
     ""
     "    function renderScratchList() {"
     "      scratchListEl.innerHTML = '';"
     "      if (!state.scratchPads.length) {"
     "        const empty = document.createElement('div');"
     "        empty.className = 'scratch-empty';"
     "        empty.textContent = 'Create a pad when you want Xia to keep a persistent draft or intermediate note for this session.';"
     "        scratchListEl.appendChild(empty);"
     "        return;"
     "      }"
     "      state.scratchPads.forEach((pad) => {"
     "        const button = document.createElement('button');"
     "        button.type = 'button';"
     "        button.className = 'scratch-item' + (pad.id === state.activePadId ? ' active' : '');"
     "        const title = document.createElement('div');"
     "        title.className = 'scratch-item-title';"
     "        title.textContent = padTitle(pad);"
     "        const meta = document.createElement('div');"
     "        meta.className = 'scratch-item-meta';"
     "        meta.textContent = 'Updated ' + formatStamp(pad.updated_at);"
     "        button.appendChild(title);"
     "        button.appendChild(meta);"
     "        button.addEventListener('click', () => {"
     "          loadScratchPad(pad.id);"
     "        });"
     "        scratchListEl.appendChild(button);"
     "      });"
     "    }"
     ""
     "    function setScratchEditorEnabled(enabled) {"
     "      scratchTitleEl.disabled = !enabled || state.scratchSaving;"
     "      scratchEditorEl.disabled = !enabled || state.scratchSaving;"
     "      saveScratchEl.disabled = !enabled || state.scratchSaving || !state.scratchDirty;"
     "      deleteScratchEl.disabled = !enabled || state.scratchSaving;"
     "      insertScratchEl.disabled = !enabled || !scratchEditorEl.value.length;"
     "    }"
     ""
     "    function syncScratchEditor(statusText) {"
     "      if (state.activePad) {"
     "        scratchTitleEl.value = state.activePad.title || '';"
     "        scratchEditorEl.value = state.activePad.content || '';"
     "        scratchStatusEl.textContent = statusText || (state.scratchSaving ? 'Saving scratch pad...' : state.scratchDirty ? 'Unsaved changes.' : 'Saved in Xia DB.');"
     "        setScratchEditorEnabled(true);"
     "      } else {"
     "        scratchTitleEl.value = '';"
     "        scratchEditorEl.value = '';"
     "        scratchStatusEl.textContent = statusText || 'No scratch pad selected.';"
     "        setScratchEditorEnabled(false);"
     "      }"
     "      renderScratchList();"
     "    }"
     ""
     "    function discardScratchChanges() {"
     "      if (!state.scratchDirty) {"
     "        return true;"
     "      }"
     "      return window.confirm('Discard unsaved scratch pad changes?');"
     "    }"
     ""
     "    async function ensureSession() {"
     "      if (state.sessionId) {"
     "        return state.sessionId;"
     "      }"
     "      const response = await fetch('/sessions', { method: 'POST' });"
     "      const data = await response.json();"
     "      if (!response.ok) {"
     "        throw new Error(data.error || 'Failed to create session');"
     "      }"
     "      state.sessionId = data.session_id || '';"
     "      persistSession();"
     "      return state.sessionId;"
     "    }"
     ""
     "    async function loadScratchPads(options) {"
     "      const keepActive = !options || options.keepActive !== false;"
     "      if (!state.sessionId) {"
     "        state.scratchPads = [];"
     "        state.activePadId = '';"
     "        state.activePad = null;"
     "        state.scratchDirty = false;"
     "        syncScratchEditor('No scratch pad selected.');"
     "        return;"
     "      }"
     "      try {"
     "        const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads');"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Failed to load scratch pads');"
     "        }"
     "        state.scratchPads = Array.isArray(data.pads) ? data.pads : [];"
     "        sortScratchPads();"
     "        renderScratchList();"
     "        if (keepActive && state.activePadId && state.scratchPads.some((pad) => pad.id === state.activePadId)) {"
     "          return;"
     "        }"
     "        if (state.scratchPads.length) {"
     "          await loadScratchPad(state.scratchPads[0].id, true);"
     "        } else {"
     "          state.activePadId = '';"
     "          state.activePad = null;"
     "          state.scratchDirty = false;"
     "          syncScratchEditor('No scratch pad selected.');"
     "        }"
     "      } catch (err) {"
     "        scratchStatusEl.textContent = err.message || 'Failed to load scratch pads.';"
     "      }"
     "    }"
     ""
     "    async function loadScratchPad(padId, bypassDirtyCheck) {"
     "      if (!padId) {"
     "        return;"
     "      }"
     "      if (!bypassDirtyCheck && !discardScratchChanges()) {"
     "        return;"
     "      }"
     "      try {"
     "        const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(padId));"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Failed to load scratch pad');"
     "        }"
     "        state.activePadId = padId;"
     "        state.activePad = data.pad || null;"
     "        state.scratchDirty = false;"
     "        if (state.activePad) {"
     "          upsertScratchMeta(state.activePad);"
     "        }"
     "        syncScratchEditor();"
     "      } catch (err) {"
     "        scratchStatusEl.textContent = err.message || 'Failed to load scratch pad.';"
     "      }"
     "    }"
     ""
     "    async function createScratchPad() {"
     "      if (!discardScratchChanges()) {"
     "        return;"
     "      }"
     "      try {"
     "        await ensureSession();"
     "        const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads', {"
     "          method: 'POST',"
     "          headers: { 'Content-Type': 'application/json' },"
     "          body: JSON.stringify({ title: 'Untitled scratch pad', content: '' })"
     "        });"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Failed to create scratch pad');"
     "        }"
     "        state.activePad = data.pad || null;"
     "        state.activePadId = state.activePad ? state.activePad.id : '';"
     "        state.scratchDirty = false;"
     "        if (state.activePad) {"
     "          upsertScratchMeta(state.activePad);"
     "        }"
     "        syncScratchEditor('New scratch pad ready.');"
     "        scratchTitleEl.focus();"
     "        scratchTitleEl.select();"
     "      } catch (err) {"
     "        scratchStatusEl.textContent = err.message || 'Failed to create scratch pad.';"
     "      }"
     "    }"
     ""
     "    async function saveScratchPad() {"
     "      if (!state.activePad || state.scratchSaving) {"
     "        return;"
     "      }"
     "      state.scratchSaving = true;"
     "      syncScratchEditor('Saving scratch pad...');"
     "      try {"
     "        const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(state.activePad.id), {"
     "          method: 'PUT',"
     "          headers: { 'Content-Type': 'application/json' },"
     "          body: JSON.stringify({"
     "            title: scratchTitleEl.value,"
     "            content: scratchEditorEl.value,"
     "            expected_version: state.activePad.version"
     "          })"
     "        });"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Failed to save scratch pad');"
     "        }"
     "        state.activePad = data.pad || null;"
     "        state.activePadId = state.activePad ? state.activePad.id : '';"
     "        state.scratchDirty = false;"
     "        if (state.activePad) {"
     "          upsertScratchMeta(state.activePad);"
     "        }"
     "        syncScratchEditor('Scratch pad saved.');"
     "      } catch (err) {"
     "        scratchStatusEl.textContent = err.message || 'Failed to save scratch pad.';"
     "      } finally {"
     "        state.scratchSaving = false;"
     "        syncScratchEditor();"
     "      }"
     "    }"
     ""
     "    async function deleteScratchPad() {"
     "      if (!state.activePad || state.scratchSaving) {"
     "        return;"
     "      }"
     "      if (!window.confirm('Delete this scratch pad?')) {"
     "        return;"
     "      }"
     "      try {"
     "        const deletingId = state.activePad.id;"
     "        const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/scratch-pads/' + encodeURIComponent(deletingId), { method: 'DELETE' });"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Failed to delete scratch pad');"
     "        }"
     "        state.scratchPads = state.scratchPads.filter((pad) => pad.id !== deletingId);"
     "        state.activePad = null;"
     "        state.activePadId = '';"
     "        state.scratchDirty = false;"
     "        if (state.scratchPads.length) {"
     "          await loadScratchPad(state.scratchPads[0].id, true);"
     "        } else {"
     "          syncScratchEditor('Scratch pad deleted.');"
     "        }"
     "      } catch (err) {"
     "        scratchStatusEl.textContent = err.message || 'Failed to delete scratch pad.';"
     "      }"
     "    }"
     ""
     "    function insertScratchIntoComposer() {"
     "      const text = scratchEditorEl.value;"
     "      if (!text) {"
     "        return;"
     "      }"
     "      const start = composerEl.selectionStart == null ? composerEl.value.length : composerEl.selectionStart;"
     "      const end = composerEl.selectionEnd == null ? composerEl.value.length : composerEl.selectionEnd;"
     "      const prefix = composerEl.value.slice(0, start);"
     "      const suffix = composerEl.value.slice(end);"
     "      const before = prefix && !prefix.endsWith('\\n') ? '\\n' : '';"
     "      const after = suffix && !text.endsWith('\\n') ? '\\n' : '';"
     "      composerEl.value = prefix + before + text + after + suffix;"
     "      updateComposerState();"
     "      composerEl.focus();"
     "      setStatus('Scratch pad inserted into chat composer');"
     "    }"
     ""
     "    function trackScratchInput() {"
     "      if (!state.activePad) {"
     "        return;"
     "      }"
     "      state.activePad = Object.assign({}, state.activePad, {"
     "        title: scratchTitleEl.value,"
     "        content: scratchEditorEl.value"
     "      });"
     "      state.scratchDirty = true;"
     "      syncScratchEditor('Unsaved changes.');"
     "    }"
     ""
     "    async function pollApproval() {"
     "      if (!state.sessionId) {"
     "        state.pendingApproval = null;"
     "        renderApproval();"
     "        return;"
     "      }"
     "      try {"
     "        const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/approval');"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Failed to load approval state');"
     "        }"
     "        state.pendingApproval = data.pending ? (data.approval || null) : null;"
     "        renderApproval();"
     "        if (state.pendingApproval) {"
     "          setStatus('Approval required');"
     "        } else if (!state.sending) {"
     "          setStatus('Ready');"
     "        }"
     "      } catch (_err) {"
     "      }"
     "    }"
     ""
     "    async function submitApproval(decision) {"
     "      if (!state.sessionId || !state.pendingApproval || state.approvalSubmitting) {"
     "        return;"
     "      }"
     "      state.approvalSubmitting = true;"
     "      renderApproval();"
     "      try {"
     "        const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/approval', {"
     "          method: 'POST',"
     "          headers: { 'Content-Type': 'application/json' },"
     "          body: JSON.stringify({"
     "            approval_id: state.pendingApproval.approval_id,"
     "            decision: decision"
     "          })"
     "        });"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Failed to submit approval');"
     "        }"
     "        state.pendingApproval = null;"
     "        renderApproval();"
     "        setStatus(decision === 'allow' ? 'Approval submitted' : 'Approval denied');"
     "      } catch (err) {"
     "        setStatus(err.message || 'Failed to submit approval');"
     "      } finally {"
     "        state.approvalSubmitting = false;"
     "        renderApproval();"
     "      }"
     "    }"
     ""
     "    async function sendMessage(text) {"
     "      state.sending = true;"
     "      updateComposerState();"
     "      setStatus('Waiting for Xia');"
     "      try {"
     "        await ensureSession();"
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
     "          persistSession();"
     "        }"
     "        addMessage('assistant', data.content || '');"
     "        setStatus('Ready');"
     "      } catch (err) {"
     "        addMessage('error', err.message || 'Request failed');"
     "        setStatus('Request failed');"
     "      } finally {"
     "        state.sending = false;"
     "        updateComposerState();"
     "      }"
     "    }"
     ""
     "    async function loadSessionMessages() {"
     "      if (!state.sessionId) {"
     "        state.messages = [];"
     "        renderMessages();"
     "        return;"
     "      }"
     "      try {"
     "        const response = await fetch('/sessions/' + encodeURIComponent(state.sessionId) + '/messages');"
     "        const data = await response.json();"
     "        if (!response.ok) {"
     "          throw new Error(data.error || 'Failed to load transcript');"
     "        }"
     "        state.messages = Array.isArray(data.messages) ? data.messages : [];"
     "        renderMessages();"
     "      } catch (err) {"
     "        state.messages = [];"
     "        renderMessages();"
     "        setStatus(err.message || 'Failed to load transcript');"
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
     "    scratchTitleEl.addEventListener('input', trackScratchInput);"
     "    scratchEditorEl.addEventListener('input', trackScratchInput);"
     ""
     "    scratchTitleEl.addEventListener('keydown', (event) => {"
     "      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {"
     "        event.preventDefault();"
     "        saveScratchPad();"
     "      }"
     "    });"
     ""
     "    scratchEditorEl.addEventListener('keydown', (event) => {"
     "      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {"
     "        event.preventDefault();"
     "        saveScratchPad();"
     "      }"
     "    });"
     ""
     "    allowApprovalEl.addEventListener('click', () => submitApproval('allow'));"
     "    denyApprovalEl.addEventListener('click', () => submitApproval('deny'));"
     ""
     "    newScratchEl.addEventListener('click', () => {"
     "      createScratchPad();"
     "    });"
     ""
     "    saveScratchEl.addEventListener('click', () => {"
     "      saveScratchPad();"
     "    });"
     ""
     "    deleteScratchEl.addEventListener('click', () => {"
     "      deleteScratchPad();"
     "    });"
     ""
     "    insertScratchEl.addEventListener('click', () => {"
     "      insertScratchIntoComposer();"
     "    });"
     ""
     "    newChatEl.addEventListener('click', () => {"
     "      if (!discardScratchChanges()) {"
     "        return;"
     "      }"
     "      state.sessionId = '';"
     "      state.messages = [];"
     "      state.pendingApproval = null;"
     "      state.scratchPads = [];"
     "      state.activePadId = '';"
     "      state.activePad = null;"
     "      state.scratchDirty = false;"
     "      persistSession();"
     "      renderApproval();"
     "      renderMessages();"
     "      syncScratchEditor('No scratch pad selected.');"
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
     "    persistSession();"
     "    renderApproval();"
     "    renderMessages();"
     "    syncScratchEditor('No scratch pad selected.');"
     "    updateComposerState();"
     "    composerEl.focus();"
     "    loadSessionMessages();"
     "    loadScratchPads();"
     "    window.setInterval(() => {"
     "      if (state.sessionId) {"
     "        pollApproval();"
     "      }"
     "    }, 1000);"
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

(defn- request-header
  [req header-name]
  (let [target (str/lower-case header-name)]
    (or (get-in req [:headers header-name])
        (get-in req [:headers target])
        (some (fn [[k v]]
                (when (= target (str/lower-case (str k)))
                  v))
              (:headers req)))))

(defn- session-secret []
  @local-session-secret)

(defn- session-cookie-value []
  (str local-session-cookie-name "=" (session-secret)))

(defn- session-cookie-header []
  (str (session-cookie-value) "; Path=/; HttpOnly; SameSite=Strict"))

(defn- cookie-map
  [req]
  (let [cookie-header (request-header req "cookie")]
    (into {}
          (keep (fn [part]
                  (let [[k v] (str/split (str/trim part) #"=" 2)]
                    (when (and (seq k) (some? v))
                      [k v]))))
          (str/split (or cookie-header "") #";"))))

(defn- local-origin?
  [origin]
  (try
    (let [host (.getHost (java.net.URI. origin))]
      (contains? local-hosts host))
    (catch Exception _
      false)))

(defn- valid-session-secret?
  [req]
  (= (get (cookie-map req) local-session-cookie-name)
     (session-secret)))

(defn- trusted-local-origin?
  "Allow loopback browser origins and direct local clients with no origin
   headers. Origin checks prevent cross-site requests from using the cookie."
  [req]
  (let [origin  (request-header req "origin")
        referer (request-header req "referer")]
    (cond
      (seq origin)  (local-origin? origin)
      (seq referer) (local-origin? referer)
      :else         true)))

(defn- trusted-browser-request?
  "Stateful local UI/API routes require both a local browser origin (when
   present) and the per-process session secret cookie."
  [req]
  (and (trusted-local-origin? req)
       (valid-session-secret? req)))

(defn- json-response [status body]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/write-json-str body)})

(defn- html-response [body]
  {:status  200
   :headers {"Content-Type" "text/html; charset=utf-8"}
   :body    body})

(defn- forbidden-response []
  (json-response 403 {:error "forbidden origin"}))

(defn- unauthorized-response []
  (json-response 401 {:error "missing or invalid local session secret"}))

(defn- protected-route-response
  [req allowed-fn]
  (cond
    (not (trusted-local-origin? req))
    (forbidden-response)

    (not (valid-session-secret? req))
    (unauthorized-response)

    :else
    (allowed-fn)))

(defn- approval->body
  [{:keys [approval-id tool-id tool-name description arguments reason policy created-at]}]
  {:approval_id approval-id
   :tool_id     (name tool-id)
   :tool_name   tool-name
   :description description
   :arguments   arguments
   :reason      reason
   :policy      (name policy)
   :created_at  (some-> created-at .toInstant str)})

(defn- clear-pending-approval!
  [session-id approval-id]
  (swap! pending-approvals
         (fn [pending]
           (let [current (get pending session-id)]
             (if (= approval-id (:approval-id current))
               (dissoc pending session-id)
               pending)))))

(defn- http-approval-handler
  [{:keys [session-id tool-id tool-name description arguments reason policy]}]
  (let [sid (some-> session-id str)]
    (when-not sid
      (throw (ex-info "HTTP approval requires a session id"
                      {:tool-id tool-id})))
    (let [approval-id (str (random-uuid))
          decision    (promise)
          approval    {:approval-id approval-id
                       :tool-id     tool-id
                       :tool-name   (or tool-name (name tool-id))
                       :description description
                       :arguments   arguments
                       :reason      reason
                       :policy      policy
                       :created-at  (java.util.Date.)
                       :decision    decision}]
      (swap! pending-approvals assoc sid approval)
      (try
        (let [result (deref decision approval-timeout-ms ::timeout)]
          (case result
            :allow true
            :deny  false
            (throw (ex-info "Timed out waiting for tool approval"
                            {:tool-id tool-id
                             :session-id sid}))))
        (finally
          (clear-pending-approval! sid approval-id))))))

(defn- parse-session-id
  [session-id]
  (try
    (java.util.UUID/fromString session-id)
    session-id
    (catch IllegalArgumentException _
      nil)))

(defn- session-exists?
  [session-id]
  (when-let [sid (parse-session-id session-id)]
    (boolean
      (ffirst (db/q '[:find ?e :in $ ?sid
                      :where
                      [?e :session/id ?sid]]
                    (java.util.UUID/fromString sid))))))

(defn- instant->str [value]
  (some-> value .toInstant str))

(defn- scratch-pad->body
  [pad]
  {:id         (:id pad)
   :scope      (name (:scope pad))
   :session_id (:session-id pad)
   :title      (:title pad)
   :content    (:content pad)
   :mime       (:mime pad)
   :version    (:version pad)
   :created_at (instant->str (:created-at pad))
   :updated_at (instant->str (:updated-at pad))})

(defn- scratch-metadata->body
  [pad]
  (dissoc (scratch-pad->body pad) :content))

(defn- session-scratch-pad
  [session-id pad-id]
  (let [pad (scratch/get-pad pad-id)]
    (when (and pad
               (= :session (:scope pad))
               (= session-id (:session-id pad)))
      pad)))

(defn- handle-create-session []
  (let [sid (db/create-session! :http)]
    (json-response 200 {:session_id (str sid)})))

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

(defn- handle-get-approval [session-id]
  (if-not (parse-session-id session-id)
    (json-response 400 {:error "invalid session id"})
    (if-let [approval (get @pending-approvals session-id)]
      (json-response 200 {:pending true
                          :approval (approval->body approval)})
      (json-response 200 {:pending false}))))

(defn- handle-submit-approval [session-id req]
  (if-not (parse-session-id session-id)
    (json-response 400 {:error "invalid session id"})
    (let [data        (read-body req)
          approval-id (get data "approval_id")
          decision    (get data "decision")
          current     (get @pending-approvals session-id)
          decision*   (case decision
                        "allow" :allow
                        "deny"  :deny
                        nil)]
      (cond
        (nil? current)
        (json-response 404 {:error "no pending approval"})

        (not= approval-id (:approval-id current))
        (json-response 409 {:error "stale approval id"})

        (nil? decision*)
        (json-response 400 {:error "invalid decision"})

        :else
        (do
          (deliver (:decision current) decision*)
          (clear-pending-approval! session-id approval-id)
          (json-response 200 {:status "recorded"}))))))

(defn- handle-session-messages [session-id]
  (try
    (let [sid      (java.util.UUID/fromString session-id)
          messages (->> (db/session-messages sid)
                        (filter #(#{:user :assistant} (:role %)))
                        (mapv (fn [{:keys [role content created-at]}]
                                {:role       (name role)
                                 :content    content
                                 :created_at (some-> created-at .toInstant str)})))]
      (json-response 200 {:session_id session-id
                          :messages   messages}))
    (catch IllegalArgumentException _
      (json-response 400 {:error "invalid session id"}))))

(defn- handle-list-scratch-pads [session-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (json-response 200
                   {:session_id session-id
                    :pads       (mapv scratch-metadata->body
                                      (scratch/list-pads {:scope :session
                                                          :session-id session-id}))})))

(defn- handle-create-scratch-pad [session-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (let [data (or (read-body req) {})
          pad  (scratch/create-pad! {:scope      :session
                                     :session-id session-id
                                     :title      (get data "title")
                                     :content    (get data "content")
                                     :mime       (get data "mime")})]
      (json-response 201 {:session_id session-id
                          :pad        (scratch-pad->body pad)}))))

(defn- handle-get-scratch-pad [session-id pad-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (if-let [pad (session-scratch-pad session-id pad-id)]
      (json-response 200 {:session_id session-id
                          :pad        (scratch-pad->body pad)})
      (json-response 404 {:error "scratch pad not found"}))))

(defn- handle-save-scratch-pad [session-id pad-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (let [data    (or (read-body req) {})
          updates (cond-> {}
                    (contains? data "title")            (assoc :title (get data "title"))
                    (contains? data "content")          (assoc :content (get data "content"))
                    (contains? data "mime")             (assoc :mime (get data "mime"))
                    (contains? data "expected_version") (assoc :expected-version
                                                               (get data "expected_version")))]
      (try
        (json-response 200
                       {:session_id session-id
                        :pad        (scratch-pad->body (scratch/save-pad! pad-id updates))})
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type]} (ex-data e)]
            (case type
              :scratch/version-conflict
              (json-response 409 {:error "scratch pad version conflict"
                                  :details (select-keys (ex-data e)
                                                        [:expected-version :actual-version])})
              :scratch/not-found
              (json-response 404 {:error "scratch pad not found"})
              (json-response 400 {:error (.getMessage e)}))))))))

(defn- handle-edit-scratch-pad [session-id pad-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (let [data      (or (read-body req) {})
          operation (if (map? (get data "operation"))
                      (get data "operation")
                      data)
          edit      (cond-> {:op (get operation "op")}
                      (contains? operation "text")          (assoc :text (get operation "text"))
                      (contains? operation "separator")     (assoc :separator (get operation "separator"))
                      (contains? operation "match")         (assoc :match (get operation "match"))
                      (contains? operation "replacement")   (assoc :replacement (get operation "replacement"))
                      (contains? operation "occurrence")    (assoc :occurrence (get operation "occurrence"))
                      (contains? operation "offset")        (assoc :offset (get operation "offset"))
                      (contains? operation "start_line")    (assoc :start-line (get operation "start_line"))
                      (contains? operation "end_line")      (assoc :end-line (get operation "end_line"))
                      (contains? data "expected_version")   (assoc :expected-version
                                                                   (get data "expected_version"))
                      (contains? operation "expected_version") (assoc :expected-version
                                                                     (get operation "expected_version")))]
      (try
        (json-response 200
                       {:session_id session-id
                        :pad        (scratch-pad->body (scratch/edit-pad! pad-id edit))})
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type]} (ex-data e)]
            (case type
              :scratch/version-conflict
              (json-response 409 {:error "scratch pad version conflict"
                                  :details (select-keys (ex-data e)
                                                        [:expected-version :actual-version])})
              :scratch/not-found
              (json-response 404 {:error "scratch pad not found"})
              (json-response 400 {:error (.getMessage e)}))))))))

(defn- handle-delete-scratch-pad [session-id pad-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (do
      (scratch/delete-pad! pad-id)
      (json-response 200 {:status "deleted"
                          :session_id session-id
                          :pad_id pad-id}))))

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
  (assoc-in (html-response local-ui-html)
            [:headers "Set-Cookie"]
            (session-cookie-header)))

;; ---------------------------------------------------------------------------
;; Router
;; ---------------------------------------------------------------------------

(defn- router [req]
  (let [uri    (:uri req)
        method (:request-method req)
        session-match      (re-matches #"/sessions/([0-9a-fA-F-]+)/messages" uri)
        approval-match     (re-matches #"/sessions/([0-9a-fA-F-]+)/approval" uri)
        scratch-list-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads" uri)
        scratch-pad-match  (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)" uri)
        scratch-edit-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)/edit" uri)]
    (cond
      (and (= method :get) (= uri "/"))
      (handle-home req)

      ;; WebSocket upgrade
      (and (= uri "/ws")
           (http/websocket-handshake-check req)
           (trusted-browser-request? req))
      (ws-handler req)

      (and (= uri "/ws") (http/websocket-handshake-check req))
      (if (trusted-local-origin? req)
        (unauthorized-response)
        (forbidden-response))

      ;; REST
      (and (= method :post) (= uri "/sessions"))
      (protected-route-response req handle-create-session)

      (and (= method :post) (= uri "/chat"))
      (protected-route-response req #(handle-chat req))

      (and (= method :get) approval-match)
      (protected-route-response req #(handle-get-approval (second approval-match)))

      (and (= method :post) approval-match)
      (protected-route-response req #(handle-submit-approval (second approval-match) req))

      (and (= method :get) session-match)
      (protected-route-response req #(handle-session-messages (second session-match)))

      (and (= method :get) scratch-list-match)
      (protected-route-response req #(handle-list-scratch-pads (second scratch-list-match)))

      (and (= method :post) scratch-list-match)
      (protected-route-response req #(handle-create-scratch-pad (second scratch-list-match) req))

      (and (= method :get) scratch-pad-match)
      (protected-route-response req #(handle-get-scratch-pad (second scratch-pad-match)
                                                             (nth scratch-pad-match 2)))

      (and (= method :put) scratch-pad-match)
      (protected-route-response req #(handle-save-scratch-pad (second scratch-pad-match)
                                                              (nth scratch-pad-match 2)
                                                              req))

      (and (= method :delete) scratch-pad-match)
      (protected-route-response req #(handle-delete-scratch-pad (second scratch-pad-match)
                                                                (nth scratch-pad-match 2)))

      (and (= method :post) scratch-edit-match)
      (protected-route-response req #(handle-edit-scratch-pad (second scratch-edit-match)
                                                              (nth scratch-edit-match 2)
                                                              req))

      (and (= method :get) (= uri "/skills"))
      (protected-route-response req #(handle-skills req))

      (and (= method :get) (= uri "/health"))
      (handle-health req)

      :else
      (json-response 404 {:error "not found"}))))

;; ---------------------------------------------------------------------------
;; Server lifecycle
;; ---------------------------------------------------------------------------

(defn start!
  "Start the HTTP/WebSocket server.
   Defaults to loopback-only binding."
  ([port]
   (start! "127.0.0.1" port))
  ([bind-host port]
   (when @server-atom
     (log/warn "Server already running"))
   (prompt/register-approval! :http http-approval-handler)
   (prompt/register-approval! :websocket http-approval-handler)
   (let [s (http/run-server router {:ip bind-host :port port})]
     (reset! server-atom s)
     (log/info "HTTP/WebSocket server started on" bind-host ":" port)
     s)))

(defn stop! []
  (when-let [s @server-atom]
    (s) ; http-kit stop fn
    (prompt/register-approval! :http nil)
    (prompt/register-approval! :websocket nil)
    (reset! pending-approvals {})
    (reset! server-atom nil)
    (log/info "Server stopped")))
