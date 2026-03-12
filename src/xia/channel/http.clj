(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [clojure.string :as str]
            [org.httpkit.server :as http]
            [charred.api :as json]
            [clojure.tools.logging :as log]
            [xia.scratch :as scratch]
            [xia.db :as db]
            [xia.oauth :as oauth]
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
(def ^:private service-auth-types #{:bearer :basic :api-key-header :query-param :oauth-account})
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
     "    #site-extra-fields {"
     "      min-height: 150px;"
     "      border-radius: 18px;"
     "      padding: 14px 16px;"
     "      font-family: \"SFMono-Regular\", Consolas, \"Liberation Mono\", monospace;"
     "      font-size: 0.9rem;"
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
     "    .admin-panel { gap: 18px; }"
     "    .admin-grid {"
     "      display: grid;"
     "      gap: 16px;"
     "      grid-template-columns: repeat(2, minmax(0, 1fr));"
     "    }"
     "    .admin-card {"
     "      border: 1px solid rgba(23, 33, 25, 0.12);"
     "      border-radius: 22px;"
     "      padding: 18px;"
     "      background: rgba(255, 255, 255, 0.5);"
     "      display: grid;"
     "      gap: 14px;"
     "      min-height: 0;"
     "    }"
     "    .admin-card-wide { grid-column: 1 / -1; }"
     "    .admin-list {"
     "      display: grid;"
     "      gap: 10px;"
     "      max-height: 220px;"
     "      overflow: auto;"
     "      padding-right: 4px;"
     "    }"
     "    .admin-list-empty {"
     "      padding: 16px;"
     "      border-radius: 16px;"
     "      border: 1px dashed rgba(23, 33, 25, 0.2);"
     "      color: var(--muted);"
     "      background: rgba(255, 255, 255, 0.42);"
     "      line-height: 1.5;"
     "    }"
     "    .admin-item {"
     "      width: 100%;"
     "      border-radius: 18px;"
     "      border: 1px solid rgba(23, 33, 25, 0.12);"
     "      background: rgba(255, 255, 255, 0.68);"
     "      padding: 12px 14px;"
     "      text-align: left;"
     "      display: grid;"
     "      gap: 6px;"
     "    }"
     "    .admin-item.active {"
     "      border-color: rgba(178, 76, 50, 0.28);"
     "      background: rgba(255, 245, 240, 0.92);"
     "    }"
     "    .admin-item-title {"
     "      font-size: 0.95rem;"
     "      font-weight: 700;"
     "      color: var(--ink);"
     "    }"
     "    .admin-item-meta {"
     "      color: var(--muted);"
     "      font-size: 0.82rem;"
     "      line-height: 1.45;"
     "    }"
     "    .field-grid {"
     "      display: grid;"
     "      gap: 12px;"
     "      grid-template-columns: repeat(2, minmax(0, 1fr));"
     "    }"
     "    .field-grid .field.full { grid-column: 1 / -1; }"
     "    .check-row {"
     "      display: flex;"
     "      gap: 10px;"
     "      align-items: center;"
     "      min-height: 100%;"
     "    }"
     "    .check-row input { width: 18px; height: 18px; }"
     "    .check-label {"
     "      color: var(--ink);"
     "      font-size: 0.94rem;"
     "      font-weight: 600;"
     "      line-height: 1.4;"
     "    }"
     "    .secret-note {"
     "      color: var(--muted);"
     "      font-size: 0.84rem;"
     "      line-height: 1.45;"
     "    }"
     "    .admin-status {"
     "      color: var(--muted);"
     "      font-size: 0.86rem;"
     "      line-height: 1.45;"
     "    }"
     "    .capability-grid {"
     "      display: grid;"
     "      gap: 16px;"
     "      grid-template-columns: repeat(2, minmax(0, 1fr));"
     "    }"
     "    .capability-list {"
     "      display: grid;"
     "      gap: 10px;"
     "      max-height: 260px;"
     "      overflow: auto;"
     "      padding-right: 4px;"
     "    }"
     "    .capability-item {"
     "      border: 1px solid rgba(23, 33, 25, 0.12);"
     "      border-radius: 16px;"
     "      background: rgba(255, 255, 255, 0.58);"
     "      padding: 12px 14px;"
     "      display: grid;"
     "      gap: 6px;"
     "    }"
     "    .capability-title {"
     "      display: flex;"
     "      justify-content: space-between;"
     "      gap: 10px;"
     "      align-items: center;"
     "      font-size: 0.92rem;"
     "      font-weight: 700;"
     "    }"
     "    .capability-meta {"
     "      color: var(--muted);"
     "      font-size: 0.82rem;"
     "      line-height: 1.5;"
     "    }"
     "    .badge {"
     "      display: inline-flex;"
     "      align-items: center;"
     "      border-radius: 999px;"
     "      padding: 4px 10px;"
     "      font-size: 0.76rem;"
     "      font-weight: 700;"
     "      letter-spacing: 0.06em;"
     "      text-transform: uppercase;"
     "      background: rgba(76, 122, 94, 0.14);"
     "      color: #35553f;"
     "    }"
     "    .badge.off {"
     "      background: rgba(91, 102, 88, 0.12);"
     "      color: var(--muted);"
     "    }"
     "    @media (max-width: 980px) {"
     "      .hero, .workspace, .admin-grid, .capability-grid, .field-grid { grid-template-columns: 1fr; }"
     "      .admin-card-wide { grid-column: auto; }"
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
     "      <section class=\"panel admin-panel\">"
     "        <div class=\"panel-header\">"
     "          <div>"
     "            <h2 class=\"panel-title\">Admin</h2>"
     "            <p class=\"panel-note\">Configure providers, OAuth accounts, service credentials, and site logins without dropping to the terminal. Stored secrets never come back to the browser once saved.</p>"
     "          </div>"
     "        </div>"
     "        <div class=\"admin-grid\">"
     "          <section class=\"admin-card\">"
     "            <div class=\"panel-header\">"
     "              <div>"
     "                <h3 class=\"panel-title\">Providers</h3>"
     "                <p class=\"panel-note\">LLM endpoints Xia can use for chat.</p>"
     "              </div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"new-provider\" type=\"button\">New provider</button>"
     "              </div>"
     "            </div>"
     "            <div class=\"admin-list\" id=\"provider-list\"></div>"
     "            <div class=\"field-grid\">"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"provider-id\">Id</label>"
     "                <input class=\"text-input\" id=\"provider-id\" type=\"text\" placeholder=\"openai\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"provider-name\">Name</label>"
     "                <input class=\"text-input\" id=\"provider-name\" type=\"text\" placeholder=\"OpenAI\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"provider-base-url\">Base URL</label>"
     "                <input class=\"text-input\" id=\"provider-base-url\" type=\"url\" placeholder=\"https://api.openai.com/v1\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"provider-model\">Model</label>"
     "                <input class=\"text-input\" id=\"provider-model\" type=\"text\" placeholder=\"gpt-5\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"provider-api-key\">API Key</label>"
     "                <input class=\"text-input\" id=\"provider-api-key\" type=\"password\" autocomplete=\"off\" placeholder=\"Leave blank to keep stored key\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"check-row\" for=\"provider-default\">"
     "                  <input id=\"provider-default\" type=\"checkbox\">"
     "                  <span class=\"check-label\">Use as Xia's default provider</span>"
     "                </label>"
     "              </div>"
     "            </div>"
     "            <div class=\"composer-foot\">"
     "              <div class=\"admin-status\" id=\"provider-status\">No provider selected.</div>"
     "              <div class=\"actions\">"
     "                <button class=\"primary\" id=\"save-provider\" type=\"button\">Save provider</button>"
     "              </div>"
     "            </div>"
     "          </section>"
     "          <section class=\"admin-card\">"
     "            <div class=\"panel-header\">"
     "              <div>"
     "                <h3 class=\"panel-title\">OAuth Accounts</h3>"
     "                <p class=\"panel-note\">Authorization-code + PKCE accounts for APIs that cannot use static tokens alone.</p>"
     "              </div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"new-oauth-account\" type=\"button\">New account</button>"
     "              </div>"
     "            </div>"
     "            <div class=\"admin-list\" id=\"oauth-account-list\"></div>"
     "            <div class=\"field-grid\">"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"oauth-account-id\">Id</label>"
     "                <input class=\"text-input\" id=\"oauth-account-id\" type=\"text\" placeholder=\"google-work\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"oauth-account-name\">Name</label>"
     "                <input class=\"text-input\" id=\"oauth-account-name\" type=\"text\" placeholder=\"Google Workspace\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"oauth-authorize-url\">Authorize URL</label>"
     "                <input class=\"text-input\" id=\"oauth-authorize-url\" type=\"url\" placeholder=\"https://accounts.google.com/o/oauth2/v2/auth\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"oauth-token-url\">Token URL</label>"
     "                <input class=\"text-input\" id=\"oauth-token-url\" type=\"url\" placeholder=\"https://oauth2.googleapis.com/token\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"oauth-client-id\">Client ID</label>"
     "                <input class=\"text-input\" id=\"oauth-client-id\" type=\"text\" placeholder=\"client-id\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"oauth-client-secret\">Client Secret</label>"
     "                <input class=\"text-input\" id=\"oauth-client-secret\" type=\"password\" autocomplete=\"off\" placeholder=\"Leave blank to keep stored secret\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"oauth-scopes\">Scopes</label>"
     "                <input class=\"text-input\" id=\"oauth-scopes\" type=\"text\" placeholder=\"repo read:user offline_access\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"oauth-redirect-uri\">Redirect URI</label>"
     "                <input class=\"text-input\" id=\"oauth-redirect-uri\" type=\"url\" placeholder=\"Optional. Leave blank to use this Xia server's /oauth/callback\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"oauth-auth-params\">Authorize Params JSON</label>"
     "                <textarea id=\"oauth-auth-params\" spellcheck=\"false\" placeholder='{\"access_type\":\"offline\",\"prompt\":\"consent\"}'></textarea>"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"oauth-token-params\">Token Params JSON</label>"
     "                <textarea id=\"oauth-token-params\" spellcheck=\"false\" placeholder='{\"audience\":\"https://api.example.com\"}'></textarea>"
     "              </div>"
     "            </div>"
     "            <div class=\"composer-foot\">"
     "              <div class=\"admin-status\" id=\"oauth-account-status\">No OAuth account selected.</div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"connect-oauth-account\" type=\"button\">Connect</button>"
     "                <button class=\"secondary\" id=\"refresh-oauth-account\" type=\"button\">Refresh</button>"
     "                <button class=\"secondary\" id=\"delete-oauth-account\" type=\"button\">Delete</button>"
     "                <button class=\"primary\" id=\"save-oauth-account\" type=\"button\">Save account</button>"
     "              </div>"
     "            </div>"
     "          </section>"
     "          <section class=\"admin-card\">"
     "            <div class=\"panel-header\">"
     "              <div>"
     "                <h3 class=\"panel-title\">Services</h3>"
     "                <p class=\"panel-note\">Registered API endpoints Xia can call through the service proxy.</p>"
     "              </div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"new-service\" type=\"button\">New service</button>"
     "              </div>"
     "            </div>"
     "            <div class=\"admin-list\" id=\"service-list\"></div>"
     "            <div class=\"field-grid\">"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"service-id\">Id</label>"
     "                <input class=\"text-input\" id=\"service-id\" type=\"text\" placeholder=\"github\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"service-name\">Name</label>"
     "                <input class=\"text-input\" id=\"service-name\" type=\"text\" placeholder=\"GitHub\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"service-base-url\">Base URL</label>"
     "                <input class=\"text-input\" id=\"service-base-url\" type=\"url\" placeholder=\"https://api.github.com\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"service-auth-type\">Auth Type</label>"
     "                <select class=\"text-input\" id=\"service-auth-type\">"
     "                  <option value=\"bearer\">Bearer token</option>"
     "                  <option value=\"basic\">Basic auth</option>"
     "                  <option value=\"api-key-header\">API key header</option>"
     "                  <option value=\"query-param\">Query parameter</option>"
     "                  <option value=\"oauth-account\">OAuth account</option>"
     "                </select>"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"service-auth-header\">Header / Param</label>"
     "                <input class=\"text-input\" id=\"service-auth-header\" type=\"text\" placeholder=\"X-API-Key\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"service-oauth-account\">OAuth Account</label>"
     "                <select class=\"text-input\" id=\"service-oauth-account\"></select>"
     "                <div class=\"secret-note\">Pick a connected OAuth account when auth type is OAuth account. Otherwise leave this blank.</div>"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"service-auth-key\">Auth Secret</label>"
     "                <input class=\"text-input\" id=\"service-auth-key\" type=\"password\" autocomplete=\"off\" placeholder=\"Leave blank to keep stored secret\">"
     "                <div class=\"secret-note\">For bearer/basic auth, the header field can stay blank. OAuth services use the linked OAuth account instead of this field.</div>"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"check-row\" for=\"service-enabled\">"
     "                  <input id=\"service-enabled\" type=\"checkbox\" checked>"
     "                  <span class=\"check-label\">Enabled for tool calls</span>"
     "                </label>"
     "              </div>"
     "            </div>"
     "            <div class=\"composer-foot\">"
     "              <div class=\"admin-status\" id=\"service-status\">No service selected.</div>"
     "              <div class=\"actions\">"
     "                <button class=\"primary\" id=\"save-service\" type=\"button\">Save service</button>"
     "              </div>"
     "            </div>"
     "          </section>"
     "          <section class=\"admin-card\">"
     "            <div class=\"panel-header\">"
     "              <div>"
     "                <h3 class=\"panel-title\">Site Logins</h3>"
     "                <p class=\"panel-note\">Stored credentials for browser login flows. Leave secret fields blank to keep what is already stored.</p>"
     "              </div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"new-site\" type=\"button\">New site</button>"
     "              </div>"
     "            </div>"
     "            <div class=\"admin-list\" id=\"site-list\"></div>"
     "            <div class=\"field-grid\">"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"site-id\">Id</label>"
     "                <input class=\"text-input\" id=\"site-id\" type=\"text\" placeholder=\"github\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"site-name\">Name</label>"
     "                <input class=\"text-input\" id=\"site-name\" type=\"text\" placeholder=\"GitHub login\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"site-login-url\">Login URL</label>"
     "                <input class=\"text-input\" id=\"site-login-url\" type=\"url\" placeholder=\"https://github.com/login\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"site-username-field\">Username Field</label>"
     "                <input class=\"text-input\" id=\"site-username-field\" type=\"text\" placeholder=\"login\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"site-password-field\">Password Field</label>"
     "                <input class=\"text-input\" id=\"site-password-field\" type=\"text\" placeholder=\"password\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"site-username\">Username</label>"
     "                <input class=\"text-input\" id=\"site-username\" type=\"text\" autocomplete=\"off\" placeholder=\"Leave blank to keep stored username\">"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\" for=\"site-password\">Password</label>"
     "                <input class=\"text-input\" id=\"site-password\" type=\"password\" autocomplete=\"off\" placeholder=\"Leave blank to keep stored password\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"site-form-selector\">Form Selector</label>"
     "                <input class=\"text-input\" id=\"site-form-selector\" type=\"text\" placeholder=\"Optional CSS selector for the login form\">"
     "              </div>"
     "              <div class=\"field full\">"
     "                <label class=\"field-label\" for=\"site-extra-fields\">Extra Fields JSON</label>"
     "                <textarea id=\"site-extra-fields\" spellcheck=\"false\" placeholder='{\"remember_me\":\"1\"}'></textarea>"
     "              </div>"
     "            </div>"
     "            <div class=\"composer-foot\">"
     "              <div class=\"admin-status\" id=\"site-status\">No site selected.</div>"
     "              <div class=\"actions\">"
     "                <button class=\"secondary\" id=\"delete-site\" type=\"button\">Delete</button>"
     "                <button class=\"primary\" id=\"save-site\" type=\"button\">Save site</button>"
     "              </div>"
     "            </div>"
     "          </section>"
     "          <section class=\"admin-card admin-card-wide\">"
     "            <div class=\"panel-header\">"
     "              <div>"
     "                <h3 class=\"panel-title\">Capabilities</h3>"
     "                <p class=\"panel-note\">Installed tools and skills currently available to Xia.</p>"
     "              </div>"
     "            </div>"
     "            <div class=\"capability-grid\">"
     "              <div class=\"field\">"
     "                <label class=\"field-label\">Tools</label>"
     "                <div class=\"capability-list\" id=\"tool-list\"></div>"
     "              </div>"
     "              <div class=\"field\">"
     "                <label class=\"field-label\">Skills</label>"
     "                <div class=\"capability-list\" id=\"skill-list\"></div>"
     "              </div>"
     "            </div>"
     "          </section>"
     "        </div>"
     "      </section>"
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
     "      scratchSaving: false,"
     "      admin: {"
     "        providers: [],"
     "        oauthAccounts: [],"
     "        services: [],"
     "        sites: [],"
     "        tools: [],"
     "        skills: []"
     "      },"
     "      activeProviderId: '',"
     "      activeOauthAccountId: '',"
     "      activeServiceId: '',"
     "      activeSiteId: '',"
     "      providerSaving: false,"
     "      oauthSaving: false,"
     "      serviceSaving: false,"
     "      siteSaving: false"
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
     "    const providerListEl = document.getElementById('provider-list');"
     "    const providerIdEl = document.getElementById('provider-id');"
     "    const providerNameEl = document.getElementById('provider-name');"
     "    const providerBaseUrlEl = document.getElementById('provider-base-url');"
     "    const providerModelEl = document.getElementById('provider-model');"
     "    const providerApiKeyEl = document.getElementById('provider-api-key');"
     "    const providerDefaultEl = document.getElementById('provider-default');"
     "    const providerStatusEl = document.getElementById('provider-status');"
     "    const newProviderEl = document.getElementById('new-provider');"
     "    const saveProviderEl = document.getElementById('save-provider');"
     "    const oauthAccountListEl = document.getElementById('oauth-account-list');"
     "    const oauthAccountIdEl = document.getElementById('oauth-account-id');"
     "    const oauthAccountNameEl = document.getElementById('oauth-account-name');"
     "    const oauthAuthorizeUrlEl = document.getElementById('oauth-authorize-url');"
     "    const oauthTokenUrlEl = document.getElementById('oauth-token-url');"
     "    const oauthClientIdEl = document.getElementById('oauth-client-id');"
     "    const oauthClientSecretEl = document.getElementById('oauth-client-secret');"
     "    const oauthScopesEl = document.getElementById('oauth-scopes');"
     "    const oauthRedirectUriEl = document.getElementById('oauth-redirect-uri');"
     "    const oauthAuthParamsEl = document.getElementById('oauth-auth-params');"
     "    const oauthTokenParamsEl = document.getElementById('oauth-token-params');"
     "    const oauthAccountStatusEl = document.getElementById('oauth-account-status');"
     "    const newOauthAccountEl = document.getElementById('new-oauth-account');"
     "    const saveOauthAccountEl = document.getElementById('save-oauth-account');"
     "    const connectOauthAccountEl = document.getElementById('connect-oauth-account');"
     "    const refreshOauthAccountEl = document.getElementById('refresh-oauth-account');"
     "    const deleteOauthAccountEl = document.getElementById('delete-oauth-account');"
     "    const serviceListEl = document.getElementById('service-list');"
     "    const serviceIdEl = document.getElementById('service-id');"
     "    const serviceNameEl = document.getElementById('service-name');"
     "    const serviceBaseUrlEl = document.getElementById('service-base-url');"
     "    const serviceAuthTypeEl = document.getElementById('service-auth-type');"
     "    const serviceAuthHeaderEl = document.getElementById('service-auth-header');"
     "    const serviceOauthAccountEl = document.getElementById('service-oauth-account');"
     "    const serviceAuthKeyEl = document.getElementById('service-auth-key');"
     "    const serviceEnabledEl = document.getElementById('service-enabled');"
     "    const serviceStatusEl = document.getElementById('service-status');"
     "    const newServiceEl = document.getElementById('new-service');"
     "    const saveServiceEl = document.getElementById('save-service');"
     "    const siteListEl = document.getElementById('site-list');"
     "    const siteIdEl = document.getElementById('site-id');"
     "    const siteNameEl = document.getElementById('site-name');"
     "    const siteLoginUrlEl = document.getElementById('site-login-url');"
     "    const siteUsernameFieldEl = document.getElementById('site-username-field');"
     "    const sitePasswordFieldEl = document.getElementById('site-password-field');"
     "    const siteUsernameEl = document.getElementById('site-username');"
     "    const sitePasswordEl = document.getElementById('site-password');"
     "    const siteFormSelectorEl = document.getElementById('site-form-selector');"
     "    const siteExtraFieldsEl = document.getElementById('site-extra-fields');"
     "    const siteStatusEl = document.getElementById('site-status');"
     "    const newSiteEl = document.getElementById('new-site');"
     "    const saveSiteEl = document.getElementById('save-site');"
     "    const deleteSiteEl = document.getElementById('delete-site');"
     "    const toolListEl = document.getElementById('tool-list');"
     "    const skillListEl = document.getElementById('skill-list');"
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
     "    async function fetchJson(url, options) {"
     "      const response = await fetch(url, options || {});"
     "      const data = await response.json();"
     "      if (!response.ok) {"
     "        throw new Error(data.error || 'Request failed');"
     "      }"
     "      return data;"
     "    }"
     ""
     "    function firstNonEmpty(value, fallback) {"
     "      return value && String(value).trim() ? String(value).trim() : (fallback || '');"
     "    }"
     ""
     "    function renderSelectableList(target, items, activeId, emptyText, titleFn, metaFn, onSelect) {"
     "      target.innerHTML = '';"
     "      if (!items.length) {"
     "        const empty = document.createElement('div');"
     "        empty.className = 'admin-list-empty';"
     "        empty.textContent = emptyText;"
     "        target.appendChild(empty);"
     "        return;"
     "      }"
     "      items.forEach((item) => {"
     "        const button = document.createElement('button');"
     "        button.type = 'button';"
     "        button.className = 'admin-item' + (item.id === activeId ? ' active' : '');"
     "        const title = document.createElement('div');"
     "        title.className = 'admin-item-title';"
     "        title.textContent = titleFn(item);"
     "        const meta = document.createElement('div');"
     "        meta.className = 'admin-item-meta';"
     "        meta.textContent = metaFn(item);"
     "        button.appendChild(title);"
     "        button.appendChild(meta);"
     "        button.addEventListener('click', () => onSelect(item));"
     "        target.appendChild(button);"
     "      });"
     "    }"
     ""
     "    function renderCapabilityList(target, items, emptyText, detailFn) {"
     "      target.innerHTML = '';"
     "      if (!items.length) {"
     "        const empty = document.createElement('div');"
     "        empty.className = 'admin-list-empty';"
     "        empty.textContent = emptyText;"
     "        target.appendChild(empty);"
     "        return;"
     "      }"
     "      items.forEach((item) => {"
     "        const card = document.createElement('div');"
     "        card.className = 'capability-item';"
     "        const title = document.createElement('div');"
     "        title.className = 'capability-title';"
     "        const name = document.createElement('span');"
     "        name.textContent = firstNonEmpty(item.name, item.id);"
     "        const badge = document.createElement('span');"
     "        badge.className = 'badge' + (item.enabled ? '' : ' off');"
     "        badge.textContent = item.enabled ? 'Enabled' : 'Disabled';"
     "        title.appendChild(name);"
     "        title.appendChild(badge);"
     "        const meta = document.createElement('div');"
     "        meta.className = 'capability-meta';"
     "        meta.textContent = detailFn(item);"
     "        card.appendChild(title);"
     "        card.appendChild(meta);"
     "        target.appendChild(card);"
     "      });"
     "    }"
     ""
     "    function providerMeta(provider) {"
     "      const bits = [];"
     "      if (provider.model) bits.push(provider.model);"
     "      if (provider.default) bits.push('Default');"
     "      bits.push(provider.api_key_configured ? 'API key stored' : 'No API key');"
     "      return bits.join(' • ');"
     "    }"
     ""
     "    function oauthAccountMeta(account) {"
     "      const bits = [];"
     "      bits.push(account.connected ? 'Connected' : 'Not connected');"
     "      if (account.refresh_token_configured) bits.push('Refresh token stored');"
     "      if (account.expires_at) bits.push('Expires ' + formatStamp(account.expires_at));"
     "      return bits.join(' • ');"
     "    }"
     ""
     "    function serviceMeta(service) {"
     "      const bits = [];"
     "      if (service.auth_type) bits.push(service.auth_type);"
     "      if (service.oauth_account_name) bits.push(service.oauth_account_name);"
     "      if (service.auth_type === 'oauth-account') bits.push(service.oauth_account_connected ? 'OAuth connected' : 'OAuth not connected');"
     "      bits.push(service.enabled ? 'Enabled' : 'Disabled');"
     "      bits.push(service.auth_type === 'oauth-account'"
     "        ? (service.oauth_account ? 'Account linked' : 'No account linked')"
     "        : (service.auth_key_configured ? 'Secret stored' : 'No secret'));"
     "      return bits.join(' • ');"
     "    }"
     ""
     "    function siteMeta(site) {"
     "      const bits = [];"
     "      if (site.login_url) bits.push(site.login_url);"
     "      bits.push(site.username_configured ? 'Username stored' : 'No username');"
     "      bits.push(site.password_configured ? 'Password stored' : 'No password');"
     "      return bits.join(' • ');"
     "    }"
     ""
     "    function updateAdminButtons() {"
     "      providerIdEl.disabled = state.providerSaving || !!state.activeProviderId;"
     "      saveProviderEl.disabled = state.providerSaving;"
     "      newProviderEl.disabled = state.providerSaving;"
     "      oauthAccountIdEl.disabled = state.oauthSaving || !!state.activeOauthAccountId;"
     "      saveOauthAccountEl.disabled = state.oauthSaving;"
     "      newOauthAccountEl.disabled = state.oauthSaving;"
     "      connectOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId;"
     "      refreshOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId;"
     "      deleteOauthAccountEl.disabled = state.oauthSaving || !state.activeOauthAccountId;"
     "      serviceIdEl.disabled = state.serviceSaving || !!state.activeServiceId;"
     "      saveServiceEl.disabled = state.serviceSaving;"
     "      newServiceEl.disabled = state.serviceSaving;"
     "      siteIdEl.disabled = state.siteSaving || !!state.activeSiteId;"
     "      saveSiteEl.disabled = state.siteSaving;"
     "      newSiteEl.disabled = state.siteSaving;"
     "      deleteSiteEl.disabled = state.siteSaving || !state.activeSiteId;"
     "    }"
     ""
     "    function renderProviderList() {"
     "      renderSelectableList("
     "        providerListEl,"
     "        state.admin.providers,"
     "        state.activeProviderId,"
     "        'No providers configured yet. Add one so Xia can talk to an LLM.',"
     "        (provider) => firstNonEmpty(provider.name, provider.id),"
     "        providerMeta,"
     "        selectProvider"
     "      );"
     "    }"
     ""
     "    function renderOauthAccountList() {"
     "      renderSelectableList("
     "        oauthAccountListEl,"
     "        state.admin.oauthAccounts,"
     "        state.activeOauthAccountId,"
     "        'No OAuth accounts configured yet. Add one for APIs that need a real OAuth flow.',"
     "        (account) => firstNonEmpty(account.name, account.id),"
     "        oauthAccountMeta,"
     "        selectOauthAccount"
     "      );"
     "    }"
     ""
     "    function renderOauthAccountOptions() {"
     "      const previous = serviceOauthAccountEl.value;"
     "      serviceOauthAccountEl.innerHTML = '';"
     "      const blank = document.createElement('option');"
     "      blank.value = '';"
     "      blank.textContent = 'None';"
     "      serviceOauthAccountEl.appendChild(blank);"
     "      state.admin.oauthAccounts.forEach((account) => {"
     "        const option = document.createElement('option');"
     "        option.value = account.id || '';"
     "        option.textContent = firstNonEmpty(account.name, account.id)"
     "          + (account.connected ? ' (connected)' : ' (not connected)');"
     "        serviceOauthAccountEl.appendChild(option);"
     "      });"
     "      if (state.activeServiceId && state.admin.services.some((service) => service.id === state.activeServiceId)) {"
     "        const current = state.admin.services.find((service) => service.id === state.activeServiceId);"
     "        serviceOauthAccountEl.value = (current && current.oauth_account) || '';"
     "      } else {"
     "        serviceOauthAccountEl.value = previous || '';"
     "      }"
     "    }"
     ""
     "    function renderServiceList() {"
     "      renderSelectableList("
     "        serviceListEl,"
     "        state.admin.services,"
     "        state.activeServiceId,"
     "        'No services configured yet. Add one when Xia needs an authenticated API.',"
     "        (service) => firstNonEmpty(service.name, service.id),"
     "        serviceMeta,"
     "        selectService"
     "      );"
     "    }"
     ""
     "    function renderSiteList() {"
     "      renderSelectableList("
     "        siteListEl,"
     "        state.admin.sites,"
     "        state.activeSiteId,"
     "        'No site logins configured yet. Add one when browser automation needs stored credentials.',"
     "        (site) => firstNonEmpty(site.name, site.id),"
     "        siteMeta,"
     "        selectSite"
     "      );"
     "    }"
     ""
     "    function renderCapabilities() {"
     "      renderCapabilityList("
     "        toolListEl,"
     "        state.admin.tools,"
     "        'No tools installed.',"
     "        (tool) => [tool.id, tool.approval ? ('approval: ' + tool.approval) : '', tool.description || '']"
     "          .filter(Boolean)"
     "          .join(' • ')"
     "      );"
     "      renderCapabilityList("
     "        skillListEl,"
     "        state.admin.skills,"
     "        'No skills installed.',"
     "        (skill) => [skill.id, skill.version ? ('version ' + skill.version) : '', skill.description || '']"
     "          .filter(Boolean)"
     "          .join(' • ')"
     "      );"
     "    }"
     ""
     "    function resetProviderForm(statusText) {"
     "      state.activeProviderId = '';"
     "      providerIdEl.value = '';"
     "      providerNameEl.value = '';"
     "      providerBaseUrlEl.value = '';"
     "      providerModelEl.value = '';"
     "      providerApiKeyEl.value = '';"
     "      providerDefaultEl.checked = !state.admin.providers.some((provider) => provider.default);"
     "      providerStatusEl.textContent = statusText || 'Create a provider or select an existing one.';"
     "      renderProviderList();"
     "      updateAdminButtons();"
     "    }"
     ""
     "    function resetOauthAccountForm(statusText) {"
     "      state.activeOauthAccountId = '';"
     "      oauthAccountIdEl.value = '';"
     "      oauthAccountNameEl.value = '';"
     "      oauthAuthorizeUrlEl.value = '';"
     "      oauthTokenUrlEl.value = '';"
     "      oauthClientIdEl.value = '';"
     "      oauthClientSecretEl.value = '';"
     "      oauthScopesEl.value = '';"
     "      oauthRedirectUriEl.value = '';"
     "      oauthAuthParamsEl.value = '';"
     "      oauthTokenParamsEl.value = '';"
     "      oauthAccountStatusEl.textContent = statusText || 'Create an OAuth account or select an existing one.';"
     "      renderOauthAccountList();"
     "      updateAdminButtons();"
     "    }"
     ""
     "    function resetServiceForm(statusText) {"
     "      state.activeServiceId = '';"
     "      serviceIdEl.value = '';"
     "      serviceNameEl.value = '';"
     "      serviceBaseUrlEl.value = '';"
     "      serviceAuthTypeEl.value = 'bearer';"
     "      serviceAuthHeaderEl.value = '';"
     "      serviceOauthAccountEl.value = '';"
     "      serviceAuthKeyEl.value = '';"
     "      serviceEnabledEl.checked = true;"
     "      serviceStatusEl.textContent = statusText || 'Create a service or select an existing one.';"
     "      renderServiceList();"
     "      renderOauthAccountOptions();"
     "      updateAdminButtons();"
     "    }"
     ""
     "    function resetSiteForm(statusText) {"
     "      state.activeSiteId = '';"
     "      siteIdEl.value = '';"
     "      siteNameEl.value = '';"
     "      siteLoginUrlEl.value = '';"
     "      siteUsernameFieldEl.value = 'username';"
     "      sitePasswordFieldEl.value = 'password';"
     "      siteUsernameEl.value = '';"
     "      sitePasswordEl.value = '';"
     "      siteFormSelectorEl.value = '';"
     "      siteExtraFieldsEl.value = '';"
     "      siteStatusEl.textContent = statusText || 'Create a site login or select an existing one.';"
     "      renderSiteList();"
     "      updateAdminButtons();"
     "    }"
     ""
     "    function selectProvider(provider) {"
     "      state.activeProviderId = provider.id || '';"
     "      providerIdEl.value = provider.id || '';"
     "      providerNameEl.value = provider.name || '';"
     "      providerBaseUrlEl.value = provider.base_url || '';"
     "      providerModelEl.value = provider.model || '';"
     "      providerApiKeyEl.value = '';"
     "      providerDefaultEl.checked = !!provider.default;"
     "      providerStatusEl.textContent = provider.api_key_configured"
     "        ? 'API key stored. Leave the field blank to keep it, or enter a new one to replace it.'"
     "        : 'No API key stored yet.';"
     "      renderProviderList();"
     "      updateAdminButtons();"
     "    }"
     ""
     "    function selectOauthAccount(account) {"
     "      state.activeOauthAccountId = account.id || '';"
     "      oauthAccountIdEl.value = account.id || '';"
     "      oauthAccountNameEl.value = account.name || '';"
     "      oauthAuthorizeUrlEl.value = account.authorize_url || '';"
     "      oauthTokenUrlEl.value = account.token_url || '';"
     "      oauthClientIdEl.value = account.client_id || '';"
     "      oauthClientSecretEl.value = '';"
     "      oauthScopesEl.value = account.scopes || '';"
     "      oauthRedirectUriEl.value = account.redirect_uri || '';"
     "      oauthAuthParamsEl.value = account.auth_params || '';"
     "      oauthTokenParamsEl.value = account.token_params || '';"
     "      oauthAccountStatusEl.textContent = ["
     "        account.client_secret_configured ? 'Client secret stored.' : 'No client secret stored.',"
     "        account.connected ? 'OAuth connection is active.' : 'Not connected yet.',"
     "        account.refresh_token_configured ? 'Refresh token stored.' : 'No refresh token stored.'"
     "      ].join(' ');"
     "      renderOauthAccountList();"
     "      updateAdminButtons();"
     "    }"
     ""
     "    function selectService(service) {"
     "      state.activeServiceId = service.id || '';"
     "      serviceIdEl.value = service.id || '';"
     "      serviceNameEl.value = service.name || '';"
     "      serviceBaseUrlEl.value = service.base_url || '';"
     "      serviceAuthTypeEl.value = service.auth_type || 'bearer';"
     "      serviceAuthHeaderEl.value = service.auth_header || '';"
     "      serviceOauthAccountEl.value = service.oauth_account || '';"
     "      serviceAuthKeyEl.value = '';"
     "      serviceEnabledEl.checked = !!service.enabled;"
     "      serviceStatusEl.textContent = service.auth_type === 'oauth-account'"
     "        ? ((service.oauth_account_name || 'No OAuth account') + (service.oauth_account_connected ? ' is connected.' : ' is not connected yet.'))"
     "        : (service.auth_key_configured"
     "          ? 'Auth secret stored. Leave the field blank to keep it, or enter a new one to replace it.'"
     "          : 'No auth secret stored yet.');"
      "      renderServiceList();"
     "      renderOauthAccountOptions();"
     "      updateAdminButtons();"
     "    }"
     ""
     "    function selectSite(site) {"
     "      state.activeSiteId = site.id || '';"
     "      siteIdEl.value = site.id || '';"
     "      siteNameEl.value = site.name || '';"
     "      siteLoginUrlEl.value = site.login_url || '';"
     "      siteUsernameFieldEl.value = site.username_field || 'username';"
     "      sitePasswordFieldEl.value = site.password_field || 'password';"
     "      siteUsernameEl.value = '';"
     "      sitePasswordEl.value = '';"
     "      siteFormSelectorEl.value = site.form_selector || '';"
     "      siteExtraFieldsEl.value = site.extra_fields || '';"
     "      siteStatusEl.textContent = ["
     "        site.username_configured ? 'Username stored.' : 'No username stored.',"
     "        site.password_configured ? 'Password stored. Leave both fields blank to keep current secrets.' : 'No password stored yet.'"
     "      ].join(' ');"
     "      renderSiteList();"
     "      updateAdminButtons();"
     "    }"
     ""
     "    async function loadAdminConfig() {"
     "      try {"
     "        const data = await fetchJson('/admin/config');"
     "        state.admin.providers = Array.isArray(data.providers) ? data.providers : [];"
     "        state.admin.oauthAccounts = Array.isArray(data.oauth_accounts) ? data.oauth_accounts : [];"
     "        state.admin.services = Array.isArray(data.services) ? data.services : [];"
     "        state.admin.sites = Array.isArray(data.sites) ? data.sites : [];"
     "        state.admin.tools = Array.isArray(data.tools) ? data.tools : [];"
     "        state.admin.skills = Array.isArray(data.skills) ? data.skills : [];"
     "        const provider = state.admin.providers.find((entry) => entry.id === state.activeProviderId);"
     "        const oauthAccount = state.admin.oauthAccounts.find((entry) => entry.id === state.activeOauthAccountId);"
     "        const service = state.admin.services.find((entry) => entry.id === state.activeServiceId);"
     "        const site = state.admin.sites.find((entry) => entry.id === state.activeSiteId);"
     "        renderOauthAccountOptions();"
     "        if (provider) {"
     "          selectProvider(provider);"
     "        } else if (!state.activeProviderId) {"
     "          resetProviderForm('Create a provider or select an existing one.');"
     "        } else {"
     "          resetProviderForm('Selected provider no longer exists.');"
     "        }"
     "        if (oauthAccount) {"
     "          selectOauthAccount(oauthAccount);"
     "        } else if (!state.activeOauthAccountId) {"
     "          resetOauthAccountForm('Create an OAuth account or select an existing one.');"
     "        } else {"
     "          resetOauthAccountForm('Selected OAuth account no longer exists.');"
     "        }"
     "        if (service) {"
     "          selectService(service);"
     "        } else if (!state.activeServiceId) {"
     "          resetServiceForm('Create a service or select an existing one.');"
     "        } else {"
     "          resetServiceForm('Selected service no longer exists.');"
     "        }"
     "        if (site) {"
     "          selectSite(site);"
     "        } else if (!state.activeSiteId) {"
     "          resetSiteForm('Create a site login or select an existing one.');"
     "        } else {"
     "          resetSiteForm('Selected site login no longer exists.');"
     "        }"
     "        renderCapabilities();"
     "      } catch (err) {"
     "        providerStatusEl.textContent = err.message || 'Failed to load admin configuration.';"
     "        oauthAccountStatusEl.textContent = err.message || 'Failed to load admin configuration.';"
     "        serviceStatusEl.textContent = err.message || 'Failed to load admin configuration.';"
     "        siteStatusEl.textContent = err.message || 'Failed to load admin configuration.';"
     "      }"
     "    }"
     ""
     "    async function saveProvider() {"
     "      if (state.providerSaving) {"
     "        return;"
     "      }"
     "      state.providerSaving = true;"
     "      providerStatusEl.textContent = 'Saving provider...';"
     "      updateAdminButtons();"
     "      try {"
     "        const data = await fetchJson('/admin/providers', {"
     "          method: 'POST',"
     "          headers: { 'Content-Type': 'application/json' },"
     "          body: JSON.stringify({"
     "            id: providerIdEl.value,"
     "            name: providerNameEl.value,"
     "            base_url: providerBaseUrlEl.value,"
     "            model: providerModelEl.value,"
     "            api_key: providerApiKeyEl.value,"
     "            default: providerDefaultEl.checked"
     "          })"
     "        });"
     "        const provider = data.provider || null;"
     "        state.activeProviderId = provider && provider.id ? provider.id : state.activeProviderId;"
     "        providerApiKeyEl.value = '';"
     "        providerStatusEl.textContent = 'Provider saved.';"
     "        await loadAdminConfig();"
     "        setStatus('Provider saved');"
     "      } catch (err) {"
     "        providerStatusEl.textContent = err.message || 'Failed to save provider.';"
     "      } finally {"
     "        state.providerSaving = false;"
     "        updateAdminButtons();"
     "      }"
     "    }"
     ""
     "    async function saveOauthAccount() {"
     "      if (state.oauthSaving) {"
     "        return;"
     "      }"
     "      state.oauthSaving = true;"
     "      oauthAccountStatusEl.textContent = 'Saving OAuth account...';"
     "      updateAdminButtons();"
     "      try {"
     "        const data = await fetchJson('/admin/oauth-accounts', {"
     "          method: 'POST',"
     "          headers: { 'Content-Type': 'application/json' },"
     "          body: JSON.stringify({"
     "            id: oauthAccountIdEl.value,"
     "            name: oauthAccountNameEl.value,"
     "            authorize_url: oauthAuthorizeUrlEl.value,"
     "            token_url: oauthTokenUrlEl.value,"
     "            client_id: oauthClientIdEl.value,"
     "            client_secret: oauthClientSecretEl.value,"
     "            scopes: oauthScopesEl.value,"
     "            redirect_uri: oauthRedirectUriEl.value,"
     "            auth_params: oauthAuthParamsEl.value,"
     "            token_params: oauthTokenParamsEl.value"
     "          })"
     "        });"
     "        const account = data.oauth_account || null;"
     "        state.activeOauthAccountId = account && account.id ? account.id : state.activeOauthAccountId;"
     "        oauthClientSecretEl.value = '';"
     "        oauthAccountStatusEl.textContent = 'OAuth account saved.';"
     "        await loadAdminConfig();"
     "        setStatus('OAuth account saved');"
     "      } catch (err) {"
     "        oauthAccountStatusEl.textContent = err.message || 'Failed to save OAuth account.';"
     "      } finally {"
     "        state.oauthSaving = false;"
     "        updateAdminButtons();"
     "      }"
     "    }"
     ""
     "    async function connectOauthAccount() {"
     "      if (!state.activeOauthAccountId || state.oauthSaving) {"
     "        oauthAccountStatusEl.textContent = 'Save the OAuth account first, then connect it.';"
     "        return;"
     "      }"
     "      state.oauthSaving = true;"
     "      oauthAccountStatusEl.textContent = 'Opening OAuth consent flow...';"
     "      updateAdminButtons();"
     "      try {"
     "        const data = await fetchJson('/admin/oauth-accounts/' + encodeURIComponent(state.activeOauthAccountId) + '/connect', { method: 'POST' });"
     "        const popup = window.open(data.authorization_url, 'xia-oauth-connect', 'popup,width=720,height=860');"
     "        if (!popup) {"
     "          oauthAccountStatusEl.textContent = 'Popup blocked. Allow popups for this Xia page and try again.';"
     "        } else {"
     "          oauthAccountStatusEl.textContent = 'OAuth flow opened in a new window.';"
     "          popup.focus();"
     "        }"
     "      } catch (err) {"
     "        oauthAccountStatusEl.textContent = err.message || 'Failed to start OAuth flow.';"
     "      } finally {"
     "        state.oauthSaving = false;"
     "        updateAdminButtons();"
     "      }"
     "    }"
     ""
     "    async function refreshOauthAccount() {"
     "      if (!state.activeOauthAccountId || state.oauthSaving) {"
     "        return;"
     "      }"
     "      state.oauthSaving = true;"
     "      oauthAccountStatusEl.textContent = 'Refreshing OAuth token...';"
     "      updateAdminButtons();"
     "      try {"
     "        await fetchJson('/admin/oauth-accounts/' + encodeURIComponent(state.activeOauthAccountId) + '/refresh', { method: 'POST' });"
     "        oauthAccountStatusEl.textContent = 'OAuth token refreshed.';"
     "        await loadAdminConfig();"
     "        setStatus('OAuth token refreshed');"
     "      } catch (err) {"
     "        oauthAccountStatusEl.textContent = err.message || 'Failed to refresh OAuth token.';"
     "      } finally {"
     "        state.oauthSaving = false;"
     "        updateAdminButtons();"
     "      }"
     "    }"
     ""
     "    async function deleteOauthAccount() {"
     "      if (!state.activeOauthAccountId || state.oauthSaving) {"
     "        return;"
     "      }"
     "      if (!window.confirm('Delete this OAuth account?')) {"
     "        return;"
     "      }"
     "      state.oauthSaving = true;"
     "      oauthAccountStatusEl.textContent = 'Deleting OAuth account...';"
     "      updateAdminButtons();"
     "      try {"
     "        await fetchJson('/admin/oauth-accounts/' + encodeURIComponent(state.activeOauthAccountId), { method: 'DELETE' });"
     "        const deletedId = state.activeOauthAccountId;"
     "        state.activeOauthAccountId = '';"
     "        resetOauthAccountForm('OAuth account deleted.');"
     "        await loadAdminConfig();"
     "        setStatus('Deleted OAuth account ' + deletedId);"
     "      } catch (err) {"
     "        oauthAccountStatusEl.textContent = err.message || 'Failed to delete OAuth account.';"
     "      } finally {"
     "        state.oauthSaving = false;"
     "        updateAdminButtons();"
     "      }"
     "    }"
     ""
     "    async function saveService() {"
     "      if (state.serviceSaving) {"
     "        return;"
     "      }"
     "      state.serviceSaving = true;"
     "      serviceStatusEl.textContent = 'Saving service...';"
     "      updateAdminButtons();"
     "      try {"
     "        const data = await fetchJson('/admin/services', {"
     "          method: 'POST',"
     "          headers: { 'Content-Type': 'application/json' },"
     "          body: JSON.stringify({"
     "            id: serviceIdEl.value,"
     "            name: serviceNameEl.value,"
     "            base_url: serviceBaseUrlEl.value,"
     "            auth_type: serviceAuthTypeEl.value,"
     "            auth_header: serviceAuthHeaderEl.value,"
     "            oauth_account: serviceOauthAccountEl.value,"
     "            auth_key: serviceAuthKeyEl.value,"
     "            enabled: serviceEnabledEl.checked"
     "          })"
     "        });"
     "        const service = data.service || null;"
     "        state.activeServiceId = service && service.id ? service.id : state.activeServiceId;"
     "        serviceAuthKeyEl.value = '';"
     "        serviceStatusEl.textContent = 'Service saved.';"
     "        await loadAdminConfig();"
     "        setStatus('Service saved');"
     "      } catch (err) {"
     "        serviceStatusEl.textContent = err.message || 'Failed to save service.';"
     "      } finally {"
     "        state.serviceSaving = false;"
     "        updateAdminButtons();"
     "      }"
     "    }"
     ""
     "    async function saveSite() {"
     "      if (state.siteSaving) {"
     "        return;"
     "      }"
     "      state.siteSaving = true;"
     "      siteStatusEl.textContent = 'Saving site login...';"
     "      updateAdminButtons();"
     "      try {"
     "        const data = await fetchJson('/admin/sites', {"
     "          method: 'POST',"
     "          headers: { 'Content-Type': 'application/json' },"
     "          body: JSON.stringify({"
     "            id: siteIdEl.value,"
     "            name: siteNameEl.value,"
     "            login_url: siteLoginUrlEl.value,"
     "            username_field: siteUsernameFieldEl.value,"
     "            password_field: sitePasswordFieldEl.value,"
     "            username: siteUsernameEl.value,"
     "            password: sitePasswordEl.value,"
     "            form_selector: siteFormSelectorEl.value,"
     "            extra_fields: siteExtraFieldsEl.value"
     "          })"
     "        });"
     "        const site = data.site || null;"
     "        state.activeSiteId = site && site.id ? site.id : state.activeSiteId;"
     "        siteUsernameEl.value = '';"
     "        sitePasswordEl.value = '';"
     "        siteStatusEl.textContent = 'Site login saved.';"
     "        await loadAdminConfig();"
     "        setStatus('Site login saved');"
     "      } catch (err) {"
     "        siteStatusEl.textContent = err.message || 'Failed to save site login.';"
     "      } finally {"
     "        state.siteSaving = false;"
     "        updateAdminButtons();"
     "      }"
     "    }"
     ""
     "    async function deleteSite() {"
     "      if (!state.activeSiteId || state.siteSaving) {"
     "        return;"
     "      }"
     "      if (!window.confirm('Delete this stored site login?')) {"
     "        return;"
     "      }"
     "      state.siteSaving = true;"
     "      siteStatusEl.textContent = 'Deleting site login...';"
     "      updateAdminButtons();"
     "      try {"
     "        await fetchJson('/admin/sites/' + encodeURIComponent(state.activeSiteId), { method: 'DELETE' });"
     "        const deletedId = state.activeSiteId;"
     "        state.activeSiteId = '';"
     "        resetSiteForm('Site login deleted.');"
     "        await loadAdminConfig();"
     "        setStatus('Deleted site login ' + deletedId);"
     "      } catch (err) {"
     "        siteStatusEl.textContent = err.message || 'Failed to delete site login.';"
     "      } finally {"
     "        state.siteSaving = false;"
     "        updateAdminButtons();"
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
     "    newProviderEl.addEventListener('click', () => {"
     "      resetProviderForm('Create a new provider.');"
     "      providerIdEl.focus();"
     "    });"
     ""
     "    saveProviderEl.addEventListener('click', () => {"
     "      saveProvider();"
     "    });"
     ""
     "    newOauthAccountEl.addEventListener('click', () => {"
     "      resetOauthAccountForm('Create a new OAuth account.');"
     "      oauthAccountIdEl.focus();"
     "    });"
     ""
     "    saveOauthAccountEl.addEventListener('click', () => {"
     "      saveOauthAccount();"
     "    });"
     ""
     "    connectOauthAccountEl.addEventListener('click', () => {"
     "      connectOauthAccount();"
     "    });"
     ""
     "    refreshOauthAccountEl.addEventListener('click', () => {"
     "      refreshOauthAccount();"
     "    });"
     ""
     "    deleteOauthAccountEl.addEventListener('click', () => {"
     "      deleteOauthAccount();"
     "    });"
     ""
     "    newServiceEl.addEventListener('click', () => {"
     "      resetServiceForm('Create a new service.');"
     "      serviceIdEl.focus();"
     "    });"
     ""
     "    saveServiceEl.addEventListener('click', () => {"
     "      saveService();"
     "    });"
     ""
     "    newSiteEl.addEventListener('click', () => {"
     "      resetSiteForm('Create a new site login.');"
     "      siteIdEl.focus();"
     "    });"
     ""
     "    saveSiteEl.addEventListener('click', () => {"
     "      saveSite();"
     "    });"
     ""
     "    deleteSiteEl.addEventListener('click', () => {"
     "      deleteSite();"
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
     "    window.addEventListener('message', (event) => {"
     "      if (!event || event.origin !== window.location.origin) {"
     "        return;"
     "      }"
     "      const data = event.data || {};"
     "      if (data.type === 'xia-oauth-complete') {"
     "        loadAdminConfig();"
     "        oauthAccountStatusEl.textContent = data.status === 'ok'"
     "          ? 'OAuth account connected.'"
     "          : 'OAuth flow ended with an error.';"
     "        setStatus(data.status === 'ok' ? 'OAuth account connected' : 'OAuth flow failed');"
     "      }"
     "    });"
     ""
     "    persistSession();"
     "    renderApproval();"
     "    renderMessages();"
     "    syncScratchEditor('No scratch pad selected.');"
     "    resetProviderForm('Loading providers...');"
     "    resetOauthAccountForm('Loading OAuth accounts...');"
     "    resetServiceForm('Loading services...');"
     "    resetSiteForm('Loading site logins...');"
     "    renderCapabilities();"
     "    updateAdminButtons();"
     "    updateComposerState();"
     "    composerEl.focus();"
     "    loadSessionMessages();"
     "    loadScratchPads();"
     "    loadAdminConfig();"
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
         (wm/ensure-wm! sid)
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
       (when-let [sid (get @ws-sessions ch)]
         (wm/snapshot! sid)
         (wm/clear-wm! sid))
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

(declare nonblank-str)

(defn- first-forwarded
  [value]
  (some-> value str (str/split #",") first str/trim nonblank-str))

(defn- request-base-url
  [req]
  (or (when-let [origin (nonblank-str (request-header req "origin"))]
        (let [uri (java.net.URI. origin)]
          (str (.getScheme uri) "://" (.getAuthority uri))))
      (let [scheme (or (first-forwarded (request-header req "x-forwarded-proto"))
                       (some-> (:scheme req) name)
                       "http")
            host   (or (first-forwarded (request-header req "x-forwarded-host"))
                       (nonblank-str (request-header req "host")))]
        (when host
          (str scheme "://" host)))))

(defn- parse-query-string
  [query-string]
  (into {}
        (keep (fn [part]
                (let [[k v] (str/split (str part) #"=" 2)]
                  (when (seq k)
                    [(java.net.URLDecoder/decode k "UTF-8")
                     (some-> v (java.net.URLDecoder/decode "UTF-8"))]))))
        (str/split (or query-string "") #"&")))

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

(defn- escape-html
  [value]
  (-> (str (or value ""))
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")
      (str/replace "\"" "&quot;")
      (str/replace "'" "&#39;")))

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

(defn- nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn- parse-keyword-id
  [value field-name]
  (let [id-str (nonblank-str value)]
    (cond
      (nil? id-str)
      (throw (ex-info (str "missing '" field-name "' field") {:field field-name}))

      (re-find #"\s" id-str)
      (throw (ex-info (str "'" field-name "' must not contain whitespace")
                      {:field field-name
                       :value value}))

      :else
      (keyword id-str))))

(defn- parse-extra-fields
  [value]
  (let [text (nonblank-str value)]
    (when text
      (try
        (json/write-json-str (json/read-json text))
        (catch Exception _
          (throw (ex-info "extra_fields must be valid JSON"
                          {:field "extra_fields"})))))))

(defn- parse-json-object-string
  [value field-name]
  (let [text (nonblank-str value)]
    (when text
      (try
        (let [parsed (json/read-json text)]
          (when-not (map? parsed)
            (throw (ex-info (str field-name " must be a JSON object")
                            {:field field-name})))
          (json/write-json-str parsed))
        (catch clojure.lang.ExceptionInfo e
          (throw e))
        (catch Exception _
          (throw (ex-info (str field-name " must be valid JSON")
                          {:field field-name})))))))

(defn- parse-auth-type
  [value]
  (let [auth-type (some-> value nonblank-str keyword)]
    (when-not (contains? service-auth-types auth-type)
      (throw (ex-info "invalid auth_type"
                      {:field "auth_type"
                       :value value})))
    auth-type))

(defn- sort-by-name
  [entries]
  (->> entries
       (sort-by (fn [entry]
                  (str/lower-case (or (:name entry) (:id entry) ""))))
       vec))

(defn- provider->admin-body
  [provider]
  {:id                 (some-> (:llm.provider/id provider) name)
   :name               (:llm.provider/name provider)
   :base_url           (:llm.provider/base-url provider)
   :model              (:llm.provider/model provider)
   :default            (boolean (:llm.provider/default? provider))
   :api_key_configured (boolean (nonblank-str (:llm.provider/api-key provider)))})

(defn- service->admin-body
  [service]
  (let [oauth-account (some-> (:service/oauth-account service) db/get-oauth-account)]
    {:id                     (some-> (:service/id service) name)
     :name                   (:service/name service)
     :base_url               (:service/base-url service)
     :auth_type              (some-> (:service/auth-type service) name)
     :auth_header            (:service/auth-header service)
     :oauth_account          (some-> (:service/oauth-account service) name)
     :oauth_account_name     (:oauth.account/name oauth-account)
     :oauth_account_connected (boolean (nonblank-str (:oauth.account/access-token oauth-account)))
     :enabled                (boolean (:service/enabled? service))
     :auth_key_configured    (boolean (nonblank-str (:service/auth-key service)))}))

(defn- oauth-account->admin-body
  [account]
  {:id                       (some-> (:oauth.account/id account) name)
   :name                     (:oauth.account/name account)
   :authorize_url            (:oauth.account/authorize-url account)
   :token_url                (:oauth.account/token-url account)
   :client_id                (:oauth.account/client-id account)
   :scopes                   (:oauth.account/scopes account)
   :redirect_uri             (:oauth.account/redirect-uri account)
   :auth_params              (:oauth.account/auth-params account)
   :token_params             (:oauth.account/token-params account)
   :client_secret_configured (boolean (nonblank-str (:oauth.account/client-secret account)))
   :access_token_configured  (boolean (nonblank-str (:oauth.account/access-token account)))
   :refresh_token_configured (boolean (nonblank-str (:oauth.account/refresh-token account)))
   :connected                (boolean (nonblank-str (:oauth.account/access-token account)))
   :expires_at               (instant->str (:oauth.account/expires-at account))
   :connected_at             (instant->str (:oauth.account/connected-at account))})

(defn- site->admin-body
  [site]
  {:id                  (some-> (:site-cred/id site) name)
   :name                (:site-cred/name site)
   :login_url           (:site-cred/login-url site)
   :username_field      (:site-cred/username-field site)
   :password_field      (:site-cred/password-field site)
   :form_selector       (:site-cred/form-selector site)
   :extra_fields        (:site-cred/extra-fields site)
   :username_configured (boolean (nonblank-str (:site-cred/username site)))
   :password_configured (boolean (nonblank-str (:site-cred/password site)))})

(defn- tool->admin-body
  [tool]
  {:id          (some-> (:tool/id tool) name)
   :name        (:tool/name tool)
   :description (:tool/description tool)
   :approval    (some-> (:tool/approval tool) name)
   :enabled     (boolean (:tool/enabled? tool))})

(defn- skill->admin-body
  [skill]
  {:id          (some-> (:skill/id skill) name)
   :name        (:skill/name skill)
   :description (:skill/description skill)
   :version     (:skill/version skill)
   :enabled     (boolean (:skill/enabled? skill))})

(defn- session-scratch-pad
  [session-id pad-id]
  (let [pad (scratch/get-pad pad-id)]
    (when (and pad
               (= :session (:scope pad))
               (= session-id (:session-id pad)))
      pad)))

(defn- handle-create-session []
  (let [sid (db/create-session! :http)]
    (wm/ensure-wm! sid)
    (json-response 200 {:session_id (str sid)})))

(defn- handle-chat [req]
  (let [data       (read-body req)
        message    (get data "message")
        session-id (get data "session_id")]
    (if-not message
      (json-response 400 {:error "missing 'message' field"})
      (if (and session-id (not (session-exists? session-id)))
        (json-response 404 {:error "unknown session id"})
        (let [sid      (if session-id
                         (java.util.UUID/fromString session-id)
                         (db/create-session! :http))
              _        (wm/ensure-wm! sid)
            response (agent/process-message sid message :channel :http)]
          (json-response 200 {:session_id (str sid)
                              :role       "assistant"
                              :content    response}))))))

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

(defn- handle-admin-config [_req]
  (json-response
    200
    {:providers (->> (db/list-providers)
                     (map provider->admin-body)
                     sort-by-name)
     :oauth_accounts (->> (db/list-oauth-accounts)
                          (map oauth-account->admin-body)
                          sort-by-name)
     :services  (->> (db/list-services)
                     (map service->admin-body)
                     sort-by-name)
     :sites     (->> (db/list-site-creds)
                     (map site->admin-body)
                     sort-by-name)
     :tools     (->> (db/list-tools)
                     (map tool->admin-body)
                     sort-by-name)
     :skills    (->> (db/list-skills)
                     (map skill->admin-body)
                     sort-by-name)}))

(defn- handle-save-provider [req]
  (try
    (let [data         (or (read-body req) {})
          provider-id  (parse-keyword-id (get data "id") "id")
          base-url     (nonblank-str (get data "base_url"))
          model        (nonblank-str (get data "model"))
          name         (or (nonblank-str (get data "name"))
                           (name provider-id))
          api-key      (nonblank-str (get data "api_key"))
          make-default (true? (get data "default"))
          has-default? (some? (db/get-default-provider))]
      (when-not base-url
        (throw (ex-info "missing 'base_url' field" {:field "base_url"})))
      (when-not model
        (throw (ex-info "missing 'model' field" {:field "model"})))
      (db/upsert-provider! (cond-> {:id       provider-id
                                    :name     name
                                    :base-url base-url
                                    :model    model}
                             api-key
                             (assoc :api-key api-key)))
      (when (or make-default (not has-default?))
        (db/set-default-provider! provider-id))
      (json-response 200 {:provider (provider->admin-body (db/get-provider provider-id))}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-save-service [req]
  (try
    (let [data              (or (read-body req) {})
          service-id        (parse-keyword-id (get data "id") "id")
          existing          (db/get-service service-id)
          base-url          (nonblank-str (get data "base_url"))
          name              (or (nonblank-str (get data "name"))
                                (name service-id))
          auth-type         (parse-auth-type (get data "auth_type"))
          entered-auth-key  (nonblank-str (get data "auth_key"))
          enabled?          (if (contains? data "enabled")
                              (true? (get data "enabled"))
                              true)
          oauth-account-id  (when (= :oauth-account auth-type)
                              (let [value (or (nonblank-str (get data "oauth_account"))
                                              (some-> (:service/oauth-account existing) name))]
                                (when-not value
                                  (throw (ex-info "oauth_account is required for oauth-account auth_type"
                                                  {:field "oauth_account"})))
                                (let [account-id (keyword value)]
                                  (when-not (db/get-oauth-account account-id)
                                    (throw (ex-info "unknown oauth_account"
                                                    {:field "oauth_account"
                                                     :value value})))
                                  account-id)))
          entered-header    (nonblank-str (get data "auth_header"))
          auth-header       (when (#{:api-key-header :query-param} auth-type)
                              (or entered-header
                                  (:service/auth-header existing)))
          auth-key          (when-not (= :oauth-account auth-type)
                              (or entered-auth-key
                                  (:service/auth-key existing)
                                  ""))]
      (when-not base-url
        (throw (ex-info "missing 'base_url' field" {:field "base_url"})))
      (when (and (#{:api-key-header :query-param} auth-type)
                 (nil? auth-header))
        (throw (ex-info "auth_header is required for the selected auth_type"
                        {:field "auth_header"})))
      (db/save-service! {:id          service-id
                         :name        name
                         :base-url    base-url
                         :auth-type   auth-type
                         :auth-key    (or auth-key "")
                         :auth-header auth-header
                         :oauth-account oauth-account-id
                         :enabled?    enabled?})
      (json-response 200 {:service (service->admin-body (db/get-service service-id))}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-save-oauth-account [req]
  (try
    (let [data           (or (read-body req) {})
          account-id     (parse-keyword-id (get data "id") "id")
          existing       (db/get-oauth-account account-id)
          name           (or (nonblank-str (get data "name"))
                             (name account-id))
          authorize-url  (nonblank-str (get data "authorize_url"))
          token-url      (nonblank-str (get data "token_url"))
          client-id      (nonblank-str (get data "client_id"))
          client-secret  (or (nonblank-str (get data "client_secret"))
                             (:oauth.account/client-secret existing)
                             "")
          scopes         (or (nonblank-str (get data "scopes")) "")
          redirect-uri   (nonblank-str (get data "redirect_uri"))
          auth-params    (parse-json-object-string (get data "auth_params") "auth_params")
          token-params   (parse-json-object-string (get data "token_params") "token_params")]
      (when-not authorize-url
        (throw (ex-info "missing 'authorize_url' field" {:field "authorize_url"})))
      (when-not token-url
        (throw (ex-info "missing 'token_url' field" {:field "token_url"})))
      (when-not client-id
        (throw (ex-info "missing 'client_id' field" {:field "client_id"})))
      (db/save-oauth-account! {:id            account-id
                               :name          name
                               :authorize-url authorize-url
                               :token-url     token-url
                               :client-id     client-id
                               :client-secret client-secret
                               :scopes        scopes
                               :redirect-uri  redirect-uri
                               :auth-params   auth-params
                               :token-params  token-params
                               :access-token  (:oauth.account/access-token existing)
                               :refresh-token (:oauth.account/refresh-token existing)
                               :token-type    (:oauth.account/token-type existing)
                               :expires-at    (:oauth.account/expires-at existing)
                               :connected-at  (:oauth.account/connected-at existing)})
      (json-response 200 {:oauth_account (oauth-account->admin-body
                                           (db/get-oauth-account account-id))}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-delete-oauth-account [account-id]
  (try
    (let [oauth-id (parse-keyword-id account-id "oauth_account_id")]
      (cond
        (nil? (db/get-oauth-account oauth-id))
        (json-response 404 {:error "oauth account not found"})

        (db/oauth-account-in-use? oauth-id)
        (json-response 409 {:error "oauth account is still referenced by a service"})

        :else
        (do
          (db/remove-oauth-account! oauth-id)
          (json-response 200 {:status "deleted"
                              :oauth_account_id (name oauth-id)}))))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-start-oauth-connect [account-id req]
  (try
    (let [oauth-id    (parse-keyword-id account-id "oauth_account_id")
          callback-url (str (or (request-base-url req)
                                (throw (ex-info "cannot determine callback base URL"
                                                {:field "host"})))
                            "/oauth/callback")
          started     (oauth/start-authorization! oauth-id callback-url)]
      (json-response 200 {:oauth_account_id (name oauth-id)
                          :authorization_url (:authorization-url started)
                          :redirect_uri (:redirect-uri started)}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-refresh-oauth-account [account-id]
  (try
    (let [oauth-id (parse-keyword-id account-id "oauth_account_id")
          account  (oauth/refresh-account! oauth-id)]
      (json-response 200 {:oauth_account (oauth-account->admin-body account)}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- oauth-callback-page
  [status title message account-id]
  (let [title*   (escape-html title)
        message* (escape-html message)
        account* (some-> account-id name escape-html)]
    (str "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">"
       "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
       "<title>Xia OAuth</title>"
       "<style>body{margin:0;font-family:\"Avenir Next\",\"Segoe UI\",sans-serif;background:#f5efe3;color:#172119;display:grid;place-items:center;min-height:100vh;padding:24px;}main{max-width:36rem;background:rgba(255,252,246,.96);border:1px solid rgba(23,33,25,.12);border-radius:24px;padding:28px;box-shadow:0 20px 50px rgba(23,33,25,.12);}h1{margin:0 0 12px;font-size:2rem;}p{line-height:1.6;margin:0 0 10px;}code{font-family:\"SFMono-Regular\",Consolas,monospace;background:rgba(23,33,25,.06);padding:2px 6px;border-radius:8px;}</style>"
       "</head><body><main><h1>" title* "</h1><p>" message* "</p>"
       (when account*
         (str "<p>OAuth account: <code>" account* "</code></p>"))
       "<p>You can close this window and return to Xia.</p>"
       "<script>"
       "try {"
       "  if (window.opener && window.opener !== window) {"
       "    window.opener.postMessage({type:'xia-oauth-complete', status:'" status "', account_id:" (json/write-json-str (some-> account-id name)) "}, window.location.origin);"
       "  }"
       "} catch (_err) {}"
       "setTimeout(() => { try { window.close(); } catch (_err) {} }, 1200);"
       "</script></main></body></html>")))

(defn- handle-oauth-callback [req]
  (let [params            (parse-query-string (:query-string req))
        state             (get params "state")
        pending-account-id (some-> (and (seq state) (oauth/callback-account-id state)) name)
        code              (get params "code")
        error-code        (get params "error")
        error-description (or (get params "error_description") error-code)]
    (cond
      (not (seq state))
      (html-response (oauth-callback-page "error"
                                          "OAuth failed"
                                          "Missing authorization state."
                                          nil))

      (seq error-code)
      (html-response (oauth-callback-page "error"
                                          "OAuth was not completed"
                                          (str "Provider returned: " error-description)
                                          pending-account-id))

      (not (seq code))
      (html-response (oauth-callback-page "error"
                                          "OAuth failed"
                                          "Missing authorization code."
                                          pending-account-id))

      :else
      (try
        (let [account (oauth/complete-authorization! state code)]
          (html-response (oauth-callback-page "ok"
                                              "OAuth connected"
                                              "Xia stored the new access token and can now use this account for online work."
                                              (some-> (:oauth.account/id account) name))))
        (catch clojure.lang.ExceptionInfo e
          (html-response (oauth-callback-page "error"
                                              "OAuth failed"
                                              (.getMessage e)
                                              pending-account-id)))))))

(defn- handle-save-site [req]
  (try
    (let [data            (or (read-body req) {})
          site-id         (parse-keyword-id (get data "id") "id")
          existing        (db/get-site-cred site-id)
          login-url       (nonblank-str (get data "login_url"))
          name            (or (nonblank-str (get data "name"))
                              (name site-id))
          username-field  (or (nonblank-str (get data "username_field"))
                              "username")
          password-field  (or (nonblank-str (get data "password_field"))
                              "password")
          username        (or (nonblank-str (get data "username"))
                              (:site-cred/username existing)
                              "")
          password        (or (nonblank-str (get data "password"))
                              (:site-cred/password existing)
                              "")
          form-selector   (nonblank-str (get data "form_selector"))
          extra-fields    (parse-extra-fields (get data "extra_fields"))]
      (when-not login-url
        (throw (ex-info "missing 'login_url' field" {:field "login_url"})))
      (db/save-site-cred! {:id             site-id
                           :name           name
                           :login-url      login-url
                           :username-field username-field
                           :password-field password-field
                           :username       username
                           :password       password
                           :form-selector  form-selector
                           :extra-fields   extra-fields})
      (json-response 200 {:site (site->admin-body (db/get-site-cred site-id))}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-delete-site [site-id]
  (try
    (let [site-key (parse-keyword-id site-id "site_id")]
      (if (db/get-site-cred site-key)
        (do
          (db/remove-site-cred! site-key)
          (json-response 200 {:status "deleted"
                              :site_id (name site-key)}))
        (json-response 404 {:error "site credential not found"})))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

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
        scratch-edit-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)/edit" uri)
        admin-site-match   (re-matches #"/admin/sites/([^/]+)" uri)
        admin-oauth-match  (re-matches #"/admin/oauth-accounts/([^/]+)" uri)
        admin-oauth-connect-match (re-matches #"/admin/oauth-accounts/([^/]+)/connect" uri)
        admin-oauth-refresh-match (re-matches #"/admin/oauth-accounts/([^/]+)/refresh" uri)]
    (cond
      (and (= method :get) (= uri "/"))
      (handle-home req)

      (and (= method :get) (= uri "/oauth/callback"))
      (handle-oauth-callback req)

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

      (and (= method :get) (= uri "/admin/config"))
      (protected-route-response req #(handle-admin-config req))

      (and (= method :post) (= uri "/admin/providers"))
      (protected-route-response req #(handle-save-provider req))

      (and (= method :post) (= uri "/admin/oauth-accounts"))
      (protected-route-response req #(handle-save-oauth-account req))

      (and (= method :post) admin-oauth-connect-match)
      (protected-route-response req #(handle-start-oauth-connect (second admin-oauth-connect-match) req))

      (and (= method :post) admin-oauth-refresh-match)
      (protected-route-response req #(handle-refresh-oauth-account (second admin-oauth-refresh-match)))

      (and (= method :delete) admin-oauth-match)
      (protected-route-response req #(handle-delete-oauth-account (second admin-oauth-match)))

      (and (= method :post) (= uri "/admin/services"))
      (protected-route-response req #(handle-save-service req))

      (and (= method :post) (= uri "/admin/sites"))
      (protected-route-response req #(handle-save-site req))

      (and (= method :delete) admin-site-match)
      (protected-route-response req #(handle-delete-site (second admin-site-match)))

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
