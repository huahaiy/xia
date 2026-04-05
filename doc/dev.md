# Development Notes

## Recommended Flow

For local development, start a Lein REPL in the Xia checkout:

```bash
lein repl
```

Then in the REPL:

```clojure
(go "your-dev-passphrase")
```

That starts Xia in server mode with web dev mode enabled, serves the local UI at:

```text
http://127.0.0.1:3008/
```

and uses the repo-local development DB at:

```text
.xia/dev/db
```

## Day-To-Day Loop

- Edit `resources/web/*`: the browser reloads automatically.
- Edit `src/*`: run `(reset)` in the REPL.
- Stop the running Xia process with `(stop)`.
- Wipe `.xia/dev/` when you want a clean local dev instance.

## REPL Helpers

The `user` namespace exposes these helpers in `lein repl`:

```clojure
(go "your-dev-passphrase")
(go)
(go {:port 3010})
(go {:db "/tmp/xia-dev" :passphrase "your-dev-passphrase"})
(reset)
(stop)
(options)
(set-options! {:port 3010
               :db "/tmp/xia-dev"})
(nrepl-info)
(stop-nrepl)
```

What they do:

- `(go ...)` starts or restarts Xia in server mode with `:web-dev true`.
- `(go ...)` also starts a loopback-only nREPL listener and writes its port to `.nrepl-port`.
- `(go)` without a passphrase only works if you already configured a master key/passphrase through `set-options!` or the supported env vars.
- `(reset)` stops Xia, refreshes changed namespaces under `src/`, and starts Xia again.
- `(stop)` stops the HTTP server and runtime services in the current process.
- `(options)` shows the current dev runtime options.
- `(set-options! ...)` updates the defaults used by later `(go)` calls.
- `(nrepl-info)` shows the current dev nREPL bind, port, and port file.
- `(stop-nrepl)` shuts down the dev nREPL listener.

## What Web Dev Mode Does

- Serves web assets directly from `resources/web/`.
- Disables browser caching for local UI assets.
- Injects the live-reload client into the root page.
- Reloads the browser automatically when files under `resources/web/` change.

This is intended for editing:

- `resources/web/index.html`
- `resources/web/app.js`
- `resources/web/style.css`
- files under `resources/web/favicon/`

## Limitations

- `--web-dev` only works when the web resources are file-backed on the classpath, which is the normal source checkout workflow.
- In packaged or native-image distributions, Xia falls back to the bundled web assets and live reload is not enabled.

## Notes

- The live-reload endpoint is `GET /__dev/web-reload`.
- The browser polls for changes roughly once per second.
- Without web dev mode, edits to `resources/web/` are not picked up until Xia restarts.
