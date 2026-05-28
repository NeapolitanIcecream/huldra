# Run Huldra Locally

Huldra runs in the foreground. Use your process manager to keep it alive.

## Foreground

```bash
uv run huldra store init --db ~/.local/share/huldra/huldra.db
uv run huldra daemon --db ~/.local/share/huldra/huldra.db --host 127.0.0.1 --port 8765
```

Run the worker in another supervised process:

```bash
uv run huldra worker --db ~/.local/share/huldra/huldra.db --poll-interval-seconds 300 --json
```

Check status:

```bash
uv run huldra status --db ~/.local/share/huldra/huldra.db --json
```

The status payload shows `cooldown_until` and `cooldown_active` so supervisors
can tell when arXiv returned HTTP 429 and the worker is waiting.

## Sync And Backfill Jobs

Use `sync` for an explicit submitted-date day. `--wait` drains that request set
through the same queue, limiter, and fetcher used by the worker. The default
mode completes one legacy search slice.

```bash
uv run huldra sync \
  --db ~/.local/share/huldra/huldra.db \
  --search-query 'cat:cs.AI' \
  --date 2026-05-20 \
  --max-results 60 \
  --wait \
  --json
```

Use `--mode complete-window` when the caller needs every page in a bounded
legacy search window:

```bash
uv run huldra sync \
  --db ~/.local/share/huldra/huldra.db \
  --search-query 'cat:cs.AI' \
  --date 2026-05-20 \
  --max-results 60 \
  --mode complete-window \
  --wait \
  --json
```

Use `backfill` to enqueue daily submitted-date windows for a date range:

```bash
uv run huldra backfill \
  --db ~/.local/share/huldra/huldra.db \
  --search-query 'cat:cs.AI' \
  --start-date 2026-05-01 \
  --end-date 2026-05-20 \
  --max-results 60 \
  --json
```

The JSON summary reports only work attributed to that command. For example,
`upstream_requests_total` increments only when the command's inline wait path
executes the fetch. If another worker completes a joined queue item, the window
can still count as completed without adding an upstream request to that command.
For complete-window jobs, inspect `coverage_status`, `pages_total`, and
`pages_completed_total`; `overflow` means the window exceeded the configured
legacy search cap and was not treated as complete.

You can run `sync --wait` without a separate worker for short pre-syncs. Keep a
supervised worker running for normal background draining and stale refresh work.

## OAI-PMH Harvest Jobs

Use OAI-PMH for full mirrors, category-scoped mirrors, and datestamp-based
incremental sync:

```bash
uv run huldra harvest oai \
  --db ~/.local/share/huldra/huldra.db \
  --metadata-prefix arXiv \
  --set cs:cs:AI \
  --mode incremental \
  --json
```

Harvests store page state and advance the `(metadata_prefix, set_spec)`
watermark only after every resumption-token page succeeds. If a harvest stops
after receiving a token, rerunning the same command continues from the saved
token. Use `--resumption-token` to continue from a specific token.

## systemd User Service

```ini
[Unit]
Description=Huldra arXiv metadata API

[Service]
WorkingDirectory=%h/gits/huldra
ExecStart=uv run huldra daemon --db %h/.local/share/huldra/huldra.db --host 127.0.0.1 --port 8765
Restart=on-failure

[Install]
WantedBy=default.target
```

Create a second service for the worker:

```ini
[Unit]
Description=Huldra arXiv metadata worker

[Service]
WorkingDirectory=%h/gits/huldra
ExecStart=uv run huldra worker --db %h/.local/share/huldra/huldra.db --poll-interval-seconds 300 --json
Restart=on-failure

[Install]
WantedBy=default.target
```

## launchd

Use one plist for the API and one for the worker. Keep `RunAtLoad` enabled and
set `KeepAlive` to true.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
 "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>tech.voile.huldra.api</string>
  <key>WorkingDirectory</key><string>/Users/YOUR_USER/gits/huldra</string>
  <key>ProgramArguments</key>
  <array>
    <string>uv</string><string>run</string><string>huldra</string>
    <string>daemon</string>
    <string>--db</string><string>/Users/YOUR_USER/.local/share/huldra/huldra.db</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
</dict>
</plist>
```

## Docker

The MVP does not require Docker. If you containerize it, mount a persistent
volume for `/data/huldra.db`, bind the service to `127.0.0.1` on the host, and
run one API process plus one worker process against the same database.

## Backup And Cleanup

Stop the API and worker, then copy the SQLite files:

```bash
cp ~/.local/share/huldra/huldra.db* /path/to/backup/
```

Huldra does not implement retention cleanup yet. Delete or archive the database
only when consumers no longer need the cached metadata.

## Multi-Machine Limit

Do not run separate Huldra databases on several machines to increase arXiv
throughput. The arXiv legacy API limit applies across machines you control.
Use one centralized broker for the deployment, or wait for a future shared
rate-state backend.
