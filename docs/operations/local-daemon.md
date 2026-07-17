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
can tell when arXiv returned HTTP 429 and the worker is waiting. It also reports
`events_total`, `queue_items_total`, `queue_terminal_total`, `sync_jobs_total`,
`sync_jobs_terminal_total`, and `sync_job_pages_total` without returning the
underlying event or workflow rows.

Idle worker passes are silent by default. With `--json`, each non-idle pass is
one compact JSON line. Use `--emit-idle` only when debugging a short run; a
continuously supervised worker can otherwise produce a large stdout log even
when it has no work. The idle poll interval defaults to 300 seconds and has a
minimum of 1 second.

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

Use one plist for the API and one for the worker. Before loading it, confirm
that no other service owns the API port and that only one supervisor owns each
Huldra process:

```bash
lsof -nP -iTCP:8765 -sTCP:LISTEN
curl --fail http://127.0.0.1:8765/v1/status
```

If the port already serves a healthy Huldra instance, `huldra daemon` exits
successfully instead of starting a duplicate. If another program owns the
port, stop it or choose a different port before loading the plist.

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
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key><false/>
  </dict>
  <key>ThrottleInterval</key><integer>30</integer>
</dict>
</plist>
```

This restarts the daemon only after an unsuccessful exit and caps rapid
failure loops. A healthy pre-existing Huldra produces a successful exit, so
`launchd` does not repeatedly retry it.

Huldra does not rotate files owned by `launchd`, systemd, or another process
manager. Apply the platform's log size and retention limits to captured stdout
and stderr. In particular, do not point a long-running service at an
unbounded file and enable `--emit-idle` at the same time.

## Docker

The MVP does not require Docker. If you containerize it, mount a persistent
volume for `/data/huldra.db`, bind the service to `127.0.0.1` on the host, and
run one API process plus one worker process against the same database.

## Backup And Retention

Stop the API and worker, then copy the SQLite files:

```bash
cp ~/.local/share/huldra/huldra.db* /path/to/backup/
```

Preview rows older than 30 days that are eligible for cleanup:

```bash
uv run huldra store gc \
  --db ~/.local/share/huldra/huldra.db \
  --older-than-days 30 \
  --json
```

The command requires an existing, initialized Huldra database and does not run
schema initialization or migrations. It is a read-only dry run unless
`--apply` is present. Review its eligible counts, then apply the same cutoff
explicitly:

```bash
uv run huldra store gc \
  --db ~/.local/share/huldra/huldra.db \
  --older-than-days 30 \
  --apply \
  --json
```

Retention deletes old `events`, queue items in `completed` or `failed` state,
and sync jobs in an explicit terminal-state allowlist. An expired async sync
job handed off as queued, delayed, or claimed also becomes eligible after all
cache/queue records associated with every page are completed or failed and
older than the same cutoff. Active or recent work keeps the job ineligible.
Deleting an eligible sync job also deletes its `sync_job_pages`; leases are
never touched. Cached papers, cache entries, OAI harvest state, and OAI
watermarks are outside this command's scope.

SQLite reuses pages freed by deletion, but the database file normally does not
shrink immediately. If physical file size must decrease, stop the API and
worker, make the backup shown above, confirm that enough temporary disk space
is available, and invoke the separate rewrite explicitly:

```bash
uv run huldra store vacuum \
  --db ~/.local/share/huldra/huldra.db \
  --json
```

`store vacuum` has no dry-run mode and does not initialize or migrate a
database. Do not include it in unattended retention: SQLite needs additional
disk space while rewriting the file and an exclusive maintenance window.

## Multi-Machine Limit

Do not run separate Huldra databases on several machines to increase arXiv
throughput. The arXiv legacy API limit applies across machines you control.
Use one centralized broker for the deployment, or wait for a future shared
rate-state backend.
