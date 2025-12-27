# el-price-checker

A small `uv`-managed Python tool that periodically checks product pages (x-kom, morele.net, Amazon) and stores price history in SQLite, with a simple TUI for browsing.

## Notes / caveats

- This tool works best when you add **direct product URLs**. Searching/crawling is intentionally out of scope.
- Some sites (especially Amazon) may block automated requests (CAPTCHA/robot checks). When that happens, the tool stores the error so you can see failures in history.
- Respect the target sites’ Terms of Service and rate limits.
- If you hit HTTP 403 on x-kom/morele/amazon, copy the `Cookie` header from your browser’s DevTools for that site and set an env var before running: `ELPC_COOKIE_XKOM`, `ELPC_COOKIE_MORELE`, `ELPC_COOKIE_AMAZON`, or generic `ELPC_COOKIE`.
- x-kom may return zero results to automated searches; if you see “No results found”, try a more precise phrase and/or set `ELPC_COOKIE_XKOM` from your browser session. Direct product URLs are most reliable.

## Install (uv)

```bash
uv sync
```

## Usage

Initialize the database:

```bash
uv run elpc init
```

Add a product URL:

```bash
uv run elpc add "https://www.x-kom.pl/..." --name "RTX 4070 Super"
```

Add top N search results (x-kom or morele):

```bash
uv run elpc add-search --store xkom --search "4090" --top 10
```

Fetch once for all products:

```bash
uv run elpc once
```

Run periodic polling:

```bash
uv run elpc run --interval 900
```

Open the TUI:

```bash
uv run elpc tui
```

- Start the web UI (simple dashboard + charts):

```bash
uv run elpc serve --host 0.0.0.0 --port 8000
```

## Docker / Compose

Build the image locally:

```bash
docker build -t elpc .
```

Run web + worker with Docker Compose (shared SQLite volume at /data/prices.sqlite3):

```bash
docker compose up --build
```

Web UI: http://localhost:8000 (or your host). The worker polls every 900s by default.
To change the web port: `ELPC_WEB_PORT=8080 docker compose up --build` (then open http://localhost:8080).
You can set cookies to bypass WAFs via env vars: `ELPC_COOKIE_XKOM`, `ELPC_COOKIE_MORELE`, `ELPC_COOKIE_AMAZON`.

- Clear all data (confirmation required unless --yes):

```bash
uv run elpc clear --yes
```

- `add-search` currently supports x-kom and morele search pages; more stores can be added the same way if needed.

## Data location

By default the SQLite DB is stored under your user data directory (via `platformdirs`). You can override with `--db /path/to/file.sqlite3` on commands.
