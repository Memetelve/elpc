from __future__ import annotations

import asyncio
import csv
import sqlite3
from pathlib import Path
from typing import Annotated, Optional

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from .db import Database
from .fetch import detect_source, fetch_html
from .parse import extract_price
from .runner import poll_all, run_forever
from .search import SearchHit, search_products
from .settings import Settings
from .tui import PriceTuiApp
from .web import create_app


app = typer.Typer(add_completion=False, help="Track electronics prices (x-kom, morele.net, Amazon) with SQLite + TUI.")
console = Console()


def _db_from_option(db: Path | None) -> Database:
    settings = Settings()
    return Database(db or settings.default_db_path())


async def _fetch_and_parse(url: str, source: str):
    res = await fetch_html(url, source=source)
    parsed = extract_price(res.text)
    return parsed


def _insert_product_and_observation(
    database: Database,
    *,
    url: str,
    source: str,
    name_hint: str | None,
    price_cents: int | None,
    currency: str | None,
    in_stock: bool | None,
    title: str | None,
    raw_price_text: str | None,
    error: str | None,
) -> int | None:
    product_name = name_hint or title or url
    try:
        pid = database.add_product(product_name, url, source)
    except sqlite3.IntegrityError:
        console.print(f"[yellow]Skip (already exists): {url}")
        return None

    database.add_observation(
        pid,
        price_cents=price_cents,
        currency=currency,
        in_stock=in_stock,
        title=title,
        raw_price_text=raw_price_text,
        error=error,
    )
    return pid


@app.command()
def init(
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """Initialize the SQLite database."""
    database = _db_from_option(db)
    database.init()
    console.print(f"DB ready: {database.path}")


@app.command()
def add(
    url: str,
    name: Annotated[Optional[str], typer.Option(help="Optional display name")] = None,
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """Add a product URL and immediately record one observation."""
    database = _db_from_option(db)
    database.init()

    source = detect_source(url)
    parsed = asyncio.run(_fetch_and_parse(url, source))
    pid = _insert_product_and_observation(
        database,
        url=url,
        source=source,
        name_hint=name,
        price_cents=parsed.price_cents,
        currency=parsed.currency,
        in_stock=parsed.in_stock,
        title=parsed.title,
        raw_price_text=parsed.raw_price_text,
        error=parsed.error,
    )

    if pid is not None:
        product_name = name or parsed.title or url
        console.print(f"Added product #{pid}: {product_name} [{source}]")


@app.command()
def list(
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """List tracked products with their latest observation."""
    database = _db_from_option(db)
    database.init()
    products = database.get_products()
    latest = database.get_latest_observations()

    table = Table(title="Tracked products")
    table.add_column("ID", justify="right")
    table.add_column("Name")
    table.add_column("Source")
    table.add_column("Last price", justify="right")
    table.add_column("Currency")
    table.add_column("Last seen")
    table.add_column("Error")

    for p in products:
        o = latest.get(p.id)
        table.add_row(
            str(p.id),
            p.name,
            p.source,
            "" if not o or o.price_cents is None else f"{o.price_cents / 100:.2f}",
            "" if not o else (o.currency or ""),
            "" if not o else str(o.ts),
            "" if not o else (o.error or ""),
        )

    console.print(table)


@app.command("add-search")
def add_search(
    store: Annotated[str, typer.Option("--store", "-s", help="Store key, e.g. xkom")],
    search: Annotated[str, typer.Option("--search", "-q", help="Search phrase")],
    top: Annotated[int, typer.Option("--top", "-n", help="Number of results to add")] = 10,
    concurrency: Annotated[int, typer.Option(help="Concurrent fetches for product pages")] = 5,
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """Search the store, take top N results, add them, and record initial observations."""

    database = _db_from_option(db)
    database.init()

    try:
        hits = asyncio.run(search_products(store, search, limit=top))
    except ValueError as e:
        raise typer.Exit(code=1, message=str(e))

    if not hits:
        console.print("[yellow]No results found")
        return

    console.print(f"Found {len(hits)} result(s); fetching product pages...")

    async def _process(hits_in: list[SearchHit]):
        sem = asyncio.Semaphore(concurrency)

        async def one(hit: SearchHit):
            async with sem:
                parsed = await _fetch_and_parse(hit.url, hit.source)
                return hit, parsed

        return await asyncio.gather(*[one(h) for h in hits_in])

    results = asyncio.run(_process(hits))

    added = 0
    for hit, parsed in results:
        pid = _insert_product_and_observation(
            database,
            url=hit.url,
            source=hit.source,
            name_hint=hit.name,
            price_cents=parsed.price_cents if parsed.price_cents is not None else hit.price_cents,
            currency=parsed.currency if parsed.currency is not None else hit.currency,
            in_stock=parsed.in_stock,
            title=parsed.title,
            raw_price_text=parsed.raw_price_text,
            error=parsed.error,
        )
        if pid:
            added += 1
            console.print(f"Added product #{pid}: {hit.name} [{hit.source}]")

    console.print(f"Done. Added {added}/{len(results)} new products")


@app.command()
def once(
    concurrency: Annotated[int, typer.Option(help="Max concurrent requests")] = 6,
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """Fetch all tracked products once and store observations."""
    database = _db_from_option(db)
    database.init()
    results = asyncio.run(poll_all(database, concurrency=concurrency))
    ok = sum(1 for r in results if r.ok)
    console.print(f"Done. OK: {ok}/{len(results)}")


@app.command()
def run(
    interval: Annotated[int, typer.Option(help="Polling interval in seconds")] = 900,
    concurrency: Annotated[int, typer.Option(help="Max concurrent requests")] = 6,
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """Run periodic polling forever."""
    database = _db_from_option(db)
    database.init()
    console.print(f"Polling every {interval}s. DB: {database.path}")
    asyncio.run(run_forever(database, interval_s=interval, concurrency=concurrency))


@app.command()
def tui(
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """Open the Textual TUI to browse products and price history."""
    database = _db_from_option(db)
    database.init()
    PriceTuiApp(database.path).run()


@app.command()
def export(
    out: Annotated[Path, typer.Argument(help="Output CSV file path")],
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """Export observations to CSV."""
    database = _db_from_option(db)
    database.init()

    rows = database.iter_observations()
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "id",
                "product_id",
                "ts",
                "price_cents",
                "currency",
                "in_stock",
                "title",
                "raw_price_text",
                "error",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r["id"],
                    r["product_id"],
                    r["ts"],
                    r["price_cents"],
                    r["currency"],
                    r["in_stock"],
                    r["title"],
                    r["raw_price_text"],
                    r["error"],
                ]
            )

    console.print(f"Wrote: {out}")


@app.command()
def clear(
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation")] = False,
) -> None:
    """Delete all products and observations from the database."""
    database = _db_from_option(db)
    database.init()

    if not yes:
        proceed = typer.confirm(
            f"This will delete ALL products and observations in {database.path}. Continue?",
            default=False,
        )
        if not proceed:
            console.print("Aborted")
            raise typer.Exit(code=1)

    with database.connect() as conn:
        conn.execute("DELETE FROM observations")
        conn.execute("DELETE FROM products")
        conn.commit()
    console.print("Database cleared.")


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Bind host")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Bind port")] = 8000,
    db: Annotated[Optional[Path], typer.Option(help="Path to SQLite database file")] = None,
) -> None:
    """Start the web UI for managing tracked items."""

    database = _db_from_option(db)
    database.init()
    app = create_app(database.path)
    uvicorn.run(app, host=host, port=port, log_level="info")
