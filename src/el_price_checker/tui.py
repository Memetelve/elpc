from __future__ import annotations

from datetime import datetime
from pathlib import Path

from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import DataTable, Footer, Header, Static

from .db import Database


def _fmt_ts(ts: int | None) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_price(price_cents: int | None, currency: str | None) -> str:
    if price_cents is None:
        return ""
    cur = currency or ""
    return f"{price_cents / 100:.2f} {cur}".strip()


class PriceTuiApp(App):
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(self, db_path: Path):
        super().__init__()
        self.db = Database(db_path)
        self.products_table = DataTable(id="products")
        self.history_table = DataTable(id="history")
        self.status = Static(id="status")

    def compose(self) -> ComposeResult:
        yield Header()
        yield self.status
        with Horizontal():
            yield self.products_table
            yield self.history_table
        yield Footer()

    def on_mount(self) -> None:
        self.db.init()

        self.products_table.add_columns("ID", "Name", "Source", "Last price", "Last seen", "Error")
        self.products_table.cursor_type = "row"
        self.products_table.zebra_stripes = True

        self.history_table.add_columns("Time", "Price", "Currency", "Raw", "Error")
        self.history_table.cursor_type = "row"
        self.history_table.zebra_stripes = True

        self.action_refresh()

    def action_refresh(self) -> None:
        products = self.db.get_products()
        latest = self.db.get_latest_observations()

        self.products_table.clear()
        for p in products:
            o = latest.get(p.id)
            self.products_table.add_row(
                str(p.id),
                p.name,
                p.source,
                _fmt_price(None if not o else o.price_cents, None if not o else o.currency),
                _fmt_ts(None if not o else o.ts),
                "" if not o else (o.error or ""),
                key=str(p.id),
            )

        self.status.update(f"Products: {len(products)}   (press r to refresh, q to quit)")

        if products:
            self.products_table.move_cursor(row=0)
            self._load_history_for_product(products[0].id)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id != "products":
            return
        try:
            pid = int(event.row_key.value)
        except Exception:
            return
        self._load_history_for_product(pid)

    def _load_history_for_product(self, product_id: int) -> None:
        history = self.db.get_history(product_id, limit=200)
        self.history_table.clear()
        for o in history:
            self.history_table.add_row(
                _fmt_ts(o.ts),
                "" if o.price_cents is None else f"{o.price_cents / 100:.2f}",
                o.currency or "",
                o.raw_price_text or "",
                o.error or "",
            )
