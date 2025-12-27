from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = 2


@dataclass(frozen=True)
class Product:
    id: int
    name: str
    url: str
    source: str
    created_at: int
    display_order: int


@dataclass(frozen=True)
class Observation:
    id: int
    product_id: int
    ts: int
    price_cents: int | None
    currency: str | None
    in_stock: bool | None
    title: str | None
    raw_price_text: str | None
    error: str | None


class Database:
    def __init__(self, path: Path):
        self.path = path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            current = conn.execute("PRAGMA user_version").fetchone()[0]
            if current == 0:
                self._create_v2(conn)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            elif current == 1:
                self._migrate_1_to_2(conn)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            elif current == SCHEMA_VERSION:
                return
            else:
                raise RuntimeError(f"Unsupported schema version: {current}")

    def _create_v1(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS products (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              url TEXT NOT NULL UNIQUE,
              source TEXT NOT NULL,
              created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS observations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              product_id INTEGER NOT NULL,
              ts INTEGER NOT NULL,
              price_cents INTEGER,
              currency TEXT,
              in_stock INTEGER,
              title TEXT,
              raw_price_text TEXT,
              error TEXT,
              FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_observations_product_ts
              ON observations(product_id, ts);
            """
        )

    def _create_v2(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS products (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              url TEXT NOT NULL UNIQUE,
              source TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              display_order INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS observations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              product_id INTEGER NOT NULL,
              ts INTEGER NOT NULL,
              price_cents INTEGER,
              currency TEXT,
              in_stock INTEGER,
              title TEXT,
              raw_price_text TEXT,
              error TEXT,
              FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_observations_product_ts
              ON observations(product_id, ts);
            """
        )

    def _migrate_1_to_2(self, conn: sqlite3.Connection) -> None:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()}
        if "display_order" not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN display_order INTEGER")
        conn.execute("UPDATE products SET display_order = id WHERE display_order IS NULL")
        conn.commit()

    def add_product(self, name: str, url: str, source: str) -> int:
        now = int(time.time())
        with self.connect() as conn:
            next_order = conn.execute(
                "SELECT COALESCE(MAX(display_order), 0) + 1 FROM products"
            ).fetchone()[0]
            cur = conn.execute(
                "INSERT INTO products(name, url, source, created_at, display_order) VALUES (?, ?, ?, ?, ?)",
                (name, url, source, now, int(next_order)),
            )
            pid = int(cur.lastrowid)
            conn.commit()
            return pid

    def get_products(self) -> list[Product]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, name, url, source, created_at, display_order FROM products ORDER BY display_order, id"
            ).fetchall()
        return [Product(**dict(r)) for r in rows]

    def get_product(self, product_id: int) -> Product | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT id, name, url, source, created_at, display_order FROM products WHERE id = ?",
                (product_id,),
            ).fetchone()
        return Product(**dict(row)) if row else None

    def upsert_product_name(self, product_id: int, name: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE products SET name = ? WHERE id = ?", (name, product_id)
            )
            conn.commit()

    def move_product(self, product_id: int, *, direction: str) -> None:
        if direction not in {"up", "down"}:
            raise ValueError("direction must be 'up' or 'down'")

        with self.connect() as conn:
            row = conn.execute(
                "SELECT id, display_order FROM products WHERE id = ?",
                (product_id,),
            ).fetchone()
            if not row:
                return
            current_order = int(row["display_order"])

            if direction == "up":
                other = conn.execute(
                    """
                    SELECT id, display_order
                    FROM products
                    WHERE display_order < ?
                    ORDER BY display_order DESC, id DESC
                    LIMIT 1
                    """,
                    (current_order,),
                ).fetchone()
            else:
                other = conn.execute(
                    """
                    SELECT id, display_order
                    FROM products
                    WHERE display_order > ?
                    ORDER BY display_order ASC, id ASC
                    LIMIT 1
                    """,
                    (current_order,),
                ).fetchone()

            if not other:
                return

            other_id = int(other["id"])
            other_order = int(other["display_order"])
            conn.execute(
                "UPDATE products SET display_order = ? WHERE id = ?",
                (other_order, product_id),
            )
            conn.execute(
                "UPDATE products SET display_order = ? WHERE id = ?",
                (current_order, other_id),
            )
            conn.commit()

    def add_observation(
        self,
        product_id: int,
        *,
        ts: int | None = None,
        price_cents: int | None = None,
        currency: str | None = None,
        in_stock: bool | None = None,
        title: str | None = None,
        raw_price_text: str | None = None,
        error: str | None = None,
    ) -> int:
        ts_val = int(time.time()) if ts is None else int(ts)
        in_stock_val = None if in_stock is None else (1 if in_stock else 0)
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO observations(product_id, ts, price_cents, currency, in_stock, title, raw_price_text, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product_id,
                    ts_val,
                    price_cents,
                    currency,
                    in_stock_val,
                    title,
                    raw_price_text,
                    error,
                ),
            )
            return int(cur.lastrowid)

    def get_priced_observation_at_or_before(
        self, product_id: int, ts: int
    ) -> Observation | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT id, product_id, ts, price_cents, currency, in_stock, title, raw_price_text, error
                FROM observations
                WHERE product_id = ?
                  AND ts <= ?
                  AND price_cents IS NOT NULL
                ORDER BY ts DESC
                LIMIT 1
                """,
                (product_id, int(ts)),
            ).fetchone()
        if not row:
            return None
        return Observation(
            id=int(row["id"]),
            product_id=int(row["product_id"]),
            ts=int(row["ts"]),
            price_cents=row["price_cents"],
            currency=row["currency"],
            in_stock=(None if row["in_stock"] is None else bool(int(row["in_stock"]))),
            title=row["title"],
            raw_price_text=row["raw_price_text"],
            error=row["error"],
        )

    def get_latest_observations(self) -> dict[int, Observation]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT o.*
                FROM observations o
                JOIN (
                  SELECT product_id, MAX(ts) AS max_ts
                  FROM observations
                  GROUP BY product_id
                ) last
                ON last.product_id = o.product_id AND last.max_ts = o.ts
                """
            ).fetchall()
        latest: dict[int, Observation] = {}
        for r in rows:
            latest[int(r["product_id"])] = Observation(
                id=int(r["id"]),
                product_id=int(r["product_id"]),
                ts=int(r["ts"]),
                price_cents=r["price_cents"],
                currency=r["currency"],
                in_stock=(None if r["in_stock"] is None else bool(int(r["in_stock"]))),
                title=r["title"],
                raw_price_text=r["raw_price_text"],
                error=r["error"],
            )
        return latest

    def get_history(self, product_id: int, limit: int = 200) -> list[Observation]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, product_id, ts, price_cents, currency, in_stock, title, raw_price_text, error
                FROM observations
                WHERE product_id = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (product_id, limit),
            ).fetchall()
        out: list[Observation] = []
        for r in rows:
            out.append(
                Observation(
                    id=int(r["id"]),
                    product_id=int(r["product_id"]),
                    ts=int(r["ts"]),
                    price_cents=r["price_cents"],
                    currency=r["currency"],
                    in_stock=(
                        None if r["in_stock"] is None else bool(int(r["in_stock"]))
                    ),
                    title=r["title"],
                    raw_price_text=r["raw_price_text"],
                    error=r["error"],
                )
            )
        return out

    def iter_observations(self, product_ids: Iterable[int] | None = None):
        with self.connect() as conn:
            if product_ids is None:
                rows = conn.execute(
                    "SELECT * FROM observations ORDER BY ts DESC"
                ).fetchall()
            else:
                ids = list(product_ids)
                if not ids:
                    return []
                placeholders = ",".join(["?"] * len(ids))
                rows = conn.execute(
                    f"SELECT * FROM observations WHERE product_id IN ({placeholders}) ORDER BY ts DESC",
                    tuple(ids),
                ).fetchall()
        return rows
