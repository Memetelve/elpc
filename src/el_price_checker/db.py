from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = 3


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


@dataclass(frozen=True)
class Tag:
    id: int
    name: str
    color: str


class Database:
    def __init__(self, path: Path):
        self.path = path

    @staticmethod
    def _median(values: list[int]) -> float:
        if not values:
            return 0.0
        vals = sorted(values)
        n = len(vals)
        mid = n // 2
        if n % 2 == 1:
            return float(vals[mid])
        return (vals[mid - 1] + vals[mid]) / 2.0

    @staticmethod
    def _normalize_color(color: str) -> str:
        raw = color.strip()
        if not raw:
            return "#666666"
        c = raw.upper()
        if not c.startswith("#"):
            c = "#" + c
        if len(c) == 4:  # #RGB -> #RRGGBB
            c = "#" + "".join([ch * 2 for ch in c[1:]])
        if len(c) != 7 or any(ch not in "#0123456789ABCDEF" for ch in c):
            return "#666666"
        return c

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            # Run migrations if needed; always proceed to cleaning.
            current = conn.execute("PRAGMA user_version").fetchone()[0]
            if current == 0:
                self._create_v3(conn)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            elif current == 1:
                self._migrate_1_to_2(conn)
                self._migrate_2_to_3(conn)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            elif current == 2:
                self._migrate_2_to_3(conn)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            elif current != SCHEMA_VERSION:
                raise RuntimeError(f"Unsupported schema version: {current}")

        # Clean any pre-existing extreme outliers so future reads use sane baselines.
        self.clean_price_outliers()

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

    def _create_v3(self, conn: sqlite3.Connection) -> None:
        self._create_v2(conn)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tags (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE,
              color TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS product_tags (
              product_id INTEGER NOT NULL,
              tag_id INTEGER NOT NULL,
              PRIMARY KEY(product_id, tag_id),
              FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
              FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE
            );
            """
        )

    def _migrate_1_to_2(self, conn: sqlite3.Connection) -> None:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()}
        if "display_order" not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN display_order INTEGER")
        conn.execute(
            "UPDATE products SET display_order = id WHERE display_order IS NULL"
        )
        conn.commit()

    def _migrate_2_to_3(self, conn: sqlite3.Connection) -> None:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(tags)").fetchall()}
        if not cols:
            self._create_v3(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            return

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tags (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE,
              color TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS product_tags (
              product_id INTEGER NOT NULL,
              tag_id INTEGER NOT NULL,
              PRIMARY KEY(product_id, tag_id),
              FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
              FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE
            );
            """
        )
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

    def get_all_tags(self) -> list[Tag]:
        with self.connect() as conn:
            rows = conn.execute("SELECT id, name, color FROM tags ORDER BY name").fetchall()
        return [Tag(id=int(r[0]), name=r[1], color=r[2]) for r in rows]

    def upsert_tag(self, name: str, color: str) -> int:
        tag_name = name.strip()
        if not tag_name:
            raise ValueError("Tag name cannot be empty")
        color_norm = self._normalize_color(color)

        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO tags(name, color)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET color=excluded.color
                """,
                (tag_name, color_norm),
            )
            tag_id = cur.lastrowid
            # If update, lastrowid may be 0; fetch id.
            if not tag_id:
                tag_id = conn.execute(
                    "SELECT id FROM tags WHERE name = ?", (tag_name,)
                ).fetchone()[0]
            conn.commit()
            return int(tag_id)

    def tag_product(self, product_id: int, name: str, color: str) -> int:
        if not self.get_product(product_id):
            raise ValueError("Product not found")
        tag_id = self.upsert_tag(name, color)
        with self.connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO product_tags(product_id, tag_id) VALUES (?, ?)",
                (product_id, tag_id),
            )
            conn.commit()
        return tag_id

    def attach_tag(self, product_id: int, tag_id: int) -> None:
        if not self.get_product(product_id):
            raise ValueError("Product not found")
        with self.connect() as conn:
            row = conn.execute("SELECT id FROM tags WHERE id = ?", (tag_id,)).fetchone()
            if not row:
                raise ValueError("Tag not found")
            conn.execute(
                "INSERT OR IGNORE INTO product_tags(product_id, tag_id) VALUES (?, ?)",
                (product_id, tag_id),
            )
            conn.commit()

    def remove_tag_from_product(self, product_id: int, tag_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM product_tags WHERE product_id = ? AND tag_id = ?",
                (product_id, tag_id),
            )
            conn.commit()

    def get_tags_for_product(self, product_id: int) -> list[Tag]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT t.id, t.name, t.color
                FROM product_tags pt
                JOIN tags t ON t.id = pt.tag_id
                WHERE pt.product_id = ?
                ORDER BY t.name
                """,
                (product_id,),
            ).fetchall()
        return [Tag(id=int(r[0]), name=r[1], color=r[2]) for r in rows]

    def get_tags_for_products(self, product_ids: list[int]) -> dict[int, list[Tag]]:
        if not product_ids:
            return {}
        placeholders = ",".join(["?"] * len(product_ids))
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT pt.product_id, t.id, t.name, t.color
                FROM product_tags pt
                JOIN tags t ON t.id = pt.tag_id
                WHERE pt.product_id IN ({placeholders})
                ORDER BY t.name
                """,
                tuple(product_ids),
            ).fetchall()
        out: dict[int, list[Tag]] = {pid: [] for pid in product_ids}
        for r in rows:
            pid = int(r[0])
            out.setdefault(pid, []).append(
                Tag(id=int(r[1]), name=r[2], color=r[3])
            )
        return out

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

    def set_product_order(self, ordered_product_ids: list[int]) -> None:
        if not ordered_product_ids:
            return

        with self.connect() as conn:
            rows = conn.execute("SELECT id FROM products").fetchall()
            existing = {int(r["id"]) for r in rows}

            # Validate: caller must provide a permutation of all products.
            provided = [int(x) for x in ordered_product_ids]
            if len(set(provided)) != len(provided):
                raise ValueError("Duplicate product ids")
            if set(provided) != existing:
                raise ValueError("Order must include all product ids")

            conn.executemany(
                "UPDATE products SET display_order = ? WHERE id = ?",
                [(idx + 1, pid) for idx, pid in enumerate(provided)],
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
            # Discard impossible or extreme prices before insert.
            price_to_store = price_cents
            err_to_store = error

            if price_cents is not None:
                if price_cents <= 0:
                    price_to_store = None
                    if not err_to_store:
                        err_to_store = "Discarded non-positive price"
                else:
                    is_outlier, ref_median = self._is_outlier(
                        conn, product_id, price_cents
                    )
                    if is_outlier:
                        price_to_store = None
                        if not err_to_store:
                            err_to_store = (
                                f"Discarded outlier vs median {ref_median / 100:.2f}"
                                if ref_median
                                else "Discarded outlier"
                            )

            cur = conn.execute(
                """
                INSERT INTO observations(product_id, ts, price_cents, currency, in_stock, title, raw_price_text, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product_id,
                    ts_val,
                    price_to_store,
                    currency,
                    in_stock_val,
                    title,
                    raw_price_text,
                    err_to_store,
                ),
            )
            return int(cur.lastrowid)

    def _is_outlier(
        self,
        conn: sqlite3.Connection,
        product_id: int,
        price_cents: int,
        *,
        factor: float = 6.0,
        min_samples: int = 3,
        sample_limit: int = 50,
    ) -> tuple[bool, float | None]:
        rows = conn.execute(
            """
            SELECT price_cents
            FROM observations
            WHERE product_id = ? AND price_cents IS NOT NULL
            ORDER BY ts DESC
            LIMIT ?
            """,
            (product_id, sample_limit),
        ).fetchall()
        prices = [int(r[0]) for r in rows if r[0] is not None]
        if len(prices) < min_samples:
            return (False, None)
        median = self._median(prices)
        if median <= 0:
            return (False, None)
        lower = median / factor
        upper = median * factor
        if price_cents < lower or price_cents > upper:
            return (True, median)
        return (False, median)

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

    def clean_price_outliers(
        self, *, factor: float = 6.0, min_samples: int = 3, sample_limit: int = 200
    ) -> int:
        """Remove historical observations that are extreme outliers per product.

        Returns the number of observations deleted.
        """

        removed = 0
        with self.connect() as conn:
            # First drop impossible non-positive prices.
            cur = conn.execute(
                "DELETE FROM observations WHERE price_cents IS NOT NULL AND price_cents <= 0"
            )
            removed += cur.rowcount if cur.rowcount is not None else 0

            product_ids = [int(r[0]) for r in conn.execute("SELECT id FROM products")]
            for pid in product_ids:
                rows = conn.execute(
                    """
                    SELECT id, price_cents
                    FROM observations
                    WHERE product_id = ? AND price_cents IS NOT NULL
                    ORDER BY ts DESC
                    LIMIT ?
                    """,
                    (pid, sample_limit),
                ).fetchall()

                prices = [int(r[1]) for r in rows if r[1] is not None]
                if len(prices) < min_samples:
                    continue

                median = self._median(prices)
                if median <= 0:
                    continue
                lower = median / factor
                upper = median * factor

                bad_ids = [int(r[0]) for r in rows if r[1] < lower or r[1] > upper]
                if bad_ids:
                    conn.executemany(
                        "DELETE FROM observations WHERE id = ?",
                        [(bid,) for bid in bad_ids],
                    )
                    removed += len(bad_ids)

            conn.commit()

        return removed

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
