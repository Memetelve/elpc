from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from .db import Database
from .fetch import detect_source, fetch_html
from .parse import extract_price


@dataclass(frozen=True)
class PollResult:
    product_id: int
    ok: bool
    error: str | None


async def poll_product(db: Database, product_id: int) -> PollResult:
    product = db.get_product(product_id)
    if not product:
        return PollResult(product_id=product_id, ok=False, error="Unknown product")

    source = product.source or detect_source(product.url)

    try:
        res = await fetch_html(product.url, source=source)
        if res.status_code >= 400:
            # Try browser fallback once before recording error.
            res_fallback = await fetch_html(product.url, source=source, prefer_browser=True)
            if res_fallback.status_code < 400:
                res = res_fallback
            else:
                db.add_observation(
                    product_id,
                    price_cents=None,
                    currency=None,
                    in_stock=None,
                    title=None,
                    raw_price_text=None,
                    error=f"HTTP {res_fallback.status_code}",
                )
                return PollResult(product_id=product_id, ok=False, error=f"HTTP {res_fallback.status_code}")

        parsed = extract_price(res.text)
        if parsed.title and (not product.name or product.name.startswith("http")):
            db.upsert_product_name(product_id, parsed.title)

        db.add_observation(
            product_id,
            price_cents=parsed.price_cents,
            currency=parsed.currency,
            in_stock=parsed.in_stock,
            title=parsed.title,
            raw_price_text=parsed.raw_price_text,
            error=parsed.error,
        )
        return PollResult(product_id=product_id, ok=(parsed.error is None), error=parsed.error)

    except Exception as e:
        db.add_observation(
            product_id,
            price_cents=None,
            currency=None,
            in_stock=None,
            title=None,
            raw_price_text=None,
            error=f"Exception: {type(e).__name__}: {e}",
        )
        return PollResult(product_id=product_id, ok=False, error=str(e))


async def poll_all(db: Database, concurrency: int = 6) -> list[PollResult]:
    products = db.get_products()
    sem = asyncio.Semaphore(concurrency)

    async def _one(pid: int) -> PollResult:
        async with sem:
            return await poll_product(db, pid)

    tasks = [_one(p.id) for p in products]
    return await asyncio.gather(*tasks)


async def run_forever(db: Database, interval_s: int, concurrency: int = 6) -> None:
    while True:
        start = time.time()
        await poll_all(db, concurrency=concurrency)
        elapsed = time.time() - start
        sleep_for = max(0.0, interval_s - elapsed)
        await asyncio.sleep(sleep_for)
