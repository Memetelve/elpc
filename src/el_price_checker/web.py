from __future__ import annotations

import datetime
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .db import Database
from .fetch import detect_source, fetch_html
from .parse import extract_price
from .search import search_products
from .settings import Settings

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


async def _fetch_and_parse(url: str, source: str):
    res = await fetch_html(url, source=source)
    parsed = extract_price(res.text)
    return parsed


def _fmt_ts(ts: int | None) -> str:
    if ts is None:
        return ""
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def create_app(db_path: Path | None = None) -> FastAPI:
    settings = Settings()
    database = Database(db_path or settings.default_db_path())
    database.init()

    templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
    app = FastAPI(title="el-price-checker", docs_url=None, redoc_url=None)

    def _product_views() -> list[dict[str, Any]]:
        products = database.get_products()
        latest = database.get_latest_observations()
        tags_map = database.get_tags_for_products([p.id for p in products])
        now_ts = int(datetime.datetime.now().timestamp())
        cutoff_24h = now_ts - 24 * 60 * 60
        out: list[dict[str, Any]] = []
        for p in products:
            o = latest.get(p.id)

            change_24h = None
            if o and o.price_cents is not None:
                prev = database.get_priced_observation_at_or_before(p.id, cutoff_24h)
                if (
                    prev
                    and prev.price_cents is not None
                    and (prev.currency == o.currency)
                ):
                    change_24h = (o.price_cents - prev.price_cents) / 100.0

            out.append(
                {
                    "id": p.id,
                    "name": p.name,
                    "source": p.source,
                    "url": p.url,
                    "tags": [
                        {"id": t.id, "name": t.name, "color": t.color}
                        for t in tags_map.get(p.id, [])
                    ],
                    "last_price": None
                    if not o or o.price_cents is None
                    else o.price_cents / 100.0,
                    "currency": "" if not o else (o.currency or ""),
                    "last_seen": _fmt_ts(None if not o else o.ts),
                    "error": None if not o else o.error,
                    "change_24h": change_24h,
                }
            )
        return out

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request, msg: str | None = None, err: str | None = None):
        products = _product_views()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "products": products,
                "msg": msg,
                "err": err,
                "search_store": None,
                "search_query": None,
                "search_results": None,
            },
        )

    @app.get("/tags", response_class=HTMLResponse)
    def tags_page(request: Request, msg: str | None = None, err: str | None = None):
        return templates.TemplateResponse(
            "tags.html",
            {
                "request": request,
                "tags": database.get_all_tags(),
                "msg": msg,
                "err": err,
            },
        )

    @app.post("/tags")
    def create_tag(name: str = Form(...), color: str = Form("#666666")):
        try:
            database.upsert_tag(name, color)
        except ValueError as e:
            return RedirectResponse(url=f"/tags?err={str(e)}", status_code=303)
        return RedirectResponse(url="/tags?msg=Saved", status_code=303)

    @app.post("/search", response_class=HTMLResponse)
    async def search(request: Request, store: str = Form(...), query: str = Form(...)):
        q = query.strip()
        if not q:
            products = _product_views()
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "products": products,
                    "msg": None,
                    "err": "Search query cannot be empty",
                    "search_store": store,
                    "search_query": q,
                    "search_results": [],
                },
            )

        try:
            hits = await search_products(store, q, limit=5)
        except ValueError as e:
            products = _product_views()
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "products": products,
                    "msg": None,
                    "err": str(e),
                    "search_store": store,
                    "search_query": q,
                    "search_results": [],
                },
            )
        except Exception:
            products = _product_views()
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "products": products,
                    "msg": None,
                    "err": "Search failed",
                    "search_store": store,
                    "search_query": q,
                    "search_results": [],
                },
            )

        results: list[dict[str, Any]] = []
        for h in hits:
            results.append(
                {
                    "name": h.name,
                    "url": h.url,
                    "source": h.source,
                    "price_cents": h.price_cents,
                    "currency": h.currency,
                }
            )

        products = _product_views()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "products": products,
                "msg": None,
                "err": None,
                "search_store": store,
                "search_query": q,
                "search_results": results,
            },
        )

    @app.post("/add-search")
    async def add_from_search(request: Request):
        form = await request.form()
        try:
            count = int(str(form.get("count") or "0"))
        except ValueError:
            count = 0

        if count <= 0:
            return RedirectResponse(url="/?err=Nothing%20to%20add", status_code=303)

        added = 0
        skipped = 0
        for i in range(count):
            if form.get(f"select_{i}") is None:
                continue

            url = str(form.get(f"url_{i}") or "").strip()
            source = str(form.get(f"source_{i}") or "").strip()
            name = str(form.get(f"name_{i}") or "").strip()
            if not name:
                name = url

            price_cents_raw = form.get(f"price_cents_{i}")
            currency = str(form.get(f"currency_{i}") or "").strip() or None
            try:
                price_cents = (
                    None if price_cents_raw in (None, "") else int(str(price_cents_raw))
                )
            except ValueError:
                price_cents = None

            if not url or not source:
                skipped += 1
                continue

            try:
                pid = database.add_product(name, url, source)
            except sqlite3.IntegrityError:
                skipped += 1
                continue

            database.add_observation(
                pid,
                price_cents=price_cents,
                currency=currency,
                in_stock=None,
                title=None,
                raw_price_text=None,
                error=None,
            )
            added += 1

        if added == 0 and skipped > 0:
            return RedirectResponse(
                url=f"/?err=Nothing%20added%20(skipped%20{skipped})", status_code=303
            )
        if skipped:
            return RedirectResponse(
                url=f"/?msg=Added%20{added}%20(skipped%20{skipped})", status_code=303
            )
        return RedirectResponse(url=f"/?msg=Added%20{added}", status_code=303)

    @app.get("/product/{product_id}", response_class=HTMLResponse)
    def product_page(product_id: int, request: Request):
        product = database.get_product(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        latest = database.get_latest_observations().get(product_id)
        now_ts = int(datetime.datetime.now().timestamp())
        cutoff_24h = now_ts - 24 * 60 * 60

        change_24h = None
        if latest and latest.price_cents is not None:
            prev = database.get_priced_observation_at_or_before(product_id, cutoff_24h)
            if (
                prev
                and prev.price_cents is not None
                and (prev.currency == latest.currency)
            ):
                change_24h = (latest.price_cents - prev.price_cents) / 100.0

        view = {
            "id": product.id,
            "name": product.name,
            "source": product.source,
            "url": product.url,
            "last_price": None
            if not latest or latest.price_cents is None
            else latest.price_cents / 100.0,
            "currency": "" if not latest else (latest.currency or ""),
            "last_seen": _fmt_ts(None if not latest else latest.ts),
            "error": None if not latest else latest.error,
            "change_24h": change_24h,
        }
        return templates.TemplateResponse(
            "product.html",
            {
                "request": request,
                "product": view,
                "tags": database.get_tags_for_product(product_id),
                "all_tags": database.get_all_tags(),
            },
        )

    @app.get("/api/products")
    def api_products():
        return _product_views()

    @app.get("/api/products/{product_id}/history")
    def api_history(product_id: int):
        product = database.get_product(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        history = database.get_history(product_id, limit=500)
        out: list[dict[str, Any]] = []
        for obs in reversed(history):
            out.append(
                {
                    "ts_ms": obs.ts * 1000,
                    "price": None
                    if obs.price_cents is None
                    else obs.price_cents / 100.0,
                    "currency": obs.currency,
                    "error": obs.error,
                }
            )
        return out

    @app.post("/add")
    async def add_product(url: str = Form(...), name: str | None = Form(None)):
        source = detect_source(url)
        parsed = await _fetch_and_parse(url, source)
        product_name = name or parsed.title or url
        try:
            pid = database.add_product(product_name, url, source)
        except sqlite3.IntegrityError:
            return RedirectResponse(url="/?err=Already%20tracking", status_code=303)

        database.add_observation(
            pid,
            price_cents=parsed.price_cents,
            currency=parsed.currency,
            in_stock=parsed.in_stock,
            title=parsed.title,
            raw_price_text=parsed.raw_price_text,
            error=parsed.error,
        )
        return RedirectResponse(url="/?msg=Added", status_code=303)

    @app.post("/delete/{product_id}")
    def delete_product(product_id: int):
        with database.connect() as conn:
            cur = conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Product not found")
            conn.commit()
        return RedirectResponse(url="/?msg=Deleted", status_code=303)

    @app.post("/product/{product_id}/tag")
    def add_tag(product_id: int, tag_id: int = Form(...)):
        try:
            database.attach_tag(product_id, tag_id)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return RedirectResponse(
            url=f"/product/{product_id}?msg=Tag%20added", status_code=303
        )

    @app.post("/product/{product_id}/remove-tag/{tag_id}")
    def remove_tag(product_id: int, tag_id: int):
        if not database.get_product(product_id):
            raise HTTPException(status_code=404, detail="Product not found")
        database.remove_tag_from_product(product_id, tag_id)
        return RedirectResponse(
            url=f"/product/{product_id}?msg=Tag%20removed", status_code=303
        )

    @app.post("/rename/{product_id}")
    def rename_product(product_id: int, name: str = Form(...)):
        new_name = name.strip()
        if not new_name:
            return RedirectResponse(
                url="/?err=Name%20cannot%20be%20empty", status_code=303
            )
        if not database.get_product(product_id):
            raise HTTPException(status_code=404, detail="Product not found")
        database.upsert_product_name(product_id, new_name)
        return RedirectResponse(url="/?msg=Renamed", status_code=303)

    @app.post("/move/{product_id}")
    def move_product(product_id: int, direction: str = Form(...)):
        if not database.get_product(product_id):
            raise HTTPException(status_code=404, detail="Product not found")
        try:
            database.move_product(product_id, direction=direction)
        except ValueError:
            return RedirectResponse(url="/?err=Invalid%20direction", status_code=303)
        return RedirectResponse(url="/?msg=Reordered", status_code=303)

    @app.post("/reorder")
    def reorder_products(order: str = Form(...)):
        raw = [part.strip() for part in order.split(",") if part.strip()]
        try:
            ids = [int(x) for x in raw]
        except ValueError:
            return RedirectResponse(url="/?err=Invalid%20order", status_code=303)

        try:
            database.set_product_order(ids)
        except ValueError:
            return RedirectResponse(url="/?err=Invalid%20order", status_code=303)

        return RedirectResponse(url="/?msg=Reordered", status_code=303)

    return app
