from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from selectolax.parser import HTMLParser


@dataclass(frozen=True)
class ParsedPrice:
    price_cents: int | None
    currency: str | None
    title: str | None
    raw_price_text: str | None
    in_stock: bool | None
    error: str | None = None


_PRICE_RE = re.compile(
    r"(?P<num>\d[\d\s\u00A0.,]*)\s*(?P<cur>z\u0142|PLN|EUR|\u20ac)",
    re.IGNORECASE,
)


def _clean_number(text: str) -> str:
    # common formats:
    # 5 999,00  | 5 999,00 | 5,999.00 | 5999
    t = text.replace("\u00A0", " ")
    t = t.replace(" ", "")
    # If both separators appear, assume last is decimal.
    if "," in t and "." in t:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "")
            t = t.replace(",", ".")
        else:
            t = t.replace(",", "")
    else:
        # If only comma, treat as decimal separator.
        if "," in t:
            t = t.replace(",", ".")
    return t


def _decimal_to_cents(value: str) -> int | None:
    try:
        d = Decimal(value)
    except InvalidOperation:
        return None
    return int((d * 100).quantize(Decimal("1")))


def _iter_jsonld_objects(doc: HTMLParser) -> list[Any]:
    out: list[Any] = []
    for node in doc.css('script[type="application/ld+json"]'):
        raw = (node.text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        out.append(data)
    return out


def _walk(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _walk(it)


def _extract_title(doc: HTMLParser) -> str | None:
    og = doc.css_first('meta[property="og:title"]')
    if og and og.attributes.get("content"):
        return og.attributes.get("content")
    title = doc.css_first("title")
    if title:
        t = (title.text() or "").strip()
        if t:
            return t
    h1 = doc.css_first("h1")
    if h1:
        t = (h1.text() or "").strip()
        if t:
            return t
    return None


def _parse_offer_price(offers: Any) -> tuple[int | None, str | None, str | None]:
    # returns (price_cents, currency, raw_text)
    if offers is None:
        return (None, None, None)

    candidates = offers if isinstance(offers, list) else [offers]
    for offer in candidates:
        if not isinstance(offer, dict):
            continue
        price = offer.get("price") or offer.get("priceSpecification", {}).get("price")
        currency = offer.get("priceCurrency")
        if price is None:
            continue

        if isinstance(price, (int, float)):
            cents = _decimal_to_cents(str(price))
            return (cents, currency, str(price))

        if isinstance(price, str):
            cleaned = _clean_number(price)
            cents = _decimal_to_cents(cleaned)
            return (cents, currency, price)

    return (None, None, None)


def extract_price(html: str) -> ParsedPrice:
    doc = HTMLParser(html)
    title = _extract_title(doc)

    lower = html.lower()
    if "robot check" in lower or "captcha" in lower and "amazon" in lower:
        return ParsedPrice(
            price_cents=None,
            currency=None,
            title=title,
            raw_price_text=None,
            in_stock=None,
            error="Blocked by anti-bot / CAPTCHA",
        )

    # 1) JSON-LD schema.org
    for data in _iter_jsonld_objects(doc):
        for obj in _walk(data):
            if not isinstance(obj, dict):
                continue
            offers = obj.get("offers")
            cents, currency, raw = _parse_offer_price(offers)
            if cents is not None:
                return ParsedPrice(
                    price_cents=cents,
                    currency=currency,
                    title=title,
                    raw_price_text=raw,
                    in_stock=None,
                )

    # 2) OpenGraph / meta tags
    meta_amt = doc.css_first('meta[property="product:price:amount"]')
    if meta_amt and meta_amt.attributes.get("content"):
        amt = meta_amt.attributes["content"]
        cents = _decimal_to_cents(_clean_number(amt))
        meta_cur = doc.css_first('meta[property="product:price:currency"]')
        cur = meta_cur.attributes.get("content") if meta_cur else None
        if cents is not None and cents > 0:
            return ParsedPrice(
                price_cents=cents,
                currency=cur,
                title=title,
                raw_price_text=amt,
                in_stock=None,
            )

    # 3) Regex fallback
    text = doc.text(separator=" ")
    m = _PRICE_RE.search(text)
    if m:
        raw_num = m.group("num")
        cur_raw = m.group("cur")
        currency = "PLN" if cur_raw.lower() in {"zł", "pln"} else ("EUR" if cur_raw in {"EUR", "€"} else None)
        cents = _decimal_to_cents(_clean_number(raw_num))
        if cents is not None and cents > 0:
            return ParsedPrice(
                price_cents=cents,
                currency=currency,
                title=title,
                raw_price_text=f"{raw_num} {cur_raw}",
                in_stock=None,
            )

    return ParsedPrice(
        price_cents=None,
        currency=None,
        title=title,
        raw_price_text=None,
        in_stock=None,
        error="Price not found",
    )
