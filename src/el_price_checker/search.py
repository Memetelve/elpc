from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import quote_plus, urljoin, urlparse

import re

from selectolax.parser import HTMLParser

from .fetch import detect_source, fetch_html


@dataclass(frozen=True)
class SearchHit:
    name: str
    url: str
    price_cents: int | None
    currency: str | None
    source: str


async def search_products(store: str, query: str, limit: int = 10) -> list[SearchHit]:
    store_key = _normalize_store(store)
    if store_key == "x-kom":
        return await _search_xkom(query, limit)
    if store_key == "morele":
        return await _search_morele(query, limit)
    raise ValueError(f"Unsupported store: {store}")


def _normalize_store(store: str) -> str:
    s = store.lower().strip()
    if s in {"xkom", "x-kom", "x-kom.pl", "xkom.pl"}:
        return "x-kom"
    if s in {"morele", "morele.net"}:
        return "morele"
    if s.startswith("amazon"):
        return "amazon"
    return s


def _parse_price_fields(offer: dict) -> tuple[int | None, str | None]:
    price = offer.get("price") or offer.get("priceSpecification", {}).get("price")
    currency = offer.get("priceCurrency") or offer.get("priceSpecification", {}).get("priceCurrency")
    if price is None:
        return (None, currency)
    try:
        cents = int(round(float(str(price)) * 100))
    except Exception:
        return (None, currency)
    return (cents, currency)


_RATING_RE = re.compile(r"^\d+[\.,]\d+\s*\(\d+\)")


def _name_from_slug(url: str) -> str | None:
    try:
        path = urlparse(url).path
        m = re.search(r"/p/\d+-([^/]+)\.html", path)
        if not m:
            return None
        slug = m.group(1)
        slug = slug.replace("-", " ")
        slug = re.sub(r"\bpl\b|\bgb\b|\bgddr\b", lambda m: m.group(0).upper(), slug)
        return slug.strip()
    except Exception:
        return None


def _clean_hit_name(raw: str | None, url: str) -> str:
    if raw:
        name = raw.strip()
        if name and not _RATING_RE.match(name):
            return name
    derived = _name_from_slug(url)
    if derived:
        return derived
    return url


def _extract_hits_from_itemlist(obj: dict, base_url: str) -> list[SearchHit]:
    hits: list[SearchHit] = []
    items: Iterable = obj.get("itemListElement", []) if isinstance(obj.get("itemListElement"), Iterable) else []
    for elem in items:
        if isinstance(elem, dict) and "item" in elem:
            candidate = elem.get("item")
        else:
            candidate = elem
        if not isinstance(candidate, dict):
            continue
        url = candidate.get("url") or candidate.get("@id")
        name_raw = candidate.get("name") or ""
        offers = candidate.get("offers")
        price_cents = None
        currency = None
        if isinstance(offers, dict):
            price_cents, currency = _parse_price_fields(offers)
        elif isinstance(offers, list) and offers:
            price_cents, currency = _parse_price_fields(offers[0])

        if url:
            full_url = url if url.startswith("http") else urljoin(base_url, url)
            hits.append(
                SearchHit(
                    name=_clean_hit_name(name_raw, full_url),
                    url=full_url,
                    price_cents=price_cents,
                    currency=currency,
                    source=detect_source(full_url),
                )
            )
    return hits


def _extract_hits_from_html(html: str, base_url: str) -> list[SearchHit]:
    doc = HTMLParser(html)
    hits: list[SearchHit] = []

    # Prefer JSON-LD ItemList entries.
    for node in doc.css('script[type="application/ld+json"]'):
        raw = (node.text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            if obj.get("@type") == "ItemList" and obj.get("itemListElement"):
                hits.extend(_extract_hits_from_itemlist(obj, base_url))

    # Fallback: simple anchor parsing when JSON-LD is absent.
    if hits:
        return hits

    for a in doc.css("a"):
        href = a.attributes.get("href")
        if not href:
            continue
        full_url = href if href.startswith("http") else urljoin(base_url, href)
        src = detect_source(full_url)

        if src == "x-kom" and "/p/" not in href:
            continue
        if src == "morele" and not re.search(r"-\d+/?(?:[#?]|$)", full_url):
            continue

        title = (a.attributes.get("title") or a.text() or "").strip()
        hits.append(
            SearchHit(
                name=_clean_hit_name(title, full_url),
                url=full_url,
                price_cents=None,
                currency=None,
                source=src,
            )
        )
    return hits


async def _search_xkom(query: str, limit: int) -> list[SearchHit]:
    url = f"https://www.x-kom.pl/szukaj?q={quote_plus(query)}"
    res = await fetch_html(url, source="x-kom")
    hits = _extract_hits_from_html(res.text, base_url="https://www.x-kom.pl")
    hits = _filter_hits_by_query(hits, query)
    deduped: dict[str, SearchHit] = {}
    for hit in hits:
        if hit.url not in deduped:
            deduped[hit.url] = hit
        if len(deduped) >= limit:
            break
    return list(deduped.values())


async def _search_morele(query: str, limit: int) -> list[SearchHit]:
    base = "https://www.morele.net"
    urls = [
        f"{base}/wyszukiwarka/?search={quote_plus(query)}",
        f"{base}/kategoria/karty-graficzne-12/?q={quote_plus(query)}",
    ]
    hits: list[SearchHit] = []
    anchor_backup: list[SearchHit] = []
    for idx, url in enumerate(urls):
        prefer_browser = idx == 0
        res = await fetch_html(url, source="morele", prefer_browser=prefer_browser)
        hits = _extract_hits_from_morele_cards(res.text, base_url=base)
        if hits:
            break
        fallback = _extract_hits_from_html(res.text, base_url=base)
        if fallback and not anchor_backup:
            anchor_backup = fallback
    if not hits:
        hits = anchor_backup
    hits = _filter_hits_by_query(hits, query)
    deduped: dict[str, SearchHit] = {}
    for hit in hits:
        if hit.url not in deduped:
            deduped[hit.url] = hit
        if len(deduped) >= limit:
            break
    return list(deduped.values())


def _parse_price(text: str) -> tuple[int | None, str | None]:
    m = re.search(r"(\d[\d\s]*[\.,]\d{2})", text)
    if not m:
        return (None, None)
    raw = m.group(1).replace(" ", "").replace(",", ".")
    try:
        cents = int(round(float(raw) * 100))
        return (cents, "PLN")
    except Exception:
        return (None, None)


def _extract_hits_from_morele_cards(html: str, base_url: str) -> list[SearchHit]:
    doc = HTMLParser(html)
    hits: list[SearchHit] = []
    for card in doc.css("[data-product-id]"):
        name = card.attributes.get("data-product-name") or ""
        link_node = card.css_first("[data-link-href-param]") or card.css_first("a[href]")
        href = link_node.attributes.get("data-link-href-param") if link_node else ""
        if not href and link_node and link_node.attributes.get("href"):
            href = link_node.attributes.get("href")
        full_url = href if href.startswith("http") else urljoin(base_url, href)
        if not href or full_url == base_url:
            continue
        price_text = card.attributes.get("data-product-price") or ""
        if not price_text:
            price_node = card.css_first("[data-product-price]") or card.css_first(".price-new")
            price_text = price_node.text().strip() if price_node else ""
        price_cents, currency = _parse_price(price_text)
        hits.append(
            SearchHit(
                name=_clean_hit_name(name, full_url),
                url=full_url,
                price_cents=price_cents,
                currency=currency,
                source="morele",
            )
        )
    return hits


def _filter_hits_by_query(hits: list[SearchHit], query: str) -> list[SearchHit]:
    tokens = [t.lower() for t in query.split() if t.strip()]
    if not tokens:
        return hits
    out: list[SearchHit] = []
    for h in hits:
        haystack = f"{h.name} {h.url}".lower()
        if all(tok in haystack for tok in tokens):
            out.append(h)
    return out
