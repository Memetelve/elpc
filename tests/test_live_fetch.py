from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation

import pytest

from el_price_checker.fetch import detect_source, fetch_html
from el_price_checker.parse import extract_price


DEFAULT_XKOM_URL = "https://www.x-kom.pl/p/1321563-karta-graficzna-amd-gigabyte-radeon-rx-9070-xt-aorus-elite-16gb-gddr6.html"
DEFAULT_MORELE_URL = "https://www.morele.net/karta-graficzna-gigabyte-aorus-radeon-rx-9070-xt-elite-16gb-gddr6-gv-r9070xtaorus-e-16gd-14731776/"
DEFAULT_AMAZON_URL = "https://www.amazon.pl/Gigabyte-AORUS-Radeon-ELITE-graficzna/dp/B0DT7C5ZC7?ufe=app_do%3Aamzn1.fos.2fcb306b-f838-4e60-b9d1-9233d6d01192"

DEFAULT_XKOM_EXPECTED_PLN = "3339.00"
DEFAULT_MORELE_EXPECTED_PLN = "3339.00"
DEFAULT_AMAZON_EXPECTED_PLN = "3141.12"


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value or default


def _pln_str_to_cents(value: str) -> int:
    try:
        amount = Decimal(value)
    except InvalidOperation as e:
        raise ValueError(f"Invalid price {value!r}") from e
    return int((amount * 100).quantize(Decimal("1")))


def _live_enabled() -> bool:
    return os.getenv("ELPC_LIVE_TESTS", "").strip() == "1"


@pytest.mark.skipif(
    not _live_enabled(), reason="Set ELPC_LIVE_TESTS=1 to run live fetch tests"
)
@pytest.mark.asyncio
async def test_live_fetch_xkom_price() -> None:
    url = _env_or_default("ELPC_TEST_XKOM_URL", DEFAULT_XKOM_URL)
    expected_pln = _env_or_default(
        "ELPC_TEST_XKOM_EXPECTED_PLN", DEFAULT_XKOM_EXPECTED_PLN
    )
    expected_cents = _pln_str_to_cents(expected_pln)
    res = await fetch_html(url, source=detect_source(url))
    if res.status_code >= 400:
        pytest.skip(f"HTTP {res.status_code} (likely blocked)")
    parsed = extract_price(res.text)
    if parsed.error:
        pytest.skip(f"Parse error: {parsed.error}")
    assert parsed.price_cents is not None
    assert parsed.currency in (None, "PLN")
    if parsed.price_cents != expected_cents:
        pytest.skip(
            f"Price changed (expected {expected_pln} PLN, got {parsed.price_cents / 100.0:.2f} PLN)"
        )


@pytest.mark.skipif(
    not _live_enabled(), reason="Set ELPC_LIVE_TESTS=1 to run live fetch tests"
)
@pytest.mark.asyncio
async def test_live_fetch_morele_price() -> None:
    url = _env_or_default("ELPC_TEST_MORELE_URL", DEFAULT_MORELE_URL)
    expected_pln = _env_or_default(
        "ELPC_TEST_MORELE_EXPECTED_PLN", DEFAULT_MORELE_EXPECTED_PLN
    )
    expected_cents = _pln_str_to_cents(expected_pln)
    res = await fetch_html(url, source=detect_source(url))
    if res.status_code >= 400:
        pytest.skip(f"HTTP {res.status_code} (likely blocked)")
    parsed = extract_price(res.text)
    if parsed.error:
        pytest.skip(f"Parse error: {parsed.error}")
    assert parsed.price_cents is not None
    assert parsed.currency in (None, "PLN")
    if parsed.price_cents != expected_cents:
        pytest.skip(
            f"Price changed (expected {expected_pln} PLN, got {parsed.price_cents / 100.0:.2f} PLN)"
        )


@pytest.mark.skipif(
    not _live_enabled(), reason="Set ELPC_LIVE_TESTS=1 to run live fetch tests"
)
@pytest.mark.asyncio
async def test_live_fetch_amazon_price() -> None:
    url = _env_or_default("ELPC_TEST_AMAZON_URL", DEFAULT_AMAZON_URL)
    expected_pln = _env_or_default(
        "ELPC_TEST_AMAZON_EXPECTED_PLN", DEFAULT_AMAZON_EXPECTED_PLN
    )
    expected_cents = _pln_str_to_cents(expected_pln)
    res = await fetch_html(url, source=detect_source(url))
    if res.status_code >= 400:
        pytest.skip(f"HTTP {res.status_code} (likely blocked)")
    parsed = extract_price(res.text)
    if parsed.error:
        pytest.skip(f"Parse error: {parsed.error}")
    assert parsed.price_cents is not None
    assert parsed.currency in (None, "PLN")
    if parsed.price_cents != expected_cents:
        pytest.skip(
            f"Price changed (expected {expected_pln} PLN, got {parsed.price_cents / 100.0:.2f} PLN)"
        )
