from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx


@dataclass(frozen=True)
class FetchResult:
    url: str
    final_url: str
    status_code: int
    text: str


def detect_source(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if host.endswith("x-kom.pl") or host.endswith("www.x-kom.pl"):
        return "x-kom"
    if host.endswith("morele.net") or host.endswith("www.morele.net"):
        return "morele"
    if "amazon." in host:
        return "amazon"
    return host or "unknown"


def _normalize_cookie(raw: str) -> str:
    s = raw.strip()
    # Allow pasting JSON from DevTools export
    try:
        import json

        data = json.loads(s)
        if isinstance(data, dict):
            if "Request Cookies" in data and isinstance(data["Request Cookies"], dict):
                pairs = data["Request Cookies"]
                return "; ".join(f"{k}={v}" for k, v in pairs.items())
            if all(isinstance(v, str) for v in data.values()):
                return "; ".join(f"{k}={v}" for k, v in data.items())
    except Exception:
        pass

    for prefix in ["cookie:", "Cookie:"]:
        if s.lower().startswith(prefix.rstrip(":").lower()):
            s = s[len(prefix):].strip()

    s = s.replace("\r", " ").replace("\n", "; ")
    return s


def _looks_like_block(text: str) -> bool:
    lower = text.lower()
    return any(
        token in lower
        for token in [
            "captcha",
            "robot check",
            "access denied",
            "forbidden",
            "cloudflare",
        ]
    )


def _headers_for_source(source: str) -> dict[str, str]:
    # Keep it simple: reasonable browser UA and language.
    # Some sites block aggressively; we store errors when blocked.
    base = {
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "pl-PL,pl;q=0.9,en-US;q=0.7,en;q=0.6",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "upgrade-insecure-requests": "1",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "sec-ch-ua": '"Chromium";v="126", "Not=A?Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    }
    if source in {"x-kom", "morele"}:
        base["referer"] = "https://www.x-kom.pl/"
        base["sec-fetch-site"] = "same-origin"
    if source == "amazon":
        base["accept-language"] = "pl-PL,pl;q=0.9,en-US;q=0.7,en;q=0.6"

    # Optional cookie injection to bypass WAFs. Copy from browser DevTools.
    cookie_env = os.getenv(
        {
            "x-kom": "ELPC_COOKIE_XKOM",
            "morele": "ELPC_COOKIE_MORELE",
            "amazon": "ELPC_COOKIE_AMAZON",
        }.get(source, "ELPC_COOKIE"),
    ) or os.getenv("ELPC_COOKIE")
    if cookie_env:
        normalized = _normalize_cookie(cookie_env)
        if normalized:
            base["cookie"] = normalized
    return base


async def fetch_html(
    url: str,
    source: str | None = None,
    timeout_s: float = 20.0,
    prefer_browser: bool = False,
) -> FetchResult:
    src = source or detect_source(url)

    if prefer_browser:
        fallback = await _try_playwright(url, headers=_headers_for_source(src))
        if fallback:
            return fallback

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout_s),
            headers=_headers_for_source(src),
            http2=False,
        ) as client:
            resp = await client.get(url)
            if resp.status_code == 403 or (resp.status_code < 500 and _looks_like_block(resp.text)):
                fallback = await _try_playwright(url, headers=_headers_for_source(src))
                if fallback:
                    return fallback

            return FetchResult(
                url=url,
                final_url=str(resp.url),
                status_code=resp.status_code,
                text=resp.text,
            )
    except Exception:
        fallback = await _try_playwright(url, headers=_headers_for_source(src))
        if fallback:
            return fallback
        raise


_PLAYWRIGHT_INSTALL_LOCK = asyncio.Lock()
_PLAYWRIGHT_READY = False


async def _ensure_playwright() -> bool:
    global _PLAYWRIGHT_READY
    if _PLAYWRIGHT_READY:
        return True
    async with _PLAYWRIGHT_INSTALL_LOCK:
        if _PLAYWRIGHT_READY:
            return True
        try:
            import playwright  # noqa: F401
        except Exception:
            return False

        # Try to launch quickly; if browser missing, install chromium once.
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                await browser.close()
            _PLAYWRIGHT_READY = True
            return True
        except Exception:
            # attempt install chromium quietly
            try:
                proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "playwright",
                    "install",
                    "chromium",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await proc.communicate()
                if proc.returncode != 0:
                    return False
                _PLAYWRIGHT_READY = True
                return True
            except Exception:
                return False


async def _try_playwright(url: str, headers: dict[str, str]) -> FetchResult | None:
    ok = await _ensure_playwright()
    if not ok:
        return None
    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                extra_http_headers=headers,
                locale="pl-PL",
            )
            page = await context.new_page()
            response = await page.goto(url, wait_until="networkidle", timeout=30000)
            content = await page.content()
            final_url = page.url
            status = response.status if response else 0
            await browser.close()
            return FetchResult(url=url, final_url=final_url, status_code=status, text=content)
    except Exception:
        return None
