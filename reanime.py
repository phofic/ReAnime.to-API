#!/usr/bin/env python3
"""
ReAnime.to → JSON API (Vercel/Railway friendly).

Scrapes the public reanime.to API (v1) and fully decrypts flixcloud.cc HLS
streams without a browser. Works as a drop-in Consumet-style source.

Why proxies?
  reanime.to sits behind Cloudflare and blocks datacenter IP ranges
  (Vercel/AWS/GCP...) with HTTP 403. We therefore:
    1. try the target DIRECT first (fast when the egress IP isn't blocked)
    2. if blocked/timeout → race ALL relay proxies CONCURRENTLY under a hard
       deadline and take the first valid JSON response
  Broken relays are remembered (health cache) so they cost ~0ms next time.

Env vars
  PROXY_MODE      auto | always | off   (default auto)
  DIRECT_TIMEOUT  seconds for the direct attempt   (default 8)
  RELAY_TIMEOUT   seconds per relay                (default 10)
  RELAY_BUDGET    hard deadline for the relay race (default 8)
  RELAY_COOLDOWN  seconds a failing relay is skipped (default 60)
  PROXY_SERVICES  "name|template;name2|template2"  (override relay list)
  PROXY_URL       optional premium HTTP(S) proxy (Bright Data / Oxylabs / ...)
                  ALL upstream traffic (direct + relays) exits through it.
  CACHE_ENABLED   0/1  in-memory TTL cache on public endpoints (default 1)
"""

import asyncio
import os
import re
import random
import time
from contextlib import asynccontextmanager
from urllib.parse import quote, urlencode
from typing import Any, Callable, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

BASE = "https://reanime.to"
FLIX = "https://flixcloud.cc"

# ---------------------------------------------------------------------------
# Browser fingerprint rotation
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Cache-Control": "max-age=0",
    # NOTE: do NOT set Accept-Encoding manually — httpx only auto-decompresses
    # gzip/br responses when it negotiates the header itself. Setting it
    # manually returns RAW compressed bytes and breaks r.json().
}

# ---------------------------------------------------------------------------
# Configuration (env driven)
# ---------------------------------------------------------------------------
PROXY_MODE = os.getenv("PROXY_MODE", "auto").strip().lower()
DIRECT_TIMEOUT = float(os.getenv("DIRECT_TIMEOUT", "8"))
RELAY_TIMEOUT = float(os.getenv("RELAY_TIMEOUT", "10"))
RELAY_BUDGET = float(os.getenv("RELAY_BUDGET", "8"))
RELAY_COOLDOWN = float(os.getenv("RELAY_COOLDOWN", "60"))
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "1").strip() not in ("0", "false", "no")
_PREMIUM_PROXY_URL = os.getenv("PROXY_URL", "").strip() or None

# Free CORS/relay proxies. Each entry is (name, url_template, json_headers,
# html_headers) where {url} in the template is the fully percent-encoded
# target. json_headers are sent for JSON API requests, html_headers for HTML
# page fetches (e.g. the flixcloud embed) — Jina needs X-Return-Format to
# switch between raw JSON and raw HTML. Other relays use default headers.
# Override with PROXY_SERVICES env var: "name|template;name2|template2"
DEFAULT_RELAY_SERVICES = [
    # Jina Reader — fast (~1-4s) and currently the most reliable free relay
    ("jina",       "https://r.jina.ai/{url}", {"X-Return-Format": "text"}, {"X-Return-Format": "html"}),
    ("allorigins", "https://api.allorigins.win/raw?url={url}", None, None),
    ("corsproxy",  "https://corsproxy.io/?url={url}", None, None),
    ("codetabs",   "https://api.codetabs.com/v1/proxy?quest={url}", None, None),
    ("thingproxy", "https://thingproxy.freeboard.io/fetch/{url}", None, None),
    ("corslol",    "https://api.cors.lol/?url={url}", None, None),
]


def _load_relay_services() -> list:
    raw = os.getenv("PROXY_SERVICES", "").strip()
    if raw:
        services = []
        for part in raw.split(";"):
            if "|" in part:
                name, template = part.split("|", 1)
                services.append((name.strip(), template.strip(), None, None))
        if services:
            return services
    return list(DEFAULT_RELAY_SERVICES)


RELAY_SERVICES = _load_relay_services()

# relay health: name -> timestamp until which it is skipped
_relay_dead_until: dict[str, float] = {}
_relay_lock = asyncio.Lock()


_RELAY_INFO = {n: (t, hj, hh) for n, t, hj, hh in RELAY_SERVICES}


def _relay_info(name: str) -> Optional[tuple]:
    return _RELAY_INFO.get(name)


def _relay_proxified_url(name: str, url: str) -> Optional[str]:
    info = _relay_info(name)
    if not info:
        return None
    return info[0].format(url=quote(url, safe=""))


def _is_relay_dead(name: str) -> bool:
    return _relay_dead_until.get(name, 0.0) > time.monotonic()


async def _mark_relay_dead(name: str, reason: str) -> None:
    async with _relay_lock:
        _relay_dead_until[name] = time.monotonic() + RELAY_COOLDOWN


async def _mark_relay_alive(name: str) -> None:
    async with _relay_lock:
        _relay_dead_until.pop(name, None)


def relay_health() -> dict:
    now = time.monotonic()
    return {
        name: {"dead": _relay_dead_until.get(name, 0.0) > now, "template": tpl}
        for name, tpl, _hj, _hh in RELAY_SERVICES
    }


# ---------------------------------------------------------------------------
# In-memory TTL cache (per-warm-instance; great for search-as-you-type)
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, Any]] = {}
_cache_lock = asyncio.Lock()


async def _cache_get(key: str) -> Optional[Any]:
    async with _cache_lock:
        hit = _cache.get(key)
        if hit and hit[0] > time.monotonic():
            return hit[1]
        if hit:
            _cache.pop(key, None)
    return None


async def _cache_set(key: str, value: Any, ttl: float) -> None:
    async with _cache_lock:
        _cache[key] = (time.monotonic() + ttl, value)


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------
_client: Optional[httpx.AsyncClient] = None


@asynccontextmanager
async def lifespan(_app):
    global _client
    client_kwargs = dict(
        http2=True,
        timeout=httpx.Timeout(max(RELAY_TIMEOUT, DIRECT_TIMEOUT) + 5.0),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        follow_redirects=True,
    )
    if _PREMIUM_PROXY_URL:
        # Optional premium/rotating proxy — all traffic exits through it
        client_kwargs["proxy"] = _PREMIUM_PROXY_URL
    _client = httpx.AsyncClient(**client_kwargs)
    yield
    await _client.aclose()


app = FastAPI(title="ReAnime Scraper", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _browser_headers(base: str, accept_html: bool = False) -> dict:
    accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" if accept_html \
        else "application/json, text/plain, */*"
    return {
        **BASE_HEADERS,
        "Accept": accept,
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": random.choice([
            f"{base}/", f"{base}/home", f"{base}/search",
            "https://www.google.com/", "https://www.bing.com/",
        ]),
        "Origin": base,
    }


# ---------------------------------------------------------------------------
# Fetch core: direct-first, then concurrent relay race
# ---------------------------------------------------------------------------
def _looks_like_valid_json(r: httpx.Response) -> bool:
    if not r.is_success:
        return False
    ctype = (r.headers.get("content-type") or "").lower()
    if ctype and "json" not in ctype and "text" not in ctype:
        return False
    try:
        r.json()
        return True
    except Exception:
        return False


async def _fetch_direct(url: str, *, params: dict = None, accept_html: bool = False,
                        timeout: float = None) -> httpx.Response:
    headers = _browser_headers(BASE if url.startswith(BASE) else FLIX, accept_html=accept_html)
    if _PREMIUM_PROXY_URL:
        headers.pop("Origin", None)  # premium proxies choke on mismatched Origin
    return await _client.get(url, params=params, headers=headers,
                             timeout=timeout or DIRECT_TIMEOUT)


async def _fetch_relay(name: str, url: str, *, params: dict = None,
                       accept_html: bool = False) -> httpx.Response:
    """Fetch through a single relay proxy. Raises on network failure; the
    caller validates the response body."""
    if params:
        query = urlencode([(k, v) for k, v in params.items() if v is not None])
        if query:
            url = f"{url}?{query}" if "?" not in url else f"{url}&{query}"
    relay_url = _relay_proxified_url(name, url)
    if not relay_url:
        raise HTTPException(500, detail=f"unknown relay '{name}'")
    info = _relay_info(name)
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
    }
    # relay-specific headers (Jina: X-Return-Format text/json vs raw html)
    extra = (info[2] if info else None) if not accept_html else (info[1] if info else None)
    if extra:
        headers.update(extra)
    if _PREMIUM_PROXY_URL:
        headers.pop("Origin", None)
    return await _client.get(relay_url, headers=headers, timeout=RELAY_TIMEOUT)


async def _relay_task(name: str, url: str, *, params: dict, accept_html: bool,
                      validator: Callable[[httpx.Response], bool]) -> tuple[Optional[httpx.Response], str]:
    """Returns (response, name) if valid, (None, name) otherwise."""
    try:
        r = await _fetch_relay(name, url, params=params, accept_html=accept_html)
        if validator(r):
            await _mark_relay_alive(name)
            return r, name
        await _mark_relay_dead(name, f"invalid response HTTP {r.status_code}")
        return None, name
    except (httpx.HTTPError, asyncio.TimeoutError) as e:
        await _mark_relay_dead(name, str(e))
        return None, name


async def _relay_race(url: str, *, params: dict, accept_html: bool,
                      validator: Callable[[httpx.Response], bool],
                      deadline: float) -> tuple[Optional[httpx.Response], Optional[str], list]:
    """
    Race relays concurrently until `deadline`, taking the first valid response.
    When a round completes with zero valid responses we immediately launch a
    fresh round (forcing every relay, ignoring the health cache) — free relays
    like allorigins are flaky and a re-roll often lands. Returns
    (response, via_name, errors).
    """
    errors: list[str] = []
    exhausted: set[str] = set()  # relays that answered definitively this request

    while time.monotonic() < deadline:
        candidate = [n for n, _t, _hj, _hh in RELAY_SERVICES if n not in exhausted]
        if not candidate:
            break
        alive = [n for n in candidate if not _is_relay_dead(n)]
        if not alive:
            # every relay is in cooldown → force one final attempt so a
            # bursty client can't turn the health cache into instant 502s
            alive = candidate
        tasks = {
            asyncio.create_task(_relay_task(n, url, params=params, accept_html=accept_html,
                                            validator=validator)): n
            for n in alive
        }
        done, pending = await asyncio.wait(
            tasks, timeout=max(0.05, deadline - time.monotonic()),
            return_when=asyncio.FIRST_COMPLETED,
        )
        reroll: set[str] = set()
        for task in done:
            resp, name = task.result()
            if resp is not None:
                await _cancel_tasks(pending)
                return resp, name, errors
            errors.append(f"{name}: invalid")
            exhausted.add(name)  # definitive failure → don't re-roll it
        for t in pending:
            reroll.add(tasks[t])  # slow/timeout → worth one more round
        await _cancel_tasks(pending)
        if not done:
            break  # deadline hit while waiting
        if not reroll:
            break  # every relay answered definitively and none worked
    return None, None, errors


async def _cancel_tasks(tasks: set) -> None:
    """Cancel and await the given tasks so they don't linger in the loop."""
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _fetch_smart(url: str, *, params: dict = None, accept_html: bool = False,
                       budget: float = None) -> tuple[httpx.Response, str]:
    """
    Fetch `url` reliably:
      1. direct attempts (rotated headers), unless PROXY_MODE=always
      2. concurrent relay race under a hard deadline
    Returns (response, via) where via is "direct" or the relay name.
    Raises HTTPException(502) when every path fails.
    """
    # ---- direct (2 attempts, rotating fingerprint) ----
    if PROXY_MODE != "always":
        for attempt in range(2):
            try:
                r = await _fetch_direct(url, params=params, accept_html=accept_html,
                                        timeout=DIRECT_TIMEOUT if attempt == 0 else DIRECT_TIMEOUT + 4)
                if r.status_code == 404:
                    raise HTTPException(404, detail="Not found")
                if r.status_code == 401:
                    raise HTTPException(401, detail="Unauthorized – reanime.to now requires a login for this endpoint")
                if _looks_like_valid_json(r) or (accept_html and r.is_success and r.content):
                    return r, "direct"
                # 403/429/5xx/HTML-where-JSON-expected → retry direct once, then relays
                if attempt == 0:
                    await asyncio.sleep(random.uniform(0.3, 0.8))
            except (httpx.HTTPError, asyncio.TimeoutError):
                if attempt == 0:
                    await asyncio.sleep(random.uniform(0.3, 0.8))

    # ---- concurrent relay race ----
    if PROXY_MODE == "off":
        raise HTTPException(502, detail="Upstream request failed and PROXY_MODE=off")

    validator = (lambda r: r.is_success and bool(r.content)) if accept_html else _looks_like_valid_json
    deadline = time.monotonic() + (budget or RELAY_BUDGET)
    resp, via, _errors = await _relay_race(url, params=params, accept_html=accept_html,
                                           validator=validator, deadline=deadline)
    if resp is not None:
        return resp, via
    raise HTTPException(502, detail="All upstream paths failed (direct + relays). "
                                    "Set PROXY_URL (premium proxy), deploy to Railway, or raise RELAY_BUDGET.")


async def _get_json(url: str, params: dict = None, *, accept_html: bool = False,
                    budget: float = None) -> Any:
    r, via = await _fetch_smart(url, params=params, accept_html=accept_html, budget=budget)
    try:
        return r.json()
    except Exception:
        raise HTTPException(502, detail=f"Relay '{via}' returned non-JSON body")


async def _get_json_cached(key: str, ttl: float, url: str, params: dict = None) -> Any:
    """TTL-cached variant of _get_json (only when CACHE_ENABLED)."""
    if not CACHE_ENABLED:
        return await _get_json(url, params)
    hit = await _cache_get(key)
    if hit is not None:
        return hit
    data = await _get_json(url, params)
    await _cache_set(key, data, ttl)
    return data


# ---------------------------------------------------------------------------
# Decryption (pure-Python WASM pipeline — no Node.js required)
# ---------------------------------------------------------------------------
from decrypt import DecryptError, decrypt_stream, parse_embed  # noqa: E402


async def get_stream_url(access_id: str, v: int = 2) -> dict:
    embed_url = f"{FLIX}/e/{access_id}?v={v}"
    # Embed page is HTML → accept_html mode; falls back to relays if blocked.
    # Use a tighter budget than general API calls so the full decrypt pipeline
    # fits inside serverless function time limits.
    r, _via = await _fetch_smart(embed_url, accept_html=True,
                                 budget=min(RELAY_BUDGET, 6.0))
    if not r.content:
        raise HTTPException(502, detail="Empty embed page")

    try:
        parsed = parse_embed(r.content.decode("utf-8", errors="replace"))
    except DecryptError as e:
        raise HTTPException(502, detail=f"Embed parse failed: {e}")

    # One-time token API — flixcloud also blocks datacenter IPs, so it goes
    # through the same smart fetch chain (direct or relays).
    token_url = f"{FLIX}/api/m3u8/{parsed['token']}"
    try:
        tok_r, _tv = await _fetch_smart(token_url)
        token_data = tok_r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, detail=f"Token API fetch failed: {e}")

    try:
        return decrypt_stream(parsed, token_data)
    except DecryptError as e:
        raise HTTPException(502, detail=f"Decrypt failed: {e}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _anilist_from_anime(anime: dict) -> Optional[int]:
    if not anime:
        return None
    if anime.get("anilist_id"):
        return int(anime["anilist_id"])
    for key in ("extra_large", "large", "medium"):
        url = (anime.get("cover_image") or {}).get(key, "")
        m = re.search(r"/bx(\d+)-", url)
        if m:
            return int(m.group(1))
    return None


def _unwrap_episodes(data: Any) -> list:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data") or data.get("episodes") or []
    return []


_SERVER_ORDER = {"HD-2": 0, "HD-1": 1}


def _sort_servers(servers: list) -> list:
    return sorted(servers, key=lambda s: _SERVER_ORDER.get(s.get("serverName", ""), 9))


async def _servers(slug: str, ep: int, anilist_id: Optional[int] = None) -> dict:
    # anime info (public v1) → derive anilist_id if not provided
    anime = {}
    aid = anilist_id
    try:
        anime = await _get_json_cached(f"anime:{slug}", 3600, f"{BASE}/api/v1/anime/{slug}")
        aid = aid or _anilist_from_anime(anime)
    except HTTPException:
        pass

    flix: dict = {}
    if aid:
        try:
            flix = await _get_json_cached(
                f"flix:{aid}:{ep}", 300, f"{BASE}/api/flix/{aid}/{ep}")
        except HTTPException:
            pass

    links = []
    if flix.get("success") and flix.get("servers"):
        links = list(flix["servers"])

    return {
        "sub":         _sort_servers([s for s in links if s.get("dataType") in ("sub", "s-sub")]),
        "dub":         _sort_servers([s for s in links if s.get("dataType") in ("dub", "s-dub")]),
        "anime":       anime or None,
        "current":     None,  # requires auth (reanime.to /api/v1/watch)
        "duration":    anime.get("duration") if isinstance(anime, dict) else None,
        "intro_start": None,  # requires auth
        "intro_end":   None,
        "outro_start": None,
        "outro_end":   None,
        "anilist_id":  aid,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {
        "status": "ok",
        "mode": PROXY_MODE,
        "proxies": relay_health(),
        "endpoints": {
            "search":          "GET /search?q=...&limit=20",
            "home":            "GET /home?limit=20",
            "new":             "GET /new?limit=20",
            "upcoming":        "GET /upcoming?limit=20",
            "top":             "GET /top?period=week&limit=20",
            "schedule":        "GET /schedule",
            "anime":           "GET /anime/{slug}",
            "info":            "GET /info/{slug}",
            "episodes":        "GET /episodes/{slug}",
            "servers":         "GET /servers/{slug}/{episode}[?anilist_id=...]",
            "stream":          "GET /stream/{access_id}[?v=2]",
            "stream_link":     "GET /stream/from-link?link={flixcloud_url}",
            "thumbnails":      "GET /thumbnails/{anilist_id}",
            "recommendations": "GET /recommendations/{slug}",
            "proxy":           "GET /proxy?url={reanime.to_or_flixcloud_url}",
        },
    }


@app.get("/search")
async def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    return await _get_json_cached(f"search:{q}:{limit}:{offset}", 60,
                                  f"{BASE}/api/v1/search",
                                  {"q": q, "limit": limit, "offset": offset})


@app.get("/home")
async def home(limit: int = Query(20, ge=1, le=100)):
    latest, top = await asyncio.gather(
        _get_json_cached(f"home:latest:{limit}", 120, f"{BASE}/api/v1/home/latest-aired", {"limit": limit}),
        _get_json_cached(f"home:top:{limit}", 300, f"{BASE}/api/v1/top/anime", {"period": "week", "limit": limit}),
    )
    return {"latest_aired": latest, "top_weekly": top}


@app.get("/new")
async def new_on_site(limit: int = Query(20, ge=1, le=100)):
    return await _get_json_cached(f"home:new:{limit}", 300,
                                  f"{BASE}/api/v1/home/new-on-site", {"limit": limit})


@app.get("/upcoming")
async def upcoming(limit: int = Query(20, ge=1, le=100)):
    return await _get_json_cached(f"home:upcoming:{limit}", 300,
                                  f"{BASE}/api/v1/home/upcoming", {"limit": limit})


@app.get("/top")
async def top(
    period: str = Query("week", pattern="^(day|week|month)$"),
    limit: int = Query(20, ge=1, le=100),
):
    return await _get_json_cached(f"top:{period}:{limit}", 300,
                                  f"{BASE}/api/v1/top/anime",
                                  {"period": period, "limit": limit})


@app.get("/schedule")
async def schedule():
    return await _get_json_cached("schedule", 600, f"{BASE}/api/v1/schedule")


@app.get("/anime/{slug}")
async def anime_info_raw(slug: str):
    return await _get_json_cached(f"anime:{slug}", 3600, f"{BASE}/api/v1/anime/{slug}")


@app.get("/info/{slug}")
async def anime_info(slug: str):
    anime, eps_data = await asyncio.gather(
        _get_json_cached(f"anime:{slug}", 3600, f"{BASE}/api/v1/anime/{slug}"),
        _get_json_cached(f"episodes:{slug}", 3600, f"{BASE}/api/v1/anime/{slug}/episodes"),
    )
    return {**anime, "episodes": _unwrap_episodes(eps_data),
            "anilist_id": _anilist_from_anime(anime)}


@app.get("/episodes/{slug}")
async def episodes(slug: str):
    data = await _get_json_cached(f"episodes:{slug}", 3600,
                                  f"{BASE}/api/v1/anime/{slug}/episodes")
    return _unwrap_episodes(data)


@app.get("/servers/{slug}/{episode}")
async def servers(slug: str, episode: int, anilist_id: Optional[int] = Query(None)):
    return await _servers(slug, episode, anilist_id)


@app.get("/stream/from-link")
async def stream_from_link(link: str = Query(...)):
    m = re.search(r"/e/([^?#\s]+)\?v=(\d+)", link)
    if not m:
        raise HTTPException(400, detail="Expected URL: https://flixcloud.cc/e/{id}?v={1|2}")
    return await get_stream_url(m.group(1), int(m.group(2)))


@app.get("/stream/{access_id}")
async def stream(access_id: str, v: int = Query(2, ge=1, le=2)):
    return await get_stream_url(access_id, v)


@app.get("/thumbnails/{anilist_id}")
async def thumbnails(anilist_id: int):
    return await _get_json_cached(f"thumbnails:{anilist_id}", 3600,
                                  f"{BASE}/api/thumbnails/{anilist_id}")


@app.get("/recommendations/{slug}")
async def recommendations(slug: str):
    return await _get_json_cached(f"rec:{slug}", 600,
                                  f"{BASE}/api/v1/anime/{slug}/recommendations")


@app.get("/proxy")
async def proxy_passthrough(url: str = Query(..., description="Full reanime.to / flixcloud.cc URL to fetch through the smart fetch layer")):
    if not (url.startswith(BASE) or url.startswith(FLIX)):
        raise HTTPException(400, detail="Only reanime.to and flixcloud.cc URLs are allowed")
    accept_html = not url.startswith(f"{BASE}/api")
    r, via = await _fetch_smart(url, accept_html=accept_html)
    try:
        payload = r.json()
    except Exception:
        payload = r.text[:2000]
    return {
        "proxifiedSource": [_relay_proxified_url(n, url) for n, _t, _hj, _hh in RELAY_SERVICES],
        "via": via,
        "data": payload,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("reanime:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), workers=1, reload=False)
