#!/usr/bin/env python3
import asyncio
import json
import os
import re
import random
from contextlib import asynccontextmanager
from urllib.parse import quote, urlencode
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

BASE = "https://reanime.to"
FLIX = "https://flixcloud.cc"

# ---- NEW: list of realistic User-Agents ----
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

# ---- Base headers (without User-Agent, we'll set it per request) ----
BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Cache-Control": "max-age=0",
    "Origin": BASE,
}

# ---------------------------------------------------------------------------
# PROXY CHAIN (architecture mirroring Proxify-Streams' ProxyGenerator)
# ---------------------------------------------------------------------------
# reanime.to (and flixcloud.cc) sit behind Cloudflare-style bot protection and
# block datacenter IP ranges (Vercel, Railway, ...) with HTTP 403 — rotating
# User-Agents can't fix an IP-based block. The fix is to route upstream
# requests through relay proxies: the upstream site then sees the relay's IP
# instead of ours, exactly like Proxify-Streams does for HLS streams.
#
# Each relay is (name, url_template) where {url} is the fully percent-encoded
# target. Override with PROXY_SERVICES="name|template;name2|template2".
DEFAULT_PROXY_SERVICES = [
    ("allorigins", "https://api.allorigins.win/raw?url={url}"),
    ("corsproxy",  "https://corsproxy.io/?url={url}"),
    ("codetabs",   "https://api.codetabs.com/v1/proxy?quest={url}"),
]

# auto  = try direct first, fall back to relays when blocked (default)
# always = skip direct, always go through the relay chain
# off   = original behaviour (never proxy)
PROXY_MODE = os.getenv("PROXY_MODE", "auto").strip().lower()

# Optional premium/private HTTP(S) proxy (e.g. Bright Data, Oxylabs, ...).
# When set, ALL upstream traffic (direct + relay) exits through this proxy,
# giving you a rotating-residential escape hatch if the free relays also 403.
_PREMIUM_PROXY_URL = os.getenv("PROXY_URL", "").strip() or None


class ReanimeProxy:
    """
    Proxy chain — the ReAnime equivalent of Proxify-Streams' ProxyGenerator.
    Builds relay URLs that fetch a target on our behalf from another IP.
    """

    def __init__(self, services: Optional[list] = None):
        self.services = services if services is not None else _load_proxy_services()

    def proxified_url(self, name: str, url: str) -> Optional[str]:
        template = dict(self.services).get(name)
        if not template:
            return None
        return template.format(url=quote(url, safe=""))

    def all_proxified(self, url: str) -> list:
        return [self.proxified_url(name, url) for name, _ in self.services]


def _load_proxy_services() -> list:
    raw = os.getenv("PROXY_SERVICES", "").strip()
    if raw:
        services = []
        for part in raw.split(";"):
            if "|" in part:
                name, template = part.split("|", 1)
                services.append((name.strip(), template.strip()))
        if services:
            return services
    return list(DEFAULT_PROXY_SERVICES)


proxy_chain = ReanimeProxy()


async def _fetch_via_proxy(url: str, *, params: dict = None, as_json: bool = True,
                           headers: dict = None) -> tuple:
    """
    Fetch a URL through the relay chain (shuffled for load spreading).
    Returns (data, relay_name) — parsed JSON if as_json else an httpx.Response.
    Raises HTTPException(502) if every relay fails.
    """
    if params:
        query = urlencode([(k, v) for k, v in params.items() if v is not None])
        if query:
            url = f"{url}?{query}" if "?" not in url else f"{url}&{query}"

    services = proxy_chain.services[:]
    random.shuffle(services)
    last_err = f"upstream blocked ({PROXY_MODE} mode)"

    for name, _ in services:
        relay = proxy_chain.proxified_url(name, url)
        if not relay:
            continue
        relay_headers = headers or {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": random.choice(USER_AGENTS),
        }
        try:
            r = await _client.get(relay, headers=relay_headers, timeout=30.0)
            if r.status_code != 200:
                last_err = f"relay '{name}' → HTTP {r.status_code}"
                continue
            if as_json:
                try:
                    return r.json(), name
                except Exception:
                    last_err = f"relay '{name}' → non-JSON response"
                    continue
            return r, name
        except httpx.HTTPError as e:
            last_err = f"relay '{name}' → {e}"
            continue

    raise HTTPException(502, detail=f"All proxy relays failed ({last_err})")


_DECRYPT_MJS = str(Path(__file__).parent / "decrypt.mjs")

_client: Optional[httpx.AsyncClient] = None


@asynccontextmanager
async def lifespan(_app):
    global _client
    client_kwargs = dict(
        http2=True,
        timeout=httpx.Timeout(20.0),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        follow_redirects=True,
        # Do not set global headers here; we'll pass per request
    )
    if _PREMIUM_PROXY_URL:
        # All traffic exits through your premium proxy (rotating IPs → no 403s)
        client_kwargs["proxy"] = _PREMIUM_PROXY_URL
    _client = httpx.AsyncClient(**client_kwargs)
    yield
    await _client.aclose()


app = FastAPI(title="ReAnime Scraper", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _browser_headers(base: str) -> dict:
    """Browser-like headers with a rotating User-Agent and Referer."""
    return {
        **BASE_HEADERS,
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": random.choice([
            f"{base}/",
            f"{base}/search",
            f"{base}/home",
            "https://www.google.com/",
            "https://www.bing.com/",
        ]),
        "Origin": base,
    }


# ---- _get with rotating headers, retry AND proxy-chain fallback ----
async def _get(path: str, params: dict = None, base: str = BASE) -> Any:
    """
    GET with rotating User-Agent/browser headers and a retry loop.

    If the upstream still blocks us (HTTP 403/429 — Cloudflare IP block),
    the request is re-routed through the relay proxy chain so the upstream
    sees the relay's IP instead of ours (Proxify-Streams style).
    """
    url = f"{base}{path}"
    mode = PROXY_MODE
    if mode == "always":
        data, _via = await _fetch_via_proxy(url, params=params, as_json=True)
        return data

    max_direct = 2 if mode == "auto" else 3  # fall back to relays fast in auto
    last_err = None

    for attempt in range(max_direct):
        headers = _browser_headers(base)
        if attempt > 0:
            await asyncio.sleep(random.uniform(0.4, 1.0))

        try:
            r = await _client.get(url, params=params, headers=headers)
        except httpx.HTTPError as e:
            last_err = f"network: {e}"
            continue

        if r.status_code in (403, 429):
            last_err = f"HTTP {r.status_code}"
            if mode == "off" and attempt == max_direct - 1:
                raise HTTPException(403, detail="Access denied – try again later")
            continue

        if r.status_code == 404:
            raise HTTPException(404, detail="Not found")

        if r.is_success:
            try:
                return r.json()
            except Exception:
                last_err = "Invalid JSON from upstream"
                continue

        last_err = f"HTTP {r.status_code}"
        if r.status_code >= 500 or attempt == max_direct - 1:
            raise HTTPException(r.status_code, detail=r.text[:300])

    # Direct attempts exhausted → relay through the proxy chain
    if mode == "off":
        raise HTTPException(502, detail=f"Upstream request failed ({last_err})")
    data, _via = await _fetch_via_proxy(url, params=params, as_json=True)
    return data


# ---- Everything below remains exactly as in your original ----
def _anilist_from_anime(anime: dict) -> Optional[int]:
    if not anime:
        return None
    if anime.get("anilist"):
        return int(anime["anilist"])
    for key in ("extra_large", "large", "medium"):
        url = (anime.get("cover_image") or {}).get(key, "")
        m = re.search(r"/bx(\d+)-", url)
        if m:
            return int(m.group(1))
    return None


async def _decrypt_embed(html: bytes) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "node", _DECRYPT_MJS, "-",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(input=html), timeout=20.0)
    except asyncio.TimeoutError:
        proc.kill()
        raise HTTPException(504, detail="Decrypt subprocess timed out")
    if proc.returncode != 0:
        raise HTTPException(502, detail=f"Decrypt error: {stderr.decode()[:300]}")
    return json.loads(stdout)


async def get_stream_url(access_id: str, v: int = 2) -> dict:
    # For stream endpoints, we also need to pass proper headers.
    # Use a random User-Agent here as well.
    headers = {
        **BASE_HEADERS,
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": f"{BASE}/",
        "Origin": BASE,
    }
    embed_url = f"{FLIX}/e/{access_id}?v={v}"
    r = await _client.get(embed_url, headers=headers)
    if r.status_code in (403, 429) or PROXY_MODE == "always":
        # flixcloud also blocks datacenter IPs → fetch the embed HTML via relays
        r, _via = await _fetch_via_proxy(embed_url, as_json=False)
    elif not r.is_success:
        raise HTTPException(r.status_code, detail=f"Embed fetch failed: {r.status_code}")
    return await _decrypt_embed(r.content)


async def _servers(slug: str, ep: int, anilist_id: Optional[int] = None) -> dict:
    watch = await _get(f"/api/watch/{slug}/{ep}")
    aid = anilist_id or _anilist_from_anime(watch.get("anime"))

    flix: dict = {}
    if aid:
        try:
            flix = await _get(f"/api/flix/{aid}/{ep}")
        except HTTPException:
            pass

    links = list(watch.get("episode_links") or [])
    if flix.get("success") and flix.get("servers"):
        seen = {s.get("$id") for s in links}
        for s in flix["servers"]:
            if s.get("$id") not in seen:
                links.append(s)

    _order = {"HD-2": 0, "HD-1": 1}
    _sort = lambda lst: sorted(lst, key=lambda s: _order.get(s.get("serverName", ""), 9))

    return {
        "sub":         _sort([s for s in links if s.get("dataType") in ("sub", "s-sub")]),
        "dub":         _sort([s for s in links if s.get("dataType") in ("dub", "s-dub")]),
        "anime":       watch.get("anime"),
        "current":     watch.get("current"),
        "duration":    watch.get("duration"),
        "intro_start": watch.get("intro_start"),
        "intro_end":   watch.get("intro_end"),
        "outro_start": watch.get("outro_start"),
        "outro_end":   watch.get("outro_end"),
        "anilist_id":  aid,
    }


@app.get("/")
async def root():
    return {
        "status": "ok",
        "endpoints": {
            "search":          "GET /search?q=...&limit=20",
            "home":            "GET /home?limit=20",
            "top":             "GET /top?period=week&limit=20",
            "schedule":        "GET /schedule",
            "proxy":           "GET /proxy?url={reanime.to_url} (relay fetch)",
            "info":            "GET /info/{slug}",
            "episodes":        "GET /episodes/{slug}",
            "servers":         "GET /servers/{slug}/{episode}[?anilist_id=...]",
            "stream":          "GET /stream/{access_id}[?v=2]",
            "stream_link":     "GET /stream/from-link?link={flixcloud_url}",
            "thumbnails":      "GET /thumbnails/{anilist_id}",
            "recommendations": "GET /recommendations/{slug}",
        },
    }


@app.get("/search")
async def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    return await _get("/api/search", {"q": q, "limit": limit, "offset": offset})


@app.get("/home")
async def home(limit: int = Query(20, ge=1, le=100)):
    latest, top = await asyncio.gather(
        _get("/api/home/latest-aired", {"limit": limit}),
        _get("/api/top/anime", {"period": "week", "limit": limit}),
    )
    return {"latest_aired": latest, "top_weekly": top}


@app.get("/top")
async def top(
    period: str = Query("week", pattern="^(day|week|month)$"),
    limit: int = Query(20, ge=1, le=100),
):
    return await _get("/api/top/anime", {"period": period, "limit": limit})


@app.get("/schedule")
async def schedule():
    return await _get("/api/schedule")


@app.get("/info/{slug}")
async def anime_info(slug: str):
    meta, eps = await asyncio.gather(
        _get(f"/api/watch/{slug}/1"),
        _get(f"/api/episodes/{slug}"),
    )
    anime = meta.get("anime") or {}
    anilist_id = _anilist_from_anime(anime)
    ep_list = eps if isinstance(eps, list) else eps.get("data", eps.get("episodes", []))
    return {**anime, "episodes": ep_list, "anilist_id": anilist_id}


@app.get("/episodes/{slug}")
async def episodes(slug: str):
    data = await _get(f"/api/episodes/{slug}")
    return data if isinstance(data, list) else data.get("data", data.get("episodes", data))


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
    return await _get(f"/api/thumbnails/{anilist_id}")


@app.get("/recommendations/{slug}")
async def recommendations(slug: str):
    return await _get(f"/api/anime/{slug}/recommendations")


@app.get("/proxy")
async def proxy_passthrough(url: str = Query(..., description="Full reanime.to / flixcloud.cc URL to fetch through the relay chain (Proxify-Streams style)")):
    """
    Generic passthrough — fetch ANY reanime.to / flixcloud.cc URL through the
    relay proxies and return its payload. Mirrors Proxify-Streams' /proxy route:
    response includes the `proxifiedSource` relay URLs that were used.
    """
    if not (url.startswith(BASE) or url.startswith(FLIX)):
        raise HTTPException(400, detail="Only reanime.to and flixcloud.cc URLs are allowed")
    r, via = await _fetch_via_proxy(url, as_json=False)
    try:
        payload = r.json()
    except Exception:
        payload = r.text[:2000]
    return {
        "proxifiedSource": proxy_chain.all_proxified(url),
        "via": via,
        "data": payload,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("reanime:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), workers=1, reload=False)
