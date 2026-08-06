import asyncio
import json
import os
import re
import random
from pathlib import Path
from typing import Any, Optional, Dict

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ===== CONFIGURATION =====
BASE = "https://reanime.to"
FLIX = "https://flixcloud.cc"
API_BASE = "https://reanime.to/api"

# Rotating User Agents - critical for bypassing 403
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# Real browser headers
BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# Simple in-memory cache for Vercel (since Redis isn't available)
_cache: Dict[str, Dict] = {}
_cache_times: Dict[str, float] = {}
CACHE_TTL = 300  # 5 minutes

app = FastAPI(title="ReAnime API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Global HTTP client
_client: Optional[httpx.AsyncClient] = None

# ===== LIFECYCLE =====
@app.on_event("startup")
async def startup():
    global _client
    _client = httpx.AsyncClient(
        http2=True,
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
        follow_redirects=True,
    )
    print("✅ HTTP Client initialized")

@app.on_event("shutdown")
async def shutdown():
    if _client:
        await _client.aclose()
        print("✅ HTTP Client closed")

# ===== CACHE HELPERS =====
def get_cached(key: str) -> Optional[Any]:
    """Simple memory cache for Vercel"""
    if key in _cache and key in _cache_times:
        if asyncio.get_event_loop().time() - _cache_times[key] < CACHE_TTL:
            return _cache[key]
        else:
            del _cache[key]
            del _cache_times[key]
    return None

def set_cached(key: str, value: Any):
    _cache[key] = value
    _cache_times[key] = asyncio.get_event_loop().time()

# ===== REQUEST FUNCTION WITH RETRY =====
async def _make_request(
    path: str,
    params: dict = None,
    base: str = BASE,
    retries: int = 2
) -> Any:
    """Make request with retry and rotating headers"""
    
    # Check cache first
    cache_key = f"{path}:{str(params)}" if params else path
    cached = get_cached(cache_key)
    if cached is not None:
        return cached
    
    last_error = None
    
    for attempt in range(retries + 1):
        try:
            # Rotate headers for each attempt
            headers = {
                **BASE_HEADERS,
                "User-Agent": random.choice(USER_AGENTS),
                "Referer": random.choice([
                    f"{base}/",
                    f"{base}/search",
                    f"{base}/home",
                    "https://www.google.com/",
                ]),
                "Origin": base,
                "X-Requested-With": "XMLHttpRequest",
            }
            
            # Add random delay only on retry to avoid rate limiting
            if attempt > 0:
                await asyncio.sleep(random.uniform(1, 3) * attempt)
            
            url = f"{base}{path}"
            response = await _client.get(url, params=params, headers=headers)
            
            # Handle 403 with different approach
            if response.status_code == 403:
                if attempt < retries:
                    # Try with completely different headers
                    headers["User-Agent"] = random.choice(USER_AGENTS)
                    headers["Accept"] = "application/json"
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    continue
                else:
                    raise HTTPException(403, detail="Access denied. Please try again later.")
            
            if response.status_code == 404:
                raise HTTPException(404, detail="Not found")
            
            if not response.is_success:
                raise HTTPException(
                    response.status_code,
                    detail=f"Request failed: {response.text[:200]}"
                )
            
            # Parse JSON
            data = response.json()
            
            # Cache successful responses
            if data:
                set_cached(cache_key, data)
            
            return data
            
        except httpx.TimeoutException as e:
            last_error = e
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
                continue
            raise HTTPException(504, detail=f"Timeout after {retries} retries")
        
        except HTTPException:
            raise
        
        except Exception as e:
            last_error = e
            if attempt < retries:
                continue
            raise HTTPException(500, detail=f"Request failed: {str(e)}")
    
    # Should never reach here
    raise HTTPException(500, detail="All retry attempts failed")

# ===== HELPER FUNCTIONS =====
def _anilist_from_anime(anime: dict) -> Optional[int]:
    if not anime: return None
    if anime.get("anilist"): return int(anime["anilist"])
    
    for key in ("extra_large", "large", "medium"):
        url = (anime.get("cover_image") or {}).get(key, "")
        m = re.search(r"/bx(\d+)-", url)
        if m: return int(m.group(1))
    return None

async def _decrypt_embed(html: bytes) -> dict:
    """Decrypt embed using Node.js"""
    decrypt_path = Path(__file__).parent.parent / "decrypt.mjs"
    
    # If decrypt.mjs doesn't exist, try to find it
    if not decrypt_path.exists():
        decrypt_path = Path(__file__).parent / "decrypt.mjs"
    
    if not decrypt_path.exists():
        raise HTTPException(500, detail="decrypt.mjs not found")
    
    proc = await asyncio.create_subprocess_exec(
        "node", str(decrypt_path), "-",
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
    headers = {
        **BASE_HEADERS,
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": f"{BASE}/",
    }
    r = await _client.get(f"{FLIX}/e/{access_id}?v={v}", headers=headers)
    if not r.is_success:
        raise HTTPException(r.status_code, detail=f"Embed fetch failed: {r.status_code}")
    return await _decrypt_embed(r.content)

async def _servers(slug: str, ep: int, anilist_id: Optional[int] = None) -> dict:
    watch = await _make_request(f"/api/watch/{slug}/{ep}")
    aid = anilist_id or _anilist_from_anime(watch.get("anime"))
    
    flix: dict = {}
    if aid:
        try:
            flix = await _make_request(f"/api/flix/{aid}/{ep}")
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
        "sub": _sort([s for s in links if s.get("dataType") in ("sub", "s-sub")]),
        "dub": _sort([s for s in links if s.get("dataType") in ("dub", "s-dub")]),
        "anime": watch.get("anime"),
        "current": watch.get("current"),
        "duration": watch.get("duration"),
        "intro_start": watch.get("intro_start"),
        "intro_end": watch.get("intro_end"),
        "outro_start": watch.get("outro_start"),
        "outro_end": watch.get("outro_end"),
        "anilist_id": aid,
    }

# ===== ENDPOINTS =====
@app.get("/")
async def root():
    return {
        "status": "ok",
        "version": "2.1.0",
        "endpoints": {
            "search": "GET /search?q=...",
            "home": "GET /home",
            "top": "GET /top",
            "schedule": "GET /schedule",
            "info": "GET /info/{slug}",
            "episodes": "GET /episodes/{slug}",
            "servers": "GET /servers/{slug}/{episode}",
            "stream": "GET /stream/{access_id}",
        }
    }

@app.get("/search")
async def search(
    q: str = Query(..., min_length=1, max_length=200),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    return await _make_request("/api/search", {"q": q, "limit": limit, "offset": offset})

@app.get("/home")
async def home(limit: int = Query(20, ge=1, le=100)):
    latest, top = await asyncio.gather(
        _make_request("/api/home/latest-aired", {"limit": limit}),
        _make_request("/api/top/anime", {"period": "week", "limit": limit}),
    )
    return {"latest_aired": latest, "top_weekly": top}

@app.get("/top")
async def top(
    period: str = Query("week", pattern="^(day|week|month)$"),
    limit: int = Query(20, ge=1, le=100),
):
    return await _make_request("/api/top/anime", {"period": period, "limit": limit})

@app.get("/schedule")
async def schedule():
    return await _make_request("/api/schedule")

@app.get("/info/{slug}")
async def anime_info(slug: str):
    meta, eps = await asyncio.gather(
        _make_request(f"/api/watch/{slug}/1"),
        _make_request(f"/api/episodes/{slug}"),
    )
    anime = meta.get("anime") or {}
    anilist_id = _anilist_from_anime(anime)
    ep_list = eps if isinstance(eps, list) else eps.get("data", eps.get("episodes", []))
    return {**anime, "episodes": ep_list, "anilist_id": anilist_id}

@app.get("/episodes/{slug}")
async def episodes(slug: str):
    data = await _make_request(f"/api/episodes/{slug}")
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
    return await _make_request(f"/api/thumbnails/{anilist_id}")

@app.get("/recommendations/{slug}")
async def recommendations(slug: str):
    return await _make_request(f"/api/anime/{slug}/recommendations")

# ===== HEALTH CHECK =====
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "cache_size": len(_cache),
        "timestamp": asyncio.get_event_loop().time()
    }

# ===== ERROR HANDLER =====
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "status": exc.status_code,
            "detail": exc.detail,
            "path": request.url.path
        }
    )

# ===== VERCEL HANDLER =====
# This is the entry point for Vercel
from mangum import Mangum
handler = Mangum(app)
