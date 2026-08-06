# reanime-scraper

A self-hosted anime streaming API that scrapes [reanime.to](https://reanime.to) and fully decrypts [flixcloud.cc](https://flixcloud.cc) HLS streams. Drop-in Consumet-style source. **100% pure Python** — no headless browsers, no Node.js (decryption runs the page's own WASM via `wasmtime`). This means it works on Vercel's Python serverless runtime.

> **⚠️ Big change (2026):** reanime.to migrated its API to `/api/v1/*`. Old paths (`/api/search`, `/api/schedule`, `/api/watch/...`, `/api/episodes/...`) now return 404/HTML — the main cause of 502s. This codebase is fully migrated to the v1 endpoints. Some reanime.to endpoints (`/api/v1/watch/...`) now require a login token; everything this API exposes works without one.

## What it does

- Search anime, browse home/top/new/upcoming charts, airing schedule
- Full anime metadata + episode lists (`/api/v1/anime/{slug}` + `/api/v1/anime/{slug}/episodes`)
- All streaming servers for an episode (sub/dub, HD-1/HD-2) via the public `/api/flix/{anilistId}/{ep}`
- **Decrypt the actual `.m3u8` stream URL** (WASM + AES-256-CBC) from flixcloud.cc
- Returns subtitles (SRT/ASS, multiple languages), thumbnail VTT sprites, intro/outro chapters
- In-memory TTL cache on public endpoints (search-as-you-type friendly)
- Smart anti-blocking fetch layer (see below)

## Why was it 502-ing (and how this fixes it)

1. **API moved to `/api/v1/`** — every endpoint now hits the current public paths. ✅
2. **The old relays were dead and the headers were buggy** — `corsproxy.io` went paywalled (403), `codetabs` died (521); `allorigins` still works but is slow (6–22s). On top of that, the old code manually sent `Accept-Encoding` — which makes httpx return **raw gzip bytes**, so `r.json()` always failed and even successful relays were treated as failures → 502.
3. **Fix:** headers fixed, endpoints migrated, plus **Jina Reader** (`r.jina.ai`) added as a fast primary relay, a **concurrent relay race** under a hard deadline, and a **relay health cache** so broken relays cost ~0ms.

## Setup

**Requirements:** Python 3.11+

```bash
pip install fastapi uvicorn "httpx[http2]" pycryptodome wasmtime
uvicorn reanime:app --host 0.0.0.0 --port 8000
```

## Deployment (Vercel / Railway)

reanime.to blocks most datacenter IP ranges (Vercel, AWS, ...) with HTTP 403.

| Approach | Latency | Notes |
|----------|---------|-------|
| **Railway / home server** | fast (~1–3s) | egress IP usually not blocked → direct requests work |
| **Vercel (free relays)** | slow (6–20s) | works but rides on `allorigins`; raise `RELAY_BUDGET` if your plan allows longer functions |
| **Vercel + premium proxy** | fast | set `PROXY_URL=https://user:pass@proxy...` (Bright Data / Oxylabs / etc.) — all traffic exits through it |

`vercel.json` already sets `functions.reanime.py.maxDuration = 10` (the Hobby cap). If you're on the **Pro plan** you can raise it (e.g. `30`–`60`) to give the slow-relay path more headroom.

## Environment variables

| Var | Default | Purpose |
|-----|---------|---------|
| `PROXY_MODE` | `auto` | `auto` (direct, then relays) · `always` (skip direct) · `off` (never proxy) |
| `DIRECT_TIMEOUT` | `8` | seconds for the direct attempt |
| `RELAY_TIMEOUT` | `10` | seconds per relay |
| `RELAY_BUDGET` | `8` | hard deadline for the relay race (keep < function timeout) |
| `RELAY_COOLDOWN` | `60` | seconds a failing relay is skipped |
| `PROXY_SERVICES` | built-ins | `"name\|https://relay/?url={url};..."` — override relay list |
| `PROXY_URL` | — | premium HTTP(S) proxy, e.g. `http://user:pass@host:port` |
| `CACHE_ENABLED` | `1` | set `0` to disable the TTL cache |
> The flixcloud token API is now fetched through the same smart relay chain (no extra env var needed).

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/search?q=...&limit=20` | Search anime (v1) |
| GET | `/home?limit=20` | Latest aired + top weekly |
| GET | `/new?limit=20` | New on site |
| GET | `/upcoming?limit=20` | Upcoming |
| GET | `/top?period=week&limit=20` | Top anime (`day`/`week`/`month`) |
| GET | `/schedule` | Weekly airing schedule |
| GET | `/anime/{slug}` | Full anime metadata (includes `anilist_id`) |
| GET | `/info/{slug}` | Anime metadata + full episode list |
| GET | `/episodes/{slug}` | Episode list only |
| GET | `/servers/{slug}/{episode}` | All streaming servers for an episode (public `/api/flix`) |
| GET | `/stream/{access_id}?v=2` | Decrypt stream → HLS URL + subtitles + chapters |
| GET | `/stream/from-link?link={url}` | Same, pass a full flixcloud embed URL |
| GET | `/thumbnails/{anilist_id}` | Episode thumbnail data |
| GET | `/recommendations/{slug}` | Related anime |
| GET | `/proxy?url=...` | Fetch any reanime.to/flixcloud.cc URL through the smart fetch layer |

The `slug` (a.k.a. `anime_id`) is the URL-friendly ID from reanime.to (e.g. `demon-slayer-kimetsu-no-yaiba-wvu9v4`).

## Typical flow

```
1. GET /search?q=demon+slayer
   → pick an anime_id (slug) from results

2. GET /servers/{slug}/{episode}
   → { sub: [{serverName, dataLink, dataType}], dub: [...], anilist_id }

3. GET /stream/from-link?link={dataLink}
   → { url: "...master.m3u8?token=...", subtitles, thumbnails_vtt, intro_chapter, outro_chapter }
```

### `/servers` response

```json
{
  "sub":  [{ "serverName": "HD-2", "dataLink": "https://flixcloud.cc/e/abc123?v=2", "dataType": "sub" },
           { "serverName": "HD-1", "dataLink": "https://flixcloud.cc/e/abc123?v=1", "dataType": "sub" }],
  "dub":  [],
  "anime": { "anilist_id": 101922, "title": {...}, ... },
  "anilist_id": 101922,
  "current": null,
  "duration": 24,
  "intro_start": null
}
```

> `intro_*/outro_*` and `current` require reanime.to's authenticated `/api/v1/watch` endpoint and are `null` without a token. Intro/outro **chapters** are still available from `/stream` (decrypted from the embed).

### `/stream` response

```json
{
  "url": "https://fetch7.flixcloud.cc/_v7/{video_id}/master.m3u8?token=...",
  "subtitles": [{ "url": "...", "language": "English (Dialogue)", "format": "ass", "default": true }],
  "thumbnails_vtt": "https://fetch7.flixcloud.cc/thumbnails_vtt/{video_id}",
  "video_title": "Episode.Title.1080p.mkv",
  "intro_chapter": { "start": 133, "end": 223, "title": "OP" },
  "outro_chapter": { "start": 1361, "end": 1451, "title": "ED" },
  "video_id": "5ad3792b-..."
}
```

> **Note:** flixcloud tokens are one-time-use (`410 Gone` on reuse) and stream URLs are short-lived JWTs (~6h). **Do not cache `/stream` responses.**

## How the decryption works

flixcloud.cc embeds streams behind a rotating WASM-based AES-256-CBC scheme; every page load gets fresh WASM constants, a one-time token, and new encrypted key material. `decrypt.py` does it all in Python (no Node.js):

1. Fetch `flixcloud.cc/e/{access_id}?v={1|2}` — parse the SvelteKit SSR data block (JS-literal regex extraction — the block isn't strict JSON)
2. Derive 7 obfuscated field names via 6 rounds of SHA-256 on `obfuscation_seed`
3. Extract `frag1`, `iv` from the nested crypto object; `keyFrag2`, `token` from page data
4. `GET /api/m3u8/{token}` — one-time payload, fetched **through the smart relay chain** (flixcloud blocks datacenter IPs too)
5. Run the embed page's own WASM via **wasmtime** to derive key material
6. PBKDF2 (stdlib) + XOR + SHA-256 → AES-256-CBC key (pycryptodome) → decrypt the stream URL

`decrypt.mjs` is kept in the repo as a standalone reference implementation (and works where Node.js is available, e.g. Railway) — the API itself no longer needs it.

## Files

```
reanime/
├── reanime.py      # FastAPI app — endpoints, caching, smart proxy layer
├── decrypt.py      # pure-Python WASM + PBKDF2 + AES-256-CBC decryption (active)
└── decrypt.mjs     # Node.js reference implementation (standalone tool)
```
