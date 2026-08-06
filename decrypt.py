#!/usr/bin/env python3
"""
decrypt.py — pure-Python flixcloud.cc stream decryption.

Replaces decrypt.mjs (Node.js) so the API works on runtimes where Node.js is
NOT available — e.g. Vercel's Python serverless runtime.

Pipeline (mirrors decrypt.mjs):
  1. Parse the SvelteKit SSR data block out of the embed HTML
  2. Derive 7 obfuscated field names via 6 rounds of SHA-256 on obfuscation_seed
  3. Extract frag1 / iv from the nested crypto object; keyFrag2 + token from the page
  4. The caller provides the /api/m3u8/{token} JSON (fetched through the smart
     relay chain — flixcloud also blocks datacenter IPs)
  5. Run the embed page's own WASM (via wasmtime) to derive key material
  6. PBKDF2 + XOR + SHA-256 → AES-256-CBC key → decrypt the stream URL
"""

import base64
import hashlib
import re
from typing import Any, Optional

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from wasmtime import Engine, Instance, Module, Store


class DecryptError(Exception):
    """Raised when the embed can't be decrypted (bad shape / missing fields)."""


# ---------------------------------------------------------------------------
# Field-name derivation (identical to decrypt.mjs `le(seed)`)
# ---------------------------------------------------------------------------
def _sha256hex(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def _le(seed: str) -> dict:
    e = seed
    for i in range(3):
        e = _sha256hex(e + str(i))
    l = e
    for i in range(3):
        l = _sha256hex(l + str(i))
    return {
        "keyField":      "kf_"  + e[8:16],
        "ivField":       "ivf_" + e[16:24],
        "containerName": "cd_"  + e[24:32],
        "arrayName":     "ad_"  + e[32:40],
        "objectName":    "od_"  + e[40:48],
        "tokenField":    e[48:64] + "_" + e[56:64],
        "keyFrag2Field": l[0:16]  + "_" + l[16:24],
    }


# ---------------------------------------------------------------------------
# Embed parsing (regex-based — the SSR block is a JS literal, not strict JSON)
# ---------------------------------------------------------------------------
def _find_str(html: str, key: str) -> Optional[str]:
    m = re.search(r'["\']?' + re.escape(key) + r'["\']?\s*:\s*"([^"]*)"', html)
    return m.group(1) if m else None


def _find_b64(html: str, key: str) -> Optional[str]:
    """Find `key:"<base64>"` anywhere — the obfuscated field names are derived
    from the page seed, so they're unique within the embed HTML."""
    m = re.search(re.escape(key) + r':"([A-Za-z0-9+/=]+)"', html)
    return m.group(1) if m else None


def parse_embed(html: str) -> dict:
    """Extract all crypto fields + stream metadata from the embed page HTML."""
    seed_m = re.search(r'obfuscation_seed:"([0-9a-fA-F]+)"', html)
    if not seed_m:
        raise DecryptError("obfuscation_seed not found in embed")
    seed = seed_m.group(1)
    f = _le(seed)

    key_frag1 = _find_b64(html, f["keyField"])
    iv = _find_b64(html, f["ivField"])
    key_frag2 = _find_str(html, f["keyFrag2Field"])
    token = _find_str(html, f["tokenField"])
    wasm = _find_str(html, "w_payload")

    if not (key_frag1 and iv and key_frag2 and token and wasm):
        raise DecryptError(f"crypto fields missing (frag1={bool(key_frag1)} iv={bool(iv)} "
                           f"kf2={bool(key_frag2)} token={bool(token)} wasm={bool(wasm)})")

    return {
        "seed": seed,
        "key_frag1": key_frag1,
        "iv": iv,
        "key_frag2": key_frag2,
        "token": token,
        "wasm": wasm,
        # metadata (best-effort — the stream URL is the critical part)
        "subtitles": _parse_subtitles(html),
        "thumbnails_vtt": _find_str(html, "thumbnails_vtt"),
        "video_title": _find_str(html, "video_title"),
        "video_id": _find_str(html, "video_id"),
        "intro_chapter": _parse_chapter(html, "intro_chapter"),
        "outro_chapter": _parse_chapter(html, "outro_chapter"),
    }


def _parse_subtitles(html: str) -> list:
    """Parse `subtitles:[{...},{...}]` with a brace-aware scanner — the SSR
    block is a JS literal, so plain regex on the array is brittle."""
    start = html.find("subtitles:[")
    if start < 0:
        return []
    i = start + len("subtitles:[")
    depth = 0
    end = None
    for j in range(i, len(html)):
        c = html[j]
        if c == "{":
            depth += 1
        elif c == "}":
            depth = max(0, depth - 1)
        elif c == "]" and depth == 0:
            end = j
            break
    if end is None:
        return []
    out = []
    for block in re.findall(r'\{([^{}]*)\}', html[i:end]):
        url = re.search(r'url:"([^"]+)"', block)
        lang = re.search(r'language:"([^"]*)"', block)
        fmt = re.search(r'format:"([^"]*)"', block)
        dflt = re.search(r'default:(true|false)', block)
        if url:
            out.append({
                "url": url.group(1),
                "language": lang.group(1) if lang else "",
                "format": fmt.group(1) if fmt else None,
                "default": (dflt.group(1) == "true") if dflt else False,
            })
    return out


def _parse_chapter(html: str, name: str) -> Optional[dict]:
    m = re.search(re.escape(name) + r':\{(start:(\d+),end:(\d+)(?:,title:"([^"]*)")?)\}', html)
    if not m:
        return None
    return {"start": int(m.group(2)), "end": int(m.group(3)), "title": m.group(4)}


# ---------------------------------------------------------------------------
# WASM execution (wasmtime) — identical to decrypt.mjs runWasm()
# ---------------------------------------------------------------------------
_ENGINE: Optional[Engine] = None


def _get_engine() -> Engine:
    """Reuse one Wasmtime engine across decrypts (engine init is expensive)."""
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = Engine()
    return _ENGINE


def _run_wasm(wasm_b64: str, frag1: bytes, kf2: bytes, t_bytes: bytes,
              seed_int: int) -> bytes:
    engine = _get_engine()
    store = Store(engine)
    module = Module(engine, base64.b64decode(wasm_b64))
    instance = Instance(store, module, [])
    exports = instance.exports(store)

    memory = exports["memory"]
    length = len(frag1)
    y, v, T, out = 1000, 1000 + length, 1000 + 2 * length, 1000 + 3 * length

    memory.write(store, frag1, y)
    memory.write(store, kf2, v)
    memory.write(store, t_bytes, T)

    # wasmtime-py v47+: call exported functions via func(store, *args)
    exports["_s"](store, seed_int)
    exports["_r"](store, y, v, T, out, length)
    return memory.read(store, out, out + length)


# ---------------------------------------------------------------------------
# Full decryption
# ---------------------------------------------------------------------------
def decrypt_stream(parsed: dict, token_data: dict) -> dict:
    """
    Run the WASM + PBKDF2 + XOR + AES pipeline. `token_data` is the JSON from
    https://flixcloud.cc/api/m3u8/{token} (fetched by the caller so it can go
    through the relay chain). Returns the same shape decrypt.mjs produced.
    """
    token = parsed["token"]
    vid_key = _sha256hex(token + "vid")[:10]
    key_key = _sha256hex(token + "key")[:10]

    try:
        v_bytes = base64.b64decode(token_data[vid_key])
        t_bytes = base64.b64decode(token_data[key_key])
    except (KeyError, ValueError) as e:
        raise DecryptError(f"token payload missing fields ({vid_key}/{key_key}): "
                           f"{sorted(token_data.keys())}") from e

    wasm_out = _run_wasm(
        parsed["wasm"],
        base64.b64decode(parsed["key_frag1"]),
        base64.b64decode(parsed["key_frag2"]),
        t_bytes,
        int(parsed["seed"][:8], 16),
    )

    pbk = hashlib.pbkdf2_hmac("sha256", wasm_out, parsed["seed"].encode(), 1000, 32)
    key = bytearray(pbk)
    seed_bytes = parsed["seed"].encode()
    for i in range(32):
        key[i] ^= seed_bytes[i % len(seed_bytes)]
    aes_key = hashlib.sha256(bytes(key)).digest()

    decipher = AES.new(aes_key, AES.MODE_CBC, base64.b64decode(parsed["iv"]))
    raw = decipher.decrypt(v_bytes)
    try:
        url = unpad(raw, 16).decode("utf-8", errors="replace").strip()
    except ValueError:
        url = raw.decode("utf-8", errors="replace").strip("\x00").strip()

    if not url.startswith("http"):
        raise DecryptError(f"decrypted payload is not a URL: {url[:80]!r}")

    return {
        "url": url,
        "subtitles": parsed.get("subtitles") or [],
        "thumbnails_vtt": parsed.get("thumbnails_vtt"),
        "video_title": parsed.get("video_title"),
        "intro_chapter": parsed.get("intro_chapter"),
        "outro_chapter": parsed.get("outro_chapter"),
        "video_id": parsed.get("video_id"),
    }
