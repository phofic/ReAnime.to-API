#!/usr/bin/env node
// Polyfill for fetch if Node.js version < 18
if (typeof fetch === 'undefined') {
  const module = await import('node-fetch');
  globalThis.fetch = module.default;
}

import crypto from "node:crypto";
import { readFileSync } from "node:fs";

function sha256hex(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

function rt(b64) {
  return Buffer.from(b64, "base64");
}

async function fetchJson(url, headers = {}) {
  const r = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", ...headers },
  });
  if (!r.ok) throw new Error(`HTTP ${r.status} from ${url}`);
  return r.json();
}

function le(seed) {
  let e = seed;
  for (let i = 0; i < 3; i++) e = sha256hex(e + i);
  let l = e;
  for (let i = 0; i < 3; i++) l = sha256hex(l + i);
  return {
    keyField:      "kf_"  + e.substring(8,  16),
    ivField:       "ivf_" + e.substring(16, 24),
    containerName: "cd_"  + e.substring(24, 32),
    arrayName:     "ad_"  + e.substring(32, 40),
    objectName:    "od_"  + e.substring(40, 48),
    tokenField:    e.substring(48, 64) + "_" + e.substring(56, 64),
    keyFrag2Field: l.substring(0, 16)  + "_" + l.substring(16, 24),
  };
}

async function runWasm(wasmB64, frag1, kf2, T_bytes, seedInt) {
  const { instance } = await WebAssembly.instantiate(rt(wasmB64));
  const { _s, _r, memory } = instance.exports;
  const h = new Uint8Array(memory.buffer);
  const len = frag1.length;
  const [y, v, T, out] = [1000, 1000 + len, 1000 + 2 * len, 1000 + 3 * len];
  h.set(frag1, y);
  h.set(kf2, v);
  h.set(T_bytes, T);
  _s(seedInt);
  _r(y, v, T, out, len);
  return Buffer.from(h.subarray(out, out + len));
}

function extractSsrObj(html) {
  const m = html.match(/\{type:"data",data:(\{)/);
  if (!m) throw new Error("SSR data block not found");
  let depth = 0;
  const start = html.indexOf("{", m.index + m[0].length - 1);
  for (let i = start; i < html.length; i++) {
    if (html[i] === "{") depth++;
    else if (html[i] === "}") {
      if (--depth === 0) return html.slice(start, i + 1);
    }
  }
  throw new Error("SSR brace matching failed");
}

async function main() {
  let html;
  const arg = process.argv[2] ?? "-";
  if (arg === "-") {
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    html = Buffer.concat(chunks).toString();
  } else {
    html = readFileSync(arg, "utf8");
  }

  const data   = eval("(" + extractSsrObj(html) + ")");
  const seed   = data.obfuscation_seed;
  const fields = le(seed);
  const ocd    = data.obfuscated_crypto_data;
  const obj    = ocd[fields.containerName][fields.arrayName][0][fields.objectName];
  const frag1  = rt(obj[fields.keyField]);
  const iv     = rt(obj[fields.ivField]);
  const kf2    = rt(data[fields.keyFrag2Field]);
  const token  = data[fields.tokenField];

  if (!token) throw new Error("Token field missing from embed data");

  const tokData = await fetchJson(`https://flixcloud.cc/api/m3u8/${token}`, { Referer: "https://reanime.to/" });
  const vidKey  = sha256hex(token + "vid").substring(0, 10);
  const keyKey  = sha256hex(token + "key").substring(0, 10);
  const v_bytes = rt(tokData[vidKey]);
  const T_bytes = rt(tokData[keyKey]);

  if (!v_bytes.length || !T_bytes.length)
    throw new Error(`Token missing fields. Got: ${Object.keys(tokData).join(",")}`);

  const wasmOut = await runWasm(data.w_payload, frag1, kf2, T_bytes, parseInt(seed.substring(0, 8), 16));
  const pbk     = crypto.pbkdf2Sync(wasmOut, seed, 1000, 32, "sha256");
  const r       = Buffer.from(pbk);
  for (let i = 0; i < 32; i++) r[i] ^= seed.charCodeAt(i % seed.length);
  const aesKey  = crypto.createHash("sha256").update(r).digest();

  const decipher = crypto.createDecipheriv("aes-256-cbc", aesKey, iv);
  const url      = Buffer.concat([decipher.update(v_bytes), decipher.final()]).toString("utf8").trim();

  if (!url.startsWith("http")) throw new Error(`Unexpected URL: ${url}`);

  process.stdout.write(JSON.stringify({
    url,
    subtitles:      data.subtitles      ?? [],
    thumbnails_vtt: data.thumbnails_vtt ?? null,
    video_title:    data.video_title    ?? null,
    intro_chapter:  data.intro_chapter  ?? null,
    outro_chapter:  data.outro_chapter  ?? null,
    video_id:       data.video_id       ?? null,
  }));
}

main().catch((err) => {
  process.stderr.write(err.message + "\n");
  process.exit(1);
});
