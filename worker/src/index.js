// Breed data for the events page. The page ships with Finnish Spitz baked in;
// any other breed comes through here, one breed-month at a time.
//
//   GET /breeds                         -> { breeds: [{code, name, aka?}] }
//   GET /shows?breed=107&month=2026-11  -> { breed, month, fetched, shows: [...] }
//
// Every answer is cached in KV so AKC is asked at most once a day per breed-month,
// whoever is looking and wherever they are. A breed-month is small enough to read
// within the free plan's CPU limit; a whole year in one request wouldn't be.

import { extractRecords, mapRecord, monthAllowed, parseBreeds, parseSearchBody, searchPayload } from "./akc.js";

const SEARCH_URL = "https://webapps.akc.org/event-search/api/search/events";
const REFERENCES_URL = "https://webapps.akc.org/event-search/api/references/";
const UA = "fsca-events/1.0 (Finnish Spitz Club of America; https://finnishspitz.org)";
const AKC_HEADERS = {
  "User-Agent": UA,
  Accept: "application/json",
  Origin: "https://webapps.akc.org",
  Referer: "https://webapps.akc.org/event-search/",
};
const AKC_TIMEOUT_MS = 60000; // AKC has taken 26s on a large query

const SHOWS_TTL = 24 * 3600;
const BREEDS_TTL = 7 * 24 * 3600;
const BREEDS_KEY = "breeds";

const ORIGINS = new Set([
  "https://finnishspitzclub.github.io",
  "https://finnishspitz.org",
  "https://www.finnishspitz.org",
]);
const LOCAL_ORIGIN = /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/;

function corsHeaders(request) {
  const origin = request.headers.get("Origin");
  const headers = { Vary: "Origin" };
  if (origin && (ORIGINS.has(origin) || LOCAL_ORIGIN.test(origin))) {
    headers["Access-Control-Allow-Origin"] = origin;
  }
  return headers;
}

function respond(request, body, status, cacheControl) {
  return new Response(typeof body === "string" ? body : JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": cacheControl,
      ...corsHeaders(request),
    },
  });
}

const fail = (request, status, error) => respond(request, { error }, status, "no-store");

// A failed KV write (say, the free daily write cap) only costs us the cache.
function store(env, ctx, key, value, expirationTtl) {
  ctx.waitUntil(
    env.CACHE.put(key, value, { expirationTtl }).catch((e) => console.error("cache write failed", key, String(e))),
  );
}

async function breedList(env, ctx) {
  const cached = await env.CACHE.get(BREEDS_KEY, "json");
  if (cached) return cached;
  const res = await fetch(REFERENCES_URL, {
    headers: AKC_HEADERS,
    signal: AbortSignal.timeout(AKC_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`AKC references ${res.status}`);
  const breeds = parseBreeds(await res.json());
  if (!breeds.length) throw new Error("AKC references had no breeds");
  store(env, ctx, BREEDS_KEY, JSON.stringify(breeds), BREEDS_TTL);
  return breeds;
}

async function handleBreeds(request, env, ctx) {
  try {
    return respond(request, { breeds: await breedList(env, ctx) }, 200, "public, max-age=86400");
  } catch (e) {
    console.error("breeds failed", String(e));
    return fail(request, 502, "could not load the breed list from AKC");
  }
}

async function handleShows(request, env, ctx, params) {
  const code = params.get("breed") || "";
  const month = params.get("month") || "";
  if (!/^\d{3}$/.test(code)) return fail(request, 400, "breed must be a 3-digit AKC code");
  if (!monthAllowed(month)) return fail(request, 400, "month is outside the supported range");

  const key = `shows:${code}:${month}`;
  const cached = await env.CACHE.get(key);
  if (cached) return respond(request, cached, 200, "public, max-age=3600");

  try {
    const breed = (await breedList(env, ctx)).find((b) => b.code === code);
    if (!breed) return fail(request, 400, "unknown breed");

    // Only misses reach AKC, so this is the pace AKC actually sees (per
    // Cloudflare location; the sync's own pace is the same 30 a minute).
    if (env.AKC_LIMIT && !(await env.AKC_LIMIT.limit({ key: "akc" })).success) {
      return fail(request, 429, "busy, try again shortly");
    }

    const res = await fetch(SEARCH_URL, {
      method: "POST",
      headers: { ...AKC_HEADERS, "Content-Type": "application/json" },
      body: JSON.stringify(searchPayload(breed, month)),
      signal: AbortSignal.timeout(AKC_TIMEOUT_MS),
    });
    if (!res.ok) {
      console.error("AKC search failed", code, month, res.status);
      return fail(request, 502, "AKC did not answer");
    }
    const records = extractRecords(parseSearchBody(await res.text()));
    if (!records) {
      console.error("AKC search returned no event list", code, month);
      return fail(request, 502, "AKC sent an unexpected reply");
    }
    if (records.length >= 1000) console.warn("AKC result cap reached", code, month);

    const body = JSON.stringify({
      breed: code,
      month,
      fetched: new Date().toISOString(),
      shows: records.map(mapRecord).filter(Boolean),
    });
    store(env, ctx, key, body, SHOWS_TTL);
    return respond(request, body, 200, "public, max-age=3600");
  } catch (e) {
    console.error("shows failed", code, month, String(e));
    return fail(request, 502, "could not reach AKC");
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method !== "GET") return fail(request, 405, "GET only");
    if (url.pathname === "/breeds") return handleBreeds(request, env, ctx);
    if (url.pathname === "/shows") return handleShows(request, env, ctx, url.searchParams);
    return fail(request, 404, "not found");
  },
};
