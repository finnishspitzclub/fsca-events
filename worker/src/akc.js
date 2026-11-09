// AKC event search: the request we send and the slim show record we keep.
// mapRecord mirrors map_record + cluster_events in akcal.py; the field names are
// the ones the events page already reads. See SCHEMA.md for the source fields.

const STATES = [
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
  "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
  "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
  "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
  "WV", "WI", "WY",
];

const CONF = "CONF"; // competitionGroupCode for conformation

const DOC_URL =
  "https://www.apps.akc.org/apps/eventplans/eventsearch/blocks/dsp_generate_pdf.cfm?KEY_BINARY_CONTENT=";
const DOC_LABEL = {
  PRMLST: "Premium list",
  JDGPRO: "Judging program",
  ENTFRM: "Entry form",
  EVNINF: "Event info",
};

const trim = (v) => String(v ?? "").trim();

function akcDate(d) {
  const p = (n) => String(n).padStart(2, "0");
  return `${p(d.getUTCMonth() + 1)}/${p(d.getUTCDate())}/${d.getUTCFullYear()}`;
}

// 'YYYY-MM' -> first and last day of that month.
export function monthRange(month) {
  const [y, m] = month.split("-").map(Number);
  return { from: new Date(Date.UTC(y, m - 1, 1)), to: new Date(Date.UTC(y, m, 0)) };
}

// One month back through 19 ahead: the page asks for at most 18 ahead, plus a
// month of slack because the viewer's clock can be a calendar month off from UTC.
export function monthAllowed(month, now = new Date()) {
  const m = /^(\d{4})-(0[1-9]|1[0-2])$/.exec(month || "");
  if (!m) return false;
  const offset = +m[1] * 12 + +m[2] - 1 - (now.getUTCFullYear() * 12 + now.getUTCMonth());
  return offset >= -1 && offset <= 19;
}

export function searchPayload(breed, month) {
  const { from, to } = monthRange(month);
  return {
    address: {
      states: STATES.join(","),
      eventSetting: { indoor: true, outdoor: true, outsideCovered: true },
      searchByState: true,
      searchByCity: false,
      searchText: "All States",
    },
    breedCode: breed.code.padEnd(4), // AKC's codes carry a real trailing space
    breedName: breed.name,
    breedId: "SPECIFIC",
    dateRange: { from: akcDate(from), to: akcDate(to), type: "event" },
    competition: {
      items: [{ selected: true, value: { compType: "AB/LB" }, label: "All- Breed and Group (AB/LB)" }],
      filters: [{ compType: "AB/LB", limitCode: "NOHS" }],
    },
  };
}

// AKC's /references breeds -> [{code, name, aka?}] sorted by name. A few breeds
// are listed twice under one code ('Poodle (Toy)' / 'Toy Poodle'); keep one row
// and let the other name stay searchable.
export function parseBreeds(body) {
  const byCode = new Map();
  for (const b of body?.breeds ?? []) {
    const code = trim(b.breedCode);
    const name = trim(b.displayDescription);
    if (!/^\d{3}$/.test(code) || !name) continue;
    const seen = byCode.get(code);
    if (!seen) byCode.set(code, { code, name });
    else if (name.includes("(")) byCode.set(code, { code, name, aka: seen.name });
    else seen.aka = name;
  }
  return [...byCode.values()].sort((a, b) => a.name.localeCompare(b.name));
}

// Nearly all of a search response is items[].breeds, a 200-entry list repeated on
// every item that we never read. Cutting it out of the text first is what keeps a
// request inside the free plan's CPU limit: indexOf is far cheaper than parsing it.
export function stripBreedLists(text) {
  const marker = '"breeds":[';
  const parts = [];
  let at = 0;
  for (;;) {
    const open = text.indexOf(marker, at);
    if (open < 0) break;
    const close = text.indexOf("]", open + marker.length);
    if (close < 0) break;
    parts.push(text.slice(at, open), '"breeds":[]');
    at = close + 1;
  }
  parts.push(text.slice(at));
  return parts.join("");
}

// Parse a search response cheaply, falling back to the whole text if the cut
// ever produces something JSON can't read.
export function parseSearchBody(text) {
  try {
    return JSON.parse(stripBreedLists(text));
  } catch {
    return JSON.parse(text);
  }
}

// The event list out of a search response, or null when the body isn't one
// (so an error reply never gets cached as "no shows this month").
export function extractRecords(body) {
  if (Array.isArray(body)) return body;
  if (body && typeof body === "object") {
    for (const key of ["events", "results", "data", "items", "content"]) {
      if (Array.isArray(body[key])) return body[key];
    }
    for (const v of Object.values(body)) {
      if (Array.isArray(v) && v.length && typeof v[0] === "object") return v;
    }
  }
  return null;
}

// AKC epoch-millis -> 'YYYY-MM-DD', or null. See the note at akcal._ms: these are
// dates, not instants.
function msDate(v) {
  const n = Number.parseInt(v, 10);
  return v && Number.isFinite(n) ? new Date(n).toISOString().slice(0, 10) : null;
}

function judge(judges, key) {
  const n = trim(judges?.[key]?.name);
  return n === "UNASSIGNED" ? "" : n; // far-out events carry a placeholder
}

function docs(list) {
  const out = [];
  for (const d of list ?? []) {
    if (d.keyBinary == null) continue;
    const code = trim(d.code);
    out.push({ label: DOC_LABEL[code] || trim(d.name) || "Document", code, url: DOC_URL + d.keyBinary });
  }
  return out;
}

// One AKC record -> the show the page renders, or null without a usable date.
export function mapRecord(rec) {
  const start = trim(rec.startDate).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(start)) return null;

  const site = rec.site ?? {};
  const supt = rec.superintendentSecretary ?? {};
  const conf = (rec.items ?? []).find((i) => i.competitionGroupCode === CONF) ?? {};
  const coords = site.coordinates ?? {};

  // site.name and location1 are frequently identical
  const venue = [...new Set([trim(site.name), trim(site.location1), trim(site.location2)].filter(Boolean))].join(", ");

  // Bad timestamps exist; ISO dates compare as strings, so enforce open < close < start.
  let open = msDate(conf.openingDate);
  let close = msDate(conf.closingDate);
  if (close && !(close < start)) close = null;
  if (open && close && !(open < close)) open = null;
  if (open && !(open < start)) open = null;

  const methodCode = trim(conf.competitionMethodCode);
  const specialty = conf.bvgSpecialty || "";
  const end = trim(rec.endDate).slice(0, 10) || start;

  return {
    event_no: trim(rec.eventNumber),
    start,
    end,
    club: trim(rec.clubName) || "Unknown club",
    comp_type: trim(rec.eventType),
    high_value: Boolean(methodCode === "LB" && specialty),
    specialty,
    pended: rec.eventStatus === "Pended",
    close: close || "",
    open: open || "",
    fee: (conf.entryFee ?? [])[0] || "",
    online: rec.isAcceptingOnlineEntries ? 1 : "",
    superint: trim(supt.name),
    supt_phone: supt.phone || "",
    supt_email: supt.email || "",
    breed_judge: judge(rec.judges, "breedJudge"),
    group_judge: judge(rec.judges, "groupJudge"),
    nohs_judge: judge(rec.judges, "nohsGroupJudge"),
    bis_judge: judge(rec.judges, "bestInShowJudge"),
    clcy: rec.completedLastYear || "",
    docs: docs(rec.documents),
    venue,
    city: trim(rec.city),
    state: trim(rec.state),
    lat: coords.lat ?? null,
    lon: coords.lon ?? null,
  };
}
