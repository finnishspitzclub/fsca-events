#!/usr/bin/env python3
"""
akcal - AKC event feed -> normalized store -> iCalendar

Subcommands:
    probe   Discover the request schema of the AKC event-search endpoint.
    fetch   Pull events for a date range into the local SQLite store.
    gcal    Push the store to Google Calendar, colored by timezone.
    map     Render a self-contained HTML US map of a month's events.
    sync    fetch (rolling window) -> gcal -> map. The cron entrypoint.
    ics     Render the store to an .ics file.
    show    Print what's in the store.

National Finnish Spitz feed for the club events page. `sync` is what the cron
runs; everything else is for manual/debug use.

Notes before you run this:
  - The endpoint is undocumented. It can change shape without warning.
    `probe` exists so that when it breaks you can re-derive it in a minute.
  - Set CONTACT below to a real email. An honest User-Agent on an
    undocumented endpoint is the difference between "identifiable person
    pulling a small feed" and "anonymous bot". It also gives them somewhere
    to send a complaint other than a firewall rule.
  - Default pacing is deliberately slow. You are pulling a calendar a few
    times a week, not running a search engine.
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import hashlib
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("pip install requests")

# ---------------------------------------------------------------- config

ENDPOINT = "https://webapps.akc.org/event-search/api/search/events"
CONTACT = "ethan.waterman@hotmail.com"   # <- put a real address here
UA = f"akcal/1.0 (personal dog-show calendar; {CONTACT})"

DB_PATH = Path(__file__).with_name("events.db")
ICS_PATH = Path(__file__).with_name("akc-events.ics")
MAP_PATH = Path(__file__).with_name("events-map.html")

REQUEST_DELAY = 2.0                  # seconds between calls
POST_TIMEOUT = 90                    # server took 26s for a 1yr CO query; 30 is too tight
MAX_RETRIES = 3

# The search is breed-scoped. That is not incidental: completedLastYear (the
# per-event count of THIS breed's entries last year) only comes back when the
# request names the breed. So the store is Finnish-Spitz specific by design -
# a general all-breed store would lose the one number that makes this useful.
# breedCode's trailing space is real; do not strip it.
BREED_CODE = "313 "
BREED_NAME = "Finnish Spitz"
BREED_ID = "SPECIFIC"

# This is a NATIONAL feed (Finnish Spitz Club of America event map). One request
# per state per year window - multi-state in a single call is untested and the
# 1000/call cap makes per-state the safe way. ~50 states x 2s pacing is a couple
# of minutes per year window, fine for a twice-daily cron.
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY",
]
FETCH_STATES = ALL_STATES

# Render filter. National, so: everything.
MY_STATES = set(ALL_STATES)
MY_BREED = BREED_NAME

# ---------------------------------------------------------------- google cal
# Push target. Set these via env (preferred for cron) or edit here.
#   AKCAL_GCAL_CALENDAR_ID - the target calendar's ID. Find it in Google
#     Calendar: Settings -> <your calendar> -> "Integrate calendar" -> Calendar
#     ID (looks like ...@group.calendar.google.com).
#   AKCAL_GCAL_KEY - path to the service-account JSON key file.
GCAL_CALENDAR_ID = os.environ.get("AKCAL_GCAL_CALENDAR_ID", "")
GCAL_KEY_FILE = os.environ.get(
    "AKCAL_GCAL_KEY", str(Path(__file__).with_name("service-account.json")))

GCAL_SCOPE = "https://www.googleapis.com/auth/calendar"
GCAL_API = "https://www.googleapis.com/calendar/v3"

# Rolling window `sync` fetches, so cron never needs date edits.
SYNC_BACK_DAYS = 7          # keep just-closed shows on the calendar briefly
SYNC_AHEAD_DAYS = 540       # ~18 months out

# Color by US timezone - ~6 clean buckets that cover all 50 states, which reads
# far better on a national map than any regional clustering. Each state maps to
# its majority timezone (a few states straddle a line; majority is fine for a
# color). Arizona folds into Mountain.
STATE_TZ = {
    # Eastern
    "CT": "ET", "DE": "ET", "DC": "ET", "FL": "ET", "GA": "ET", "IN": "ET",
    "KY": "ET", "ME": "ET", "MD": "ET", "MA": "ET", "MI": "ET", "NH": "ET",
    "NJ": "ET", "NY": "ET", "NC": "ET", "OH": "ET", "PA": "ET", "RI": "ET",
    "SC": "ET", "VT": "ET", "VA": "ET", "WV": "ET",
    # Central
    "AL": "CT", "AR": "CT", "IL": "CT", "IA": "CT", "KS": "CT", "LA": "CT",
    "MN": "CT", "MS": "CT", "MO": "CT", "NE": "CT", "ND": "CT", "OK": "CT",
    "SD": "CT", "TN": "CT", "TX": "CT", "WI": "CT",
    # Mountain (+ Arizona)
    "AZ": "MT", "CO": "MT", "ID": "MT", "MT": "MT", "NM": "MT", "UT": "MT",
    "WY": "MT",
    # Pacific
    "CA": "PT", "NV": "PT", "OR": "PT", "WA": "PT",
    # Alaska / Hawaii
    "AK": "AKT", "HI": "HAT",
}

# Google Calendar colorId per timezone (11 named colors; ids used below:
# 1 Lavender, 3 Grape, 6 Tangerine, 9 Blueberry, 10 Basil, 11 Tomato).
TZ_COLOR = {
    "ET": "9",    # Blueberry
    "CT": "10",   # Basil
    "MT": "6",    # Tangerine
    "PT": "11",   # Tomato
    "AKT": "3",   # Grape
    "HAT": "1",   # Lavender
}
DEFAULT_COLOR = "8"   # Graphite - unknown/unmapped state

# Map pin colors (hex) per timezone, tuned to match the calendar palette.
TZ_HEX = {
    "ET": "#3949AB",    # blue
    "CT": "#43A047",    # green
    "MT": "#FB8C00",    # orange
    "PT": "#E53935",    # red
    "AKT": "#8E24AA",   # purple
    "HAT": "#7986CB",   # lavender
}
DEFAULT_HEX = "#757575"

TZ_LABEL = {"ET": "Eastern", "CT": "Central", "MT": "Mountain",
            "PT": "Pacific", "AKT": "Alaska", "HAT": "Hawaii"}

# Private extended property that tags every event this tool owns, so `gcal` can
# find and reconcile them without touching anything else on the calendar.
GCAL_MARK_KEY = "akcalSource"
GCAL_MARK_VAL = "1"

# ---------------------------------------------------------------- http


def _session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://webapps.akc.org",
        "Referer": "https://webapps.akc.org/event-search/",
    })
    return s


def _post(session, payload, timeout=POST_TIMEOUT):
    """POST with backoff. Returns (status, parsed_or_text)."""
    delay = REQUEST_DELAY
    for attempt in range(MAX_RETRIES):
        try:
            r = session.post(ENDPOINT, json=payload, timeout=timeout)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                return None, f"network error: {e}"
            time.sleep(delay)
            delay *= 2
            continue

        if r.status_code == 429 or r.status_code >= 500:
            if attempt == MAX_RETRIES - 1:
                return r.status_code, r.text[:2000]
            time.sleep(delay)
            delay *= 2
            continue

        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, r.text[:2000]
    return None, "exhausted retries"


# ---------------------------------------------------------------- probe

# Candidate payload shapes. The endpoint sits behind a JS front end, so the
# real shape is whatever that front end sends - open devtools on
# webapps.akc.org/event-search, run a search, and copy the request body.
# These are educated guesses to try first.
PROBE_PAYLOADS = [
    ("empty", {}),
    ("minimal-dates", {
        "startDate": "2027-04-01",
        "endDate": "2027-04-14",
    }),
    ("camel-full", {
        "startDate": "2027-04-01",
        "endDate": "2027-04-14",
        "competitionType": ["CH"],
        "state": ["CO"],
        "pageSize": 100,
        "page": 0,
    }),
    ("snake", {
        "start_date": "2027-04-01",
        "end_date": "2027-04-14",
        "competition_type": ["CH"],
    }),
    ("nested-filters", {
        "filters": {
            "startDate": "2027-04-01",
            "endDate": "2027-04-14",
        },
        "size": 100,
    }),
    ("iso-datetime", {
        "startDate": "2027-04-01T00:00:00.000Z",
        "endDate": "2027-04-14T00:00:00.000Z",
    }),
]


def cmd_probe(args):
    s = _session()
    print(f"endpoint: {ENDPOINT}")
    print(f"UA: {UA}\n")
    if CONTACT == "you@example.com":
        print("!! CONTACT is still the placeholder. Set it before real use.\n")

    # Lead with the known-good shape so probe doubles as a "still works?" check.
    from datetime import date as _date
    payloads = [("verified", build_payload(_date(2027, 4, 1), _date(2027, 4, 14)))]
    payloads += PROBE_PAYLOADS

    for name, payload in payloads:
        print(f"--- {name} ---")
        print(f"    payload: {json.dumps(payload)[:120]}")
        status, body = _post(s, payload)
        print(f"    status:  {status}")
        if isinstance(body, (dict, list)):
            print(f"    type:    {type(body).__name__}")
            if isinstance(body, dict):
                print(f"    keys:    {list(body.keys())[:15]}")
                # find the array of events wherever it lives
                for k, v in body.items():
                    if isinstance(v, list) and v:
                        print(f"    '{k}' is a list of {len(v)}")
                        if isinstance(v[0], dict):
                            print(f"      record keys: {list(v[0].keys())}")
                            print(f"      sample: {json.dumps(v[0], default=str)[:600]}")
                        break
            elif body:
                print(f"    len:     {len(body)}")
                if isinstance(body[0], dict):
                    print(f"    record keys: {list(body[0].keys())}")
        else:
            print(f"    body:    {str(body)[:400]}")
        print()
        time.sleep(REQUEST_DELAY)

    print("Whichever shape returns records is your schema. Wire it into")
    print("build_payload() and map_record() below, then run `fetch`.")


# ---------------------------------------------------------------- schema
# Fill these two in once `probe` tells you the real shape.

# Verified against the live endpoint 2026-07-29 by capturing the real request
# the event-search front end sends. The server validates the whole object and
# returns a generic 'Missing "dateRange" parameter' for any partial body - the
# three things that actually unlock it are dateRange.type, the competition
# object, and the address/breed skeleton below.
#
# compType "AB/LB" = all-breed & group conformation, which is what a
# Finnish Spitz owner tracks. filters is a list of OBJECTS, not strings;
# limitCode "NOHS" matches the captured request.
SEARCH_COMPETITION = {
    "items": [
        {"selected": True, "value": {"compType": "AB/LB"},
         "label": "All- Breed and Group (AB/LB)"},
    ],
    "filters": [{"compType": "AB/LB", "limitCode": "NOHS"}],
}


def build_payload(start: date, end: date, state: str = "CO"):
    """
    Request body for one date window in one state. Captured from devtools and
    verified (HTTP 200, ~30 events for a one-year CO query).

    The gotchas that cost time, so they don't cost it again:
      - dateRange.type must be "event" (not "startDate").
      - dates are MM/DD/YYYY, not ISO.
      - the address block has NO location/radius keys.
      - a bad breedCode returns 'Missing "dateRange" parameter' - the error
        text lies, so don't trust it when debugging.
    """
    return {
        "address": {
            "states": state,
            "eventSetting": {"indoor": True, "outdoor": True, "outsideCovered": True},
            "searchByState": True,
            "searchByCity": False,
            "searchText": "All States",
        },
        "breedCode": BREED_CODE,      # trailing space is REAL, do not strip
        "breedName": BREED_NAME,
        "breedId": BREED_ID,
        "dateRange": {
            "from": start.strftime("%m/%d/%Y"),
            "to": end.strftime("%m/%d/%Y"),
            "type": "event",
        },
        "competition": SEARCH_COMPETITION,
    }


def extract_records(body):
    """Pull the event list out of the response. ADJUST AFTER PROBING."""
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("events", "results", "data", "items", "content"):
            v = body.get(key)
            if isinstance(v, list):
                return v
        for v in body.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return []


# Closing timestamps resolve to noon Eastern (16:00Z summer / 17:00Z winter)
# or midnight Eastern (04:00Z) - NOT the superintendent's office zone, as an
# earlier reading of this data assumed. All of those instants fall on the same
# calendar day in UTC, so a plain UTC->date conversion gives the correct date.
# Treat the API value as a DATE; the exact time-of-day comes from the premium
# list. (The per-item timeZone field is separate show-local metadata.)
def _ms(v):
    """AKC epoch-millis -> 'YYYY-MM-DD', or None."""
    if not v:
        return None
    try:
        v = int(v)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(v / 1000, tz=timezone.utc).date().isoformat()


CONF = "CONF"   # competitionGroupCode for conformation


def parse_completed_last_year(s):
    """'5-1-2 (1-1) 0' -> dict, or None if absent/unparseable."""
    if not s:
        return None
    m = re.match(r"\s*(\d+)-(\d+)-(\d+)\s*\((\d+)-(\d+)\)\s*(\d+)\s*$", s)
    if not m:
        return None
    total, cd, cb, sd, sb, vet = (int(x) for x in m.groups())
    return {"total": total, "class_dogs": cd, "class_bitches": cb,
            "special_dogs": sd, "special_bitches": sb, "veterans": vet,
            "consistent": (cd + cb + sd + sb) == total}


def _strip_raw(rec: dict) -> dict:
    """
    Copy of the record with items[].breeds[] removed. That array is a 200+
    entry breed list repeated on every item of every event - essentially the
    whole 382 KB payload. Keep everything else; a mapping bug stays fixable
    from the store.
    """
    slim = dict(rec)
    items = []
    for it in (rec.get("items") or []):
        it = dict(it)
        it.pop("breeds", None)
        items.append(it)
    if "items" in rec:
        slim["items"] = items
    return slim


# The normalized columns, in one place so the table DDL and the upsert stay in
# sync. event_no is the primary key; the rest are payload.
NORM_COLUMNS = [
    "event_no", "show_id", "club", "start_date", "end_date", "city", "state",
    "venue", "postal", "lat", "lon", "comp_type", "status", "superint",
    "supt_phone", "supt_email", "open_date", "close_date", "entry_fee",
    "inside_out", "time_zone", "method", "method_code", "specialty",
    "high_value", "breed_judge", "group_judge", "bis_judge",
    "nohs_group_judge", "completed_last_year", "clcy_year", "online_entries",
    "documents",
]


def map_record(rec: dict) -> dict:
    """
    Normalize one AKC record to our schema. Field names verified against a live
    devtools capture (see SCHEMA.md).

    Most fields are top-level; venue, superintendent and judges are nested, and
    the entry window lives on the conformation item (competitionGroupCode
    "CONF") as epoch millis. The stripped raw blob is stored alongside, so a
    mapping bug is fixable without re-fetching.
    """
    site = rec.get("site") or {}
    supt = rec.get("superintendentSecretary") or {}
    jud  = rec.get("judges") or {}

    # The conformation item carries the entry window, fees and method we track.
    conf = next((i for i in (rec.get("items") or [])
                 if i.get("competitionGroupCode") == CONF), {})

    def judge(key):
        j = jud.get(key) or {}
        n = (j.get("name") or "").strip()
        # far-out events carry placeholders instead of a real panel
        return None if n in ("", "UNASSIGNED") else n

    # site.name and location1 are frequently identical; dedupe while keeping order
    venue_parts, seen = [], set()
    for p in ((site.get("name") or "").strip(),
              (site.get("location1") or "").strip(),
              (site.get("location2") or "").strip()):
        if p and p not in seen:
            seen.add(p); venue_parts.append(p)
    venue = ", ".join(venue_parts)

    open_date  = _ms(conf.get("openingDate"))
    close_date = _ms(conf.get("closingDate"))
    start_date = (rec.get("startDate") or "").strip() or None

    # Bad timestamps exist (an opening dated 2028, after its own closing). ISO
    # date strings compare lexicographically, so enforce open < close < start
    # and null out whatever violates it rather than trust it.
    if close_date and start_date and not (close_date < start_date):
        close_date = None
    if open_date and close_date and not (open_date < close_date):
        open_date = None
    if open_date and start_date and not (open_date < start_date):
        open_date = None

    method_code = (conf.get("competitionMethodCode") or "").strip() or None
    specialty   = conf.get("bvgSpecialty")
    coords      = site.get("coordinates") or {}

    # clcy describes the year BEFORE the show: a 2027 event's count is 2026's.
    clcy_year = None
    if start_date and len(start_date) >= 4 and start_date[:4].isdigit():
        clcy_year = int(start_date[:4]) - 1

    documents = [
        {"name": (d.get("name") or "").strip(),
         "code": (d.get("code") or "").strip(),
         "keyBinary": d.get("keyBinary")}
        for d in (rec.get("documents") or [])
    ]

    return {
        "event_no":    str(rec.get("eventNumber") or "").strip(),
        "show_id":     rec.get("id"),
        "club":        (rec.get("clubName") or "").strip(),
        "start_date":  start_date,                       # already ISO yyyy-mm-dd
        "end_date":    (rec.get("endDate") or "").strip() or None,
        "city":        (rec.get("city") or "").strip(),
        "state":       (rec.get("state") or "").strip(),
        "venue":       venue,
        "postal":      (site.get("postalCode") or "").strip(),
        "lat":         coords.get("lat"),
        "lon":         coords.get("lon"),
        "comp_type":   (rec.get("eventType") or "").strip(),
        "status":      rec.get("eventStatus"),           # Approved | Pended
        "superint":    (supt.get("name") or "").strip(),
        "supt_phone":  supt.get("phone"),
        "supt_email":  supt.get("email"),
        "open_date":   open_date,
        "close_date":  close_date,
        "entry_fee":   (conf.get("entryFee") or [None])[0],
        "inside_out":  (conf.get("insideOut") or "").strip(),
        "time_zone":   conf.get("timeZone"),             # show-local metadata
        "method":      conf.get("competitionMethod"),    # All Breed | Limited Breed
        "method_code": method_code,                      # AB | LB
        "specialty":   specialty,
        # LB + a group specialty (e.g. Non-Sporting) is higher-value than a
        # plain all-breed show for this dog. Flag it.
        "high_value":  1 if (method_code == "LB" and specialty) else 0,
        "breed_judge":      judge("breedJudge"),
        "group_judge":      judge("groupJudge"),
        "bis_judge":        judge("bestInShowJudge"),
        "nohs_group_judge": judge("nohsGroupJudge"),
        "completed_last_year": rec.get("completedLastYear"),
        "clcy_year":   clcy_year,
        "online_entries": 1 if rec.get("isAcceptingOnlineEntries") else 0,
        "documents":   json.dumps(documents) if documents else None,
    }


# ---------------------------------------------------------------- store

# Column types for the normalized fields (everything else defaults to TEXT).
_COL_TYPE = {
    "event_no": "TEXT PRIMARY KEY",
    "lat": "REAL", "lon": "REAL", "entry_fee": "REAL",
    "high_value": "INTEGER", "online_entries": "INTEGER", "clcy_year": "INTEGER",
}

# Bookkeeping columns appended after the mapped fields.
_META_COLS = ["raw", "fingerprint", "first_seen", "last_seen", "seq"]


def _ddl():
    cols = [f"    {c} {_COL_TYPE.get(c, 'TEXT')}" for c in NORM_COLUMNS]
    cols += ["    raw TEXT", "    fingerprint TEXT",
             "    first_seen TEXT", "    last_seen TEXT", "    seq INTEGER DEFAULT 0"]
    return "CREATE TABLE IF NOT EXISTS events (\n" + ",\n".join(cols) + "\n);\n"


def _migrate(conn):
    """Add any columns missing from a pre-existing events.db. Non-destructive;
    old rows get NULLs in new columns until the next fetch refreshes them."""
    have = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
    for c in NORM_COLUMNS + _META_COLS:
        if c == "event_no" or c in have:
            continue
        coltype = _COL_TYPE.get(c, "TEXT")
        if c == "seq":
            coltype = "INTEGER DEFAULT 0"
        elif "PRIMARY KEY" in coltype:      # can't add a PK column after the fact
            coltype = "TEXT"
        conn.execute(f"ALTER TABLE events ADD COLUMN {c} {coltype}")


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(_ddl())
    _migrate(conn)   # add columns a pre-existing table lacks...
    # ...then index, so the indexed columns are guaranteed to exist.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_start ON events(start_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_state ON events(state)")
    return conn


_INSERT_COLS = NORM_COLUMNS + ["raw", "fingerprint", "first_seen", "last_seen", "seq"]
_INSERT_SQL = (
    f"INSERT INTO events ({', '.join(_INSERT_COLS)}) "
    f"VALUES ({', '.join('?' * len(_INSERT_COLS))})"
)
_UPDATE_COLS = [c for c in NORM_COLUMNS if c != "event_no"] + ["raw", "fingerprint", "last_seen"]
_UPDATE_SQL = (
    "UPDATE events SET " + ", ".join(f"{c}=?" for c in _UPDATE_COLS)
    + ", seq=seq+1 WHERE event_no=?"
)


def upsert(conn, norm: dict, raw: dict):
    """Insert or update. Bumps seq only when mapped content actually changed."""
    fp = hashlib.sha256(
        json.dumps({k: norm[k] for k in sorted(norm)}, default=str).encode()
    ).hexdigest()[:16]
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    raw_json = json.dumps(_strip_raw(raw), default=str)

    row = conn.execute(
        "SELECT fingerprint FROM events WHERE event_no = ?",
        (norm["event_no"],),
    ).fetchone()

    if row is None:
        vals = [norm[c] for c in NORM_COLUMNS] + [raw_json, fp, now, now, 0]
        conn.execute(_INSERT_SQL, vals)
        return "new"

    if row["fingerprint"] != fp:
        vals = [norm[c] for c in NORM_COLUMNS if c != "event_no"]
        vals += [raw_json, fp, now, norm["event_no"]]
        conn.execute(_UPDATE_SQL, vals)
        return "changed"

    conn.execute("UPDATE events SET last_seen=? WHERE event_no=?",
                 (now, norm["event_no"]))
    return "same"


# ---------------------------------------------------------------- fetch


def _year_windows(start: date, end: date):
    """Yield (win_start, win_end) chunks no longer than a calendar year. A
    one-year breed query returns ~30 events, nowhere near the 1000 cap, so
    there is no need to slice finer - but multi-year ranges still get split so
    no single call can approach the cap."""
    win_start = start
    while win_start <= end:
        win_end = min(date(win_start.year, 12, 31), end)
        yield win_start, win_end
        win_start = win_end + timedelta(days=1)


def cmd_fetch(args):
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    states = args.state or FETCH_STATES
    s = _session()
    conn = db()
    tally = {"new": 0, "changed": 0, "same": 0, "skipped": 0}

    for st in states:
        for win_start, win_end in _year_windows(start, end):
            print(f"  {st}  {win_start} .. {win_end}", end=" ", flush=True)

            status, body = _post(s, build_payload(win_start, win_end, st))
            if status != 200:
                print(f"-> HTTP {status}")
                if args.strict:
                    sys.exit(f"aborting: {str(body)[:300]}")
                time.sleep(REQUEST_DELAY)
                continue

            records = extract_records(body)
            print(f"-> {len(records)} records", end="")
            if len(records) >= 1000:
                print("  !! hit the 1000-event cap, results truncated", end="")

            for rec in records:
                norm = map_record(rec)
                if not norm["event_no"]:
                    tally["skipped"] += 1
                    continue
                tally[upsert(conn, norm, rec)] += 1

            conn.commit()
            print()
            time.sleep(REQUEST_DELAY)

    conn.close()
    print(f"\nnew={tally['new']} changed={tally['changed']} "
          f"unchanged={tally['same']} skipped={tally['skipped']}")
    if tally["skipped"]:
        print("skipped rows had no event number - map_record() needs fixing")


# ---------------------------------------------------------------- ics


def _ics_escape(t):
    if t is None:
        return ""
    return (str(t).replace("\\", "\\\\").replace(";", r"\;")
            .replace(",", r"\,").replace("\n", r"\n"))


def _fold(line):
    """RFC 5545 wants lines <= 75 octets."""
    out, cur = [], line
    while len(cur.encode()) > 75:
        cut = 74
        while len(cur[:cut].encode()) > 75:
            cut -= 1
        out.append(cur[:cut])
        cur = " " + cur[cut:]
    out.append(cur)
    return "\r\n".join(out)


def _col(row, name, default=None):
    """Row column access that tolerates a column being absent."""
    try:
        return row[name]
    except (IndexError, KeyError):
        return default


def _parse_date(v):
    if not v:
        return None
    v = str(v)[:10]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            pass
    return None


def cmd_ics(args):
    conn = db()
    rows = conn.execute(
        "SELECT * FROM events ORDER BY start_date"
    ).fetchall()
    conn.close()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    L = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//akcal//AKC event feed//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{_ics_escape(args.name)}",
        "X-WR-TIMEZONE:America/Denver",
        "REFRESH-INTERVAL;VALUE=DURATION:PT12H",
        "X-PUBLISHED-TTL:PT12H",
    ]

    emitted = 0
    for r in rows:
        if args.states and (r["state"] or "") not in MY_STATES:
            continue
        sd = _parse_date(r["start_date"])
        if not sd:
            continue
        ed = _parse_date(r["end_date"]) or sd

        pended = _col(r, "status") == "Pended"
        high_value = bool(_col(r, "high_value"))

        title = f"{r['club'] or 'Unknown club'}"
        if r["comp_type"]:
            title += f" ({r['comp_type']})"
        # LB + group specialty is the reason to prioritize a weekend; mark it.
        if high_value:
            title = "★ " + title
        if pended:
            title += " [PENDED]"
        where = ", ".join(x for x in (r["city"], r["state"]) if x)

        desc_bits = [f"AKC event #{r['event_no']}"]
        if pended:
            desc_bits.append("STATUS: PENDED - applied for, not confirmed; "
                             "dates can still move.")
        if r["venue"]:
            desc_bits.append(f"Venue: {r['venue']}")
        spec = _col(r, "specialty")
        if high_value and spec:
            desc_bits.append(f"Specialty: {spec} (limited breed)")
        if r["superint"]:
            supt = f"Superintendent: {r['superint']}"
            phone = _col(r, "supt_phone")
            if phone:
                supt += f" ({phone})"
            desc_bits.append(supt)
        # Judge panel - only present/real for events that aren't far out.
        judges = [(lbl, _col(r, col)) for lbl, col in (
            ("Breed", "breed_judge"), ("Group", "group_judge"),
            ("BIS", "bis_judge"))]
        judges = [f"{lbl}: {name}" for lbl, name in judges if name]
        if judges:
            desc_bits.append("Judges - " + "; ".join(judges))
        clcy = _col(r, "completed_last_year")
        clcy_year = _col(r, "clcy_year")
        if clcy:
            yr = f" ({clcy_year})" if clcy_year else ""
            desc_bits.append(f"{MY_BREED} entered{yr}: {clcy}")
        fee = _col(r, "entry_fee")
        if fee:
            desc_bits.append(f"Entry fee: ${fee}")
        if r["close_date"]:
            desc_bits.append(f"Entries close: {r['close_date']}")
        desc_bits.append(
            "https://www.apps.akc.org/apps/events/search/index_results.cfm"
            f"?action=plan&event_number={r['event_no']}"
        )

        # the show itself - all-day, DTEND is exclusive
        L += [
            "BEGIN:VEVENT",
            f"UID:akc-{r['event_no']}@akcal",
            f"DTSTAMP:{stamp}",
            f"SEQUENCE:{r['seq']}",
            f"DTSTART;VALUE=DATE:{sd.strftime('%Y%m%d')}",
            f"DTEND;VALUE=DATE:{(ed + timedelta(days=1)).strftime('%Y%m%d')}",
            _fold(f"SUMMARY:{_ics_escape(title)}"),
            _fold(f"LOCATION:{_ics_escape(r['venue'] or where)}"),
            _fold("DESCRIPTION:" + _ics_escape("\n".join(desc_bits))),
            "TRANSP:TRANSPARENT",
            "END:VEVENT",
        ]
        emitted += 1

        # the deadline - this is the part that actually saves you money
        cd = _parse_date(r["close_date"])
        if cd:
            L += [
                "BEGIN:VEVENT",
                f"UID:akc-{r['event_no']}-close@akcal",
                f"DTSTAMP:{stamp}",
                f"SEQUENCE:{r['seq']}",
                f"DTSTART;VALUE=DATE:{cd.strftime('%Y%m%d')}",
                f"DTEND;VALUE=DATE:{(cd + timedelta(days=1)).strftime('%Y%m%d')}",
                _fold(f"SUMMARY:{_ics_escape('ENTRIES CLOSE - ' + (r['club'] or ''))}"),
                _fold("DESCRIPTION:" + _ics_escape(
                    f"Closing date (a Wednesday). The API gives the date only; "
                    f"take the exact cutoff time from the premium list.\n"
                    f"Show: {sd} in {where}\n" + desc_bits[-1])),
                "BEGIN:VALARM",
                "TRIGGER:-P3D",
                "ACTION:DISPLAY",
                _fold(f"DESCRIPTION:{_ics_escape('Entries close in 3 days: ' + (r['club'] or ''))}"),
                "END:VALARM",
                "END:VEVENT",
            ]
            emitted += 1

    L.append("END:VCALENDAR")
    # write_bytes, not write_text: text mode on Windows would translate every
    # "\n" to "\r\n", turning our CRLFs into "\r\r\n" and breaking strict parsers.
    ICS_PATH.write_bytes(("\r\n".join(L) + "\r\n").encode("utf-8"))
    print(f"wrote {ICS_PATH}  ({emitted} VEVENTs from {len(rows)} stored events)")


def cmd_show(args):
    conn = db()
    q = "SELECT * FROM events"
    if args.states:
        q += " WHERE state IN (%s)" % ",".join("?" * len(MY_STATES))
        rows = conn.execute(q + " ORDER BY start_date", tuple(MY_STATES)).fetchall()
    else:
        rows = conn.execute(q + " ORDER BY start_date").fetchall()
    for r in rows:
        flag = "★" if _col(r, "high_value") else " "
        status = "PEND" if _col(r, "status") == "Pended" else "    "
        print(f"{flag} {r['start_date']}  {r['state'] or '--':3} {status} "
              f"{(r['club'] or '')[:36]:38} {r['event_no']:12} "
              f"close={r['close_date'] or '-'}")
    print(f"\n{len(rows)} events")
    conn.close()


# ---------------------------------------------------------------- google cal
#
# Why the API and not the .ics feed: Google renders a *subscribed* ICS feed in
# one flat color and ignores per-event color hints. Per-event colors exist only
# through the Calendar API's colorId, so region colors require writing events.
#
# The design is idempotent. Each show yields two events with deterministic ids
# (show span + closing deadline); re-running upserts in place. We tag every
# event with a private extended property and, on each run, delete any of *our*
# tagged events that are no longer in the store (canceled / fell out of range).


# Google event ids must be base32hex: lowercase a-v and 0-9, length 5-1024.
_ID_OK = set("0123456789abcdefghijklmnopqrstuv")


def _gcal_id(event_no, suffix):
    """Deterministic, valid event id from an AKC event number."""
    s = "".join(c for c in ("akc" + str(event_no) + suffix).lower() if c in _ID_OK)
    return s


def _tz(state):
    return STATE_TZ.get((state or "").strip().upper())


def _color_for(state):
    return TZ_COLOR.get(_tz(state), DEFAULT_COLOR)


def _hex_for(state):
    return TZ_HEX.get(_tz(state), DEFAULT_HEX)


def _gcal_text(r):
    """(summary, location, description) for a show row. Plain text - the API
    takes raw strings, no ICS escaping/folding."""
    pended = _col(r, "status") == "Pended"
    high_value = bool(_col(r, "high_value"))
    tzlabel = TZ_LABEL.get(_tz(r["state"]))

    summary = r["club"] or "Unknown club"
    if r["comp_type"]:
        summary += f" ({r['comp_type']})"
    if high_value:
        summary = "★ " + summary
    if pended:
        summary += " [PENDED]"

    where = ", ".join(x for x in (r["city"], r["state"]) if x)
    location = r["venue"] or where

    lines = [f"AKC event #{r['event_no']}"]
    if tzlabel:
        lines.append(f"Timezone: {tzlabel}")
    if pended:
        lines.append("STATUS: PENDED - applied for, not confirmed; dates can move.")
    if r["venue"]:
        lines.append(f"Venue: {r['venue']}")
    spec = _col(r, "specialty")
    if high_value and spec:
        lines.append(f"Specialty: {spec} (limited breed)")
    if r["superint"]:
        supt = f"Superintendent: {r['superint']}"
        phone = _col(r, "supt_phone")
        if phone:
            supt += f" ({phone})"
        lines.append(supt)
    judges = [(lbl, _col(r, c)) for lbl, c in (
        ("Breed", "breed_judge"), ("Group", "group_judge"), ("BIS", "bis_judge"))]
    judges = [f"{lbl}: {name}" for lbl, name in judges if name]
    if judges:
        lines.append("Judges - " + "; ".join(judges))
    clcy = _col(r, "completed_last_year")
    clcy_year = _col(r, "clcy_year")
    if clcy:
        yr = f" ({clcy_year})" if clcy_year else ""
        lines.append(f"{MY_BREED} entered{yr}: {clcy}")
    fee = _col(r, "entry_fee")
    if fee:
        lines.append(f"Entry fee: ${fee}")
    if r["close_date"]:
        lines.append(f"Entries close: {r['close_date']}")
    lines.append("https://www.apps.akc.org/apps/events/search/index_results.cfm"
                 f"?action=plan&event_number={r['event_no']}")
    return summary, location, "\n".join(lines)


def _finalize(body: dict) -> dict:
    """Stamp a body with our marker + a content hash, so a re-run can tell a
    changed event from an unchanged one regardless of the store's seq."""
    h = hashlib.sha256(
        json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()[:16]
    body["extendedProperties"] = {"private": {
        GCAL_MARK_KEY: GCAL_MARK_VAL,
        "akcalEvent": str(body.get("_event_no", "")),
        "akcalHash": h,
    }}
    body.pop("_event_no", None)
    return body


def desired_events(rows):
    """Store rows -> {event_id: body}. Pure; the unit tests hit this."""
    out = {}
    for r in rows:
        if (r["state"] or "") not in MY_STATES:
            continue
        sd = _parse_date(r["start_date"])
        if not sd:
            continue
        ed = _parse_date(r["end_date"]) or sd
        color = _color_for(r["state"])
        summary, location, desc = _gcal_text(r)

        show = _finalize({
            "_event_no": r["event_no"],
            "summary": summary,
            "location": location,
            "description": desc,
            "start": {"date": sd.isoformat()},
            "end": {"date": (ed + timedelta(days=1)).isoformat()},   # exclusive
            "colorId": color,
            "transparency": "transparent",
            "reminders": {"useDefault": False, "overrides": []},
        })
        out[_gcal_id(r["event_no"], "s")] = show

        cd = _parse_date(r["close_date"])
        if cd:
            close = _finalize({
                "_event_no": r["event_no"],
                "summary": "ENTRIES CLOSE - " + (r["club"] or ""),
                "location": location,
                "description": (
                    "Closing date (a Wednesday). The API gives the date only; "
                    "take the exact cutoff time from the premium list.\n"
                    f"Show: {sd} in {location}\n" + desc.splitlines()[-1]),
                "start": {"date": cd.isoformat()},
                "end": {"date": (cd + timedelta(days=1)).isoformat()},
                "colorId": color,
                "transparency": "transparent",
                # popup 3 days before, mirroring the ICS VALARM -P3D
                "reminders": {"useDefault": False,
                              "overrides": [{"method": "popup", "minutes": 3 * 24 * 60}]},
            })
            out[_gcal_id(r["event_no"], "c")] = close
    return out


def reconcile(desired: dict, existing: dict):
    """Given desired {id: body} and existing {id: event}, return
    (to_insert, to_update, unchanged_ids, to_delete_ids)."""
    ins, upd, same, dele = [], [], [], []
    for eid, body in desired.items():
        cur = existing.get(eid)
        if cur is None:
            ins.append((eid, body))
            continue
        cur_hash = ((cur.get("extendedProperties") or {}).get("private") or {}).get("akcalHash")
        new_hash = body["extendedProperties"]["private"]["akcalHash"]
        (same if cur_hash == new_hash else upd).append((eid, body))
    dele = [eid for eid in existing if eid not in desired]
    return ins, [x for x in upd], [x[0] for x in same], dele


# --- side-effectful: token + REST ---

def _gcal_token():
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request as GRequest
    except ImportError:
        sys.exit("pip install google-auth   (needed for `gcal` / `sync`)")
    if not Path(GCAL_KEY_FILE).exists():
        sys.exit(f"service-account key not found: {GCAL_KEY_FILE}\n"
                 "set AKCAL_GCAL_KEY or drop the JSON key beside akcal.py")
    creds = service_account.Credentials.from_service_account_file(
        GCAL_KEY_FILE, scopes=[GCAL_SCOPE])
    creds.refresh(GRequest())
    return creds.token


def _gcal_session(token):
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}",
                      "Content-Type": "application/json"})
    return s


def _gcal_write(s, method, url, data=None, tries=6):
    """One Calendar API write with backoff on rate-limit / transient errors.

    A big first sync fires ~1800 writes; Google will intermittently answer 403
    userRateLimitExceeded or 429. Without this a single such reply would abort
    the whole run. Returns the final Response for the caller to inspect."""
    delay = 1.0
    r = None
    for attempt in range(tries):
        r = s.request(method, url, data=data, timeout=60)
        transient = r.status_code in (429, 500, 502, 503, 504) or (
            r.status_code == 403 and "ratelimit" in r.text.lower())
        if not transient or attempt == tries - 1:
            return r
        ra = r.headers.get("Retry-After")
        time.sleep(float(ra) if (ra and ra.isdigit()) else delay)
        delay = min(delay * 2, 60)
    return r


def _gcal_list_ours(s, cal):
    """All events on `cal` that carry our marker property."""
    out, page = {}, None
    while True:
        params = {"privateExtendedProperty": f"{GCAL_MARK_KEY}={GCAL_MARK_VAL}",
                  "maxResults": 2500, "showDeleted": "false", "singleEvents": "true"}
        if page:
            params["pageToken"] = page
        r = s.get(f"{GCAL_API}/calendars/{cal}/events", params=params, timeout=60)
        r.raise_for_status()
        d = r.json()
        for e in d.get("items", []):
            out[e["id"]] = e
        page = d.get("nextPageToken")
        if not page:
            return out


def cmd_gcal(args):
    cal = args.calendar or GCAL_CALENDAR_ID
    if not cal:
        sys.exit("no calendar id - set AKCAL_GCAL_CALENDAR_ID or pass --calendar")

    conn = db()
    rows = conn.execute("SELECT * FROM events ORDER BY start_date").fetchall()
    conn.close()
    desired = desired_events(rows)

    if args.dry_run:
        by_color = {}
        for eid, b in desired.items():
            by_color.setdefault(b["colorId"], []).append(b["summary"])
        cname = {TZ_COLOR[tz]: TZ_LABEL[tz] for tz in TZ_COLOR}
        print(f"[dry-run] calendar: {cal}")
        print(f"[dry-run] {len(desired)} events would be written:\n")
        for color, names in sorted(by_color.items()):
            print(f"  color {color} ({cname.get(color, 'other')}): {len(names)}")
            for n in sorted(set(names)):
                print(f"      {n}")
        return

    token = _gcal_token()
    s = _gcal_session(token)
    existing = _gcal_list_ours(s, cal)
    ins, upd, same, dele = reconcile(desired, existing)

    for eid, body in ins:
        r = _gcal_write(s, "POST", f"{GCAL_API}/calendars/{cal}/events",
                        data=json.dumps(dict(body, id=eid)))
        if r.status_code == 409:   # id exists (e.g. previously soft-deleted) -> update
            r = _gcal_write(s, "PUT", f"{GCAL_API}/calendars/{cal}/events/{eid}",
                            data=json.dumps(body))
        r.raise_for_status()
        time.sleep(0.1)
    for eid, body in upd:
        r = _gcal_write(s, "PUT", f"{GCAL_API}/calendars/{cal}/events/{eid}",
                        data=json.dumps(body))
        r.raise_for_status()
        time.sleep(0.1)

    pruned = 0
    if not args.no_prune:
        for eid in dele:
            r = _gcal_write(s, "DELETE", f"{GCAL_API}/calendars/{cal}/events/{eid}")
            if r.status_code in (200, 204, 410):
                pruned += 1
            time.sleep(0.1)

    print(f"gcal: inserted={len(ins)} updated={len(upd)} unchanged={len(same)} "
          f"deleted={pruned}" + ("  (prune off)" if args.no_prune else ""))


def cmd_sync(args):
    """fetch a rolling window, push to Google Calendar, and regenerate the map.
    One entrypoint for cron so nothing needs editing over time."""
    today = datetime.now().date()
    fetch_args = argparse.Namespace(
        start=(today - timedelta(days=args.back_days)).isoformat(),
        end=(today + timedelta(days=args.ahead_days)).isoformat(),
        state=args.state, strict=False)
    print(f"== fetch {fetch_args.start} .. {fetch_args.end} ==")
    cmd_fetch(fetch_args)
    print("== push calendar ==")
    cmd_gcal(argparse.Namespace(calendar=args.calendar, dry_run=args.dry_run,
                                no_prune=args.no_prune))
    if not args.dry_run:
        print("== regenerate map ==")
        cmd_map(argparse.Namespace(month=None, months=1, out=None))


# ---------------------------------------------------------------- map
#
# A single self-contained HTML file with the current month's events baked in as
# data and pinned on a US map, colored by timezone. Regenerated every cron run,
# so it never needs manual updates. It references Leaflet + OpenStreetMap tiles
# (ubiquitous CDNs) but embeds its own data, so it depends on nothing of ours.


def _h(s):
    """Minimal HTML escape for text baked into the page shell."""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _month_bounds(today, months=1):
    first = today.replace(day=1)
    y, m = first.year, first.month + months
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return first, date(y, m, 1) - timedelta(days=1)


def map_points(rows, first, last):
    """Store rows -> list of map point dicts for events starting in [first,last]
    that have coordinates. Pure; unit-tested."""
    pts = []
    for r in rows:
        sd = _parse_date(r["start_date"])
        if not sd or sd < first or sd > last:
            continue
        lat, lon = _col(r, "lat"), _col(r, "lon")
        if lat is None or lon is None:
            continue
        tz = _tz(r["state"])
        ed = _parse_date(r["end_date"]) or sd
        dates = sd.isoformat() if sd == ed else f"{sd.isoformat()} – {ed.isoformat()}"
        pts.append({
            "lat": lat, "lon": lon,
            "color": _hex_for(r["state"]),
            "tz": TZ_LABEL.get(tz, "—"),
            "club": r["club"] or "Unknown club",
            "where": ", ".join(x for x in (r["city"], r["state"]) if x),
            "venue": r["venue"] or "",
            "dates": dates,
            "close": r["close_date"] or "",
            "clcy": _col(r, "completed_last_year") or "",
            "breed_judge": _col(r, "breed_judge") or "",
            "pended": _col(r, "status") == "Pended",
            "high_value": bool(_col(r, "high_value")),
            "event_no": r["event_no"],
        })
    return pts


def render_map_html(points, title, subtitle):
    """Self-contained Leaflet page with `points` embedded as JSON."""
    legend = "".join(
        f'<span class="lg"><i style="background:{TZ_HEX[tz]}"></i>{TZ_LABEL[tz]}</span>'
        for tz in ("ET", "CT", "MT", "PT", "AKT", "HAT"))
    data = json.dumps(points, separators=(",", ":"))
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_h(title)}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
 integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
<style>
  html,body{{margin:0;height:100%;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#1a1a1a}}
  #wrap{{display:flex;flex-direction:column;height:100%}}
  header{{padding:10px 14px;background:#0f2b46;color:#fff}}
  header h1{{margin:0;font-size:1.05rem}}
  header p{{margin:2px 0 0;font-size:.8rem;opacity:.85}}
  #map{{flex:1 1 auto;min-height:320px}}
  .legend{{padding:8px 14px;background:#f4f6f8;font-size:.8rem;display:flex;flex-wrap:wrap;gap:14px;align-items:center}}
  .lg{{display:inline-flex;align-items:center;gap:5px}}
  .lg i{{width:12px;height:12px;border-radius:50%;display:inline-block;border:1px solid #0003}}
  .pop b{{font-size:.95rem}} .pop{{font-size:.82rem;line-height:1.35}}
  .pop .tag{{display:inline-block;font-size:.68rem;font-weight:700;padding:1px 6px;border-radius:8px;margin-left:4px}}
  .pop .pend{{background:#ffe0b2;color:#8a4b00}} .pop .hv{{background:#ffcdd2;color:#8a0000}}
  .pop a{{color:#0b5cad}}
</style>
</head>
<body>
<div id="wrap">
  <header><h1>{_h(title)}</h1><p>{_h(subtitle)}</p></header>
  <div id="map"></div>
  <div class="legend"><strong>Timezone:</strong>{legend}
    <span style="margin-left:auto;opacity:.7">★ = breed/group specialty &nbsp; PENDED = dates not final</span>
  </div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
 integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const EVENTS = {data};
const map = L.map('map', {{scrollWheelZoom:true}}).setView([39.5,-98.35], 4);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 18, attribution: '&copy; OpenStreetMap contributors'
}}).addTo(map);
function esc(s){{return String(s==null?'':s).replace(/[&<>"]/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));}}
const seen = {{}};
for (const e of EVENTS) {{
  // nudge exact-duplicate coordinates apart so stacked venues stay clickable
  let k = e.lat.toFixed(4)+','+e.lon.toFixed(4);
  const n = (seen[k]=(seen[k]||0)+1)-1;
  const off = n ? (n*0.02) : 0;
  const m = L.circleMarker([e.lat+off, e.lon+off], {{
    radius:7, color:'#fff', weight:1.5, fillColor:e.color, fillOpacity:.95
  }}).addTo(map);
  let h = '<div class="pop"><b>'+(e.high_value?'★ ':'')+esc(e.club)+'</b>';
  if (e.pended) h += '<span class="tag pend">PENDED</span>';
  h += '<br>'+esc(e.dates)+' · '+esc(e.where)+' <span style="opacity:.6">('+esc(e.tz)+')</span>';
  if (e.venue) h += '<br>'+esc(e.venue);
  if (e.close) h += '<br><b>Entries close:</b> '+esc(e.close);
  if (e.clcy) h += '<br>Finnish Spitz entered last yr: '+esc(e.clcy);
  if (e.breed_judge) h += '<br>Breed judge: '+esc(e.breed_judge);
  h += '<br><a target="_blank" rel="noopener" href="https://www.apps.akc.org/apps/events/search/index_results.cfm?action=plan&event_number='+esc(e.event_no)+'">AKC event page →</a></div>';
  m.bindPopup(h);
}}
if (!EVENTS.length) {{
  L.popup().setLatLng([39.5,-98.35]).setContent('No events this period.').openOn(map);
}}
</script>
</body>
</html>
"""


def cmd_map(args):
    month = args.month
    if month:
        y, m = (int(x) for x in month.split("-"))
        anchor = date(y, m, 1)
    else:
        anchor = datetime.now().date()
    first, last = _month_bounds(anchor, getattr(args, "months", 1) or 1)

    conn = db()
    rows = conn.execute("SELECT * FROM events ORDER BY start_date").fetchall()
    conn.close()
    points = map_points(rows, first, last)

    span = first.strftime("%B %Y")
    if (last.year, last.month) != (first.year, first.month):
        span += " – " + last.strftime("%B %Y")
    title = "Finnish Spitz — AKC Events"
    subtitle = (f"{span} · {len(points)} shows · "
                f"updated {datetime.now().strftime('%b %-d, %Y')}"
                if os.name != "nt" else
                f"{span} · {len(points)} shows · "
                f"updated {datetime.now().strftime('%b %d, %Y')}")
    out = Path(args.out) if getattr(args, "out", None) else MAP_PATH
    out.write_text(render_map_html(points, title, subtitle), encoding="utf-8")
    print(f"wrote {out}  ({len(points)} events, {first} .. {last})")


# ---------------------------------------------------------------- cli


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("probe", help="discover the request schema")

    f = sub.add_parser("fetch", help="pull events into the store")
    f.add_argument("--start", required=True, help="YYYY-MM-DD")
    f.add_argument("--end", required=True, help="YYYY-MM-DD")
    f.add_argument("--state", action="append", metavar="XX",
                   help=f"state to query; repeatable. default: {FETCH_STATES}")
    f.add_argument("--strict", action="store_true",
                   help="abort on first HTTP error instead of skipping")

    i = sub.add_parser("ics", help="render the store to .ics")
    i.add_argument("--name", default="AKC Shows", help="calendar display name")
    i.add_argument("--states", action="store_true",
                   help=f"limit to {sorted(MY_STATES)}")

    sh = sub.add_parser("show", help="print the store")
    sh.add_argument("--states", action="store_true")

    g = sub.add_parser("gcal", help="push the store to Google Calendar (colored by region)")
    g.add_argument("--calendar", help="calendar id (default: AKCAL_GCAL_CALENDAR_ID)")
    g.add_argument("--dry-run", action="store_true",
                   help="print what would be written; no API calls, no auth")
    g.add_argument("--no-prune", action="store_true",
                   help="do not delete our events that dropped out of the store")

    mp = sub.add_parser("map", help="render a self-contained HTML map of a month's events")
    mp.add_argument("--month", metavar="YYYY-MM", help="month to render (default: current)")
    mp.add_argument("--months", type=int, default=1, help="number of months to include (default 1)")
    mp.add_argument("--out", help=f"output path (default {MAP_PATH.name})")

    sy = sub.add_parser("sync", help="fetch a rolling window then push to Google Calendar")
    sy.add_argument("--state", action="append", metavar="XX",
                    help=f"state to query; repeatable. default: {FETCH_STATES}")
    sy.add_argument("--calendar", help="calendar id (default: AKCAL_GCAL_CALENDAR_ID)")
    sy.add_argument("--back-days", type=int, default=SYNC_BACK_DAYS,
                    help=f"days before today to fetch (default {SYNC_BACK_DAYS})")
    sy.add_argument("--ahead-days", type=int, default=SYNC_AHEAD_DAYS,
                    help=f"days after today to fetch (default {SYNC_AHEAD_DAYS})")
    sy.add_argument("--dry-run", action="store_true",
                    help="fetch, then print the push plan without writing")
    sy.add_argument("--no-prune", action="store_true")

    args = p.parse_args()
    {"probe": cmd_probe, "fetch": cmd_fetch, "ics": cmd_ics, "show": cmd_show,
     "gcal": cmd_gcal, "sync": cmd_sync, "map": cmd_map}[args.cmd](args)


if __name__ == "__main__":
    main()
