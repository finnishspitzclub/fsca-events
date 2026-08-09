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
from urllib.parse import quote

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
NATIONAL_PATH = Path(__file__).with_name("national-card.html")

# The FSCA National Specialty has no distinct row in the AKC feed - it's held in
# conjunction with a cluster. Point this at the anchor event (e.g. the breed/group
# specialty that weekend); the card auto-fills from whichever cluster it lands in.
# One value to update per year. Overridable via the workflow env.
NATIONAL_EVENT_NO = os.environ.get("AKCAL_NATIONAL_EVENT", "2026746503")

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

# Map pin colors (hex) per timezone. These are the EXACT hex values Google
# Calendar renders for the colorId in TZ_COLOR above, so a pin on the map and
# its event on the calendar are the same color. Google only offers 11 fixed
# event colors and no per-event hex, so matching has to run map -> calendar.
#   colorId 9 Blueberry #3F51B5, 10 Basil #0B8043, 6 Tangerine #F4511E,
#   11 Tomato #D50000, 3 Grape #8E24AA, 1 Lavender #7986CB, 8 Graphite #616161.
TZ_HEX = {
    "ET": "#3F51B5",    # Blueberry (colorId 9)
    "CT": "#0B8043",    # Basil     (colorId 10)
    "MT": "#F4511E",    # Tangerine (colorId 6)
    "PT": "#D50000",    # Tomato    (colorId 11)
    "AKT": "#8E24AA",   # Grape     (colorId 3)
    "HAT": "#7986CB",   # Lavender  (colorId 1)
}
DEFAULT_HEX = "#616161"   # Graphite (colorId 8) - unknown/unmapped state

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
        print("== national specialty card ==")
        cmd_national(argparse.Namespace(event=None, out=None))


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


def cluster_events(rows):
    """Group AKC shows into site clusters for the map and list.

    Shows at the same physical site - matched by coordinates when present, else by
    venue+city+state - whose dates run consecutively (<= 1 day apart) collapse into
    one cluster carrying every constituent show's full detail. This is what turns
    three back-to-back Arapahoe days into a single pin/row that expands to per-show
    info. The whole store is baked in; the page windows it client-side against the
    viewer's own "today". Pure; unit-tested."""
    shows = []
    for r in rows:
        sd = _parse_date(r["start_date"])
        if not sd:
            continue
        ed = _parse_date(r["end_date"]) or sd
        lat, lon = _col(r, "lat"), _col(r, "lon")
        if lat is not None and lon is not None:
            site = f"{round(lat, 3)},{round(lon, 3)}"
        else:
            site = "|".join(x for x in ((r["venue"] or "").strip().lower(),
                                        r["city"] or "", r["state"] or "") if x) \
                or (r["event_no"] or "")
        shows.append({
            "site": site, "sd": sd, "ed": ed,
            "start": sd.isoformat(), "end": ed.isoformat(),
            "dates": sd.isoformat() if sd == ed else f"{sd.isoformat()} – {ed.isoformat()}",
            "club": r["club"] or "Unknown club",
            "event_no": r["event_no"],
            "comp_type": r["comp_type"] or "",
            "high_value": bool(_col(r, "high_value")),
            "pended": _col(r, "status") == "Pended",
            "close": r["close_date"] or "",
            "open": _col(r, "open_date") or "",
            "fee": _col(r, "entry_fee") or "",
            "online": _col(r, "online_entries") or "",
            "superint": _col(r, "superint") or "",
            "supt_phone": _col(r, "supt_phone") or "",
            "supt_email": _col(r, "supt_email") or "",
            "breed_judge": _col(r, "breed_judge") or "",
            "group_judge": _col(r, "group_judge") or "",
            "bis_judge": _col(r, "bis_judge") or "",
            "clcy": _col(r, "completed_last_year") or "",
            "_venue": r["venue"] or "", "_city": r["city"] or "",
            "_state": r["state"] or "", "_lat": lat, "_lon": lon,
        })

    bysite = {}
    for s in shows:
        bysite.setdefault(s["site"], []).append(s)

    clusters = []
    for group in bysite.values():
        group.sort(key=lambda s: (s["sd"], s["ed"]))
        cur = None
        for s in group:
            if cur is not None and s["sd"] <= cur["end"] + timedelta(days=1):
                cur["members"].append(s)
                if s["ed"] > cur["end"]:
                    cur["end"] = s["ed"]
            else:
                if cur is not None:
                    clusters.append(_make_cluster(cur))
                cur = {"members": [s], "start": s["sd"], "end": s["ed"]}
        if cur is not None:
            clusters.append(_make_cluster(cur))

    clusters.sort(key=lambda c: c["start"])
    return clusters


def _make_cluster(c):
    """One cluster dict - site, date span, and every member show - for the page."""
    members = c["members"]
    m0 = members[0]
    minstart, maxend = c["start"], c["end"]
    tz = _tz(m0["_state"])
    clubs = list(dict.fromkeys(m["club"] for m in members))
    label = clubs[0] if len(clubs) == 1 else (m0["_venue"] or m0["_city"] or "Show cluster")
    dates = minstart.isoformat() if minstart == maxend \
        else f"{minstart.isoformat()} – {maxend.isoformat()}"
    closes = sorted(m["close"] for m in members if m["close"])
    shows = [{k: v for k, v in m.items()
              if not k.startswith("_") and k not in ("site", "sd", "ed")}
             for m in members]
    return {
        "start": minstart.isoformat(), "end": maxend.isoformat(), "dates": dates,
        "tz": tz, "tzLabel": TZ_LABEL.get(tz, "—"), "color": _hex_for(m0["_state"]),
        "state": m0["_state"], "city": m0["_city"], "venue": m0["_venue"],
        "where": ", ".join(x for x in (m0["_city"], m0["_state"]) if x),
        "lat": m0["_lat"], "lon": m0["_lon"],
        "label": label, "clubs": clubs, "n": len(members),
        "high_value": any(m["high_value"] for m in members),
        "pended": any(m["pended"] for m in members),
        "close": closes[0] if closes else "",
        "event_no": m0["event_no"],
        "shows": shows,
    }


def render_map_html(events, title, subtitle):
    """Self-contained page: a Leaflet map + a filterable table, both driven by one
    baked-in dataset the browser filters by a rolling date window (from the
    viewer's today) plus state / timezone / specialty / search. Depends on nothing
    of ours at runtime."""
    legend = "".join(
        f'<span class="lg"><i style="background:{TZ_HEX[tz]}"></i>{TZ_LABEL[tz]}</span>'
        for tz in ("ET", "CT", "MT", "PT", "AKT", "HAT"))
    data = json.dumps(events, separators=(",", ":")).replace("</", "<\\/")
    tpl = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
 integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
<style>
  html,body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#1a1a1a;background:#fff}
  header{padding:10px 14px;background:#0f2b46;color:#fff}
  header h1{margin:0;font-size:1.05rem}
  header p{margin:2px 0 0;font-size:.8rem;opacity:.85}
  .views{display:flex;flex-wrap:wrap;gap:14px;padding:12px 14px 4px}
  .vpane{flex:1 1 360px;min-width:270px;display:flex;flex-direction:column}
  .ph{font-size:.9rem;font-weight:600;color:#0f2b46;margin:0 0 6px}
  .ph .phn{font-weight:400;color:#8a8f98;font-size:.8rem}
  .ph2{padding:8px 14px 0}
  #map{height:404px;border:1px solid #e5e7eb;border-radius:8px}
  .calnav{display:flex;align-items:center;gap:8px;margin:0 0 6px}
  .calnav button{font:inherit;border:1px solid #cbd5e1;border-radius:6px;background:#fff;cursor:pointer;padding:3px 10px;line-height:1.2}
  .calnav .ml{font-weight:600;font-size:.9rem;min-width:122px;text-align:center}
  .calgrid{border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;background:#fff}
  .caldow-row{display:grid;grid-template-columns:repeat(7,1fr)}
  .caldow{background:#0f2b46;color:#fff;text-align:center;font-size:11px;font-weight:600;padding:3px 0}
  .calwk{position:relative;display:grid;grid-template-columns:repeat(7,1fr);border-top:1px solid #e5e7eb}
  .calday{min-height:86px;border-left:1px solid #eef0f2;padding:2px 3px}
  .calday:first-child{border-left:0}
  .calday.oth{background:#fafafa}
  .calday .dn{font-size:11px;color:#889;font-weight:600;display:inline-block;min-width:17px;text-align:center}
  .calday.today .dn{background:#b5541f;color:#fff;border-radius:8px}
  .calbar{position:absolute;height:15px;line-height:15px;border-radius:3px;color:#fff;font-size:11px;padding:0 5px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;cursor:pointer;border:0;box-sizing:border-box}
  .calbar.sel{outline:2px solid #0f2b46;outline-offset:-1px;z-index:2}
  .cmore{color:#667;font-size:10px}
  tbody tr.sel{background:#fff7e6}
  tbody tr.sel td:first-child{box-shadow:inset 3px 0 0 #b5541f}
  .legend{padding:8px 0 0;font-size:.8rem;display:flex;flex-wrap:wrap;gap:14px;align-items:center}
  .lg{display:inline-flex;align-items:center;gap:5px}
  .lg i{width:12px;height:12px;border-radius:50%;display:inline-block;border:1px solid #0003}
  .pop b{font-size:.95rem} .pop{font-size:.82rem;line-height:1.35}
  .pop .tag{display:inline-block;font-size:.68rem;font-weight:700;padding:1px 6px;border-radius:8px;margin-left:4px}
  .pop .pend{background:#ffe0b2;color:#8a4b00} .pop .hv{background:#ffcdd2;color:#8a0000}
  .pop a{color:#0b5cad}
  .filters{padding:11px 14px;margin:8px 0;display:flex;flex-wrap:wrap;gap:8px;align-items:center;border-top:1px solid #d3dbe4;border-bottom:1px solid #d3dbe4;background:#e7edf3}
  .filters input[type=search],.filters select{font:inherit;font-size:.85rem;padding:5px 8px;border:1px solid #cbd5e1;border-radius:6px;background:#fff}
  .filters input[type=search]{flex:1 1 180px;min-width:130px}
  .filters label{font-size:.8rem;display:inline-flex;align-items:center;gap:4px}
  .filters .count{margin-left:auto;font-size:.8rem;color:#555;white-space:nowrap}
  .tablewrap{overflow:auto;max-height:440px;margin:6px 14px 14px;border:1px solid #e5e7eb;border-radius:8px}
  table{border-collapse:collapse;width:100%;font-size:.82rem}
  thead th{position:sticky;top:0;background:#0f2b46;color:#fff;text-align:left;padding:7px 10px;font-weight:600;white-space:nowrap}
  td{padding:6px 10px;border-bottom:1px solid #eee;vertical-align:top}
  tbody tr:hover{background:#f7fafc}
  .dot{width:9px;height:9px;border-radius:50%;display:inline-block;border:1px solid #0003;margin-right:5px;vertical-align:middle}
  .tag{display:inline-block;font-size:.66rem;font-weight:700;padding:1px 5px;border-radius:8px}
  .pend{background:#ffe0b2;color:#8a4b00} .hv{background:#ffcdd2;color:#8a0000}
  td a{color:#0b5cad;text-decoration:none} td a:hover{text-decoration:underline}
  .empty{padding:20px;text-align:center;color:#777}
  tbody tr{cursor:pointer}
  .chev{color:#9aa5b1;font-weight:700}
  .nsub{font-size:.72rem;color:#667;font-weight:400}
  #detail{display:none;margin:8px 14px 2px;border:1px solid #e5e7eb;border-radius:8px;overflow:auto;max-height:440px}
  #detail .dhead{position:sticky;top:0;background:#0f2b46;color:#fff;padding:11px 14px;display:flex;align-items:flex-start;gap:10px;z-index:1}
  #detail .dhead h2{margin:0;font-size:1rem;flex:1;line-height:1.3}
  #detail .dhead .sub{font-size:.78rem;opacity:.85;font-weight:400}
  #detail .back{background:rgba(255,255,255,.16);border:0;color:#fff;font:inherit;font-size:.85rem;padding:6px 11px;border-radius:6px;cursor:pointer;flex:none}
  #detail .body{padding:12px 14px 30px}
  .csum{font-size:.85rem;color:#333;margin:0 0 4px}
  .csum a{color:#0b5cad}
  .showcard{border:1px solid #e5e7eb;border-radius:10px;padding:11px 13px;margin:12px 0}
  .showcard h3{margin:0 0 6px;font-size:.98rem}
  .showcard .meta{font-size:.83rem;line-height:1.55;color:#333}
  .showcard .meta b{color:#111}
  .addcal{display:flex;gap:8px;margin-top:9px;flex-wrap:wrap}
  .addcal a,.addcal button{font:inherit;font-size:.78rem;text-decoration:none;border:1px solid #0b5cad;color:#0b5cad;background:#fff;padding:5px 10px;border-radius:6px;cursor:pointer}
  .addcal .ics{border-color:#666;color:#666}
</style>
</head>
<body>
  <header><h1>__TITLE__</h1><p>__SUBTITLE__</p></header>
  <div class="views">
    <section class="vpane">
      <div class="calnav"><button id="calPrev" aria-label="Previous month">&lsaquo;</button><button id="calToday">Today</button><span class="ml" id="calLabel"></span><button id="calNext" aria-label="Next month">&rsaquo;</button></div>
      <div class="calgrid" id="calGrid"></div>
    </section>
    <section class="vpane">
      <div class="calnav" aria-hidden="true" style="visibility:hidden"><button>&lsaquo;</button><button>Today</button><span class="ml">&mdash;</span><button>&rsaquo;</button></div>
      <div id="map"></div>
      <div class="legend"><strong>Timezone:</strong>__LEGEND__</div>
    </section>
  </div>
  <div class="filters">
    <input type="search" id="q" placeholder="Search club, city, venue…">
    <select id="fState"><option value="">All states</option></select>
    <select id="fTz"><option value="">All timezones</option></select>
    <label><input type="checkbox" id="fHv"> Specialties &amp; groups</label>
    <label class="wlab">Map/list window&nbsp;<select id="fWin">
      <option value="30">30 days</option>
      <option value="60" selected>60 days</option>
      <option value="90">90 days</option>
      <option value="180">6 months</option>
      <option value="all">all upcoming</option>
    </select></label>
    <span class="count" id="count"></span>
  </div>
  <div class="tablewrap">
    <table>
      <thead><tr>
        <th>Dates</th><th>Show(s)</th><th>Location</th><th>Zone</th>
        <th>Entries close</th><th></th>
      </tr></thead>
      <tbody id="rows"></tbody>
    </table>
    <div class="empty" id="empty" style="display:none">No shows match those filters.</div>
  </div>
  <div id="detail" aria-hidden="true"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
 integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const ALL = __DATA__;
const AKC='https://www.apps.akc.org/apps/events/search/index_results.cfm?action=plan&event_number=';
const MAPS='https://www.google.com/maps/search/?api=1&query=';
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
function pd(s){const p=String(s).split('-');return new Date(+p[0],+p[1]-1,+p[2]);}
function ymd(s){return String(s).replace(/-/g,'');}
function plusDay(s){const d=pd(s);d.setDate(d.getDate()+1);return d.getFullYear()+String(d.getMonth()+1).padStart(2,'0')+String(d.getDate()).padStart(2,'0');}
const map=L.map('map',{scrollWheelZoom:true}).setView([39.5,-98.35],4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:18,attribution:'&copy; OpenStreetMap contributors'}).addTo(map);
const markers=L.layerGroup().addTo(map);
const q=document.getElementById('q'),fWin=document.getElementById('fWin'),fState=document.getElementById('fState'),fTz=document.getElementById('fTz'),fHv=document.getElementById('fHv'),rowsEl=document.getElementById('rows'),countEl=document.getElementById('count'),emptyEl=document.getElementById('empty'),detailEl=document.getElementById('detail');
const calGrid=document.getElementById('calGrid'),calLabel=document.getElementById('calLabel'),calPrev=document.getElementById('calPrev'),calNext=document.getElementById('calNext'),calToday=document.getElementById('calToday'),tableWrap=document.querySelector('.tablewrap');
const AIDX=new Map(ALL.map((c,i)=>[c,i]));
let markerByAi={},SELMK=null,calMonth=null;
function opt(sel,v,l){const o=document.createElement('option');o.value=v;o.textContent=l;sel.appendChild(o);}
[...new Set(ALL.map(c=>c.state).filter(Boolean))].sort().forEach(s=>opt(fState,s,s));
[['ET','Eastern'],['CT','Central'],['MT','Mountain'],['PT','Pacific'],['AKT','Alaska'],['HAT','Hawaii']].filter(t=>ALL.some(c=>c.tz===t[0])).forEach(t=>opt(fTz,t[0],t[1]));
let shown=[];
function baseMatch(c){
  const term=q.value.trim().toLowerCase(),st=fState.value,tz=fTz.value,hv=fHv.checked;
  if(st&&c.state!==st)return false;
  if(tz&&c.tz!==tz)return false;
  if(hv&&!c.high_value)return false;
  if(term&&!((c.label+' '+c.venue+' '+c.city+' '+c.state+' '+c.clubs.join(' ')).toLowerCase().includes(term)))return false;
  return true;
}
function filtered(){
  const now=new Date(),today=new Date(now.getFullYear(),now.getMonth(),now.getDate());
  const win=fWin.value; let end=null;
  if(win!=='all'){end=new Date(today);end.setDate(today.getDate()+(+win));}
  return ALL.filter(c=>{
    if(pd(c.end)<today)return false;
    if(end&&pd(c.start)>end)return false;
    return baseMatch(c);
  });
}
function render(refit){
  shown=filtered();
  markers.clearLayers(); markerByAi={}; SELMK=null;
  const seen={},latlngs=[];
  shown.forEach(c=>{
    if(c.lat==null||c.lon==null)return;
    let k=c.lat.toFixed(4)+','+c.lon.toFixed(4);
    const n=(seen[k]=(seen[k]||0)+1)-1,off=n?(n*0.02):0,ll=[c.lat+off,c.lon+off];
    latlngs.push(ll);
    const m=L.circleMarker(ll,{radius:7,color:'#fff',weight:1.5,fillColor:c.color,fillOpacity:.95});
    m.bindTooltip((c.high_value?'★ ':'')+c.label+(c.n>1?(' (+'+(c.n-1)+' more)'):''));
    m.on('click',()=>openDetail(c));
    markers.addLayer(m); markerByAi[AIDX.get(c)]=m;
  });
  if(refit&&latlngs.length){try{map.fitBounds(latlngs,{maxZoom:7,padding:[24,24]});}catch(_){}}
  let shows=0;
  rowsEl.innerHTML=shown.map(c=>{
    shows+=c.n;
    const loc=[c.city,c.state].filter(Boolean).join(', ');
    const name=c.n>1?(esc(c.label)+' <span class="nsub">+ '+(c.n-1)+' more show'+(c.n-1===1?'':'s')+'</span>'):esc(c.label);
    const tags=(c.high_value?' <span class="tag hv">★</span>':'')+(c.pended?' <span class="tag pend">PENDED</span>':'');
    return '<tr data-ai="'+AIDX.get(c)+'"><td style="white-space:nowrap">'+esc(c.dates)+'</td>'+
      '<td>'+name+tags+'</td>'+
      '<td style="white-space:nowrap">'+esc(loc)+'</td>'+
      '<td style="white-space:nowrap"><span class="dot" style="background:'+esc(c.color)+'"></span>'+esc(c.tzLabel)+'</td>'+
      '<td style="white-space:nowrap">'+esc(c.close)+'</td>'+
      '<td class="chev">&rsaquo;</td></tr>';
  }).join('');
  countEl.textContent=shown.length+' listing'+(shown.length===1?'':'s')+' · '+shows+' show'+(shows===1?'':'s');
  emptyEl.style.display=shown.length?'none':'block';
}
function renderCal(){
  if(!calMonth){const n=new Date();calMonth=new Date(n.getFullYear(),n.getMonth(),1);}
  const y=calMonth.getFullYear(),m=calMonth.getMonth();
  calLabel.textContent=calMonth.toLocaleString('en-US',{month:'long',year:'numeric'});
  const lead=new Date(y,m,1).getDay();
  const weeks=6;  // always 6 rows so the grid height is constant month-to-month (no layout jump)
  const gridStart=new Date(y,m,1-lead);
  const gEnd=new Date(gridStart);gEnd.setDate(gEnd.getDate()+weeks*7-1);
  const now=new Date(),tkey=now.getFullYear()+'-'+now.getMonth()+'-'+now.getDate();
  const items=ALL.filter(c=>baseMatch(c)&&pd(c.end)>=gridStart&&pd(c.start)<=gEnd);
  const gidx=dt=>Math.round((dt-gridStart)/86400000);
  let h='<div class="caldow-row">'+['Sun','Mon','Tue','Wed','Thu','Fri','Sat'].map(d=>'<div class="caldow">'+d+'</div>').join('')+'</div>';
  for(let w=0;w<weeks;w++){
    const wStart=w*7,wEnd=w*7+6;
    let cells='';
    for(let col=0;col<7;col++){
      const dt=new Date(gridStart);dt.setDate(gridStart.getDate()+wStart+col);
      const oth=dt.getMonth()!==m,tod=(dt.getFullYear()+'-'+dt.getMonth()+'-'+dt.getDate())===tkey;
      cells+='<div class="calday'+(oth?' oth':'')+(tod?' today':'')+'"><span class="dn">'+dt.getDate()+'</span></div>';
    }
    const bars=[];
    items.forEach(c=>{
      const s=Math.max(gidx(pd(c.start)),wStart),e=Math.min(gidx(pd(c.end)),wEnd);
      if(s>e)return;
      bars.push({c,s:s-wStart,e:e-wStart,cont:gidx(pd(c.start))<wStart});
    });
    bars.sort((a,b)=>a.s-b.s||b.e-a.e);
    const laneEnd=[];
    bars.forEach(b=>{let l=0;while(laneEnd[l]!==undefined&&laneEnd[l]>=b.s)l++;laneEnd[l]=b.e;b.lane=l;});
    let bh='';const MAXL=3,ov={};
    bars.forEach(b=>{
      if(b.lane>=MAXL){for(let d=b.s;d<=b.e;d++)ov[d]=(ov[d]||0)+1;return;}
      const left=(b.s/7*100),width=((b.e-b.s+1)/7*100);
      bh+='<button class="calbar" data-ai="'+AIDX.get(b.c)+'" title="'+esc(b.c.label)+' — '+esc(b.c.dates)+'" style="left:calc('+left.toFixed(3)+'% + 2px);width:calc('+width.toFixed(3)+'% - 4px);top:'+(20+b.lane*17)+'px;background:'+esc(b.c.color)+'">'+(b.c.high_value?'★ ':'')+(b.cont?'‹ ':'')+esc(b.c.label)+'</button>';
    });
    for(const d in ov)bh+='<span class="cmore" style="position:absolute;left:calc('+(d/7*100).toFixed(3)+'% + 3px);top:'+(20+MAXL*17)+'px">+'+ov[d]+'</span>';
    h+='<div class="calwk">'+cells+bh+'</div>';
  }
  calGrid.innerHTML=h;
}
function renderAll(refit){render(refit);renderCal();}
function highlight(ai){
  document.querySelectorAll('.sel').forEach(e=>e.classList.remove('sel'));
  document.querySelectorAll('[data-ai="'+ai+'"]').forEach(e=>e.classList.add('sel'));
  if(SELMK){try{SELMK.setStyle({radius:7,weight:1.5,color:'#fff'});}catch(_){}}
  const mk=markerByAi[ai];
  if(mk){try{mk.setStyle({radius:10,weight:3,color:'#0f2b46'});mk.openTooltip();map.panTo(mk.getLatLng());}catch(_){}SELMK=mk;}
}
rowsEl.addEventListener('click',e=>{const tr=e.target.closest('tr[data-ai]');if(tr)openDetail(ALL[+tr.dataset.ai]);});
calGrid.addEventListener('click',e=>{const b=e.target.closest('.calbar[data-ai]');if(b)openDetail(ALL[+b.dataset.ai]);});
calPrev.addEventListener('click',()=>{calMonth=new Date(calMonth.getFullYear(),calMonth.getMonth()-1,1);renderCal();});
calNext.addEventListener('click',()=>{calMonth=new Date(calMonth.getFullYear(),calMonth.getMonth()+1,1);renderCal();});
calToday.addEventListener('click',()=>{const n=new Date();calMonth=new Date(n.getFullYear(),n.getMonth(),1);renderCal();});
function descText(s,c){
  const L=[];
  if(s.close)L.push('Entries close: '+s.close);
  if(s.online)L.push('Enter online: '+s.online);
  if(s.superint)L.push('Superintendent: '+s.superint);
  if(s.breed_judge)L.push('Breed judge: '+s.breed_judge);
  if(s.clcy)L.push('Finnish Spitz entered last year: '+s.clcy);
  L.push('AKC: '+AKC+s.event_no);
  return L.join('\n');
}
function gcalHref(s,c){
  const text=s.club+(s.comp_type?(' ('+s.comp_type+')'):'');
  const loc=[c.venue,c.where].filter(Boolean).join(', ');
  return 'https://calendar.google.com/calendar/render?action=TEMPLATE&text='+encodeURIComponent(text)+'&dates='+ymd(s.start)+'/'+plusDay(s.end)+'&location='+encodeURIComponent(loc)+'&details='+encodeURIComponent(descText(s,c));
}
function icsEsc(s){return String(s==null?'':s).replace(/\\/g,'\\\\').replace(/;/g,'\\;').replace(/,/g,'\\,').replace(/\n/g,'\\n');}
function dlIcs(s,c){
  const loc=[c.venue,c.where].filter(Boolean).join(', ');
  const body=['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//FSCA//akcal//EN','BEGIN:VEVENT',
    'UID:'+s.event_no+'@finnishspitz.org','DTSTART;VALUE=DATE:'+ymd(s.start),'DTEND;VALUE=DATE:'+plusDay(s.end),
    'SUMMARY:'+icsEsc(s.club+(s.comp_type?(' ('+s.comp_type+')'):'')),'LOCATION:'+icsEsc(loc),
    'DESCRIPTION:'+icsEsc(descText(s,c)),'END:VEVENT','END:VCALENDAR'].join('\r\n');
  const a=document.createElement('a');
  a.href=URL.createObjectURL(new Blob([body],{type:'text/calendar'}));
  a.download=(s.club||'show').replace(/[^\w]+/g,'_')+'_'+s.start+'.ics';
  document.body.appendChild(a);a.click();a.remove();
}
let CUR=null;
function showCard(s,i){
  const m=[];
  m.push('<b>'+esc(s.dates)+'</b>'+(s.comp_type?' · '+esc(s.comp_type):''));
  const j=[];
  if(s.breed_judge&&s.breed_judge===s.group_judge){j.push('<b>Breed & Group:</b> '+esc(s.breed_judge));}
  else{if(s.breed_judge)j.push('<b>Breed:</b> '+esc(s.breed_judge));if(s.group_judge)j.push('<b>Group:</b> '+esc(s.group_judge));}
  if(s.bis_judge)j.push('<b>BIS:</b> '+esc(s.bis_judge));
  if(j.length)m.push('Judges — '+j.join(' · '));
  if(s.fee)m.push('<b>Entry fee:</b> '+esc(s.fee));
  if(s.open||s.close)m.push('<b>Entries:</b> '+(s.open?('open '+esc(s.open)):'')+(s.open&&s.close?' · ':'')+(s.close?('close '+esc(s.close)):''));
  if(s.superint){let sup=esc(s.superint);if(s.supt_phone)sup+=' · '+esc(s.supt_phone);if(s.supt_email)sup+=' · '+esc(s.supt_email);m.push('<b>Superintendent:</b> '+sup);}
  if(s.online)m.push('<a target="_blank" rel="noopener" href="'+esc(s.online)+'">Enter online →</a>');
  if(s.clcy)m.push('<b>Finnish Spitz last year:</b> '+esc(s.clcy));
  m.push('<a target="_blank" rel="noopener" href="'+AKC+encodeURIComponent(s.event_no)+'">AKC event page →</a>');
  const tags=(s.high_value?' <span class="tag hv">★</span>':'')+(s.pended?' <span class="tag pend">PENDED</span>':'');
  return '<div class="showcard"><h3>'+esc(s.club)+tags+'</h3><div class="meta">'+m.join('<br>')+'</div>'+
    '<div class="addcal"><a target="_blank" rel="noopener" href="'+gcalHref(s,CUR)+'">＋ Google Calendar</a>'+
    '<button class="ics" data-ics="'+i+'">Download .ics</button></div></div>';
}
function openDetail(c){
  if(!c)return;
  CUR=c;
  const _sd=pd(c.start);calMonth=new Date(_sd.getFullYear(),_sd.getMonth(),1);renderCal();
  highlight(AIDX.get(c));
  const loc=[c.venue,c.where].filter(Boolean).join(', ');
  const mapLink=loc?'<a target="_blank" rel="noopener" href="'+MAPS+encodeURIComponent(loc)+'">'+esc(loc)+' ↗</a>':'';
  let html='<div class="dhead"><button class="back" id="dback">‹ Back</button>'+
    '<h2>'+(c.high_value?'★ ':'')+esc(c.n>1?(c.venue||c.label):c.label)+'<br><span class="sub">'+esc(c.dates)+' · '+esc(c.tzLabel)+' time</span></h2></div>'+
    '<div class="body"><p class="csum">'+(mapLink?('📍 '+mapLink+' · '):'')+c.n+' show'+(c.n===1?'':'s')+' at this site</p>'+
    c.shows.map((s,i)=>showCard(s,i)).join('')+'</div>';
  detailEl.innerHTML=html;
  tableWrap.style.display='none';
  detailEl.style.display='block';
  detailEl.setAttribute('aria-hidden','false');
  detailEl.scrollTop=0;
  detailEl.scrollIntoView({block:'nearest'});
  document.getElementById('dback').addEventListener('click',closeDetail);
}
function closeDetail(){detailEl.style.display='none';tableWrap.style.display='';detailEl.setAttribute('aria-hidden','true');CUR=null;if(location.hash)history.replaceState(null,'',location.pathname+location.search);}
detailEl.addEventListener('click',e=>{const b=e.target.closest('[data-ics]');if(b&&CUR)dlIcs(CUR.shows[+b.dataset.ics],CUR);});
[fWin,fState,fTz,fHv].forEach(el=>el.addEventListener('change',()=>renderAll(true)));
q.addEventListener('input',()=>renderAll(false));
renderAll(true);
window.addEventListener('resize',()=>{try{map.invalidateSize();}catch(_){}});
setTimeout(()=>{try{map.invalidateSize();}catch(_){}},250);
function openFromHash(){
  const m=/#show=([\w-]+)/.exec(location.hash);
  if(!m)return;
  const c=ALL.find(c=>c.shows.some(s=>String(s.event_no)===m[1]));
  if(c)openDetail(c);
}
window.addEventListener('hashchange',openFromHash);
openFromHash();
</script>
</body>
</html>
"""
    return (tpl.replace("__TITLE__", _h(title))
               .replace("__SUBTITLE__", _h(subtitle))
               .replace("__LEGEND__", legend)
               .replace("__DATA__", data))


def cmd_map(args):
    # The whole store is baked in; the page windows it client-side against the
    # viewer's own "today", so the map/list stay correct between regenerations
    # and never get stuck showing a stale calendar month. --month/--months are
    # accepted for back-compat but no longer change the output.
    conn = db()
    rows = conn.execute("SELECT * FROM events ORDER BY start_date").fetchall()
    conn.close()
    clusters = cluster_events(rows)
    shows = sum(c["n"] for c in clusters)
    updated = datetime.now().strftime("%b %d, %Y").replace(" 0", " ")
    title = "Finnish Spitz — AKC Events"
    subtitle = (f"{shows} shows at {len(clusters)} sites · choose a window & "
                f"filters below · updated {updated}")
    out = Path(args.out) if getattr(args, "out", None) else MAP_PATH
    out.write_text(render_map_html(clusters, title, subtitle), encoding="utf-8")
    print(f"wrote {out}  ({len(clusters)} clusters / {shows} shows)")


# ---------------------------------------------------------------- national card
#
# The FSCA National Specialty as a self-contained card, built entirely from the
# feed cluster that NATIONAL_EVENT_NO lands in and embedded on /events as an
# iframe. Regenerated every sync run, so it stays current with one config value
# per year. AKC gives us judges but not the ring-schedule "judging program"
# (a superintendent doc), so this shows judges, not a program.


def _natl_gcal(text, start, end, loc, details):
    """Google Calendar 'add event' template URL for an all-day span."""
    d1 = (_parse_date(end) or _parse_date(start))
    end_excl = (d1 + timedelta(days=1)).strftime("%Y%m%d")
    return ("https://calendar.google.com/calendar/render?action=TEMPLATE"
            f"&text={quote(text)}&dates={start.replace('-', '')}/{end_excl}"
            f"&location={quote(loc)}&details={quote(details)}")


def _clcy_tip(s):
    """Plain-English gloss of the AKC entry code 'TOTAL-cd-cb (sd-sb) vet'."""
    m = re.match(r"\s*(\d+)-(\d+)-(\d+)\s*\((\d+)-(\d+)\)\s*(\d+)\s*$", str(s or ""))
    if not m:
        return ""
    tot, cd, cb, sd, sb, vet = (int(x) for x in m.groups())
    def n(k, one, many):
        return f"{k} {one if k == 1 else many}"
    bits = [n(cd, "class dog", "class dogs"), n(cb, "class bitch", "class bitches"),
            n(sd, "champion dog", "champion dogs"), n(sb, "champion bitch", "champion bitches")]
    if vet:
        bits.append(n(vet, "veteran", "veterans"))
    return f"{tot} Finnish Spitz total — " + ", ".join(bits)


def _natl_shell(inner):
    css = (
        "html,body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#4a3f36;background:#fff}"
        ".card{max-width:1400px;margin:0 auto;border:1px solid #e7ddd4;border-radius:14px;overflow:hidden}"
        ".hdr{background:#7a2e12;color:#fff;padding:15px 20px}"
        ".hdr .lbl{font-size:.7rem;letter-spacing:.14em;text-transform:uppercase;color:#f0c9b5;font-weight:700}"
        ".hdr .ttl{font-size:1.5rem;font-weight:800;margin-top:3px;line-height:1.15}"
        ".hdr .sub{color:#f3d9c9;margin-top:2px;font-size:.95rem}"
        ".bd{padding:16px 20px 18px}"
        ".grid{display:grid;grid-template-columns:1.05fr 1fr;gap:0 32px}"
        "@media(max-width:720px){.grid{grid-template-columns:1fr;gap:0}}"
        ".jh{margin:0 0 8px;font-size:1rem;color:#7a2e12;font-weight:600}"
        ".facts{display:flex;flex-wrap:wrap;gap:8px 18px;padding:11px 14px;background:#fbf3ea;border:1px solid #ece0d2;border-radius:10px;margin:0 0 14px;font-size:.92rem}"
        ".facts a{color:#b5541f;text-decoration:none}"
        ".lede{margin:0 0 16px;line-height:1.6}"
        ".cta{margin:0 0 4px}"
        ".btn{background:#b5541f;color:#fff;text-decoration:none;font-weight:700;padding:10px 16px;border-radius:8px;font-size:.92rem;display:inline-block}"
        ".sec{margin-top:16px}"
        ".sec h3{margin:0 0 7px;font-size:1rem;color:#7a2e12}"
        ".sec p{margin:0;line-height:1.55}"
        ".jr{margin:0 0 9px;font-size:.9rem;line-height:1.45}"
        ".jr .jd{font-weight:700;color:#5b4a3d}"
        ".foot{margin:18px 0 0;font-size:.78rem;color:#9a8a7a}"
    )
    return ('<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
            '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
            '<title>FSCA National Specialty</title>\n<style>' + css + '</style>\n</head>\n'
            '<body>\n<div class="card">\n' + inner + '\n</div>\n</body>\n</html>\n')


def render_national_html(cluster):
    """Self-contained National Specialty card from a feed cluster (or a graceful
    placeholder when the anchor isn't in the store)."""
    if not cluster:
        return _natl_shell(
            '<div class="bd"><p>This year’s National Specialty details will appear here '
            'once the show is posted to the AKC event calendar.</p></div>')

    year = cluster["start"][:4]
    where = ", ".join(x for x in (cluster["venue"], cluster["where"]) if x)
    maps = "https://www.google.com/maps/search/?api=1&query=" + quote(where or cluster["where"])
    supt = next((s["superint"] for s in cluster["shows"] if s["superint"]), "")
    fees = sorted({float(s["fee"]) for s in cluster["shows"] if str(s["fee"]) not in ("", "None")})
    feetxt = (f"${fees[0]:.0f}" if len(fees) == 1
              else f"${fees[0]:.0f}–${fees[-1]:.0f}") if fees else ""
    closes = sorted({s["close"] for s in cluster["shows"] if s["close"]})
    closetxt = closes[0] if closes else ""
    clcy = next((s["clcy"] for s in cluster["shows"] if s["clcy"]), "")
    gcal = _natl_gcal(
        f"{year} FSCA National Specialty", cluster["start"], cluster["end"], where,
        f"FSCA National Specialty weekend ({cluster['n']} AKC shows). "
        f"Premium & entries via {supt or 'the superintendent'}. Confirm on the premium list.")

    jr = []
    for s in cluster["shows"]:
        parts = []
        bj, gj = s["breed_judge"], s["group_judge"]
        if bj and bj == gj:                       # one judge does breed AND the group
            parts.append("<b>Breed &amp; Group:</b> " + _h(bj))
        else:
            if bj:
                parts.append("<b>Breed:</b> " + _h(bj))
            if gj:
                parts.append("<b>Group:</b> " + _h(gj))
        if s["bis_judge"]:
            parts.append("<b>BIS:</b> " + _h(s["bis_judge"]))
        if parts:
            jr.append(f'<div class="jr"><div class="jd">{_h(s["dates"])} · {_h(s["club"])}</div>'
                      f'<div>{" · ".join(parts)}</div></div>')
    judges = "".join(jr) or "<p>Judge assignments post as the show approaches.</p>"

    ent = []
    if supt:
        ent.append(f"Superintendent: <b>{_h(supt)}</b>")
    if feetxt:
        ent.append(f"Entry fee {feetxt}")
    if closetxt:
        ent.append(f"Entries close <b>{_h(closetxt)}</b>")
    entline = (" · ".join(ent) + ". " if ent else "") + "<em>Confirm on the premium list.</em>"

    tip = _clcy_tip(clcy)
    entered = (f'<abbr title="{_h(tip)}" style="text-decoration:underline dotted;cursor:help">{_h(clcy)}</abbr>'
               if tip else f'<b>{_h(clcy)}</b>')
    turnout = (f'<div class="sec"><h3>Finnish Spitz turnout</h3><p>Finnish Spitz entered at these '
               f'shows last year: {entered}. The National typically draws more from across the country.'
               f'</p></div>') if clcy else ""

    inner = f'''<div class="hdr">
  <div class="lbl">FSCA National Specialty</div>
  <div class="ttl">{year} National Specialty</div>
  <div class="sub">{_h(cluster["dates"])} · {_h(cluster["where"])}</div>
</div>
<div class="bd">
  <div class="grid">
    <div>
      <div class="facts">
        <span>\U0001f4c5 <b>{_h(cluster["dates"])}</b></span>
        <span>\U0001f4cd <a href="{maps}" target="_blank" rel="noopener">{_h(cluster["venue"] or cluster["where"])}</a></span>
        <span>\U0001f3c6 {cluster["n"]} AKC shows this weekend</span>
      </div>
      <p class="lede">The club’s premier event of the year — the one weekend the breed community gathers from across the country.</p>
      <div class="cta"><a class="btn" href="{gcal}" target="_blank" rel="noopener">＋ Add to your calendar</a></div>
      <div class="sec"><h3>Entries &amp; premium</h3><p>{entline}</p></div>
      {turnout}
    </div>
    <div>
      <div class="jh">Judges</div>
      {judges}
    </div>
  </div>
  <p class="foot">Auto-updated from the AKC event feed — confirm final details on the premium list.</p>
</div>'''
    return _natl_shell(inner)


def cmd_national(args):
    conn = db()
    rows = conn.execute("SELECT * FROM events ORDER BY start_date").fetchall()
    conn.close()
    anchor = getattr(args, "event", None) or NATIONAL_EVENT_NO
    target = next((c for c in cluster_events(rows)
                   if any(s["event_no"] == anchor for s in c["shows"])), None)
    out = Path(args.out) if getattr(args, "out", None) else NATIONAL_PATH
    out.write_text(render_national_html(target), encoding="utf-8")
    if target:
        print(f"wrote {out}  (national: {target['dates']} {target['where']}, {target['n']} shows)")
    else:
        print(f"wrote {out}  (anchor {anchor} not in store - placeholder)")


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

    n = sub.add_parser("national", help="render the National Specialty card (feed-driven)")
    n.add_argument("--event", help=f"anchor event number (default {NATIONAL_EVENT_NO})")
    n.add_argument("--out", help=f"output path (default {NATIONAL_PATH.name})")

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
     "gcal": cmd_gcal, "sync": cmd_sync, "map": cmd_map,
     "national": cmd_national}[args.cmd](args)


if __name__ == "__main__":
    main()
