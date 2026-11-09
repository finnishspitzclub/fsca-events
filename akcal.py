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
import unicodedata
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
BANNER_PATH = Path(__file__).with_name("show-banner.html")
BANNER_CANDIDATES = 60                # sites baked in; the page shows the best few
# Fewer placed breeds than this on a day means the program didn't really parse,
# so a breed's absence from it can't be taken to mean nobody entered.
MIN_BREEDS_TO_TRUST_PROGRAM = 10
RINGCARD_DIR = Path(__file__).with_name("ringcard")            # vendored generator
RINGCARDS_OUT = Path(__file__).with_name("ringcards")          # generated cards
RINGCARD_MANIFEST = Path(__file__).with_name("ringcards-manifest.json")
RINGS_OUT = Path(__file__).with_name("rings")                  # per-breed ring data
REFERENCES_URL = "https://webapps.akc.org/event-search/api/references/"
# Serves the page's breed picker (worker/); the page itself only bakes in FS.
BREED_API = os.environ.get("AKCAL_BREED_API", "https://fsca-akc-breeds.fsca-akc-breeds.workers.dev")
FS_BREED_CODE = "313"
# The first digit of an AKC breed code is its group.
GROUP_BY_CODE_PREFIX = {"1": "Sporting", "2": "Terrier", "3": "Non-Sporting", "4": "Hound",
                        "5": "Toy", "6": "Working", "8": "Herding"}
# How judging programs print breeds that AKC's list spells differently. Seen in
# real programs; anything not here or in AKC's list simply gets no ring panel.
BREED_ALIASES = {
    "Bracco Italiani": "Bracco Italiano",
    "Spinone Italiani": "Spinone Italiano",
    "Lagoti Romagnoli": "Lagotto Romagnolo",
    "Lagotto Romagnoli": "Lagotto Romagnolo",
    "Saint Bernards": "St. Bernard",
    "Retrievers (NSDT)": "Nova Scotia Duck Tolling Retriever",
    "Retriever (Nova Scotia Duck Toll)": "Nova Scotia Duck Tolling Retriever",
    "Beagles (13 Inch)": "Beagle (13 in. and under)",
    "Beagle Thirteen Inch": "Beagle (13 in. and under)",
    "Beagles (15 Inch)": "Beagle (over 13 in.)",
    "Beagle Fifteen Inch": "Beagle (over 13 in.)",
    "Spaniels (Cocker) Parti-Color": "Spaniel (Cocker) Parti",
    "Cocker Spaniel (Parti-Color)": "Spaniel (Cocker) Parti",
    "English Toy Spaniels (KC&R)": "English Toy Spaniel (King Charles & Ruby)",
    "English Toy Spaniels (K C & R)": "English Toy Spaniel (King Charles & Ruby)",
    "English Toy Spaniels (Bl & PC)": "English Toy Spaniel (Blenheim & Prince Charles)",
    "Anatolian Shepherd": "Anatolian Shepherd Dog",
    "American Eskimos": "American Eskimo Dog",
    "Am Staffordshire Terriers": "American Staffordshire Terrier",
    "Cavalier King Charles Span": "Cavalier King Charles Spaniel",
    "Pointers (German Wire)": "German Wirehaired Pointer",
}
# AKC-feed superintendent name (lowercase substring) -> ringcard/extractors name.
RINGCARD_SUPERS = {"onofrio": "onofrio", "mb-f": "mbf", "foy trent": "foytrent",
                   "baray": "baray", "jim rau": "jimrau", "bradshaw": "bradshaw",
                   "executive dog": "executive", "lorraine yu": "lorraineyu"}

# The FSCA National Specialty has no distinct row in the AKC feed - it's held in
# conjunction with a cluster. Point this at the anchor event (e.g. the breed/group
# specialty that weekend); the card auto-fills from whichever cluster it lands in.
# One value to update per year. Overridable via the workflow env.
NATIONAL_EVENT_NO = os.environ.get("AKCAL_NATIONAL_EVENT", "2026746503")

# The National promo (events-page ★ button + the embedded national-card iframe)
# only surfaces from this many days before the National through its end date, then
# hides itself until next year's anchor comes back into range. "A month or two."
NATIONAL_LEAD_DAYS = int(os.environ.get("AKCAL_NATIONAL_LEAD_DAYS", "60"))

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


def _spec_label(spec):
    """'Non-Sporting Group' -> 'Non-Sporting specialty' (mirrors the map's JS)."""
    base = re.sub(r"\s*group(\s*show)?\s*$", "", str(spec or ""), flags=re.I).strip()
    return f"{base} specialty" if base else "Group specialty"


def _cal_summary(r):
    """Calendar title for one show. ★ is reserved for the National Specialty; a
    limited-breed group specialty gets a text label instead so it never reads as
    the National. Keeps the [PENDED] suffix."""
    name = r["club"] or "Unknown club"
    if r["comp_type"]:
        name += f" ({r['comp_type']})"
    if str(r["event_no"]) == str(NATIONAL_EVENT_NO):
        name = "★ National Specialty — " + name
    elif bool(_col(r, "high_value")):
        name = f"{_spec_label(_col(r, 'specialty'))} — " + name
    if _col(r, "status") == "Pended":
        name += " [PENDED]"
    return name


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

        title = _cal_summary(r)
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

    summary = _cal_summary(r)

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
        print("== ring cards (Onofrio judging programs) ==")
        try:
            cmd_ringcards(argparse.Namespace(out=None))
        except Exception as e:              # never let a ring-card hiccup break the sync
            print("ringcards: step failed (continuing):", e)
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


# AKC serves each event document (premium list, judging program, entry form)
# from its own document server, keyed by the `keyBinary` id in the feed's
# documents[] array — no scraping needed. URL pattern lifted from the AKC
# event-search app's own bundle.
DOC_URL = ("https://www.apps.akc.org/apps/eventplans/eventsearch/blocks/"
           "dsp_generate_pdf.cfm?KEY_BINARY_CONTENT={}")
DOC_LABEL = {"PRMLST": "Premium list", "JDGPRO": "Judging program",
             "ENTFRM": "Entry form", "EVNINF": "Event info"}


def _docs(js):
    """Parse the stored documents JSON into [{label, code, url}] for the UI."""
    if not js:
        return []
    try:
        arr = json.loads(js)
    except Exception:
        return []
    out = []
    for d in arr:
        kb = d.get("keyBinary")
        if kb is None:
            continue
        code = (d.get("code") or "").strip()
        out.append({"label": DOC_LABEL.get(code, d.get("name") or "Document"),
                    "code": code, "url": DOC_URL.format(kb)})
    return out


def _hmm(mm):
    """minutes since midnight -> '2:45p' (for group est times)."""
    mm = int(round(mm)) % 1440
    h, m = divmod(mm, 60)
    return f"{(h % 12) or 12}:{m:02d}{'a' if h < 12 else 'p'}"


def _span_label(sd, ed):
    """Human date/date-range with weekday, e.g. 'Wed Aug 5 - Sun Aug 9, 2026'."""
    def one(d):
        return d.strftime("%a %b ") + str(d.day)
    if sd == ed:
        return f"{one(sd)}, {sd.year}"
    if sd.year != ed.year:
        return f"{one(sd)}, {sd.year} – {one(ed)}, {ed.year}"
    return f"{one(sd)} – {one(ed)}, {ed.year}"


def _short_supt(name):
    """Trim a superintendent name to a compact label for the list column, e.g.
    'Onofrio Dog Shows, LLC' -> 'Onofrio', 'MB-F Inc.' -> 'MB-F',
    'BaRay Event Services' -> 'BaRay', 'Jim Rau Dog Shows, LLC' -> 'Jim Rau'."""
    n = (name or "").strip()
    if not n:
        return ""
    short = re.split(r",|\bDog Shows\b|\bEvent Services\b|\bDog Show\b|\bInc\.?|\bL\.?L\.?C\.?\b",
                     n, maxsplit=1, flags=re.I)[0].strip(" ,.")
    return short or n


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
            "dates": _span_label(sd, ed),
            "club": r["club"] or "Unknown club",
            "event_no": r["event_no"],
            "comp_type": r["comp_type"] or "",
            "high_value": bool(_col(r, "high_value")),
            "specialty": _col(r, "specialty") or "",
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
            "nohs_judge": _col(r, "nohs_group_judge") or "",
            "bis_judge": _col(r, "bis_judge") or "",
            "clcy": _col(r, "completed_last_year") or "",
            "clcy_tip": _clcy_tip(_col(r, "completed_last_year") or ""),
            "docs": _docs(_col(r, "documents")),
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
    dates = _span_label(minstart, maxend)
    supts = list(dict.fromkeys(_short_supt(m["superint"]) for m in members if m["superint"]))
    closes = sorted(m["close"] for m in members if m["close"])
    shows = [{k: v for k, v in m.items()
              if not k.startswith("_") and k not in ("site", "sd", "ed")}
             for m in members]
    # Best-attended day's Finnish Spitz turnout from last year (max total across
    # the site's shows), for an at-a-glance "is this one worth it" signal.
    fs_last, fs_last_tip = None, ""
    for m in members:
        mm = re.match(r"\s*(\d+)", str(m["clcy"] or ""))
        if mm and (fs_last is None or int(mm.group(1)) > fs_last):
            fs_last, fs_last_tip = int(mm.group(1)), _clcy_tip(m["clcy"])
    # Unique documents across the site's shows, ordered premium/program/entry/info.
    _dord = {"PRMLST": 0, "JDGPRO": 1, "ENTFRM": 2, "EVNINF": 3}
    cdocs, seen = [], set()
    for m in members:
        for d in m.get("docs", []):
            if d["url"] in seen:
                continue
            seen.add(d["url"]); cdocs.append(d)
    cdocs.sort(key=lambda d: _dord.get(d["code"], 9))
    return {
        "start": minstart.isoformat(), "end": maxend.isoformat(), "dates": dates,
        "tz": tz, "tzLabel": TZ_LABEL.get(tz, "—"), "color": _hex_for(m0["_state"]),
        "state": m0["_state"], "city": m0["_city"], "venue": m0["_venue"],
        "where": ", ".join(x for x in (m0["_city"], m0["_state"]) if x),
        "lat": m0["_lat"], "lon": m0["_lon"],
        "label": label, "clubs": clubs, "n": len(members),
        "superint": supts[0] if supts else "", "supts": supts,
        "high_value": any(m["high_value"] for m in members),
        "pended": any(m["pended"] for m in members),
        "close": closes[0] if closes else "",
        "fs_last": fs_last, "fs_last_tip": fs_last_tip,
        "docs": cdocs,
        "event_no": m0["event_no"],
        "shows": shows,
    }


def national_cluster(clusters, anchor=NATIONAL_EVENT_NO):
    """The cluster the National anchor lands in, or None if it isn't in the feed."""
    if not anchor:
        return None
    return next((c for c in clusters
                 if any(str(s["event_no"]) == str(anchor) for s in c["shows"])), None)


def national_in_window(cluster, today=None):
    """True when the National should be promoted: from NATIONAL_LEAD_DAYS before its
    start through its end date. False when it's too far out, already past, or the
    anchor isn't in the store yet (no date to gate on)."""
    if not cluster:
        return False
    start = _parse_date(cluster["start"])
    if not start:
        return False
    end = _parse_date(cluster["end"]) or start
    today = today or date.today()
    return (start - timedelta(days=NATIONAL_LEAD_DAYS)) <= today <= end


def render_map_html(events, title, subtitle, national_no="", prog_read=()):
    """Self-contained page: a Leaflet map + a filterable table, both driven by one
    baked-in dataset the browser filters by a rolling date window (from the
    viewer's today) plus state / timezone / specialty / search. Depends on nothing
    of ours at runtime."""
    legend = "".join(
        f'<span class="lg"><i style="background:{TZ_HEX[tz]}"></i>{TZ_LABEL[tz]}</span>'
        for tz in ("ET", "CT", "MT", "PT", "AKT", "HAT"))
    data = json.dumps(events, separators=(",", ":")).replace("</", "<\\/")
    tzinfo = {st: [tz, TZ_HEX.get(tz, DEFAULT_HEX), TZ_LABEL.get(tz, "—")] for st, tz in STATE_TZ.items()}
    tzinfo[""] = [None, DEFAULT_HEX, "—"]
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
  header{padding:10px 14px;background:#0f2b46;color:#fff;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
  .htxt{flex:1 1 auto;min-width:200px}
  header h1{margin:0;font-size:1.05rem}
  header p{margin:2px 0 0;font-size:.8rem;opacity:.85}
  .natlbtn{flex:none;background:linear-gradient(135deg,#f6c453,#e0992e);color:#3a2a05;border:0;font:inherit;font-weight:800;font-size:.9rem;padding:9px 16px;border-radius:8px;cursor:pointer;box-shadow:0 1px 3px #0004}
  .natlbtn:hover{filter:brightness(1.06)}
  header{position:relative;z-index:1100}
  .brpick{flex:none;display:flex;align-items:center;gap:8px;font-size:.8rem;flex-wrap:wrap}
  .brpick .msbtn{max-width:250px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;color:#1a1a1a}
  .brstat{font-size:.75rem;opacity:.92}
  .brstat button{font:inherit;color:#fff;background:none;border:0;text-decoration:underline;cursor:pointer;padding:0}
  .brpick .brpanel{left:auto;right:0;width:min(300px,calc(100vw - 28px));max-height:360px;color:#1a1a1a;box-sizing:border-box}
  .brpick .brpanel input[type=search]{width:100%;box-sizing:border-box;font:inherit;font-size:.85rem;padding:5px 8px;border:1px solid #cbd5e1;border-radius:6px}
  .brhint{font-size:.72rem;color:#667;padding:5px 6px}
  .brpick .brpanel label{white-space:normal}
  .brpick .brpanel label.off{opacity:.45;cursor:not-allowed}
  @media(max-width:520px){.brpick .brpanel{left:0;right:auto}}
  .srb{font-size:.74rem;font-weight:800;color:#5a4d40;margin-bottom:3px}
  .views{display:flex;flex-wrap:wrap;gap:14px;padding:12px 14px 4px}
  .vpane{flex:1 1 360px;min-width:270px;display:flex;flex-direction:column}
  .ph{font-size:.9rem;font-weight:600;color:#0f2b46;margin:0 0 6px}
  .ph .phn{font-weight:400;color:#8a8f98;font-size:.8rem}
  .ph2{padding:8px 14px 0}
  #map{height:569px;border:1px solid #e5e7eb;border-radius:8px}
  .calnav{display:flex;align-items:center;gap:8px;margin:0 0 6px}
  .calnav button{font:inherit;border:1px solid #cbd5e1;border-radius:6px;background:#fff;cursor:pointer;padding:3px 10px;line-height:1.2}
  .calnav .ml{font-weight:600;font-size:.9rem;min-width:122px;text-align:center}
  .calgrid{border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;background:#fff}
  .caldow-row{display:grid;grid-template-columns:repeat(7,1fr)}
  .caldow{background:#0f2b46;color:#fff;text-align:center;font-size:11px;font-weight:600;padding:3px 0}
  .calwk{position:relative;display:grid;grid-template-columns:repeat(7,1fr);border-top:1px solid #e5e7eb}
  .calday{min-height:86px;border-left:1px solid #eef0f2;padding:2px 3px}
  .calday:first-child{border-left:0}
  .calday.oth{background:#e9edf1}
  .calday.oth .dn{color:#b2bcc7;font-weight:500}
  .calday .dn{font-size:11px;color:#556;font-weight:700;display:inline-block;min-width:17px;text-align:center}
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
  .pop .pend{background:#ffe0b2;color:#8a4b00} .pop .hv{background:#ffcdd2;color:#8a0000} .pop .natl{background:#7a2e12;color:#fff}
  .pop a{color:#0b5cad}
  .filters{padding:11px 14px;margin:8px 0;display:flex;flex-wrap:wrap;gap:8px;align-items:center;border-top:1px solid #d3dbe4;border-bottom:1px solid #d3dbe4;background:#e7edf3;position:relative;z-index:30}
  .zones{display:inline-flex;gap:4px;flex-wrap:wrap}
  .zchip{font:inherit;font-size:.8rem;padding:4px 9px;border:1px solid #cbd5e1;border-radius:14px;background:#fff;cursor:pointer;color:#334;transition:background-color .12s ease,color .12s ease}
  .zchip.on{background:#0f2b46;color:#fff;border-color:#0f2b46}
  .msdd{position:relative;display:inline-block}
  .msbtn{font:inherit;font-size:.85rem;padding:5px 9px;border:1px solid #cbd5e1;border-radius:6px;background:#fff;cursor:pointer}
  .mspanel{position:absolute;z-index:1000;top:calc(100% + 4px);left:0;background:#fff;border:1px solid #cbd5e1;border-radius:8px;box-shadow:0 4px 14px #0003;padding:6px;max-height:264px;overflow:auto;min-width:170px}
  .mspanel label{display:flex;align-items:center;gap:6px;font-size:.82rem;padding:3px 6px;border-radius:5px;cursor:pointer;white-space:nowrap}
  .mspanel label:hover{background:#eef3f8}
  .clearbtn{font:inherit;font-size:.82rem;padding:5px 11px;border:1px solid #cbd5e1;border-radius:6px;background:#fff;cursor:pointer;color:#334}
  .clearbtn:hover{background:#eef3f8}
  .filters input[type=search],.filters select{font:inherit;font-size:.85rem;padding:5px 8px;border:1px solid #cbd5e1;border-radius:6px;background:#fff}
  .filters input[type=search]{flex:1 1 180px;min-width:130px}
  .filters label{font-size:.8rem;display:inline-flex;align-items:center;gap:4px}
  .filters .count{margin-left:auto;font-size:.8rem;color:#555;white-space:nowrap}
  .tablewrap{overflow:auto;max-height:826px;margin:6px 14px 14px;border:1px solid #e5e7eb;border-radius:8px}
  table{border-collapse:collapse;width:100%;font-size:.82rem}
  thead th{position:sticky;top:0;background:#0f2b46;color:#fff;text-align:left;padding:7px 10px;font-weight:600;white-space:nowrap}
  td{padding:6px 10px;border-bottom:1px solid #eee;vertical-align:top}
  tbody tr:hover{background:#f7fafc}
  tbody tr{transition:background-color .15s ease,box-shadow .15s ease}
  @keyframes fadein{from{opacity:0}to{opacity:1}}
  .fade{animation:fadein .18s ease}
  .dot{width:9px;height:9px;border-radius:50%;display:inline-block;border:1px solid #0003;margin-right:5px;vertical-align:middle}
  .tag{display:inline-block;font-size:.66rem;font-weight:700;padding:1px 5px;border-radius:8px}
  .pend{background:#ffe0b2;color:#8a4b00} .hv{background:#ffcdd2;color:#8a0000} .natl{background:#7a2e12;color:#fff}
  td a{color:#0b5cad;text-decoration:none} td a:hover{text-decoration:underline}
  .empty{padding:20px;text-align:center;color:#777}
  tbody tr{cursor:pointer}
  .chev{color:#9aa5b1;font-weight:700}
  .nsub{font-size:.72rem;color:#667;font-weight:400}
  .fsnone{color:#c2c8d0}
  .tyhit{color:#1d6b1d;font-weight:800}
  .shcell{max-width:340px}
  .supt{white-space:nowrap;font-size:.86rem;color:#4a5568;max-width:210px;overflow:hidden;text-overflow:ellipsis}
  mark{background:#fff2a8;color:inherit;border-radius:2px;padding:0 1px}
  .jhit{color:#1f2937}
  .jrole{display:inline-block;font-size:.6rem;font-weight:700;color:#5b3a8a;background:#ede4fb;border-radius:3px;padding:0 4px;margin-left:3px;vertical-align:middle}
  .enterchip{display:inline-block;font-size:.66rem;font-weight:700;color:#1d6b1d !important;background:#dcf2dc;border-radius:8px;padding:1px 6px;margin-left:4px;text-decoration:none;white-space:nowrap}
  .enterchip:hover{background:#c8ecc8;text-decoration:none !important}
  .docs .doc{text-decoration:none;font-size:1.02rem;margin-right:3px;line-height:1}
  .docs .doc:hover{filter:brightness(1.15)}
  .doclink{display:inline-block;color:#0b5cad;background:#eef3f8;border-radius:5px;padding:1px 7px;margin:0 4px 3px 0;text-decoration:none;white-space:nowrap;font-size:.78rem}
  .doclink:hover{background:#dde8f2}
  .arrived .views,.arrived .filters{display:none}
  #detail{display:none;margin:8px 14px 2px;border:1px solid #e5e7eb;border-radius:8px;overflow:auto;max-height:826px}
  .calbar{transition:filter .12s ease,box-shadow .12s ease,opacity .12s ease}
  .calbar.sel{filter:brightness(1.08)}
  .calbar.oth{opacity:.42}
  .calbar.oth.sel{opacity:1}
  #detail .dhead{position:sticky;top:0;background:#0f2b46;color:#fff;padding:11px 14px;display:flex;align-items:flex-start;gap:10px;z-index:1}
  #detail .dhead h2{margin:0;font-size:1rem;flex:1;line-height:1.3}
  #detail .dhead .sub{font-size:.78rem;opacity:.85;font-weight:400}
  #detail .back{background:rgba(255,255,255,.16);border:0;color:#fff;font:inherit;font-size:.85rem;padding:6px 11px;border-radius:6px;cursor:pointer;flex:none}
  #detail .body{padding:12px 14px 30px}
  .csum{font-size:.85rem;color:#333;margin:0 0 4px}
  .clkey{font-size:.76rem;color:#5a6572;background:#f4f6f8;border:1px solid #e5e7eb;border-radius:8px;padding:8px 11px;margin:14px 0 0;line-height:1.5}
  .csum a{color:#0b5cad}
  .rcardbtn{display:inline-block;margin:2px 0 8px;background:linear-gradient(135deg,#7a2e12,#a3431c);color:#fff;font-weight:700;font-size:.85rem;text-decoration:none;padding:8px 13px;border-radius:8px;box-shadow:0 1px 3px #0003}
  .rcardbtn:hover{filter:brightness(1.07)}
  .showcard{border:1px solid #e5e7eb;border-radius:10px;padding:12px 14px;margin:10px 0;display:flex;gap:16px;align-items:stretch;flex-wrap:wrap}
  .showinfo{flex:0 0 320px;max-width:100%;min-width:0;display:flex;flex-direction:column}
  .showinfo.solo{flex:1 1 100%}
  .showinfo h3{margin:0 0 3px;font-size:.98rem;line-height:1.25}
  .sdate{font-size:.84rem;color:#333}
  .stype{font-size:.68rem;font-weight:700;color:#0f2b46;background:#e7edf3;border-radius:5px;padding:1px 6px;margin-left:5px;white-space:nowrap}
  .sfacts{display:grid;grid-template-columns:100px minmax(0,1fr);gap:6px 10px;margin:10px 0 0;font-size:.82rem;line-height:1.35}
  .sfacts dt{color:#6b7480;font-size:.75rem;padding-top:1px}
  .sfacts dd{margin:0;color:#1f2933;overflow-wrap:anywhere}
  .ssub{display:block;color:#6b7480;font-size:.76rem}
  .sact{display:flex;flex-wrap:wrap;gap:6px;margin-top:auto;padding-top:11px}
  .sbtn{font:inherit;font-size:.76rem;text-decoration:none;border:1px solid #cbd5e1;color:#0f2b46;background:#fff;padding:5px 9px;border-radius:6px;cursor:pointer;white-space:nowrap}
  .sbtn:hover{background:#eef3f8}
  .sbtn.go{background:#dcf2dc;border-color:#b7e0b7;color:#1d6b1d;font-weight:700}
  .sbtn.go:hover{background:#c8ecc8}
  .sbtn.rc{background:#fff3e8;border-color:#d9b48f;color:#7a2e12;font-weight:700}
  .sbtn.rc:hover{background:#ffe8d4}
  .showlane{flex:1 1 380px;min-width:0;display:flex;gap:10px;overflow-x:auto;align-items:stretch;padding-bottom:6px}
  /* two day-of components, no titles: breed ring (centered) + the full group
     running order with your judges (a compact ordered list) */
  .showring{box-sizing:border-box;flex:0 0 172px;background:#fff7ef;border:1px solid #f0d9c4;border-radius:9px;padding:12px 11px;text-align:center;align-self:stretch;display:flex;flex-direction:column;justify-content:center;gap:1px}
  .showring .srbig{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;justify-content:center}
  .showring .srring{font-size:1.1rem;font-weight:800;color:#7a2e12}
  .showring .srt{font-family:'JetBrains Mono',ui-monospace,monospace;font-weight:700;color:#7a2e12;font-size:.98rem}
  .showring .srj{font-size:.85rem;color:#4a3f36;margin-top:3px;font-weight:600}
  .showring .sra{font-size:.79rem;color:#6b5d50;margin-top:5px;line-height:1.4} .showring .sra b{color:#7a2e12}
  .showring .src{font-size:.79rem;color:#6b5d50;margin-top:4px} .showring .src b{color:#7a2e12}
  .showring .src .mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-size:.72rem;color:#9a8a7a}
  .showgroups{box-sizing:border-box;flex:0 0 358px;background:#fff7ef;border:1px solid #f0d9c4;border-radius:9px;padding:10px 11px;align-self:stretch;display:flex;flex-direction:column;gap:8px}
  .showgroups .gbody{display:flex;gap:11px;align-items:stretch}
  .showgroups .ghdr{display:flex;flex-wrap:wrap;gap:6px;align-items:stretch}
  .gsess{font-size:.72rem;color:#6b5d50;background:#fff;border:1px solid #f0d9c4;border-radius:6px;padding:3px 8px;white-space:nowrap}
  .gsess.first{color:#5a4d40;font-weight:600;border-color:#d9b48f;background:#fff3e8}
  .gsl{display:inline-block;box-sizing:border-box;min-width:46px;text-align:center;font-family:'JetBrains Mono',ui-monospace,monospace;font-size:.56rem;font-weight:700;color:#7a2e12;background:#f0d9c4;border-radius:3px;padding:1px 5px;margin-right:6px;vertical-align:middle;letter-spacing:.02em}
  .gnote{font-size:.66rem;color:#a08b78;font-style:italic;margin-top:1px;white-space:normal;line-height:1.3}
  .showgroups .gorder{flex:0 0 158px}
  .showgroups .grow4{display:grid;grid-template-columns:15px 1fr auto;gap:7px;align-items:center;font:inherit;font-size:.76rem;color:#6b5d50;padding:2.5px 6px 2.5px 3px;width:100%;background:none;border:0;border-radius:4px;text-align:left;cursor:pointer}
  .showgroups .grow4:hover{background:rgba(15,43,70,.08)}
  .showgroups .grow4.gmine{color:#7a2e12;font-weight:700;background:rgba(122,46,18,.1);box-shadow:inset 3px 0 0 #7a2e12}
  .showgroups .grow4.gsel,.showgroups .grow4.gsel:hover{background:#0f2b46;color:#fff;box-shadow:none}
  .showgroups .grow4.gmine.gsel,.showgroups .grow4.gmine.gsel:hover{background:#7a2e12;color:#fff}
  .showgroups .grow4.gsel .gn,.showgroups .grow4.gsel .ge{color:#fff}
  .showgroups .gn{font-family:'JetBrains Mono',ui-monospace,monospace;font-size:.64rem;color:#b09a86;text-align:right} .showgroups .gmine .gn{color:#7a2e12}
  .showgroups .gg{white-space:nowrap}
  .showgroups .ge{font-family:'JetBrains Mono',ui-monospace,monospace;font-size:.7rem;color:#9a8a7a} .showgroups .gmine .ge{color:#7a2e12}
  .showgroups .gjudges{flex:1;min-width:0;border-left:1px solid #f0d9c4;padding-left:11px;display:flex;flex-direction:column;gap:5px}
  .showgroups .gjhead{font-size:.76rem;font-weight:800;color:#0f2b46;line-height:1.25}
  .showgroups .gjhead.own{color:#7a2e12}
  .showgroups .grow3{display:grid;grid-template-columns:auto minmax(0,1fr);align-items:baseline;font-size:.82rem;color:#4a3f36;line-height:1.35}
  .showgroups .gnone{font-size:.76rem;color:#a08b78;font-style:italic}
  .showring .srn{font-size:.76rem;color:#a08b78;font-style:italic;margin-top:5px;line-height:1.3}
  @media(max-width:520px){
    .showlane .showgroups{flex-basis:min(330px,84vw)}
    .showgroups .gbody{flex-direction:column}
    .showgroups .gorder{flex:none}
    .showgroups .gjudges{border-left:0;padding-left:0;border-top:1px solid #f0d9c4;padding-top:7px}
  }
  .grl{display:inline-block;box-sizing:border-box;min-width:40px;text-align:right;font-family:'JetBrains Mono',ui-monospace,monospace;font-size:.6rem;font-weight:700;background:#f0d9c4;color:#7a2e12;padding:1px 5px;border-radius:3px;margin-right:5px;vertical-align:middle}
  .grl.n{background:#7a2e12;color:#fff} .grl.b{background:#5b4a3d;color:#fff}
  .eclosed{color:#8a4b00;font-weight:700;background:#ffe8cc;padding:1px 8px;border-radius:5px;font-size:.92em}
  .jsrc{color:#9aa5b1;font-weight:400;font-size:.9em}
</style>
</head>
<body>
  <header><div class="htxt"><h1 id="pageTitle">__TITLE__</h1><p id="pageSub">__SUBTITLE__</p></div><div class="brpick"><span class="brstat" id="breedStatus" role="status"></span><span>Breeds</span><div class="msdd" id="breedDD"><button type="button" class="msbtn" id="breedBtn" aria-haspopup="true" aria-expanded="false">Finnish Spitz ▾</button><div class="mspanel brpanel" id="breedPanel" hidden><input type="search" id="breedQ" placeholder="Find a breed…" aria-label="Find a breed"><div class="brhint" id="breedHint"></div><div id="breedList"></div></div></div></div><button id="natlBtn" class="natlbtn" hidden>★ National Specialty →</button></header>
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
    <div class="msdd" id="stateDD">
      <button type="button" class="msbtn" id="stateBtn">All states ▾</button>
      <div class="mspanel" id="statePanel" hidden></div>
    </div>
    <div class="zones" id="zoneWrap"></div>
    <label><input type="checkbox" id="fHv"> Specialties &amp; groups</label>
    <label class="wlab">Window&nbsp;<select id="fWin">
      <option value="30">30 days</option>
      <option value="60" selected>60 days</option>
      <option value="90">90 days</option>
      <option value="180">6 months</option>
      <option value="all">all upcoming</option>
    </select></label>
    <button type="button" class="clearbtn" id="clearBtn">Clear</button>
    <span class="count" id="count"></span>
  </div>
  <div class="tablewrap">
    <table>
      <thead><tr>
        <th>Dates</th><th>Show(s)</th><th>Location</th><th id="supthdr" title="Superintendent — search a judge's name to swap this to the matched judge">Superintendent</th><th>Zone</th>
        <th id="thishdr" title="Finnish Spitz entered this year (best day), from the judging program. A dash means the program isn't out yet.">FS this yr</th><th id="lasthdr" title="Finnish Spitz entered at this site last year (best-attended day)">FS last yr</th><th>Entries close</th><th title="Premium list / entry form — direct PDF from AKC">Premium</th><th title="Judging program — direct PDF from AKC; posts as the show approaches">Program</th><th></th>
      </tr></thead>
      <tbody id="rows"></tbody>
    </table>
    <div class="empty" id="empty" style="display:none">No shows match those filters.</div>
  </div>
  <div id="detail" aria-hidden="true"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
 integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const FS_ALL = __DATA__;
let ALL = FS_ALL;
const NATL = "__NATL__";
const BREED_API = "__BREED_API__";
const TZ_INFO = __TZINFO__;
const GROUP_OF = __GROUPS__;   // first digit of an AKC breed code -> its group
const PROG_READ = new Set(__PROG_READ__);   // shows whose judging program was fully read
const FS_CODE='313',FS_NAME='Finnish Spitz',MAX_BREEDS=5,BANNER_NOTE_MAX_AGE_MS=120000;
// Finnish Spitz is baked in above, so the default view asks nothing of AKC. Any
// other breed comes from the Worker a month at a time, and the shows are merged
// and re-clustered here, each show carrying one entry per selected breed in .breeds.
let selBreeds=[{code:FS_CODE,name:FS_NAME}];
function hasBreed(code){return selBreeds.some(b=>b.code===code);}
function onlyFS(){return selBreeds.length===1&&selBreeds[0].code===FS_CODE;}
FS_ALL.forEach(c=>c.shows.forEach(s=>{s.breeds=[{code:FS_CODE,name:FS_NAME,breed_judge:s.breed_judge,group_judge:s.group_judge,nohs_judge:s.nohs_judge,clcy:s.clcy,clcy_tip:s.clcy_tip,rc:s.rc}];}));
// The ★ is reserved for the National Specialty. Everything else that's notable
// (a limited-breed group specialty like Non-Sporting) gets a labeled tag instead,
// so a normal specialty weekend never looks like the National.
function isNatlShow(s){return NATL&&hasBreed(FS_CODE)&&String(s.event_no)===String(NATL);}
function isNatl(c){return NATL&&c.shows&&c.shows.some(isNatlShow);}
function specLabel(spec){
  const base=String(spec||'').replace(/\s*group(\s*show)?\s*$/i,'').trim();
  return base?base+' specialty':'Group specialty';
}
function clusterSpecTag(c){const s=(c.shows||[]).find(x=>x.high_value&&x.specialty);return specLabel(s&&s.specialty);}
const AKC='https://www.apps.akc.org/apps/events/search/index_results.cfm?action=plan&event_number=';
const MAPS='https://www.google.com/maps/search/?api=1&query=';
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
function fmtFee(v){const f=parseFloat(v);return isNaN(f)?esc(v):'$'+f.toFixed(2).replace(/\.00$/,'');}
function docIcon(code){return code==='PRMLST'?'📄':code==='JDGPRO'?'📋':code==='ENTFRM'?'📝':'ℹ️';}
function docIcons(docs){return (docs||[]).map(d=>'<a class="doc" target="_blank" rel="noopener" title="'+esc(d.label)+' — PDF from AKC" href="'+esc(d.url)+'">'+docIcon(d.code)+'</a>').join('');}
function docIconsByType(docs){var seen={},out=[];(docs||[]).forEach(function(d){if(!seen[d.code]){seen[d.code]=1;out.push(d);}});return docIcons(out);}
function docCell(docs,codes){return docIconsByType((docs||[]).filter(function(d){return codes.indexOf(d.code)>=0;}));}
function docLinks(docs){return (docs||[]).map(d=>'<a class="doclink" target="_blank" rel="noopener" href="'+esc(d.url)+'">'+docIcon(d.code)+' '+esc(d.label)+'</a>').join('');}
function pd(s){const p=String(s).split('-');return new Date(+p[0],+p[1]-1,+p[2]);}
// Highlight every occurrence of an already-lowercased search term in text.
function hl(text,term){var s=String(text==null?'':text);if(!term)return esc(s);var i=s.toLowerCase().indexOf(term);if(i<0)return esc(s);return esc(s.slice(0,i))+'<mark>'+esc(s.slice(i,i+term.length))+'</mark>'+hl(s.slice(i+term.length),term);}
const JROLES=[['breed_judge','Breed'],['group_judge','Group'],['nohs_judge','NOHS'],['bis_judge','BIS']];
function judgeVals(s,key){return key==='bis_judge'?[s.bis_judge||'']:(s.breeds||[]).map(function(b){return b[key]||'';});}
function judgeHay(c){return (c.shows||[]).map(function(s){return JROLES.map(function(r){return judgeVals(s,r[0]).join(' ');}).join(' ');}).join(' ');}
// First judge (by role priority) across a cluster's shows whose name contains term.
function judgeMatch(c,term){if(!term)return null;var sh=c.shows||[];for(var r=0;r<JROLES.length;r++)for(var i=0;i<sh.length;i++){var vs=judgeVals(sh[i],JROLES[r][0]);for(var k=0;k<vs.length;k++){if(vs[k].toLowerCase().indexOf(term)>=0)return {name:vs[k],role:JROLES[r][1]};}}return null;}
const ENTER_ONOFRIO='https://www.onofrio.com/cgi-bin/db2www/showentry/enter';
function isOnofrio(s){return /onofrio/i.test((s&&s.superint)||'');}
function entriesOpen(s){if(!s||!s.close)return false;const t=new Date(),today=new Date(t.getFullYear(),t.getMonth(),t.getDate());return pd(s.close)>=today;}
function onofrioOpen(c){return (c.shows||[]).some(function(s){return isOnofrio(s)&&entriesOpen(s);});}
function ymd(s){return String(s).replace(/-/g,'');}
function plusDay(s){const d=pd(s);d.setDate(d.getDate()+1);return d.getFullYear()+String(d.getMonth()+1).padStart(2,'0')+String(d.getDate()).padStart(2,'0');}
const map=L.map('map',{scrollWheelZoom:true}).setView([39.5,-98.35],4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:18,attribution:'&copy; OpenStreetMap contributors'}).addTo(map);
const markers=L.layerGroup().addTo(map);
const q=document.getElementById('q'),fWin=document.getElementById('fWin'),fHv=document.getElementById('fHv'),rowsEl=document.getElementById('rows'),countEl=document.getElementById('count'),emptyEl=document.getElementById('empty'),detailEl=document.getElementById('detail');
const calGrid=document.getElementById('calGrid'),calLabel=document.getElementById('calLabel'),calPrev=document.getElementById('calPrev'),calNext=document.getElementById('calNext'),calToday=document.getElementById('calToday'),tableWrap=document.querySelector('.tablewrap');
let AIDX=new Map(ALL.map((c,i)=>[c,i]));
let markerByAi={},SELMK=null,calMonth=null;
// Timezones = toggle chips (multi). States = a checkbox dropdown whose options
// are limited to the selected zones, so you can't pick a state outside them
// (kills the "Mountain + California" empty-result confusion).
const ZONES=[['ET','Eastern'],['CT','Central'],['MT','Mountain'],['PT','Pacific'],['AKT','Alaska'],['HAT','Hawaii']].filter(t=>ALL.some(c=>c.tz===t[0]));
const selZones=new Set(),selStates=new Set();
const zoneWrap=document.getElementById('zoneWrap'),stateBtn=document.getElementById('stateBtn'),statePanel=document.getElementById('statePanel'),clearBtn=document.getElementById('clearBtn');
function availStates(){return [...new Set(ALL.filter(c=>!selZones.size||selZones.has(c.tz)).map(c=>c.state).filter(Boolean))].sort();}
function syncStatesToZones(){if(!selZones.size)return;[...selStates].forEach(s=>{if(!ALL.some(c=>c.state===s&&selZones.has(c.tz)))selStates.delete(s);});}
function updateStateBtn(){stateBtn.textContent=(selStates.size?selStates.size+' state'+(selStates.size>1?'s':''):'All states')+' ▾';}
function buildStatePanel(){const st=availStates();statePanel.innerHTML=st.length?st.map(s=>'<label><input type="checkbox" value="'+s+'"'+(selStates.has(s)?' checked':'')+'> '+s+'</label>').join(''):'<div style="padding:6px;color:#889;font-size:.8rem">No states in the chosen zones</div>';}
ZONES.forEach(function(t){var b=document.createElement('button');b.type='button';b.className='zchip';b.dataset.tz=t[0];b.textContent=t[1];b.addEventListener('click',function(){if(selZones.has(t[0]))selZones.delete(t[0]);else selZones.add(t[0]);b.classList.toggle('on');syncStatesToZones();buildStatePanel();updateStateBtn();renderAll(true);});zoneWrap.appendChild(b);});
stateBtn.addEventListener('click',function(e){e.stopPropagation();statePanel.hidden=!statePanel.hidden;});
statePanel.addEventListener('change',function(e){var cb=e.target;if(cb&&cb.matches&&cb.matches('input[type=checkbox]')){if(cb.checked)selStates.add(cb.value);else selStates.delete(cb.value);updateStateBtn();renderAll(true);}});
document.addEventListener('click',function(e){if(!e.target.closest('#stateDD'))statePanel.hidden=true;});
clearBtn.addEventListener('click',function(){selZones.clear();selStates.clear();q.value='';fHv.checked=false;fWin.value='60';[...zoneWrap.querySelectorAll('.zchip')].forEach(function(b){b.classList.remove('on');});buildStatePanel();updateStateBtn();statePanel.hidden=true;renderAll(true);});
// Persist the filter set across visits (per browser) so a planner's view sticks.
function saveFilters(){try{localStorage.setItem('fsca_filters',JSON.stringify({q:q.value,win:fWin.value,hv:fHv.checked,zones:[...selZones],states:[...selStates],breeds:selBreeds}));}catch(e){}}
function loadFilters(){
  var f=null;try{f=JSON.parse(localStorage.getItem('fsca_filters')||'null');}catch(e){}
  if(f){
    if(typeof f.q==='string')q.value=f.q;
    if(f.win)fWin.value=f.win;
    if(typeof f.hv==='boolean')fHv.checked=f.hv;
    (f.zones||[]).forEach(z=>selZones.add(z));
    (f.states||[]).forEach(s=>selStates.add(s));
    const saved=(Array.isArray(f.breeds)?f.breeds:[]).filter(b=>b&&/^\d{3}$/.test(b.code)&&typeof b.name==='string').slice(0,MAX_BREEDS);
    if(saved.length)selBreeds=saved.map(b=>({code:b.code,name:b.name}));
    [...zoneWrap.querySelectorAll('.zchip')].forEach(function(b){if(selZones.has(b.dataset.tz))b.classList.add('on');});
    syncStatesToZones();
  }
  buildStatePanel();updateStateBtn();
}
loadFilters();
let shown=[];
function baseMatch(c){
  const term=q.value.trim().toLowerCase(),hv=fHv.checked;
  if(selStates.size&&!selStates.has(c.state))return false;
  if(selZones.size&&!selZones.has(c.tz))return false;
  if(hv&&!c.high_value)return false;
  if(term&&!((c.label+' '+c.venue+' '+c.city+' '+c.state+' '+c.clubs.join(' ')+' '+(c.supts||[]).join(' ')+' '+judgeHay(c)).toLowerCase().includes(term)))return false;
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
    m.bindTooltip((isNatl(c)?'★ ':'')+c.label+(c.n>1?(' (+'+(c.n-1)+' more)'):''));
    m.on('click',()=>openDetail(c));
    markers.addLayer(m); markerByAi[AIDX.get(c)]=m;
  });
  if(refit&&latlngs.length){try{map.fitBounds(latlngs,{maxZoom:7,padding:[24,24]});}catch(_){}}
  let shows=0;const term=q.value.trim().toLowerCase();let anyJudge=false;
  rowsEl.innerHTML=shown.map(c=>{
    shows+=c.n;
    const loc=[c.city,c.state].filter(Boolean).join(', ');
    const name=c.n>1?(hl(c.label,term)+' <span class="nsub">+ '+(c.n-1)+' more show'+(c.n-1===1?'':'s')+'</span>'):hl(c.label,term);
    const tags=(isNatl(c)?' <span class="tag natl">★ National Specialty</span>':(c.high_value?' <span class="tag hv">'+esc(clusterSpecTag(c))+'</span>':''))+(c.pended?' <span class="tag pend">PENDED</span>':'');
    const enter=onofrioOpen(c)?' <a class="enterchip" target="_blank" rel="noopener" title="Enter online at Onofrio — entries open" href="'+ENTER_ONOFRIO+'" onclick="event.stopPropagation()">Enter ↗</a>':'';
    // Swappable column: the matched judge (name + role, highlighted) when the
    // search hit a judge, else the superintendent.
    const jm=judgeMatch(c,term);if(jm)anyJudge=true;
    const swap=jm
      ? '<span class="jhit">'+hl(jm.name,term)+' <span class="jrole">'+jm.role+'</span></span>'
      : (c.superint?hl(c.superint,term):'<span class="fsnone">—</span>');
    return '<tr data-ai="'+AIDX.get(c)+'"><td style="white-space:nowrap">'+esc(c.dates)+'</td>'+
      '<td class="shcell">'+name+tags+enter+'</td>'+
      '<td style="white-space:nowrap">'+hl(loc,term)+'</td>'+
      '<td class="supt">'+swap+'</td>'+
      '<td style="white-space:nowrap"><span class="dot" style="background:'+esc(c.color)+'"></span>'+esc(c.tzLabel)+'</td>'+
      thisYearCell(thisYear(c))+
      '<td style="text-align:center;white-space:nowrap"'+(c.fs_last_tip?' title="'+esc(c.fs_last_tip)+'"':'')+'>'+(c.fs_last!=null?'<b>'+c.fs_last+'</b>':'<span class="fsnone">—</span>')+'</td>'+
      '<td style="white-space:nowrap">'+esc(c.close)+'</td>'+
      '<td class="docs" style="white-space:nowrap">'+docCell(c.docs,['PRMLST','ENTFRM','EVNINF'])+'</td>'+
      '<td class="docs" style="white-space:nowrap">'+docCell(c.docs,['JDGPRO'])+'</td>'+
      '<td class="chev">&rsaquo;</td></tr>';
  }).join('');
  var sh=document.getElementById('supthdr');if(sh)sh.textContent=anyJudge?'Judge':'Superintendent';
  countEl.textContent=shown.length+' listing'+(shown.length===1?'':'s')+' · '+shows+' show'+(shows===1?'':'s');
  emptyEl.textContent=breedsLoading()?'Loading shows from AKC…':'No shows match those filters.';
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
      let cur=false;
      for(let gi=s;gi<=e;gi++){const dd=new Date(gridStart);dd.setDate(gridStart.getDate()+gi);if(dd.getMonth()===m){cur=true;break;}}
      bars.push({c,s:s-wStart,e:e-wStart,cont:gidx(pd(c.start))<wStart,oth:!cur});
    });
    bars.sort((a,b)=>a.s-b.s||b.e-a.e);
    const laneEnd=[];
    bars.forEach(b=>{let l=0;while(laneEnd[l]!==undefined&&laneEnd[l]>=b.s)l++;laneEnd[l]=b.e;b.lane=l;});
    let bh='';const MAXL=3,ov={};
    bars.forEach(b=>{
      if(b.lane>=MAXL){for(let d=b.s;d<=b.e;d++)ov[d]=(ov[d]||0)+1;return;}
      const left=(b.s/7*100),width=((b.e-b.s+1)/7*100);
      bh+='<button class="calbar'+(b.oth?' oth':'')+'" data-ai="'+AIDX.get(b.c)+'" title="'+esc(b.c.label)+' — '+esc(b.c.dates)+'" style="left:calc('+left.toFixed(3)+'% + 2px);width:calc('+width.toFixed(3)+'% - 4px);top:'+(20+b.lane*17)+'px;background:'+esc(b.c.color)+'">'+(isNatl(b.c)?'★ ':'')+(b.cont?'‹ ':'')+esc(b.c.label)+'</button>';
    });
    for(const d in ov)bh+='<span class="cmore" style="position:absolute;left:calc('+(d/7*100).toFixed(3)+'% + 3px);top:'+(20+MAXL*17)+'px">+'+ov[d]+'</span>';
    h+='<div class="calwk">'+cells+bh+'</div>';
  }
  const mk=y+'-'+m;
  calGrid.innerHTML=h;
  if(calGrid._mk!==mk){calGrid.classList.remove('fade');void calGrid.offsetWidth;calGrid.classList.add('fade');calGrid._mk=mk;}
}
function renderAll(refit){saveFilters();render(refit);renderCal();}
function highlight(ai){
  document.querySelectorAll('.sel').forEach(e=>e.classList.remove('sel'));
  document.querySelectorAll('[data-ai="'+ai+'"]').forEach(e=>e.classList.add('sel'));
  if(SELMK){try{SELMK.setStyle({radius:7,weight:1.5,color:'#fff'});}catch(_){}}
  const mk=markerByAi[ai];
  if(mk){try{mk.setStyle({radius:10,weight:3,color:'#0f2b46'});mk.openTooltip();map.panTo(mk.getLatLng());}catch(_){}SELMK=mk;}
}
rowsEl.addEventListener('click',e=>{if(e.target.closest('a'))return;const tr=e.target.closest('tr[data-ai]');if(tr)openDetail(ALL[+tr.dataset.ai]);});
calGrid.addEventListener('click',e=>{const b=e.target.closest('.calbar[data-ai]');if(b)openDetail(ALL[+b.dataset.ai]);});
calPrev.addEventListener('click',()=>{calMonth=new Date(calMonth.getFullYear(),calMonth.getMonth()-1,1);renderCal();requestBreedData(false);});
calNext.addEventListener('click',()=>{calMonth=new Date(calMonth.getFullYear(),calMonth.getMonth()+1,1);renderCal();requestBreedData(false);});
calToday.addEventListener('click',()=>{const n=new Date();calMonth=new Date(n.getFullYear(),n.getMonth(),1);renderCal();requestBreedData(false);});
function descText(s,c){
  const L=[];
  if(s.close)L.push('Entries close: '+s.close);
  if(s.online)L.push('Enter online: '+s.online);
  if(s.superint)L.push('Superintendent: '+s.superint);
  const bs=s.breeds||[];
  bs.forEach(b=>{
    if(b.breed_judge)L.push((bs.length>1?b.name+' judge: ':'Breed judge: ')+b.breed_judge);
    if(b.clcy)L.push(b.name+' entered last year: '+b.clcy);
  });
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
function judgeRows(b){
  const r=[];
  if(b.breed_judge&&b.breed_judge===b.group_judge)r.push(['Breed & Group',b.breed_judge]);
  else{if(b.breed_judge)r.push(['Breed',b.breed_judge]);if(b.group_judge)r.push(['Group',b.group_judge]);}
  if(b.nohs_judge)r.push(['NOHS',b.nohs_judge]);
  return r;
}
function fact(label,value){return '<dt>'+esc(label)+'</dt><dd>'+value+'</dd>';}
function fmtDay(iso){const d=pd(iso);return MON[d.getMonth()]+' '+d.getDate();}
function groupOf(code){return GROUP_OF[String(code).charAt(0)]||'';}
function groupKey(name){return String(name||'').toLowerCase().replace(' group','').trim();}
// Which group each show's running order is showing judges for: event_no -> index.
let groupPick={};
// ---- clustering: the browser's copy of cluster_events/_make_cluster in akcal.py
const DOW=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'],MON=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function dayLabel(d){return DOW[d.getDay()]+' '+MON[d.getMonth()]+' '+d.getDate();}
function spanLabel(a,b){
  const sd=pd(a),ed=pd(b);
  if(a===b)return dayLabel(sd)+', '+sd.getFullYear();
  if(sd.getFullYear()!==ed.getFullYear())return dayLabel(sd)+', '+sd.getFullYear()+' – '+dayLabel(ed)+', '+ed.getFullYear();
  return dayLabel(sd)+' – '+dayLabel(ed)+', '+ed.getFullYear();
}
function shortSupt(name){
  const n=String(name||'').trim();
  if(!n)return '';
  return n.split(/,|\bDog Shows\b|\bEvent Services\b|\bDog Show\b|\bInc\.?|\bL\.?L\.?C\.?\b/i)[0].replace(/^[ ,.]+|[ ,.]+$/g,'')||n;
}
function clcyTip(v,name){
  const m=/^\s*(\d+)-(\d+)-(\d+)\s*\((\d+)-(\d+)\)\s*(\d+)\s*$/.exec(String(v||''));
  if(!m)return '';
  const n=(k,one,many)=>k+' '+(+k===1?one:many);
  const bits=[n(m[2],'class dog','class dogs'),n(m[3],'class bitch','class bitches'),n(m[4],'champion dog','champion dogs'),n(m[5],'champion bitch','champion bitches')];
  if(+m[6])bits.push(n(m[6],'veteran','veterans'));
  return m[1]+' '+name+' total — '+bits.join(', ');
}
function isoPlusDay(s){const d=pd(s);d.setDate(d.getDate()+1);return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');}
// Best-attended day per breed at a site; with several breeds, their sum and a breakdown.
function lastYear(members){
  const best={};
  members.forEach(m=>(m.breeds||[]).forEach(b=>{
    const hit=/^\s*(\d+)/.exec(String(b.clcy||''));
    if(hit&&(!best[b.code]||+hit[1]>best[b.code].n))best[b.code]={n:+hit[1],name:b.name,tip:b.clcy_tip};
  }));
  const v=Object.values(best);
  if(!v.length)return {n:null,tip:''};
  if(selBreeds.length===1)return {n:v[0].n,tip:v[0].tip||''};
  return {n:v.reduce((t,x)=>t+x.n,0),tip:v.map(x=>x.name+': '+x.n).join(' · ')+' (best day for each)'};
}
// This year's entries from the judging programs: best day per breed at a site, or
// null while no program is known. Zero means a program was read and nobody entered.
function thisYear(c){
  const best={};
  c.shows.forEach(s=>{
    const read=PROG_READ.has(String(s.event_no));
    (s.breeds||[]).forEach(b=>{
      if(!read&&!b.rc)return;
      const n=(b.rc&&b.rc.count)||0;
      if(!best[b.code]||n>best[b.code].n)best[b.code]={n,name:b.name};
    });
  });
  const v=Object.values(best);
  if(!v.length)return null;
  return {n:v.reduce((t,x)=>t+x.n,0),tip:v.map(x=>x.name+': '+x.n).join(' · ')+' (best day'+(v.length>1?' for each':'')+')'};
}
function thisYearCell(y){
  const inner=!y?'<span class="fsnone">—</span>':y.n?'<span class="tyhit">'+y.n+'</span>':'<span class="fsnone">0</span>';
  return '<td style="text-align:center;white-space:nowrap"'+(y?' title="'+esc(y.tip)+'"':'')+'>'+inner+'</td>';
}
function makeCluster(members,start,end){
  const m0=members[0],z=TZ_INFO[String(m0.state||'').trim().toUpperCase()]||TZ_INFO[''];
  const clubs=[...new Set(members.map(m=>m.club))];
  const supts=[...new Set(members.filter(m=>m.superint).map(m=>shortSupt(m.superint)))];
  const closes=members.map(m=>m.close).filter(Boolean).sort();
  const seen=new Set(),docs=[],ord={PRMLST:0,JDGPRO:1,ENTFRM:2,EVNINF:3},dr=d=>d.code in ord?ord[d.code]:9;
  members.forEach(m=>(m.docs||[]).forEach(d=>{if(!seen.has(d.url)){seen.add(d.url);docs.push(d);}}));
  docs.sort((a,b)=>dr(a)-dr(b));
  const last=lastYear(members);
  const c={start,end,dates:spanLabel(start,end),tz:z[0],tzLabel:z[2],color:z[1],state:m0.state,city:m0.city,venue:m0.venue,
    where:[m0.city,m0.state].filter(Boolean).join(', '),lat:m0.lat,lon:m0.lon,
    label:clubs.length===1?clubs[0]:(m0.venue||m0.city||'Show cluster'),clubs,n:members.length,
    superint:supts[0]||'',supts,high_value:members.some(m=>m.high_value),pended:members.some(m=>m.pended),
    close:closes[0]||'',fs_last:last.n,fs_last_tip:last.tip,docs,event_no:m0.event_no,shows:members};
  const cards=new Set(members.filter(m=>m.rcard).map(m=>m.rcard));
  if(cards.size===1)c.rcard=[...cards][0];
  return c;
}
function clusterShows(list){
  const bySite=new Map();
  list.forEach(s=>{
    const site=(s.lat!=null&&s.lon!=null)?(s.lat.toFixed(3)+','+s.lon.toFixed(3))
      :([String(s.venue||'').trim().toLowerCase(),s.city||'',s.state||''].filter(Boolean).join('|')||s.event_no);
    if(!bySite.has(site))bySite.set(site,[]);
    bySite.get(site).push(s);
  });
  const out=[];
  bySite.forEach(group=>{
    group.sort((a,b)=>a.start<b.start?-1:a.start>b.start?1:a.end<b.end?-1:a.end>b.end?1:0);
    let cur=null;
    group.forEach(s=>{
      if(cur&&s.start<=isoPlusDay(cur.end)){cur.members.push(s);if(s.end>cur.end)cur.end=s.end;}
      else{if(cur)out.push(makeCluster(cur.members,cur.start,cur.end));cur={members:[s],start:s.start,end:s.end};}
    });
    if(cur)out.push(makeCluster(cur.members,cur.start,cur.end));
  });
  return out.sort((a,b)=>a.start<b.start?-1:a.start>b.start?1:0);
}
// ---- breed data: FS from the page, everything else from the Worker
const monthStore=new Map();  // 'code|YYYY-MM' -> {shows} | {loading:true} | {error:true}
const ringData=new Map();    // code -> {event_no: ring info}, prebuilt each sync
const fetchQ=[];
let inFlight=0,rebuildTimer=null,refitPending=false,natlReady=false;
function monthKey(d){return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0');}
function selectedEntries(){return [...monthStore.entries()].filter(e=>hasBreed(e[0].split('|')[0]));}
function breedsLoading(){return selectedEntries().some(e=>e[1].loading);}
function monthsNeeded(){
  const now=new Date(),y=now.getFullYear(),m=now.getMonth();
  let span=18;
  if(fWin.value!=='all'){const e=new Date(y,m,now.getDate()+(+fWin.value));span=(e.getFullYear()-y)*12+e.getMonth()-m;}
  const out=[];
  // Early in a month, a show weekend can still reach back across the 1st.
  if(now.getDate()<=7)out.push(monthKey(new Date(y,m-1,1)));
  for(let i=0;i<=span;i++)out.push(monthKey(new Date(y,m+i,1)));
  if(calMonth){
    const off=(calMonth.getFullYear()-y)*12+calMonth.getMonth()-m;
    if(off>=-1&&off<=18&&!out.includes(monthKey(calMonth)))out.push(monthKey(calMonth));
  }
  return out;
}
function requestBreedData(refit){
  if(refit)refitPending=true;
  selBreeds.forEach(b=>{
    if(b.code===FS_CODE)return;
    if(!ringData.has(b.code))loadRings(b.code);
    monthsNeeded().forEach(month=>{
      const key=b.code+'|'+month;
      if(!monthStore.has(key)){monthStore.set(key,{loading:true});fetchQ.push({code:b.code,month,key,tries:0});}
    });
  });
  pump();
  updateBreedStatus();
  if(refitPending&&!breedsLoading())scheduleRebuild();
}
function loadRings(code){
  ringData.set(code,{});
  fetch('rings/'+code+'.json').then(r=>r.ok?r.json():null).then(d=>{if(d){ringData.set(code,d);scheduleRebuild();}}).catch(()=>{});
}
function pump(){
  while(inFlight<2&&fetchQ.length){
    const job=fetchQ.shift();
    if(!hasBreed(job.code)){monthStore.delete(job.key);continue;}
    inFlight++;
    fetch(BREED_API+'/shows?breed='+job.code+'&month='+job.month)
      .then(r=>{
        if(r.status===429&&job.tries<5){job.tries++;setTimeout(()=>{fetchQ.push(job);pump();},12000);return null;}
        if(!r.ok)throw new Error('HTTP '+r.status);
        return r.json();
      })
      .then(d=>{if(d)monthStore.set(job.key,{shows:d.shows||[]});})
      .catch(()=>{monthStore.set(job.key,{error:true});})
      .finally(()=>{inFlight--;pump();updateBreedStatus();scheduleRebuild();});
  }
}
function updateBreedStatus(){
  const el=document.getElementById('breedStatus'),ents=selectedEntries();
  const left=ents.filter(e=>e[1].loading).length,failed=ents.some(e=>e[1].error);
  if(left){el.textContent='Loading shows from AKC…';return;}
  if(failed){
    el.innerHTML='Some shows couldn’t be loaded. <button type="button" id="breedRetry">Try again</button>';
    document.getElementById('breedRetry').addEventListener('click',()=>{ents.forEach(e=>{if(e[1].error)monthStore.delete(e[0]);});requestBreedData(false);});
    return;
  }
  el.textContent='';
}
function rebuildData(){
  if(onlyFS())ALL=FS_ALL;
  else{
    const byNo=new Map();
    if(hasBreed(FS_CODE))FS_ALL.forEach(c=>c.shows.forEach(s=>byNo.set(String(s.event_no),Object.assign({},s,{breeds:s.breeds.slice(),venue:c.venue,city:c.city,state:c.state,lat:c.lat,lon:c.lon}))));
    selBreeds.forEach(b=>{
      if(b.code===FS_CODE)return;
      const rings=ringData.get(b.code)||{};
      monthStore.forEach((v,k)=>{
        if(!v.shows||k.split('|')[0]!==b.code)return;
        v.shows.forEach(rec=>{
          const no=String(rec.event_no);
          let s=byNo.get(no);
          if(!s){s=Object.assign({},rec,{dates:spanLabel(rec.start,rec.end),clcy_tip:'',breeds:[]});byNo.set(no,s);}
          if(!s.breeds.some(x=>x.code===b.code))s.breeds.push({code:b.code,name:b.name,breed_judge:rec.breed_judge,group_judge:rec.group_judge,nohs_judge:rec.nohs_judge,clcy:rec.clcy,clcy_tip:clcyTip(rec.clcy,b.name),rc:rings[no]});
        });
      });
    });
    ALL=clusterShows([...byNo.values()]);
  }
  AIDX=new Map(ALL.map((c,i)=>[c,i]));
}
function refreshOpenDetail(){
  if(!CUR)return;
  const nos=new Set(CUR.shows.map(s=>String(s.event_no)));
  const c=ALL.find(x=>x.shows.some(s=>nos.has(String(s.event_no))));
  if(!c){closeDetail();return;}
  CUR=c;
  const top=detailEl.scrollTop;
  renderDetail(c);
  detailEl.scrollTop=top;
  highlight(AIDX.get(c));
}
function scheduleRebuild(){
  clearTimeout(rebuildTimer);
  rebuildTimer=setTimeout(()=>{
    const refit=refitPending&&!breedsLoading();
    if(refit)refitPending=false;
    rebuildData();buildStatePanel();renderAll(refit);refreshOpenDetail();
  },150);
}
// ---- the picker
const breedBtn=document.getElementById('breedBtn'),breedPanel=document.getElementById('breedPanel'),breedQ=document.getElementById('breedQ'),breedListEl=document.getElementById('breedList'),breedHint=document.getElementById('breedHint');
let breedList=null,breedOrder=[],breedListState='';
function breedSummary(){return selBreeds[0].name+(selBreeds.length>1?' + '+(selBreeds.length-1)+' more':'');}
const BASE_SUB=document.getElementById('pageSub').textContent;
function syncBreedHeader(){
  const title=breedSummary()+' — AKC Events',last=document.getElementById('lasthdr'),natl=document.getElementById('natlBtn');
  document.getElementById('pageTitle').textContent=title;
  document.title=title;
  document.getElementById('pageSub').textContent=onlyFS()?BASE_SUB:'Live from AKC · filter & click any show below';
  breedBtn.textContent=breedSummary()+' ▾';
  last.textContent=onlyFS()?'FS last yr':'Last yr';
  last.title=(selBreeds.length===1?selBreeds[0].name:'Selected breeds')+' entered at this site last year (best-attended day)';
  const thisH=document.getElementById('thishdr');
  thisH.textContent=onlyFS()?'FS this yr':'This yr';
  thisH.title=(selBreeds.length===1?selBreeds[0].name:'Selected breeds')+' entered this year (best day), from the judging program. A dash means the program isn\'t out yet.';
  if(natl&&natlReady)natl.hidden=!hasBreed(FS_CODE);
}
function setBreeds(next){
  selBreeds=next;
  groupPick={};
  syncBreedHeader();
  rebuildData();buildStatePanel();renderAll(true);refreshOpenDetail();
  requestBreedData(true);
}
// Selected breeds float to the top when the panel opens, then hold still while you tick.
function orderBreeds(){breedOrder=[...selBreeds].concat((breedList||[]).filter(b=>!hasBreed(b.code)));}
function renderBreedList(){
  const full=selBreeds.length>=MAX_BREEDS;
  breedHint.textContent=full?'That’s the most you can pick at once ('+MAX_BREEDS+'). Untick one to swap.':'Pick up to '+MAX_BREEDS+'.';
  if(!breedList){
    const fsRow=breedListState==='error'?'<div class="brhint">Couldn’t load the breed list. Close this and try again.</div>':'<div class="brhint">Loading breeds…</div>';
    breedListEl.innerHTML=fsRow;
    return;
  }
  const term=breedQ.value.trim().toLowerCase();
  const rows=breedOrder.filter(b=>!term||(b.name+' '+(b.aka||'')).toLowerCase().includes(term));
  breedListEl.innerHTML=rows.length?rows.map(b=>{
    const on=hasBreed(b.code),off=(full&&!on)||(on&&selBreeds.length===1);
    return '<label'+(off?' class="off"':'')+'><input type="checkbox" value="'+esc(b.code)+'"'+(on?' checked':'')+(off?' disabled':'')+'> '+esc(b.name)+'</label>';
  }).join(''):'<div class="brhint">No breed by that name.</div>';
}
function loadBreedList(){
  if(breedList||breedListState==='loading')return;
  breedListState='loading';
  fetch(BREED_API+'/breeds').then(r=>{if(!r.ok)throw new Error('HTTP '+r.status);return r.json();})
    .then(d=>{breedList=d.breeds||[];breedListState='';orderBreeds();})
    .catch(()=>{breedListState='error';})
    .finally(renderBreedList);
}
function closeBreedPanel(){breedPanel.hidden=true;breedBtn.setAttribute('aria-expanded','false');}
breedBtn.addEventListener('click',e=>{
  e.stopPropagation();
  if(!breedPanel.hidden){closeBreedPanel();return;}
  breedPanel.hidden=false;
  breedBtn.setAttribute('aria-expanded','true');
  breedQ.value='';orderBreeds();loadBreedList();renderBreedList();
  breedQ.focus();
});
breedQ.addEventListener('input',renderBreedList);
breedListEl.addEventListener('change',e=>{
  const cb=e.target;
  if(!cb||!cb.matches('input[type=checkbox]'))return;
  const b=breedOrder.find(x=>x.code===cb.value);
  if(!b)return;
  setBreeds(cb.checked?selBreeds.concat([{code:b.code,name:b.name}]):selBreeds.filter(x=>x.code!==b.code));
  renderBreedList();
});
document.addEventListener('click',e=>{if(!e.target.closest('#breedDD'))closeBreedPanel();});
document.addEventListener('keydown',e=>{if(e.key==='Escape'&&!breedPanel.hidden){closeBreedPanel();breedBtn.focus();}});
function showCard(s,i){
  // Left: the show's facts, adapting to where it is in its lifecycle. Right, once
  // a judging program exists: one group card, then a card per selected breed,
  // scrolling sideways however many there are. Breed and group judges come from
  // AKC where it names them; the program supplies ring / time / running order /
  // this-year count, plus the judges of groups AKC didn't report for this search.
  const now=new Date(), today=new Date(now.getFullYear(),now.getMonth(),now.getDate());
  const bs=s.breeds||[], multi=bs.length>1, lane=bs.some(b=>b.rc);
  const closed=s.close && pd(s.close) < today;
  const f=[];
  if(!lane){
    if(multi){
      bs.forEach(b=>{const r=judgeRows(b);if(r.length)f.push(fact(b.name,r.map(x=>'<b>'+esc(x[0])+':</b> '+esc(x[1])).join('<br>')));});
    }else if(bs[0]){
      judgeRows(bs[0]).forEach(x=>f.push(fact(x[0],esc(x[1]))));
    }
    if(s.bis_judge)f.push(fact('Best in Show',esc(s.bis_judge)));
    bs.forEach(b=>{if(b.clcy)f.push(fact(b.name+' last year','<abbr title="'+esc(b.clcy_tip||'')+'" style="text-decoration:underline dotted;cursor:help">'+esc(b.clcy)+'</abbr>'));});
  }
  if(s.fee)f.push(fact('Entry fee',fmtFee(s.fee)));
  if(closed)f.push(fact('Entries','<span class="eclosed">Closed '+esc(fmtDay(s.close))+'</span>'));
  else if(s.open||s.close)f.push(fact('Entries',[s.open?'Open '+esc(fmtDay(s.open)):'',s.close?'Close '+esc(fmtDay(s.close)):''].filter(Boolean).join(' · ')));
  if(s.superint)f.push(fact('Superintendent',esc(s.superint)+((s.supt_phone||s.supt_email)?'<span class="ssub">'+[s.supt_phone,s.supt_email].filter(Boolean).map(esc).join(' · ')+'</span>':'')));
  if(s.docs&&s.docs.length)f.push(fact('Documents',docLinks(s.docs)));
  const act=[];
  if(s.online&&!closed)act.push('<a class="sbtn go" target="_blank" rel="noopener" href="'+esc(s.online)+'">Enter online →</a>');
  if(isOnofrio(s)&&entriesOpen(s))act.push('<a class="sbtn go" target="_blank" rel="noopener" href="'+ENTER_ONOFRIO+'">🖊 Enter at Onofrio →</a>');
  if(s.rcard&&!CUR.rcard)act.push('<a class="sbtn rc" target="_blank" rel="noopener" href="'+esc(s.rcard)+'">🎯 Ring card for this day →</a>');
  act.push('<a class="sbtn" target="_blank" rel="noopener" href="'+AKC+encodeURIComponent(s.event_no)+'">AKC event page ↗</a>');
  act.push('<a class="sbtn" target="_blank" rel="noopener" href="'+gcalHref(s,CUR)+'">＋ Google Calendar</a>');
  act.push('<button type="button" class="sbtn" data-ics="'+i+'">Download .ics</button>');
  const tags=(isNatlShow(s)?' <span class="tag natl">★ National Specialty</span>':(s.high_value?' <span class="tag hv">'+esc(specLabel(s.specialty))+'</span>':''))+(s.pended?' <span class="tag pend">PENDED</span>':'');
  return '<div class="showcard"><div class="showinfo'+(lane?'':' solo')+'"><h3>'+esc(s.club)+tags+'</h3>'+
    '<div class="sdate">'+esc(s.dates)+(s.comp_type?'<span class="stype">'+esc(s.comp_type)+'</span>':'')+'</div>'+
    (f.length?'<dl class="sfacts">'+f.join('')+'</dl>':'')+
    '<div class="sact">'+act.join('')+'</div></div>'+
    (lane?'<div class="showlane">'+groupPanel(s)+bs.map(b=>breedCard(b,multi)).join('')+'</div>':'')+'</div>';
}
function breedCard(b,multi){
  const name=multi?'<div class="srb">'+esc(b.name)+'</div>':'';
  const judge=b.breed_judge?'<div class="srj">'+esc(b.breed_judge)+'</div>':'';
  const rc=b.rc;
  if(!rc)return '<div class="showring">'+name+judge+'<div class="srn">No ring found in this program</div>'+
    (b.clcy?'<div class="src" title="'+esc(b.clcy_tip||'')+'">Last year <span class="mono">'+esc(b.clcy)+'</span></div>':'')+'</div>';
  const order=rc.ahead>0
    ? ('<b>'+rc.ahead+'</b> ahead'+(rc.prev?' · after '+esc(rc.prev)+(rc.prevN?' ×'+rc.prevN:''):''))
    : '<b>first</b> in the ring';
  return '<div class="showring">'+name+
    '<div class="srbig"><span class="srring">Ring '+esc(rc.ring)+'</span>'+(rc.time?'<span class="srt">'+esc(rc.time)+'</span>':'')+'</div>'+
    judge+'<div class="sra">'+order+'</div>'+
    (rc.count!=null?'<div class="src"><b>'+esc(rc.count)+'</b> '+esc(b.name)+(rc.split?' · <span class="mono">'+esc(rc.split)+'</span>':'')+'</div>':'')+
    '</div>';
}
// One running order per show: every selected breed's group is highlighted, any
// group can be clicked, and the right side shows the clicked group's judges.
function groupPanel(s){
  const bs=s.breeds||[],src=bs.find(b=>b.rc);
  if(!src)return '';
  const rc=src.rc,seq=rc.gseq||[];
  const owners=name=>bs.filter(b=>groupKey(groupOf(b.code))===groupKey(name));
  let pick=groupPick[s.event_no];
  if(!(pick>=0&&pick<seq.length)){
    pick=seq.findIndex(g=>owners(g.g).some(b=>b.code===bs[0].code));
    if(pick<0)pick=seq.findIndex(g=>owners(g.g).length);
    if(pick<0)pick=0;
  }
  const rows=seq.map((g,ix)=>'<button type="button" class="grow4'+(owners(g.g).length?' gmine':'')+(ix===pick?' gsel':'')+'" data-gs="'+esc(s.event_no)+'" data-g="'+ix+'" aria-pressed="'+(ix===pick)+'">'+
    '<span class="gn">'+(ix+1)+'</span><span class="gg">'+esc(g.g)+'</span><span class="ge">'+(g.est?'~'+esc(g.est):'')+'</span></button>').join('');
  const g=seq[pick],own=g?owners(g.g):[src];
  const b=own[0];
  const reg=(b&&b.group_judge)||(g&&g.j)||'',oh=(b&&b.nohs_judge)||(g&&g.nj)||(b&&b.rc&&b.rc.nohs)||'';
  const head=own.length?own.map(x=>x.name).join(' & ')+' judges':(g.g+' Group judges');
  let gl='<div class="gjhead'+(own.length?' own':'')+'">'+esc(head)+'</div>';
  if(reg)gl+='<div class="grow3"><span class="grl">Reg</span> '+esc(reg)+'</div>';
  if(oh)gl+='<div class="grow3"><span class="grl n">NOHS</span> '+esc(oh)+'</div>';
  if(!reg&&!oh)gl+='<div class="gnone">Not listed in the program</div>';
  if(s.bis_judge)gl+='<div class="grow3"><span class="grl b">BIS</span> '+esc(s.bis_judge)+'</div>';
  // Group-timing header: Regular and Owner-Handled (NOHS) each run in their own
  // ring, and NOHS often starts BEFORE Regular (or "follows each Regular group").
  // List whichever runs first on top, so the order reads chronologically. Times
  // are program-printed and can change day-of ("unless otherwise announced").
  const sess=[];
  if(rc.gring!=null||rc.gstart)sess.push({lab:'Reg',ring:rc.gring,time:rc.gstart,min:(rc.gmin!=null?rc.gmin:1e9)});
  if(rc.nring!=null||rc.ntime||rc.nfollows)sess.push({lab:'NOHS',ring:rc.nring,time:(rc.ntime||(rc.nfollows?'follows each Regular group':'')),min:(rc.nmin!=null?rc.nmin:1e9)});
  sess.sort((x,y)=>x.min-y.min);
  const ghdr=sess.length?'<div class="ghdr">'+sess.map((x,ix)=>{
    const p=[];if(x.ring!=null)p.push('Ring '+x.ring);if(x.time)p.push(x.time);
    return '<div class="gsess'+(ix===0?' first':'')+'"><span class="gsl">'+esc(x.lab)+'</span>'+esc(p.join(' · '))+'</div>';
  }).join('')+'</div>':'';
  const gnote=sess.length?'<div class="gnote">Program order — may change day-of (“unless otherwise announced”)</div>':'';
  return '<div class="showgroups">'+ghdr+'<div class="gbody">'+(rows?'<div class="gorder">'+rows+'</div>':'')+'<div class="gjudges">'+gl+'</div></div>'+gnote+'</div>';
}
function openDetail(c){
  if(!c)return;
  CUR=c;
  const _sd=pd(c.start);calMonth=new Date(_sd.getFullYear(),_sd.getMonth(),1);renderCal();
  highlight(AIDX.get(c));
  renderDetail(c);
  tableWrap.style.display='none';
  detailEl.style.display='block';
  detailEl.classList.remove('fade');void detailEl.offsetWidth;detailEl.classList.add('fade');
  detailEl.setAttribute('aria-hidden','false');
  detailEl.scrollTop=0;
  detailEl.scrollIntoView({block:'nearest'});
}
function renderDetail(c){
  const loc=[c.venue,c.where].filter(Boolean).join(', ');
  const mapLink=loc?'<a target="_blank" rel="noopener" href="'+MAPS+encodeURIComponent(loc)+'">'+esc(loc)+' ↗</a>':'';
  let html='<div class="dhead"><button class="back" id="dback">‹ Back</button>'+
    '<h2>'+(isNatl(c)?'★ ':'')+esc(c.n>1?(c.venue||c.label):c.label)+'<br><span class="sub">'+esc(c.dates)+' · '+esc(c.tzLabel)+' time</span></h2></div>'+
    '<div class="body"><p class="csum">'+(mapLink?('📍 '+mapLink+' · '):'')+c.n+' show'+(c.n===1?'':'s')+' at this site</p>'+
    (c.rcard?'<a class="rcardbtn" target="_blank" rel="noopener" href="'+esc(c.rcard)+'">🎯 Finnish Spitz ring card — when &amp; where FS shows, per day →</a>':'')+
    c.shows.map((s,i)=>showCard(s,i)).join('')+
    (c.shows.some(s=>(s.breeds||[]).some(b=>b.clcy))?'<p class="clkey"><b>Reading the '+esc(selBreeds.length===1?selBreeds[0].name:'entry')+' count:</b> total – class dogs – class bitches (champion dogs – champion bitches) veterans. Hover the number for the plain-English breakdown.</p>':'')+
    '</div>';
  detailEl.innerHTML=html;
  document.getElementById('dback').addEventListener('click',closeDetail);
}
function closeDetail(){if(document.body.classList.contains('arrived')){document.body.classList.remove('arrived');map.invalidateSize();}detailEl.style.display='none';tableWrap.style.display='';tableWrap.classList.remove('fade');void tableWrap.offsetWidth;tableWrap.classList.add('fade');detailEl.setAttribute('aria-hidden','true');CUR=null;if(location.hash)history.replaceState(null,'',location.pathname+location.search);}
detailEl.addEventListener('click',e=>{
  const b=e.target.closest('[data-ics]');
  if(b&&CUR){dlIcs(CUR.shows[+b.dataset.ics],CUR);return;}
  const g=e.target.closest('[data-g]');
  if(!g||!CUR)return;
  const s=CUR.shows.find(x=>String(x.event_no)===g.dataset.gs);
  if(!s)return;
  groupPick[s.event_no]=+g.dataset.g;
  const panel=g.closest('.showgroups'),sel='[data-gs="'+g.dataset.gs+'"][data-g="'+g.dataset.g+'"]';
  panel.outerHTML=groupPanel(s);
  const again=detailEl.querySelector(sel);if(again)again.focus();
});
[fWin,fHv].forEach(el=>el.addEventListener('change',()=>renderAll(true)));
fWin.addEventListener('change',()=>requestBreedData(true));
q.addEventListener('input',()=>renderAll(false));
rebuildData();buildStatePanel();
renderAll(true);
window.addEventListener('resize',()=>{try{map.invalidateSize();}catch(_){}});
setTimeout(()=>{try{map.invalidateSize();}catch(_){}},250);
function openShow(no){
  const has=c=>c.shows.some(s=>String(s.event_no)===no);
  let c=ALL.find(has);
  // A shared link names a Finnish Spitz show; a saved picker may have dropped FS.
  if(!c&&FS_ALL.some(has)){
    setBreeds([{code:FS_CODE,name:FS_NAME}].concat(selBreeds).slice(0,MAX_BREEDS));
    c=ALL.find(has);
  }
  if(c)openDetail(c);
  return !!c;
}
function openFromHash(){
  const m=/#show=([\w-]+)/.exec(location.hash);
  if(m)openShow(m[1]);
}
// A home-page banner tile left a note saying which show was clicked. Put that show
// at the top, since the panel otherwise sits below the fold; Back restores the page.
function openFromBanner(){
  let note=null;
  try{const raw=localStorage.getItem('fsca_open_show');localStorage.removeItem('fsca_open_show');note=JSON.parse(raw||'null');}catch(_){}
  if(!note||!note.no||!(Date.now()-note.at<BANNER_NOTE_MAX_AGE_MS))return;
  if(openShow(String(note.no)))document.body.classList.add('arrived');
}
window.addEventListener('hashchange',openFromHash);
openFromHash();
openFromBanner();
(function(){
  const btn=document.getElementById('natlBtn');
  if(!btn||!NATL)return;
  const nc=FS_ALL.find(c=>c.shows.some(s=>String(s.event_no)===String(NATL)));
  if(!nc)return;
  natlReady=true;
  btn.textContent='★ '+nc.start.slice(0,4)+' National Specialty →';
  btn.addEventListener('click',()=>{location.hash='#show='+NATL;openFromHash();});
})();
syncBreedHeader();
requestBreedData(true);
</script>
</body>
</html>
"""
    return (tpl.replace("__TITLE__", _h(title))
               .replace("__SUBTITLE__", _h(subtitle))
               .replace("__LEGEND__", legend)
               .replace("__NATL__", _h(national_no))
               .replace("__BREED_API__", _h(BREED_API))
               .replace("__TZINFO__", json.dumps(tzinfo, separators=(",", ":")))
               .replace("__GROUPS__", json.dumps(GROUP_BY_CODE_PREFIX, separators=(",", ":")))
               .replace("__PROG_READ__", json.dumps(sorted(prog_read), separators=(",", ":")))
               .replace("__DATA__", data))


def _breed_canon(name):
    """Comparable form of a breed name, lossy on purpose. PDF text extraction turns
    'tt', 'tti' and 'ft' into 'ti' ('Setiers', 'Scotish', 'Soti-Coated'), so both
    sides of a comparison go through the same damage."""
    s = unicodedata.normalize("NFKD", name or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = s.replace("tti", "ti").replace("tt", "ti").replace("ft", "ti")
    toks = []
    for t in re.split(r"[^a-z0-9]+", s):
        if not t or t == "and":
            continue
        t = "mountain" if t == "mtn" else t
        toks.append(t[:-1] if len(t) > 3 and t.endswith("s") and not t.endswith("ss") else t)
    return " ".join(toks)


def _breed_index(refs):
    """Canonical name -> AKC code, from AKC's /references breeds plus BREED_ALIASES.
    A name two codes could claim maps to None: no ring beats the wrong ring."""
    index, by_display = {}, {}
    for b in refs:
        code = (b.get("breedCode") or "").strip()
        by_display[b.get("displayDescription")] = code
        for field in ("displayDescription", "fullFormalDescription", "singularDescription"):
            c = _breed_canon(b.get(field))
            if c and index.get(c, code) != code:
                index[c] = None
            elif c:
                index[c] = code
    for alias, display in BREED_ALIASES.items():
        if by_display.get(display):
            index[_breed_canon(alias)] = by_display[display]
    return index


def _breed_code(name, index):
    """AKC code for a breed as a judging program prints it, or None. Exact matches
    only: a near-miss could put an exhibitor at the wrong ring."""
    if "finnish spitz" in (name or "").lower():
        return FS_BREED_CODE
    return index.get(_breed_canon(name))


def _fetch_breed_index():
    """The breed-name index, or {} if AKC's list is unavailable (FS still works)."""
    try:
        r = _session().get(REFERENCES_URL, timeout=POST_TIMEOUT)
        r.raise_for_status()
        return _breed_index(r.json().get("breeds") or [])
    except Exception as e:
        print("ringcards: AKC breed list unavailable, Finnish Spitz only:", e)
        return {}


def _ring_info(day, entry, code):
    """One breed's ring panel for one day of a judging program."""
    group = GROUP_BY_CODE_PREFIX.get(code[:1], "")
    def key(g):
        return (g.get("group") or "").lower().replace(" group", "").strip()
    grps = day.get("groups") or {}
    reg = grps.get("regular") or {}
    startMin = reg.get("startMin")
    # program's Owner-Handled (NOHS) block: each group's judge (AKC fallback) plus
    # its ring + start time. NOHS groups routinely run BEFORE Regular (AKC
    # recommends >=30 min earlier), and neither the ring nor the time is in the
    # AKC feed — only the program has them.
    nohsblk = grps.get("nohs") or {}
    nohs_judges = {key(g): g.get("judge") or "" for g in (nohsblk.get("order") or [])}
    nohs = nohs_judges.get(group.lower(), "")
    # full regular group running order with est times (start + i*30, matching
    # the ring card's minPerGroup) so the exhibitor can see who runs before
    # their group and gauge progress through the day. 30 min/group is a rough
    # planning figure (AKC ~2.5 min/dog; a group ring holds one dog per
    # competing breed) — actual length swings with the day's entry, so these
    # are deliberately "~". Each group's judges are here too, because AKC's feed
    # only names the judges for the breed being searched.
    gseq = [{"g": g.get("group") or "",
             "est": _hmm(startMin + gi * 30) if startMin is not None else "",
             "j": g.get("judge") or "", "nj": nohs_judges.get(key(g), "")}
            for gi, g in enumerate(reg.get("order") or [])]
    return {"ring": entry.get("ring"), "time": entry.get("slotTime"),
            "ahead": entry.get("ahead") or 0, "count": entry.get("entryCount"),
            "split": entry.get("split") or "",
            "prev": entry.get("prevBreed"), "prevN": entry.get("prevN"),
            "nohs": nohs, "gstart": reg.get("start"), "gmin": startMin,
            "gring": reg.get("ring"),
            "ntime": nohsblk.get("start"), "nmin": nohsblk.get("startMin"),
            "nring": nohsblk.get("ring"), "nfollows": bool(nohsblk.get("follows")),
            "gseq": gseq}


def cmd_ringcards(args):
    """Generate Finnish Spitz ring cards for shows with a judging program from
    a superintendent we have an extractor for (RINGCARD_SUPERS). One card per
    unique program (keyBinary), shared across the shows that reference it;
    writes ringcards/<kb>.html and a manifest mapping each show's event_no -> its
    card. Uses the vendored ringcard/ pipeline (parse.py + render.js). Other
    supers and parse failures are skipped, so the card only appears where we
    can actually produce a good one."""
    import subprocess, tempfile
    conn = db()
    rows = conn.execute(
        "SELECT event_no, club, start_date, superint, documents FROM events WHERE documents IS NOT NULL").fetchall()
    conn.close()

    programs, extractor, clubs = {}, {}, {}   # keyBinary -> {event_no: start_date} / extractor name / clubs
    for r in rows:
        sup = (r["superint"] or "").lower()
        ext = next((v for k, v in RINGCARD_SUPERS.items() if k in sup), None)
        if not ext:
            continue
        for d in json.loads(r["documents"] or "[]"):
            if d.get("code") == "JDGPRO" and d.get("keyBinary"):
                programs.setdefault(d["keyBinary"], {})[str(r["event_no"])] = r["start_date"]
                extractor[d["keyBinary"]] = ext
                clubs.setdefault(d["keyBinary"], set()).add(r["club"])

    out_dir = Path(args.out) if getattr(args, "out", None) else RINGCARDS_OUT
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest, made, failed = {}, 0, 0
    breed_index = _fetch_breed_index()
    rings = {}     # breed code -> {event_no: ring info}, for every breed but FS
    read = set()   # event_nos whose program we fully read, so absent means not entered
    for kb, event_nos in sorted(programs.items()):
        with tempfile.TemporaryDirectory() as td:
            pdf, ij = Path(td) / f"{kb}.pdf", Path(td) / f"{kb}.json"
            # onofrio.com's TLS/SNI defeats urllib/requests here; curl handles it
            subprocess.run(["curl", "-sL", "--max-time", "120", "-o", str(pdf),
                            "-H", "Referer: https://webapps.akc.org/event-search/",
                            DOC_URL.format(kb)], capture_output=True)
            if not pdf.exists() or pdf.stat().st_size < 1000 or pdf.read_bytes()[:5] != b"%PDF-":
                print(f"ringcard: kb{kb} download failed — skipped"); failed += 1; continue
            p = subprocess.run([sys.executable, "parse.py", str(pdf), "-o", str(ij),
                                "--super", extractor[kb],
                                "--dates", ",".join(sorted(set(event_nos.values())))],
                               cwd=str(RINGCARD_DIR), capture_output=True, text=True)
            if not ij.exists():         # extractor crashed / superintendent not detected
                print(f"ringcard: kb{kb} not parseable — skipped"); failed += 1; continue
            try:
                inter = json.loads(ij.read_text(encoding="utf-8"))
            except Exception:
                print(f"ringcard: kb{kb} bad JSON — skipped"); failed += 1; continue
            # parse.py writes the JSON even when validation fails (rc != 0). For
            # this automated pipeline (no human to hand-fix flags), drop the
            # un-placeable entries so a program that DOES have Finnish Spitz still
            # yields a card even when non-FS breeds orphaned. FS's own numbers
            # (ahead etc.) were computed during extraction, so dropping other
            # rows afterward doesn't affect them.
            if p.returncode != 0:
                for d in inter.get("days", []):
                    d["entries"] = [e for e in d.get("entries", [])
                                    if isinstance(e.get("ring"), int)
                                    and e.get("slotMin") is not None
                                    and e.get("ahead") is not None
                                    and e.get("entryCount") is not None]
            # AKC's club name, not the cover's: most covers lead with 'Licensed by
            # the American Kennel Club' and many never print the club on a line alone.
            names = " / ".join(sorted(filter(None, clubs[kb])))
            if names:
                inter.setdefault("show", {})["club"] = names
            ij.write_text(json.dumps(inter), encoding="utf-8")
            # Per-show ring info for every breed we can name, keyed by AKC breed
            # code. Deliberately NOT the judge — the AKC feed already gives us
            # breed/group/BIS judges (more reliable); the judging program's unique
            # value is ring, time, running order and this-year's count.
            by_date = {}   # date -> [{code: ring info}, in show order]
            for d in inter.get("days", []):
                day = {}
                for e in d.get("entries", []):
                    code = _breed_code(e.get("breed"), breed_index)
                    if code and isinstance(e.get("ring"), int):
                        day[code] = _ring_info(d, e, code)
                by_date.setdefault(d.get("date"), []).append(day)
            # Two shows on one date (Show #1/#2, AM/PM) share a program, and event
            # numbers don't follow running order -- the cover's listing does. If
            # the cover doesn't settle which event is which, give that day no
            # panel rather than the other show's ring.
            order = inter.get("show", {}).get("eventOrder") or []
            pick = {}      # event_no -> {code: ring info}
            for edate in set(event_nos.values()):
                enos = [e for e, d in event_nos.items() if d == edate]
                shows = by_date.get(edate) or []
                if len(shows) == 1:
                    pick.update({e: shows[0] for e in enos})
                elif len(shows) == len(enos) and all(e in order for e in enos):
                    pick.update(zip(sorted(enos, key=order.index), shows))
            for e, day in pick.items():
                if p.returncode == 0 and len(day) >= MIN_BREEDS_TO_TRUST_PROGRAM:
                    read.add(e)
                for code, info in day.items():
                    if code != FS_BREED_CODE:   # FS is baked into the page itself
                        rings.setdefault(code, {})[e] = info
            # Only make a card when Finnish Spitz is actually entered and placeable
            # — no point generating an empty "no FS scheduled" card.
            has_fs = any(FS_BREED_CODE in day
                         for days in by_date.values() for day in days)
            if not has_fs:
                print(f"ringcard: kb{kb} no Finnish Spitz entered — skipped"); failed += 1; continue
            html = out_dir / f"{kb}.html"
            r2 = subprocess.run(["node", "render.js", "-i", str(ij),
                                 "-w", "fsca-weekend.json", "-t", "template-club.html",
                                 "-o", str(html)],
                                cwd=str(RINGCARD_DIR), capture_output=True, text=True)
            if r2.returncode != 0 or not html.exists():
                print(f"ringcard: kb{kb} render failed — {(r2.stderr or '').strip()[:70]}"); failed += 1; continue
            made += 1
            for e in event_nos:
                manifest[e] = {"card": f"ringcards/{kb}.html",
                               "fs": (pick.get(e) or {}).get(FS_BREED_CODE)}
    for e in read:
        manifest.setdefault(e, {"card": None, "fs": None})["read"] = True
    RINGCARD_MANIFEST.write_text(json.dumps(manifest), encoding="utf-8")
    # One small file per breed; the page fetches only the breeds someone picks.
    RINGS_OUT.mkdir(parents=True, exist_ok=True)
    for old in RINGS_OUT.glob("*.json"):
        old.unlink()
    for code, shows in rings.items():
        (RINGS_OUT / f"{code}.json").write_text(json.dumps(shows, separators=(",", ":")), encoding="utf-8")
    print(f"ringcards: {made} card(s), {failed} skipped, {len(manifest)} shows mapped "
          f"(of {len(programs)} programs); ring data for {len(rings)} other breeds")


def _short_span(sd, ed):
    """Compact date range for a banner tile, e.g. 'Nov 12–15'."""
    def one(d):
        return d.strftime("%b ") + str(d.day)
    if sd == ed:
        return one(sd)
    if (sd.year, sd.month) == (ed.year, ed.month):
        return f"{one(sd)}–{ed.day}"
    return f"{one(sd)} – {one(ed)}"


def banner_shows(clusters, natl_no="", prog_read=(), today=None):
    """The upcoming sites the home-page banner can pick from, soonest first. `days`
    holds shows whose judging program gave a Finnish Spitz count (0 = read, none
    entered), `wait` the shows still unknown; the page applies the viewer's today."""
    today = today or date.today()
    def known(s):
        return bool(s.get("rc")) or str(s["event_no"]) in prog_read
    out = []
    for c in clusters:
        sd, ed = _parse_date(c["start"]), _parse_date(c["end"])
        if ed < today - timedelta(days=1):
            continue
        out.append({
            "no": c["event_no"], "start": c["start"], "end": c["end"],
            "dates": _short_span(sd, ed), "where": c["where"], "last": c["fs_last"],
            "natl": bool(natl_no) and any(str(s["event_no"]) == str(natl_no) for s in c["shows"]),
            "days": [{"end": s["end"], "n": (s.get("rc") or {}).get("count") or 0}
                     for s in c["shows"] if known(s)],
            "wait": [s["end"] for s in c["shows"] if not known(s)],
        })
    return out[:BANNER_CANDIDATES]


def render_banner_html(clusters, natl_no="", prog_read=()):
    """Small self-contained page for the home page's iframe. One fixed height at
    every width (the tiles scroll sideways), because an iframe can't resize itself."""
    data = json.dumps(banner_shows(clusters, natl_no, prog_read), separators=(",", ":")).replace("</", "<\\/")
    tpl = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Upcoming Finnish Spitz shows</title>
<base target="_top">
<style>
  html,body{margin:0;height:100%}
  body{box-sizing:border-box;background:linear-gradient(135deg,#33473a,#243428);color:#eaf1ea;font:15px/1.3 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;padding:10px 14px;display:flex;flex-direction:column;gap:8px;overflow:hidden}
  .top{display:flex;align-items:center;justify-content:space-between;gap:10px}
  h1{margin:0;font-size:1rem;font-weight:800;color:#fff;white-space:nowrap;min-width:0;overflow:hidden;text-overflow:ellipsis}
  .cta{flex:none;background:#fff;color:#2c3f30;font-weight:700;font-size:.86rem;padding:5px 12px;border-radius:7px;text-decoration:none;white-space:nowrap}
  .cta:hover{filter:brightness(.94)}
  .row{display:flex;gap:8px;overflow-x:auto;scrollbar-width:thin;scrollbar-color:#ffffff55 transparent;flex:1;min-height:0;padding-bottom:3px}
  .tile{flex:1 0 190px;max-width:290px;box-sizing:border-box;background:#ffffff1c;border-radius:9px;padding:6px 11px;color:inherit;text-decoration:none;display:flex;flex-direction:column;justify-content:center}
  .tile:hover{background:#ffffff30}
  .where{font-weight:700;color:#fff;font-size:.9rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .when{font-size:.78rem;color:#d6e3d6;white-space:nowrap}
  .now{background:#f6c453;color:#3a2a05;font-weight:800;font-size:.66rem;border-radius:4px;padding:0 5px;margin-left:5px}
  .count{font-size:.8rem;color:#fff;white-space:nowrap}
  .count b{font-size:1rem;color:#f6d58a}
  .count.wait{color:#d6e3d6}
  .none{align-self:center;font-size:.86rem;color:#d6e3d6}
  @media(max-width:420px){.top{gap:6px}h1{font-size:.78rem}.cta{font-size:.72rem;padding:4px 7px}}
</style>
</head>
<body>
  <div class="top"><h1>See Finnish Spitz in person</h1><a class="cta" href="https://finnishspitz.org/events">Find shows near you →</a></div>
  <div class="row" id="row"></div>
<script>
const SHOWS = __DATA__;
const MAX_TILES = 6;
const EVENTS_URL = 'https://finnishspitz.org/events';
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
const n=new Date(),today=n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0')+'-'+String(n.getDate()).padStart(2,'0');
const live=SHOWS.filter(c=>c.end>=today).map(c=>Object.assign({},c,{
  entered:c.days.filter(d=>d.end>=today&&d.n>0),
  unknown:c.wait.some(e=>e>=today)}));
// Finnish Spitz actually entered comes first. Where nobody knows yet, a site earns
// a tile only by having drawn them last year; a program showing none rules it out.
const entered=live.filter(c=>c.entered.length),lastYear=live.filter(c=>!c.entered.length&&c.unknown&&c.last);
const picks=live.filter(c=>c.natl).concat(entered,lastYear).filter((c,i,a)=>a.indexOf(c)===i).slice(0,MAX_TILES);
function countLine(c){
  if(c.entered.length){
    const ns=c.entered.map(d=>d.n),lo=Math.min(...ns),hi=Math.max(...ns),num=lo===hi?hi:lo+'–'+hi;
    return '<div class="count"><b>'+num+'</b> Finnish Spitz entered'+(c.entered.length>1?' a day':'')+'</div>';
  }
  return c.last?'<div class="count wait"><b>'+c.last+'</b> Finnish Spitz last year</div>':'';
}
document.getElementById('row').innerHTML=picks.map(c=>'<a class="tile" href="'+EVENTS_URL+'" data-no="'+esc(c.no)+'">'+
  '<div class="where">'+(c.natl?'★ ':'')+esc(c.where)+'</div>'+
  '<div class="when">'+(c.natl?'National Specialty · ':'')+esc(c.dates)+(c.start<=today?'<span class="now">NOW</span>':'')+'</div>'+
  countLine(c)+'</a>').join('')||'<div class="none">No Finnish Spitz confirmed at upcoming shows yet.</div>';
// /events embeds the dashboard at a fixed URL, so a link can't say which show to
// open. This page and the dashboard share a browser origin, so leave it a note.
document.getElementById('row').addEventListener('click',e=>{
  const tile=e.target.closest('.tile');
  if(!tile)return;
  try{localStorage.setItem('fsca_open_show',JSON.stringify({no:tile.dataset.no,at:Date.now()}));}catch(_){}
});
</script>
</body>
</html>
"""
    return tpl.replace("__DATA__", data)


def cmd_map(args):
    # The whole store is baked in; the page windows it client-side against the
    # viewer's own "today", so the map/list stay correct between regenerations
    # and never get stuck showing a stale calendar month. --month/--months are
    # accepted for back-compat but no longer change the output.
    conn = db()
    rows = conn.execute("SELECT * FROM events ORDER BY start_date").fetchall()
    conn.close()
    clusters = cluster_events(rows)
    # Attach ring-card links (generated separately by cmd_ringcards). A cluster
    # gets a card if any of its shows' event_no is in the manifest.
    prog_read = set()
    if RINGCARD_MANIFEST.exists():
        try:
            rcmap = json.loads(RINGCARD_MANIFEST.read_text(encoding="utf-8"))
        except Exception:
            rcmap = {}
        prog_read = {e for e, v in rcmap.items() if isinstance(v, dict) and v.get("read")}
        for c in clusters:
            for s in c["shows"]:
                e = rcmap.get(str(s["event_no"]))
                if not e:
                    continue
                card = e.get("card") if isinstance(e, dict) else e   # dict now; str = legacy
                if card:
                    s["rcard"] = card
                if isinstance(e, dict) and e.get("fs"):
                    s["rc"] = e["fs"]                                 # per-day FS ring info
            # One program for the whole cluster gets one button up top; supers
            # that print a program per day (MB-F, Jim Rau) get one on each show.
            cards = {s["rcard"] for s in c["shows"] if s.get("rcard")}
            if len(cards) == 1:
                c["rcard"] = cards.pop()
    shows = sum(c["n"] for c in clusters)
    updated = datetime.now().strftime("%b %d, %Y").replace(" 0", " ")
    title = "Finnish Spitz — AKC Events"
    # The National ★ button only rides along within its promo window (see
    # national_in_window); otherwise national_no is blank and nothing is marked.
    natl_no = NATIONAL_EVENT_NO if national_in_window(national_cluster(clusters)) else ""
    tail = "tap the ★ National Specialty button · " if natl_no else ""
    subtitle = (f"{shows} shows at {len(clusters)} sites · filter & click any show below · "
                f"{tail}updated {updated}")
    out = Path(args.out) if getattr(args, "out", None) else MAP_PATH
    out.write_text(render_map_html(clusters, title, subtitle, natl_no, prog_read), encoding="utf-8")
    print(f"wrote {out}  ({len(clusters)} clusters / {shows} shows)")
    BANNER_PATH.write_text(render_banner_html(clusters, natl_no, prog_read), encoding="utf-8")


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
        ".docrow{margin-top:8px;font-size:.92rem;display:flex;flex-wrap:wrap;gap:4px 14px}"
        ".docrow a{color:#0b5cad;text-decoration:none}"
        ".docrow a:hover{text-decoration:underline}"
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

    _dicon = {"PRMLST": "\U0001F4C4", "JDGPRO": "\U0001F4CB",
              "ENTFRM": "\U0001F4DD", "EVNINF": "ℹ"}
    docs_html = ""
    if cluster.get("docs"):
        _links = " · ".join(
            '<a href="{}" target="_blank" rel="noopener">{} {}</a>'.format(
                _h(d["url"]), _dicon.get(d["code"], "\U0001F4C4"), _h(d["label"]))
            for d in cluster["docs"])
        docs_html = '<div class="docrow">' + _links + '</div>'

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
      <div class="sec"><h3>Entries &amp; premium</h3><p>{entline}</p>{docs_html}</div>
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


def _natl_hidden_html():
    """Empty, self-collapsing document for when the National is out of its promo
    window — the embedded iframe then renders nothing on the page."""
    return ('<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
            '<style>html,body{margin:0;padding:0;height:0;overflow:hidden}</style>\n'
            '</head>\n<body></body>\n</html>\n')


def cmd_national(args):
    conn = db()
    rows = conn.execute("SELECT * FROM events ORDER BY start_date").fetchall()
    conn.close()
    anchor = getattr(args, "event", None) or NATIONAL_EVENT_NO
    manual = bool(getattr(args, "event", None))    # explicit --event always renders
    target = next((c for c in cluster_events(rows)
                   if any(s["event_no"] == anchor for s in c["shows"])), None)
    out = Path(args.out) if getattr(args, "out", None) else NATIONAL_PATH
    show = manual or national_in_window(target)
    out.write_text(render_national_html(target) if show else _natl_hidden_html(),
                   encoding="utf-8")
    if not show:
        why = "past or >%d days out" % NATIONAL_LEAD_DAYS if target else "anchor not in store"
        print(f"wrote {out}  (National hidden — {why})")
    elif target:
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

    rc = sub.add_parser("ringcards", help="generate FS ring cards for Onofrio judging programs")
    rc.add_argument("--out", help=f"output dir (default {RINGCARDS_OUT.name}/)")

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
     "national": cmd_national, "ringcards": cmd_ringcards}[args.cmd](args)


if __name__ == "__main__":
    main()
