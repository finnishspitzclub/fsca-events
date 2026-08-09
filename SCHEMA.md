# akcal — SCHEMA RESOLVED

Supersedes the "Next action" and "The question that decides the architecture"
sections of README.md. The probe is done. Both open questions answered.

---

## 1. Working payload

Captured from devtools. Verified: HTTP 200, ~30 events, 382 KB.

```python
def build_payload(start: date, end: date, state: str = "CO"):
    return {
        "address": {
            "states": state,
            "eventSetting": {"indoor": True, "outdoor": True, "outsideCovered": True},
            "searchByState": True,
            "searchByCity": False,
            "searchText": "All States",
        },
        "breedCode": "313 ",          # trailing space is REAL, do not strip
        "breedName": "Finnish Spitz",
        "breedId": "SPECIFIC",
        "dateRange": {
            "from": start.strftime("%m/%d/%Y"),   # MM/DD/YYYY, not ISO
            "to":   end.strftime("%m/%d/%Y"),
            "type": "event",                      # <-- THE KEY. not "startDate".
        },
        "competition": {
            "items": [{"selected": True,
                       "value": {"compType": "AB/LB"},
                       "label": "All- Breed and Group (AB/LB)"}],
            "filters": [{"compType": "AB/LB", "limitCode": "NOHS"}],
        },
    }
```

**What was wrong in the guesses:** `dateRange.type` must be `"event"`.
`competition.filters` is a list of *objects*, not strings. compType is
`"AB/LB"`, not `"CH"`. The `address` block has no `location`/`radius` keys.

**Validator lies.** A missing/!invalid `breedCode` returns
`"Missing \"dateRange\" parameter"`. Do not trust the error text when debugging.

### Request notes

- Response envelope is `{"events": [...]}`. `extract_records` already finds it.
- **Server wait was 26 seconds** for a one-year CO query. Raise the `_post`
  timeout from 30 to 90 or the first real fetch will fail.
- A full year of one state returned ~30 events, nowhere near the 1000 cap.
  **Drop `WINDOW_DAYS` windowing** — fetch a year per call instead of 26 calls.
- No auth. The captured request carried cookies and `x-csrf-token: token`
  (literally the string "token"), but the ablation scripts got 200s without
  either, so neither is required.
- To widen beyond one state, try dropping `states` and setting
  `searchByState: False`. **Untested.**

---

## 2. Both architecture questions: YES

**Judge panels are present.** Top-level `judges` object per event with
`breedJudge`, `groupJudge`, `bestInShowJudge`, `nohsGroupJudge`,
`nohsBestInShowJudge` — each with `name` and AKC judge number.

**Closing dates are present.** Epoch milliseconds in `items[]`, per competition
type.

**→ Delete superintendent ingestion (v3) from the roadmap.** Single source.

---

## 3. Closing-date rule, verified

Nine events checked. Every closing date is a **Wednesday, 15–16 days before day
one**. The rule in README.md holds.

**Correction to the README:** timestamps resolve to noon *Eastern* (16:00Z
summer / 17:00Z winter) or midnight Eastern (04:00Z) — not the superintendent's
office timezone. Treat the API value as a **date**; take time-of-day from the
premium list. The `timeZone` field ("Central"/"Mountain"/null) is separate
show-local metadata, not the deadline's zone.

---

## 4. `completedLastYear` — the breed-count field

This is per-event, filtered to the searched breed, and it comes free with the
search. It does **not** require hitting the per-event AKC detail page.

Format: `TOTAL-classDogs-classBitches (specialDogs-specialBitches) veterans`

```python
import re

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
```

Verified against every row in the capture; the sum always reconciles.

**The field can be entirely absent** (Greeley KC 2026 has no
`completedLastYear` key). Handle `None`.

**Year offset:** the value on a 2026 event describes 2025; on a 2027 event it
describes 2026. Store the show year alongside it or the data is misleading.

---

## 5. `map_record()` — real field names

```python
CONF = "CONF"   # competitionGroupCode for conformation

def _ms(v):
    """epoch-ms -> date, or None."""
    if not v:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(v / 1000, timezone.utc).date()

def map_record(rec: dict) -> dict:
    site = rec.get("site") or {}
    supt = rec.get("superintendentSecretary") or {}
    jud  = rec.get("judges") or {}

    # conformation item carries the entry window + fees we care about
    conf = next((i for i in (rec.get("items") or [])
                 if i.get("competitionGroupCode") == CONF), {})

    def judge(key):
        j = jud.get(key) or {}
        n = (j.get("name") or "").strip()
        return None if n in ("", "UNASSIGNED") else n

    venue = " ".join(x for x in (
        (site.get("name") or "").strip(),
        (site.get("location1") or "").strip(),
        (site.get("location2") or "").strip(),
    ) if x)
    # site.name and location1 are often identical — dedupe
    parts, seen = [], set()
    for p in venue.split("  "):
        p = p.strip()
        if p and p not in seen:
            seen.add(p); parts.append(p)
    venue = ", ".join(parts)

    return {
        "event_no":   str(rec.get("eventNumber") or "").strip(),
        "show_id":    rec.get("id"),
        "club":       (rec.get("clubName") or "").strip(),
        "start_date": rec.get("startDate"),      # already ISO yyyy-mm-dd
        "end_date":   rec.get("endDate"),
        "city":       (rec.get("city") or "").strip(),
        "state":      (rec.get("state") or "").strip(),
        "venue":      venue,
        "postal":     (site.get("postalCode") or "").strip(),
        "lat":        (site.get("coordinates") or {}).get("lat"),
        "lon":        (site.get("coordinates") or {}).get("lon"),
        "comp_type":  (rec.get("eventType") or "").strip(),
        "status":     rec.get("eventStatus"),     # Approved | Pended
        "superint":   (supt.get("name") or "").strip(),
        "supt_phone": supt.get("phone"),
        "supt_email": supt.get("email"),
        "open_date":  _ms(conf.get("openingDate")),
        "close_date": _ms(conf.get("closingDate")),
        "entry_fee":  (conf.get("entryFee") or [None])[0],
        "inside_out": (conf.get("insideOut") or "").strip(),
        "time_zone":  conf.get("timeZone"),
        "method":     conf.get("competitionMethod"),      # All Breed | Limited Breed
        "method_code": conf.get("competitionMethodCode"), # AB | LB
        "specialty":  conf.get("bvgSpecialty"),
        "breed_judge": judge("breedJudge"),
        "group_judge": judge("groupJudge"),
        "bis_judge":   judge("bestInShowJudge"),
        "nohs_group_judge": judge("nohsGroupJudge"),
        "completed_last_year": rec.get("completedLastYear"),
        "online_entries": rec.get("isAcceptingOnlineEntries"),
    }
```

The `events` table in akcal.py needs the new columns. Keep `event_no` as PK.

---

## 6. Gotchas that will bite

**Strip everything.** Trailing spaces are endemic: `"OK  "`, `"MO  "`,
`"IN  "`, `"Arapahoe County Fairgrounds "`, and `breedCode` itself.

**One record per DAY, not per cluster.** Greeley KC Aug 15/16/17 is three
separate events with three event numbers. Design decision for the ICS: either
one VEVENT per event number (correct, but a 3-day cluster becomes 3 calendar
entries) or merge consecutive same-club records into one span. Each day has its
own judge panel, which argues for keeping them separate.

**`days` is not always 1 and not always right.** Colorado KC event 2027041104
claims `days: 8`, `startDate 2027-02-13`, `endDate 2027-02-20` — and overlaps
with 2027041101 (Feb 12) and 2027041107 (Feb 14) at the same venue. Sanity-check
spans; do not trust `days`.

**Bad timestamps exist.** In that same record, one item has
`openingDate: 1830186000000` — year 2028, *after* its own closing date. Validate
`open_date < close_date < start_date` and null out anything that fails.

**`judges` names are `"UNASSIGNED"` / number `"0000"` / status `"INIT"`** for
events far out. The helper above maps those to `None`.

**Strip `items[].breeds[]` before storing the raw blob.** It's a 200+ entry
array repeated on every item of every event — it is essentially the entire
382 KB payload. Without stripping, the SQLite `raw` column will be enormous for
no benefit. Keep the rest of the raw record.

**`documents[]`** carries `{name, code, keyBinary}` with codes `PRMLST`
(premium list) and `JDGPRO` (judging program). The `keyBinary` is a numeric
handle — if the fetch URL pattern can be found, the premium parser feeds itself
with no superintendent scraping. Store these.

**`competitionMethodCode`** distinguishes `AB` (all-breed) from `LB` (limited
breed / group show). `LB` + `bvgSpecialty: "Non-Sporting Group"` is how the
Rocky Mountain Non-Sporting Club shows are identified — flag these, they're
higher-value than all-breed for this dog.

**`eventStatus`**: `Approved` (AOVD) vs `Pended` (PEND). Terry-All and Flatirons
2027 are both **Pended** — applied for, not confirmed, dates can still move.
Surface this in the ICS description; don't present pended dates as settled.

---

## 7. Reference data from the capture

Use to validate the mapping end-to-end.

| Event no. | Club | Start | Closes | Status | Supt |
|---|---|---|---|---|---|
| 2026746503 | Rocky Mountain Non-Sporting | 2026-09-04 | 2026-08-19 | Approved | Onofrio |
| 2026096013 | Evergreen Colorado KC | 2026-09-05 | 2026-08-19 | Approved | Onofrio |
| 2026035401 | Colorado Springs KC | 2026-10-23 | 2026-10-07 | Approved | Foy Trent |
| 2026035801 | Southern Colorado KC | 2026-11-06 | 2026-10-21 | Approved | Foy Trent |
| 2027041101 | Colorado KC | 2027-02-12 | 2027-01-27 | Approved | Onofrio |
| 2027062604 | Terry-All KC | 2027-04-10 | 2027-03-24 | **Pended** | Onofrio |
| 2027062301 | Flatirons KC | 2027-06-04 | 2027-05-19 | **Pended** | Onofrio |

Finnish Spitz `completedLastYear` for the same set:

| Event | Describes | Value | Total |
|---|---|---|---|
| RM Non-Sporting 2026 | 2025 | `5-1-2 (1-1) 0` | 5 |
| Evergreen 2026 | 2025 | `5-1-2 (1-1) 0` | 5 |
| So. Colorado 2026 d1 | 2025 | `4-0-2 (2-0) 0` | 4 |
| Terry-All 2027 | 2026 | `3-0-0 (3-0) 0` | 3 |
| Colorado KC 2027 | 2026 | `2-0-0 (2-0) 0` | 2 |
| Flatirons 2027 | 2026 | `2-0-0 (2-0) 0` | 2 |
| Arapahoe 2026 | 2025 | `2-0-0 (2-0) 0` | 2 |
| Colorado Springs 2026 | 2025 | `2-0-0 (2-0) 0` | 2 |
| Greeley KC 2026 | — | *(field absent)* | — |

---

## 8. Revised roadmap

- **v1** — probe ✅, fetch, store, ics. ← finish this
- **v2** — cron + push `.ics` to static host. Unchanged.
- **v3** — ~~superintendent ingestion~~ **cut**. Replaced by: resolve the
  `documents[].keyBinary` fetch URL so premium lists and judging programs pull
  from the same source.
- **v4** — diff on fetch → Discord. Change detection is already implemented.
- **v5** — repoint premium parser / ring card / show-day tools at the store.

**Backfill priority:** pull wide now. A one-year, one-state query is a single
call and returns ~30 events. Do CO and WY for 2026 and 2027 immediately.
