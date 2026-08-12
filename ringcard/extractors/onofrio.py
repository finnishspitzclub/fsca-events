"""Onofrio judging-program extractor.

Onofrio prints ring detail in TWO columns per page. Left/right breed lines share
a text row, so we split by x-coordinate (page mid), then read newspaper-order:
whole left column top-to-bottom, then whole right column, then the next page.
Ring blocks flow continuously through that stream; the most recent 'RING n' +
judge header governs each breed line, and each printed time ('9:25 am') resets
the running 'ahead' count for its block.

Everything here is a fact off the page. Estimated times and clash flags are the
renderer's job, not ours.
"""
from __future__ import annotations
import re
import pdfplumber
from .base import Extractor, register, clock_to_min, min_to_compact, MONTHS

# ---- line classifiers ----------------------------------------------------
RING_RE   = re.compile(r"^RING\s+(\d+)\s*$")
TIME_RE   = re.compile(r"^(\d{1,2}:\d{2}\s*[ap]\.?m?\.?)\s*$", re.I)
BREED_RE  = re.compile(r"^(\d+)\s+(.+?)\s+(\d+-\d+-\d+-\d+)(\*?)\s*$")
JUDGE_RE  = re.compile(r"^(MR|MRS|MS|MISS|DR)?\s*[A-Z][A-Z .'\-]*[A-Z](?:\s*\((\d+)\))?\s*$")
GROUP_RE  = re.compile(r"^(.+?)\s+GROUP\s*-\s*(.+?)\*?\s*$", re.I)
DATE_RE   = re.compile(
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", re.I | re.M)
HONORIFIC = re.compile(r"^(MR|MRS|MS|MISS|DR)\b\.?\s*", re.I)

# control lines: carry no breed, don't count, don't set judge
CONTROL_RE = re.compile(
    r"(MOVES TO RING|RETURNS TO RING|GOES TO RING|GOES TO LUNCH|"
    r"CONTINUING FROM RING|minute Lunch Break|^LUNCH$|Unless otherwise announced|"
    r"^AFTER NOON$|^CONFORMATION$|WILL JUDGE|IN PLACE OF)", re.I)

GROUP_HDR   = re.compile(r"(REGULAR VARIETY GROUPS|NOHS GROUPS|BRED-BY-EXHIBITOR GROUPS)", re.I)
# the seven AKC variety groups — anything else matching "X GROUP - judge"
# (Sweepstakes, Owner-Handled, Bred-by, etc.) is not a variety group.
VARIETY_GROUPS = {"sporting", "hound", "working", "terrier", "toy", "non-sporting", "herding"}
JUDGE_COUNT = re.compile(r"\s*\(\d+\)\s*$")


def titlecase_judge(name: str) -> str:
    name = HONORIFIC.sub("", name.strip())
    name = JUDGE_COUNT.sub("", name).strip()
    return " ".join(w if (len(w) == 1) else w.capitalize() for w in name.split())


# ---- page -> column-ordered lines ---------------------------------------
def _is_content(s):
    """A columnar body line: a RING header, a bare time, or an 'N Breed d-d-d-d'
    line. Used to find where the header/intro ends — NOT a fixed y-cutoff, because
    a continuation page's content starts high (right under a 2-line header) while a
    day's first page carries a tall intro paragraph before its first ring."""
    s = s.strip()
    return bool(RING_RE.match(s) or TIME_RE.match(s) or BREED_RE.match(s))


def page_lines(page):
    """Return lines in newspaper reading order: left column then right column.

    Each line is (col, text). The page header (club/day) and any first-page
    intro paragraph are skipped by locating the first real content line rather
    than assuming a fixed header height — so a breed that continues at the top of
    a column (e.g. a 1:45p strand carried over from the previous page) is kept.
    """
    from itertools import groupby
    mid = page.width / 2
    allw = page.extract_words()
    allw.sort(key=lambda w: (round(w["top"] / 2) * 2, w["x0"]))
    # first visual line (either column) that looks like content -> body starts there
    body_top = 0
    for key, grp in groupby(allw, key=lambda w: round(w["top"] / 2) * 2):
        grp = list(grp)
        left = " ".join(w["text"] for w in grp if w["x0"] < mid)
        right = " ".join(w["text"] for w in grp if w["x0"] >= mid)
        if _is_content(left) or _is_content(right):
            body_top = key - 2
            break
    words = [w for w in allw if w["top"] >= body_top]
    out = []
    for col in (0, 1):
        cw = [w for w in words if (w["x0"] < mid) == (col == 0)]
        cw.sort(key=lambda w: (round(w["top"] / 2) * 2, w["x0"]))
        # group into visual lines
        line, cur = [], None
        for w in cw:
            key = round(w["top"] / 2) * 2
            if cur is None or key == cur:
                line.append(w); cur = key
            else:
                out.append((col, " ".join(x["text"] for x in line)))
                line, cur = [w], key
        if line:
            out.append((col, " ".join(x["text"] for x in line)))
    return [(c, t) for c, t in out if t.strip()]


# ---- show header (best-effort; render can override for display) ---------
def extract_show(pdf) -> dict:
    cover = "\n".join((pdf.pages[i].extract_text() or "") for i in range(min(3, len(pdf.pages))))
    club = ""
    m = re.search(r"^([A-Z][A-Z .,&'\-]+KENNEL CLUB[A-Z .,&'\-]*)", cover, re.M)
    if m:
        club = re.sub(r",?\s*INC\.?\s*$", "", m.group(1).strip(), flags=re.I).title()
    # date range from the cover day headers
    days = re.findall(r"(?:MON|TUE|WED|THU|FRI|SAT|SUN)[A-Z]*,\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})",
                      cover, re.I)
    # Build the range from the earliest and latest date found, not the order the
    # headers happen to appear in (Onofrio covers aren't always chronological).
    parsed = []
    for mname, dnum, yr in days:
        mo = MONTHS.get(mname.lower())
        if mo:
            parsed.append((int(yr), mo, int(dnum), mname.title()[:3]))
    dates = ""
    if parsed:
        parsed.sort()
        y0, m0, d0, mon0 = parsed[0]
        y1, m1, d1, mon1 = parsed[-1]
        if (y0, m0, d0) == (y1, m1, d1):
            dates = f"{mon0} {d0}, {y0}"
        elif (y0, m0) == (y1, m1):
            dates = f"{mon0} {d0}–{d1}, {y0}"
        else:
            dates = f"{mon0} {d0} – {mon1} {d1}, {y1}"
    venue = ""
    mv = re.search(r"^([A-Z][A-Za-z .]+FAIRGROUNDS[A-Za-z .&]*)", cover, re.M)
    if mv:
        venue = mv.group(1).strip().title()
    return {"club": club or "Dog Show", "dates": dates, "venue": venue, "super": "Onofrio"}


_MON3 = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _range_label(iso0, iso1):
    """'2026-08-13','2026-08-17' -> 'Aug 13–17, 2026' (handles cross-month/year)."""
    from datetime import date
    a, b = date.fromisoformat(iso0), date.fromisoformat(iso1)
    if a > b:
        a, b = b, a
    if a == b:
        return f"{_MON3[a.month-1]} {a.day}, {a.year}"
    if (a.year, a.month) == (b.year, b.month):
        return f"{_MON3[a.month-1]} {a.day}–{b.day}, {a.year}"
    if a.year == b.year:
        return f"{_MON3[a.month-1]} {a.day} – {_MON3[b.month-1]} {b.day}, {a.year}"
    return f"{_MON3[a.month-1]} {a.day}, {a.year} – {_MON3[b.month-1]} {b.day}, {b.year}"


# ---- alphabetical index (independent source for ring + slot time) --------
# The "JUDGING PROGRAM BY ALPHABETICAL ORDER" pages list, per breed, the ring
# and printed slot time for each day. Unreliable for JUDGES (substitutions hide
# there) but solid for ring + slot — so it's the cross-check that rescues a
# continuation whose block header got orphaned in another column.
IDX_BUCKETS = [("breed", 0, 130), ("d0r", 130, 160), ("d0t", 160, 205),
               ("d1r", 205, 235), ("d1t", 235, 278), ("d2r", 278, 308), ("d2t", 308, 400)]


def _idx_bucket(x):
    for n, a, b in IDX_BUCKETS:
        if a <= x < b:
            return n
    return None


def index_key(breed: str) -> str:
    return re.sub(r"[^A-Z ]", "", breed.upper()).strip()


def breed_index_match(entry_breed: str, idx_words: list[str]) -> bool:
    """Index abbreviations are word-prefixes of the full name: NOR BUHUN -> NORWEGIAN BUHUNDS."""
    ew = index_key(entry_breed).split()
    if len(idx_words) > len(ew):
        return False
    return all(ew[i].startswith(idx_words[i]) for i in range(len(idx_words)))


def extract_index(pdf) -> list[dict]:
    from itertools import groupby
    rows = []
    for page in pdf.pages:
        txt = page.extract_text() or ""
        if "ALPHABETICAL ORDER" not in txt.upper():
            continue
        words = [w for w in page.extract_words() if w["top"] > 120]
        words.sort(key=lambda w: (round(w["top"] / 2) * 2, w["x0"]))
        for _, grp in groupby(words, key=lambda w: round(w["top"] / 2) * 2):
            cells: dict = {}
            for w in grp:
                cells.setdefault(_idx_bucket(w["x0"]), []).append(w["text"])
            breed = " ".join(cells.get("breed", [])).strip()
            if not breed or breed.upper() in ("BREED", "JUDGING PROGRAM", "TIME"):
                continue
            days = {}
            for di, (rk, tk) in enumerate((("d0r", "d0t"), ("d1r", "d1t"), ("d2r", "d2t"))):
                rr = cells.get(rk, [])
                tt = " ".join(cells.get(tk, []))
                tm = re.search(r"\d{1,2}:\d{2}\s*[ap]", tt)
                ring = int(rr[0]) if rr and rr[0].isdigit() else None
                smin = clock_to_min(tm.group(0)) if tm else None
                if ring is not None or smin is not None:
                    days[di] = {"ring": ring, "slotMin": smin}
            if days:
                rows.append({"words": index_key(breed).split(), "days": days})
    return rows


def index_lookup(index_rows, breed, day_i):
    for row in index_rows:
        if day_i in row["days"] and breed_index_match(breed, row["words"]):
            return row["days"][day_i]
    return None


# ---- main parse ----------------------------------------------------------
def parse(pdf_path: str) -> dict:
    with pdfplumber.open(pdf_path) as pdf:   # close the handle (Windows locks the file otherwise)
        show = extract_show(pdf)
        index_rows = extract_index(pdf)

        # 1) assign every page to a day via the repeated day-header line
        day_of_page = {}       # page_idx -> date iso
        days_meta = {}         # iso -> {'label','date'}
        order = []             # iso in first-seen order
        cur = None
        for pi, page in enumerate(pdf.pages):
            txt = page.extract_text() or ""
            m = DATE_RE.search(txt)
            if m:
                mon = MONTHS.get(m.group(2).lower())
                if mon:
                    iso = f"{m.group(4)}-{mon:02d}-{int(m.group(3)):02d}"
                    if iso not in days_meta:
                        days_meta[iso] = {"label": m.group(1).title(), "date": iso}
                        order.append(iso)
                    cur = iso
            day_of_page[pi] = cur

        # Onofrio sometimes prints day sections out of date order. Sort so the
        # card reads chronologically AND the day index (di) lines up with the
        # alphabetical index's chronological day columns (d0 = earliest day),
        # which is what reconcile_with_index relies on.
        order.sort()

        # 2) walk each day's pages in reading order, parse ring + group blocks
        days = []
        for iso in order:
            pidx = [pi for pi in range(len(pdf.pages)) if day_of_page[pi] == iso]
            stream = []
            for pi in pidx:
                for col, text in page_lines(pdf.pages[pi]):
                    stream.append((pi, col, text))
            entries, groups = parse_day_stream(stream)
            if not entries and not groups["regular"]["order"]:
                continue                   # obedience-only / non-conformation day slice
            di = len(days)
            reconcile_with_index(entries, index_rows, di)
            days.append({
                "date": days_meta[iso]["date"], "label": days_meta[iso]["label"],
                "entries": entries, "groups": groups,
            })

    # Prefer the real judging-day range over whatever dates the cover text held.
    if days:
        show = dict(show, dates=_range_label(days[0]["date"], days[-1]["date"]))
    return {"show": show, "days": days}


def reconcile_with_index(entries, index_rows, day_i):
    """Cross-check every entry's ring against the alphabetical index. For a
    flagged continuation (block header orphaned elsewhere), trust the index for
    ring + slot and clear the block-derived fields (judge / ahead) that came from
    the wrong strand — leaving a tight, honest "fill these by hand" flag."""
    for e in entries:
        idx = index_lookup(index_rows, e["breed"], day_i)
        if not idx:
            continue
        flagged = bool(e.get("flags"))
        ring_ok = idx["ring"] is None or idx["ring"] == e["ring"]
        if flagged and (not ring_ok or idx["slotMin"] is not None):
            if idx["ring"] is not None:
                e["ring"] = idx["ring"]
            if idx["slotMin"] is not None:
                e["slotMin"] = idx["slotMin"]
                e["slotTime"] = min_to_compact(idx["slotMin"])
            e["judge"] = ""
            e["ahead"] = 0
            e["prevBreed"] = None
            e["prevN"] = None
            e["flags"] = ["continuation: ring+slot recovered from index — enter judge & ahead by hand"]
        # Note: a non-flagged entry whose detail ring differs from the index ring
        # is usually a breed pulled into an overflow ring — the detail (physical)
        # ring is what the card wants, so we don't flag it.



def parse_day_stream(stream):
    entries = []
    groups = {"regular": {"start": None, "startMin": None, "order": []},
              "nohs": {"start": None, "startMin": None, "order": []}}

    cur_ring = None
    cur_judge = None
    need_judge = False
    cur_time = None
    cur_min = None
    accum = 0                # dogs ahead in the current time-block
    prev_breed = None
    prev_n = None
    # group_ctx tracks which group section we're in. Onofrio isn't consistent:
    # some days head the Regular groups with "REGULAR VARIETY GROUPS", others just
    # start listing them with no header. So a bare "X GROUP - JUDGE" line defaults
    # to Regular; only NOHS / Bred-by / Puppy sections have to announce themselves.
    group_ctx = None         # None(->regular) | 'nohs' | 'bred'
    regular_armed = False     # saw the Regular header, watching for its start time
    where = None             # (page, col) of the previous line
    anchored = False         # have we seen a RING/time in THIS column yet?

    for page, col, raw in stream:
        line = raw.strip()
        if (page, col) != where:     # entered a new column: state is now inherited
            where = (page, col)
            anchored = False

        gh = GROUP_HDR.search(line)
        if gh:
            u = gh.group(0).upper()
            group_ctx = "nohs" if "NOHS" in u else ("bred" if "BRED" in u else "regular")
            regular_armed = group_ctx == "regular"
            continue

        gm = GROUP_RE.match(line)
        if gm and gm.group(1).strip().lower() in VARIETY_GROUPS:
            lane = "nohs" if group_ctx == "nohs" else ("bred" if group_ctx == "bred" else "regular")
            if lane in ("regular", "nohs"):
                name = gm.group(1).strip().title()
                dst = groups[lane]["order"]
                if not any(g["group"].lower() == name.lower() for g in dst):
                    dst.append({"group": name, "judge": titlecase_judge(gm.group(2))})
            continue

        rm = RING_RE.match(line)
        if rm:
            cur_ring = int(rm.group(1))
            need_judge = True
            anchored = True
            continue

        if CONTROL_RE.search(line):
            continue

        tm = TIME_RE.match(line)
        if tm:
            cur_min = clock_to_min(tm.group(1))
            cur_time = min_to_compact(cur_min) if cur_min is not None else None
            accum = 0
            prev_breed = None
            prev_n = None
            anchored = True
            if regular_armed and not groups["regular"]["order"] and groups["regular"]["start"] is None:
                groups["regular"]["start"] = cur_time
                groups["regular"]["startMin"] = cur_min
            continue

        bm = BREED_RE.match(line)
        if bm:
            count = int(bm.group(1))
            breed = bm.group(2).strip()
            split = bm.group(3)
            entry = {
                "breed": breed, "ring": cur_ring, "judge": cur_judge or "",
                "slotTime": cur_time, "slotMin": cur_min if cur_min is not None else 0,
                "ahead": accum, "prevBreed": prev_breed, "prevN": prev_n,
                "entryCount": count, "split": split,
            }
            if not anchored:
                # breed at a column top with no local RING/time header: its block
                # header lives in another column (an interrupted ring strand).
                # ring/judge/slotTime are inherited guesses — verify by hand.
                entry["flags"] = ["continuation: verify ring / judge / slotTime"]
            entries.append(entry)
            accum += count
            prev_breed = breed
            prev_n = count
            need_judge = False
            regular_armed = False   # a breed ends the Regular start-time window
            continue

        if need_judge and JUDGE_RE.match(line):
            cur_judge = titlecase_judge(line)
            need_judge = False
            continue

    return entries, groups


register(Extractor(name="onofrio",
                   detect=lambda t: "onofrio" in t.lower(),
                   parse=parse))
