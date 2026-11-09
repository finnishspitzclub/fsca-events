"""Shared extractor for the non-Onofrio superintendents.

MB-F, Foy Trent, BaRay, Jim Rau, Jack Bradshaw, Executive and Lorraine Yu all
print the same skeleton as Onofrio -- a ring header, a printed time, then
'N Breed split' lines, in two columns -- and differ only in spelling: 'RING TwO'
vs 'Ring No 4', '0-0-1-0' vs '0-0-(1-0)' vs '0-0-1/0', a weekday vs a full date.
So there is one reader here and the differences live in the patterns below.

Everything is a fact off the page; a value we can't read stays None so the
pipeline drops the entry rather than publishing a guess.
"""
from __future__ import annotations
import re
import pdfplumber
from itertools import groupby
from datetime import date
from .base import Extractor, register, clock_to_min, min_to_compact, MONTHS

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
_WD = "|".join(WEEKDAYS)
WORD_NUM = {w: i + 1 for i, w in enumerate(
    "one two three four five six seven eight nine ten eleven twelve thirteen "
    "fourteen fifteen sixteen seventeen eighteen nineteen twenty".split())}
_NUM = r"\d+|" + "|".join(WORD_NUM)

RING_RE  = re.compile(rf"^RING(?:\s+No\.?)?\s*({_NUM})\b\s*[-–—:]?\s*(.*)$", re.I)
RINGS_RE = re.compile(rf"^RINGS\s+({_NUM})\b", re.I)
TIME_RE  = re.compile(r"^(\d{1,2}:\d{2})\s*([ap]\.?m?\.?|noon)\b\.?\s*(.*)$", re.I)
# count, optional '-', optional '*', breed, dotted leader, split. The split is
# 'd-b-bd-bb', 'd-b-(bd-bb)', 'd-b (bd-bb)' or 'd-b-bd/bb' -- or bare 'd-b' for
# a variety with no specials (Bradshaw), which is otherwise a puppy-ring shape.
BREED_RE = re.compile(
    r"^(\d{1,3})(?:\s*-)?\s+\*?([A-Za-z].*?)[\s.]*(?<=[\s.])"
    r"(\d+-\d+(?:-\d+-\d+|-\(\d+-\d+\)|\s*\(\d+-\d+\)|-\d+/\d+)?)\*?\s*$")
JUDGE_LINE = re.compile(r"^JUDGE:?\s+(.+)$", re.I)
TITLED     = re.compile(r"^(Mr|Mrs|Ms|Miss|Dr)\.?\s+[A-Z]", re.I)
FULL_DATE  = re.compile(rf"\b({_WD}),?\s+([A-Za-z]+)\s+(\d{{1,2}}),\s+(\d{{4}})", re.I)
HEAD_WD    = re.compile(rf"^({_WD})\b(?:\s*#\s*(\d))?", re.I)
COL_WD     = re.compile(rf"^({_WD})(?:\s+(AM|PM))?(?:\s+SHOW)?$", re.I)
SHOW_NO    = re.compile(r"SHOW\s*#\s*(\d)", re.I)

SKIP_RE   = re.compile(r"rally|obedience|junior|lunch|total dogs|fast ?cat", re.I)
PUPPY_RE  = re.compile(r"4-6\s*mo|beginner\s*pupp|group\s+pupp", re.I)

NOHS_HDR = re.compile(r"NOHS\s+groups?|owner[- ]handled\s+(variety\s+)?groups?", re.I)
REG_HDR  = re.compile(r"regular\s+(variety\s+)?groups?|variety\s+groups?|variety\s+judging|"
                      r"group\s+judge\s+assignments|"
                      r"^\d{1,2}:\d{2}\s*[ap]\.?m?\.?\s+variety\b", re.I)
# 'follows each Regular group', but not 'in the following order'
FOLLOWS  = re.compile(r"follow(?:s|ing)?\s+(?:each|their|the\s+regular|regular|respective)", re.I)
GROUP_LINE = re.compile(
    r"^(NOHS\s+|Owner[- ]Handled\s+)?"
    r"(Non-?\s?Spor\S*|Spor\S*|Hounds?|Working|Terriers?|Toys?|Herding)"
    r"(?:\s+Groups?)?\b[\s.]*(.*)$", re.I)
GROUP_TIME = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*([ap])\.?m", re.I)
GROUP_RING = re.compile(rf"\bin\s+Rings?\s+({_NUM})\b", re.I)
BIS_LINE   = re.compile(r"^(NOHS\s+|Owner[- ]Handled\s+)?Best\s+in\s+Show\b", re.I)
# How far past its header a group list may run before we stop believing a
# stray 'Working ...' line belongs to it.
GROUP_WINDOW = 18

# Foy Trent's fonts drop ligatures to U+FFFD. Names only -- nothing numeric.
_LIGATURE = [("Wol�ounds", "Wolfhounds"), ("Ro�weiler", "Rottweiler"),
             ("Be�y", "Betty"), ("�", "ti")]


def _clean(s: str) -> str:
    for a, b in _LIGATURE:
        s = s.replace(a, b)
    return s


def _despace(line: str) -> str:
    """Undo wide letter-spacing, which arrives as 'R I N G  S I X' / '1 0 :3 0 A M'.
    Left alone it silently hides a ring or time and mis-files the breeds under it."""
    toks = line.split()
    if len(toks) < 4 or sum(len(t) <= 2 for t in toks) / len(toks) < 0.6:
        return line
    return re.sub(r"(?<=[a-z.])(?=[A-Z])", " ", "".join(toks))


def _num(tok: str):
    return int(tok) if tok.isdigit() else WORD_NUM.get(tok.lower())


def _clock(hm: str, ap: str):
    """('12:00','NOON') | ('8:00','am') -> minutes since midnight."""
    if ap.lower() == "noon":
        return 720
    return clock_to_min(f"{hm} {ap[0]}")


def _judge(s: str) -> str:
    s = re.sub(r"\(.*?\)|=\s*\d+|\bJUDGE\b:?", "", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip(" -–—")


def _group_name(tok: str) -> str:
    low = re.sub(r"[-\s]", "", tok.lower())
    if low.startswith("non"):
        return "Non-Sporting"
    for key, name in (("spor", "Sporting"), ("hound", "Hound"), ("work", "Working"),
                      ("terr", "Terrier"), ("toy", "Toy"), ("herd", "Herding")):
        if low.startswith(key):
            return name
    return tok.title()


def _split4(split: str) -> str:
    """Normalize every printed form to 'd-b-bd-bb'; a bare 'd-b' stays as printed."""
    nums = re.findall(r"\d+", split)
    return "-".join(nums) if len(nums) == 4 else split


# ---- page -> reading-order lines -----------------------------------------
def _cover(words, lo, hi):
    """How many words sit over each 2pt strip between lo and hi."""
    bins = {x: 0 for x in range(int(lo), int(hi), 2)}
    for w in words:
        for x in bins:
            if w["x0"] <= x <= w["x1"]:
                bins[x] += 1
    return bins


def _emptiest(bins, near):
    best = min(bins.values())
    return best, min((x for x, n in bins.items() if n == best), key=lambda x: abs(x - near))


def _gutters(words, width):
    """x of each column break: [] for one column, one for two, two for BaRay's
    three-column letter pages. Measured, not page/2 -- columns are rarely even."""
    if not words:
        return []
    lines = len({round(w["top"] / 2) for w in words})
    if width >= 600:
        found = [_emptiest(_cover(words, c - 45, c + 45), c) for c in (width / 3, 2 * width / 3)]
        if all(n <= 0.12 * lines for n, _ in found):
            return [x for _, x in found]
    n, x = _emptiest(_cover(words, width * 0.3, width * 0.7), width / 2)
    return [] if n > 0.6 * lines else [x]


def page_columns(page):
    """[(col, text)] in reading order: each column top-to-bottom, left to right."""
    words = page.extract_words()
    guts = _gutters(words, page.width)
    out = []
    for col in range(len(guts) + 1):
        cw = [w for w in words if sum(w["x0"] >= g for g in guts) == col]
        cw.sort(key=lambda w: (round(w["top"] / 2) * 2, w["x0"]))
        for _, grp in groupby(cw, key=lambda w: round(w["top"] / 2) * 2):
            out.append((col, _clean(" ".join(w["text"] for w in grp))))
    return out


# ---- which show does a page belong to ------------------------------------
class DayTracker:
    """Resolves page and column headers to (iso date, show number). Some
    programs print a full date, others only a weekday -- for those the show
    dates the AKC feed gave us (`dates`) name the actual day."""

    def __init__(self, dates):
        self.by_wd = {WEEKDAYS[date.fromisoformat(d).weekday()]: d for d in (dates or [])}
        self.only = dates[0] if dates and len(dates) == 1 else None
        self.cur = (self.only, 1) if self.only else None

    def _full(self, m):
        mon = MONTHS.get(m.group(2).lower())
        if not mon:
            return None
        iso = f"{m.group(4)}-{mon:02d}-{int(m.group(3)):02d}"
        self.by_wd.setdefault(m.group(1).lower(), iso)
        return iso

    def page(self, text):
        for line in text.splitlines()[:4]:
            m = FULL_DATE.search(line)
            if m and (iso := self._full(m)):
                sn = SHOW_NO.search(line)
                self.cur = (iso, int(sn.group(1)) if sn else 1)
                return
            m = HEAD_WD.match(line.strip())
            if m and (iso := self.by_wd.get(m.group(1).lower())):
                self.cur = (iso, int(m.group(2) or 1))
                return

    def column(self, line):
        """Lorraine Yu heads each COLUMN ('FRIDAY PM SHOW'). True if consumed."""
        m = COL_WD.match(line)
        if not m:
            return False
        iso = self.by_wd.get(m.group(1).lower())
        if iso:
            self.cur = (iso, 2 if (m.group(2) or "").upper() == "PM" else 1)
        return True


# ---- one show's line stream -> entries + groups ---------------------------
def _new_groups():
    return {"regular": {"start": None, "startMin": None, "ring": None, "follows": False, "order": []},
            "nohs": {"start": None, "startMin": None, "ring": None, "follows": False, "order": []}}


class Show:
    """Line-at-a-time reader for one show. State survives column and page
    breaks because a ring routinely continues at the top of the next column."""

    def __init__(self):
        self.entries, self.groups = [], _new_groups()
        self.ring = self.judge = self.time = self.mins = None
        self.accum, self.prev, self.prevn = 0, None, None
        self.puppy = False
        self.need_judge = False
        self.lane, self.since = None, 0
        self.rings_hdr = None          # ring from a 'RINGS 1 & 2' / bare 'RING 2' just above
        self.saved, self.fresh_ring = {}, False
        self.age = 99                  # lines since the last RING header

    def _block(self):
        return (self.judge, self.time, self.mins, self.accum, self.prev, self.prevn)

    def _resume(self):
        judge, self.time, self.mins, self.accum, self.prev, self.prevn = self.saved[self.ring]
        self.judge = self.judge or judge
        self.need_judge = not self.judge

    def _group_header(self, line):
        if self.lane == "nohs" and FOLLOWS.search(line):
            # 'Immediately following each Regular Variety Group' is timing, not a header
            self.groups["nohs"]["follows"] = True
            return True
        lane = "nohs" if NOHS_HDR.search(line) else ("regular" if REG_HDR.search(line) else None)
        if not lane or PUPPY_RE.search(line):
            return False
        self.lane, self.since = lane, 0
        g = self.groups[lane]
        self._group_facts(line, g)
        if g["ring"] is None and self.rings_hdr is not None and self.age <= 1:
            g["ring"] = self.rings_hdr     # only a RING printed directly above it
        return True

    def _group_facts(self, line, g):
        if g["start"] is None and (m := GROUP_TIME.search(line)):
            h, mm = int(m.group(1)) % 12, int(m.group(2) or 0)
            mins = h * 60 + (720 if m.group(3).lower() == "p" else 0) + mm
            g["start"], g["startMin"] = min_to_compact(mins), mins
        if m := GROUP_RING.search(line):
            g["ring"] = _num(m.group(1))   # stated outright: beats a nearby header
        if FOLLOWS.search(line):
            g["follows"] = True

    def feed(self, line):
        line = _despace(line.strip())
        if not line:
            return
        self.age += 1
        m = RINGS_RE.match(line)
        if m:
            self.rings_hdr, self.age = _num(m.group(1)), 0
            if self._group_header(line):
                return
            return
        if self._group_header(line):
            return

        if self.lane:
            self.since += 1
            if self.since > GROUP_WINDOW:
                self.lane = None
            else:
                m = GROUP_LINE.match(line)
                if m and not BREED_RE.match(line):
                    lane = "nohs" if m.group(1) else self.lane
                    name = _group_name(m.group(2))
                    dst = self.groups[lane]["order"]
                    if not any(g["group"] == name for g in dst):
                        dst.append({"group": name, "judge": _judge(m.group(3))})
                    return
                if BIS_LINE.match(line):
                    self.lane = None
                    return
                if not self.groups[self.lane]["order"]:
                    self._group_facts(line, self.groups[self.lane])

        m = RING_RE.match(line)
        if m and _num(m.group(1)) is not None:
            if self.ring is not None:
                self.saved[self.ring] = self._block()
            self.ring = _num(m.group(1))
            self.rings_hdr, self.age = self.ring, 0
            rest = m.group(2).strip()
            self.puppy = bool(PUPPY_RE.search(rest))
            self.judge = _judge(rest) if rest and not self.puppy else None
            self.need_judge = not self.judge
            self.time, self.mins, self.accum, self.prev, self.prevn = None, None, 0, None, None
            self.fresh_ring = True
            if re.search(r"\(cont", rest, re.I) and self.ring in self.saved:
                self._resume()
            return
        if self.fresh_ring and re.match(r"^continued", line, re.I):
            # 'Ring 4 / Continued' at a column top: same block, carry on counting
            self.fresh_ring = False
            if self.ring in self.saved:
                self._resume()
            return
        self.fresh_ring = False
        if PUPPY_RE.search(line):
            self.puppy = True
            return

        m = JUDGE_LINE.match(line)
        if m:
            self.judge = _judge(m.group(1))
            self.need_judge = False
            return

        m = TIME_RE.match(line)
        if m:
            if re.search(r"lunch", m.group(3), re.I):
                return
            self.mins = _clock(m.group(1), m.group(2))
            self.time = min_to_compact(self.mins) if self.mins is not None else None
            self.accum, self.prev, self.prevn = 0, None, None
            if not self.judge and m.group(3).lower().startswith("judge"):
                self.judge = _judge(m.group(3))
            return

        m = BREED_RE.match(line)
        if m and not SKIP_RE.search(line):
            if self.puppy:
                return
            if self.mins is None and self.saved.get(self.ring, (0, 0, None))[2] is not None:
                self._resume()             # ring re-headed mid-block with no new time
            n, breed, split = int(m.group(1)), m.group(2).strip(), m.group(3)
            self.entries.append({
                "breed": breed, "ring": self.ring, "judge": self.judge or "",
                "slotTime": self.time, "slotMin": self.mins,
                "ahead": self.accum, "prevBreed": self.prev, "prevN": self.prevn,
                "entryCount": n, "split": _split4(split),
            })
            self.accum += n
            self.prev, self.prevn = breed, n
            self.need_judge = False
            return

        if self.need_judge and TITLED.match(line):
            self.judge = _judge(line)
            self.need_judge = False


def _drop_puppy_rings(entries):
    """A ring whose every line is a bare 'd-b' split is a beginner-puppy ring
    whose header we didn't recognize, not regular breed judging."""
    by_ring = {}
    for e in entries:
        by_ring.setdefault((e["ring"], e["judge"]), []).append(e)
    bad = {k for k, v in by_ring.items() if all(len(e["split"].split("-")) == 2 for e in v)}
    return [e for e in entries if (e["ring"], e["judge"]) not in bad]


def _index_pairs(text, breed, count_first):
    """Every (ring, 'h:mm') the program's own index prints for a breed. Bradshaw's
    index reads count-time-ring; the rest read ring-time, one pair per day."""
    out = set()
    if count_first:
        for m in re.finditer(breed + r"[a-z]*\s*\.+\s*\d+\s+(\d{1,2}):(\d{2})\s*[AP]M\s+(\d+)", text, re.I):
            out.add((int(m.group(3)), f"{int(m.group(1))}:{m.group(2)}"))
        return out
    for m in re.finditer(breed + r"[a-z]*(?:\s*\.+\s*|\s+)((?:\d{1,2}\s+\d{1,2}:\d{2}\s*(?:[ap]\.?m\.?)?\s*)+)", text, re.I):
        for p in re.finditer(r"(\d{1,2})\s+(\d{1,2}):(\d{2})", m.group(1)):
            out.add((int(p.group(1)), f"{int(p.group(2))}:{p.group(3)}"))
    return out


def _check_against_index(days, text, count_first):
    """The index is printed separately from the ring detail, so a Finnish Spitz
    line it contradicts means we misread the page. Clear the ring: the pipeline
    then drops the entry instead of sending someone to the wrong ring."""
    index = _index_pairs(text, "finnish spitz", count_first)
    if not index:
        return
    for d in days:
        for e in d["entries"]:
            if "finnish spitz" not in e["breed"].lower() or e["ring"] is None or not e["slotTime"]:
                continue
            if (e["ring"], re.match(r"\d+:\d+", e["slotTime"]).group(0)) not in index:
                e["ring"] = None
                e["flags"] = ["disagrees with the program's printed index"]


def _club(cover: str) -> str:
    m = re.search(r"^([A-Z][A-Za-z .,&'\-]*KENNEL CLUB)", cover, re.M | re.I)
    return re.sub(r",?\s*INC\.?\s*$", "", m.group(1).strip(), flags=re.I).title() if m else "Dog Show"


def _group_ring_from_page(text, show):
    """A centred 'RING TWELVE' above the groups straddles the gutter, so the
    column reader never sees it whole. The full-width page text does."""
    prev = None
    for line in text.splitlines():
        m = RING_RE.match(line.strip()) or RINGS_RE.match(line.strip())
        lane = None
        if NOHS_HDR.search(line):
            lane = "nohs"
        elif REG_HDR.search(line):
            lane = "regular"
        if lane and prev is not None and show.groups[lane]["ring"] is None:
            show.groups[lane]["ring"] = prev
        prev = _num(m.group(1)) if m else None


def _parse(pdf_path, dates, label):
    shows, cover, alltext = {}, "", []
    days = DayTracker(dates)
    with pdfplumber.open(pdf_path) as pdf:
        for pi, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            alltext.append(text)
            if pi < 3:
                cover += text + "\n"
            days.page(text)
            for _, line in page_columns(page):
                if days.column(line.strip()) or not days.cur:
                    continue
                shows.setdefault(days.cur, Show()).feed(line)
            if days.cur in shows:
                _group_ring_from_page(text, shows[days.cur])

    out = []
    multi = {}
    for iso, _ in shows:
        multi[iso] = multi.get(iso, 0) + 1
    for (iso, no), s in sorted(shows.items()):
        entries = _drop_puppy_rings(s.entries)
        if not entries:
            continue                       # obedience-only / empty slice
        wd = WEEKDAYS[date.fromisoformat(iso).weekday()].title()
        out.append({"date": iso, "label": f"{wd} #{no}" if multi[iso] > 1 else wd,
                    "show": no, "entries": entries, "groups": s.groups})
    for d in out:
        for g in d["groups"].values():
            if len(g["order"]) != 7:       # a partial list would misstate the running order
                g["order"] = []
    _check_against_index(out, _clean("\n".join(alltext)), label == "Jack Bradshaw")
    span = ""
    if out:
        a, b = out[0]["date"], out[-1]["date"]
        span = a if a == b else f"{a} – {b}"
    # Event numbers in the order the cover lists them, which is running order.
    # The numbers themselves aren't: BaRay's Show 1 can be ...614 and Show 2 ...603.
    events = list(dict.fromkeys(re.findall(r"\b20\d{8}\b", cover)))
    return {"show": {"club": _club(cover), "dates": span, "venue": "", "super": label,
                     "eventOrder": events}, "days": out}


for _name, _label, _needle in (
        ("mbf", "MB-F", "mb-f"),
        ("foytrent", "Foy Trent", "foy trent"),
        ("baray", "BaRay", "baray"),
        ("jimrau", "Jim Rau", "jim rau"),
        ("bradshaw", "Jack Bradshaw", "bradshaw"),
        ("executive", "Executive", "executive dog"),
        ("lorraineyu", "Lorraine Yu", "lorraine yu")):
    register(Extractor(
        name=_name,
        detect=(lambda n: lambda t: n in t.lower())(_needle),
        parse=(lambda l: lambda path, dates=None: _parse(path, dates, l))(_label)))
