"""Extractor contract + registry.

Every superintendent has its own extractor (Onofrio, BaRay, MB-F ...), but all
of them produce the SAME thing: a dict matching schemas/intermediate.schema.json.
The renderer never learns which superintendent a program came from — it only
ever sees the intermediate contract. Add a new superintendent by writing one
module here and registering it; nothing downstream changes.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Callable

MONTHS = {m: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"])}


def clock_to_min(text: str):
    """'10:50 am' | '2:45p' -> minutes since midnight, or None."""
    m = re.match(r"(\d{1,2}):(\d{2})\s*([ap])\.?m?\.?$", text.strip(), re.I)
    if not m:
        return None
    h = int(m.group(1)) % 12
    if m.group(3).lower() == "p":
        h += 12
    return h * 60 + int(m.group(2))


def min_to_compact(mins: int) -> str:
    """650 -> '10:50a'."""
    mins %= 1440
    h, mm = divmod(mins, 60)
    ap = "a" if h < 12 else "p"
    h12 = h % 12 or 12
    return f"{h12}:{mm:02d}{ap}"


# --- registry ------------------------------------------------------------
_REGISTRY: dict[str, "Extractor"] = {}


@dataclass
class Extractor:
    name: str                                  # 'onofrio'
    detect: Callable[[str], bool]              # given cover text, is this us?
    parse: Callable[[str], dict]               # given pdf path, -> intermediate dict


def register(ext: Extractor):
    _REGISTRY[ext.name] = ext


def all_extractors():
    return list(_REGISTRY.values())


def pick(cover_text: str):
    for ext in _REGISTRY.values():
        if ext.detect(cover_text):
            return ext
    return None
