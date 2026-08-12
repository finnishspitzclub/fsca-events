"""Minimal, dependency-free validator for the intermediate contract.

Not a full JSON-Schema engine — just enough to catch the mistakes an extractor
actually makes: missing days, entries without a ring/time, groups without an
order. Returns a list of human-readable error strings ([] == valid).
"""
from __future__ import annotations

ENTRY_REQ = ("breed", "ring", "judge", "slotTime", "slotMin", "ahead", "entryCount")


def validate_intermediate(inter) -> list[str]:
    errs: list[str] = []
    if not isinstance(inter, dict):
        return ["root is not an object"]
    for k in ("show", "days"):
        if k not in inter:
            errs.append(f"missing top-level '{k}'")
    show = inter.get("show", {})
    for k in ("club", "dates", "venue", "super"):
        if k not in show:
            errs.append(f"show missing '{k}'")
    for di, d in enumerate(inter.get("days", [])):
        for k in ("date", "label", "entries", "groups"):
            if k not in d:
                errs.append(f"day[{di}] missing '{k}'")
        for ei, e in enumerate(d.get("entries", [])):
            for k in ENTRY_REQ:
                if e.get(k) is None:
                    errs.append(f"day[{di}].entry[{ei}] ({e.get('breed','?')}) missing '{k}'")
            if not isinstance(e.get("ring"), int):
                errs.append(f"day[{di}].entry[{ei}] ({e.get('breed','?')}) ring not an int: {e.get('ring')!r}")
            if not isinstance(e.get("slotMin"), int):
                errs.append(f"day[{di}].entry[{ei}] ({e.get('breed','?')}) slotMin not an int")
        g = d.get("groups", {})
        for lane in ("regular", "nohs"):
            if lane not in g:
                errs.append(f"day[{di}].groups missing '{lane}'")
            elif "order" not in g[lane]:
                errs.append(f"day[{di}].groups.{lane} missing 'order'")
    return errs
