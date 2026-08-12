#!/usr/bin/env python3
"""parse.py — judging-program PDF -> intermediate.json

The fragile layer, isolated on purpose. Detects the superintendent, dispatches
to that extractor, validates against schemas/intermediate.schema.json, and
writes a clean, hand-editable intermediate.json. When a weird program breaks
the parser, you fix three fields in that JSON by hand — never fight the regex.

Usage:
  python parse.py samples/onofrio-roaring-fork.pdf -o intermediate.json
  python parse.py program.pdf --super onofrio        # force an extractor
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

import pdfplumber

import extractors
from validate import validate_intermediate


def cover_text(pdf_path: str, pages: int = 3) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join((pdf.pages[i].extract_text() or "")
                         for i in range(min(pages, len(pdf.pages))))


def main(argv=None):
    ap = argparse.ArgumentParser(description="Judging-program PDF -> intermediate.json")
    ap.add_argument("pdf", help="path to the judging-program PDF")
    ap.add_argument("-o", "--out", default="intermediate.json")
    ap.add_argument("--super", dest="sup", default=None,
                    help="force a superintendent extractor (e.g. onofrio)")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    if not Path(args.pdf).exists():
        ap.error(f"no such file: {args.pdf}")

    if args.sup:
        ext = next((e for e in extractors.all_extractors() if e.name == args.sup), None)
        if not ext:
            ap.error(f"unknown --super {args.sup}; have "
                     f"{[e.name for e in extractors.all_extractors()]}")
    else:
        ext = extractors.pick(cover_text(args.pdf))
        if not ext:
            print("parse: could not detect superintendent. Force one with --super "
                  f"(have {[e.name for e in extractors.all_extractors()]}).", file=sys.stderr)
            return 2

    inter = ext.parse(args.pdf)
    errors = validate_intermediate(inter)
    if errors:
        print("parse: extracted JSON FAILED validation:", file=sys.stderr)
        for e in errors:
            print("  - " + e, file=sys.stderr)
        # still write it so the human can inspect / hand-fix
    Path(args.out).write_text(json.dumps(inter, indent=2, ensure_ascii=False), encoding="utf-8")

    if not args.quiet:
        nd = len(inter["days"])
        ne = sum(len(d["entries"]) for d in inter["days"])
        print(f"parse: {ext.name} -> {args.out}  ({nd} days, {ne} breed entries)"
              + ("  [VALIDATION ERRORS ABOVE]" if errors else ""), file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
