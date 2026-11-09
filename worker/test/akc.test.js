import assert from "node:assert/strict";
import { test } from "node:test";

import {
  extractRecords,
  mapRecord,
  monthAllowed,
  monthRange,
  parseBreeds,
  parseSearchBody,
  searchPayload,
} from "../src/akc.js";

const NOW = new Date(Date.UTC(2026, 10, 9)); // Nov 2026

test("monthRange covers the whole month, including leap February", () => {
  const { from, to } = monthRange("2028-02");
  assert.equal(from.toISOString().slice(0, 10), "2028-02-01");
  assert.equal(to.toISOString().slice(0, 10), "2028-02-29");
});

test("monthAllowed bounds how far a caller can reach", () => {
  assert.equal(monthAllowed("2026-10", NOW), true);
  assert.equal(monthAllowed("2026-09", NOW), false);
  assert.equal(monthAllowed("2028-06", NOW), true);
  assert.equal(monthAllowed("2028-07", NOW), false);
  for (const bad of ["", null, "2026-13", "2026-1", "2026-11-01", "abcd-ef"]) {
    assert.equal(monthAllowed(bad, NOW), false, String(bad));
  }
});

test("searchPayload pads the code and uses MM/DD/YYYY month bounds", () => {
  const p = searchPayload({ code: "107", name: "Golden Retriever" }, "2026-11");
  assert.equal(p.breedCode, "107 ");
  assert.equal(p.dateRange.from, "11/01/2026");
  assert.equal(p.dateRange.to, "11/30/2026");
  assert.equal(p.dateRange.type, "event");
});

test("parseBreeds trims, merges double listings and sorts", () => {
  const out = parseBreeds({
    breeds: [
      { breedCode: "546 ", displayDescription: "Toy Poodle" },
      { breedCode: "546 ", displayDescription: "Poodle (Toy)" },
      { breedCode: "313 ", displayDescription: "Finnish Spitz" },
      { breedCode: "bad", displayDescription: "Nope" },
    ],
  });
  assert.deepEqual(out, [
    { code: "313", name: "Finnish Spitz" },
    { code: "546", name: "Poodle (Toy)", aka: "Toy Poodle" },
  ]);
});

test("extractRecords never reads an error body as an empty month", () => {
  assert.deepEqual(extractRecords([]), []);
  assert.deepEqual(extractRecords({ events: [] }), []);
  assert.equal(extractRecords({ message: 'Missing "dateRange" parameter' }), null);
  assert.equal(extractRecords(null), null);
});

test("parseSearchBody drops the repeated breed lists and keeps everything else", () => {
  const body = JSON.stringify([
    { eventNumber: 1, items: [{ a: 1, breeds: [{ code: "1" }, { code: "2" }] }, { b: 2, breeds: [] }] },
    { eventNumber: 2, documents: [{ code: "X" }], items: [{ breeds: [{ code: "3" }] }] },
  ]);
  assert.deepEqual(parseSearchBody(body), [
    { eventNumber: 1, items: [{ a: 1, breeds: [] }, { b: 2, breeds: [] }] },
    { eventNumber: 2, documents: [{ code: "X" }], items: [{ breeds: [] }] },
  ]);
});

test("parseSearchBody still reads a breed list the cut can't handle", () => {
  const body = JSON.stringify([{ items: [{ breeds: [{ tags: ["x"] }, { name: "y" }] }] }]);
  assert.deepEqual(parseSearchBody(body), JSON.parse(body));
});

function record(over = {}) {
  return {
    eventNumber: 2026100001,
    startDate: "2026-11-20",
    clubName: " Sample KC ",
    eventType: "AB",
    city: "Denver",
    state: "CO",
    site: { name: "Hall", location1: "Hall", location2: "Annex", coordinates: { lat: 39.7, lon: -105 } },
    judges: {
      breedJudge: { name: "A Judge" },
      groupJudge: { name: "UNASSIGNED" },
    },
    items: [
      {
        competitionGroupCode: "CONF",
        openingDate: Date.UTC(2026, 9, 1, 16),
        closingDate: Date.UTC(2026, 10, 4, 17),
        entryFee: [32],
        competitionMethodCode: "LB ",
        bvgSpecialty: "Non-Sporting Group",
      },
    ],
    documents: [
      { code: "PRMLST ", keyBinary: 123, name: "x" },
      { code: "ZZZ", name: "Other thing", keyBinary: 124 },
      { code: "JDGPRO", keyBinary: null },
    ],
    ...over,
  };
}

test("mapRecord normalizes a show the way the page expects", () => {
  const s = mapRecord(record());
  assert.equal(s.event_no, "2026100001");
  assert.equal(s.end, "2026-11-20");
  assert.equal(s.club, "Sample KC");
  assert.equal(s.venue, "Hall, Annex");
  assert.equal(s.open, "2026-10-01");
  assert.equal(s.close, "2026-11-04");
  assert.equal(s.fee, 32);
  assert.equal(s.high_value, true);
  assert.equal(s.breed_judge, "A Judge");
  assert.equal(s.group_judge, "");
  assert.equal(s.lat, 39.7);
  assert.deepEqual(s.docs.map((d) => d.label), ["Premium list", "Other thing"]);
  assert.match(s.docs[0].url, /KEY_BINARY_CONTENT=123$/);
});

test("mapRecord drops impossible entry dates", () => {
  const s = mapRecord(
    record({
      items: [{ competitionGroupCode: "CONF", openingDate: Date.UTC(2028, 0, 1), closingDate: Date.UTC(2026, 10, 4) }],
    }),
  );
  assert.equal(s.open, "");
  assert.equal(s.close, "2026-11-04");
});

test("mapRecord skips a record with no usable date", () => {
  assert.equal(mapRecord(record({ startDate: "" })), null);
});
