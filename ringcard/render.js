#!/usr/bin/env node
'use strict';
/*
 * render.js — intermediate.json + weekend.json -> out.html
 *
 * The clean layer. Reads the faithful program extract (intermediate.json) and
 * the per-show config (weekend.json), computes the DERIVED values the schema
 * deliberately does not store — estimated times and clash flags — from
 * slotMin + ahead + estimates, then injects a DATA blob into template.html.
 *
 * Retuning estimates in weekend.json and re-running re-derives everything.
 * Zero dependencies (Node stdlib only).
 */
const fs = require('fs');
const path = require('path');

// ---------- tiny helpers ----------
const die = m => { console.error('render: ' + m); process.exit(1); };
const norm = s => s.toLowerCase().replace(/\s+/g, ' ').trim();
const round = x => Math.round(x);

function fmt(min) {                       // 650 -> "10:50a"
  min = ((round(min) % 1440) + 1440) % 1440;
  let h = Math.floor(min / 60), m = min % 60;
  const ap = h < 12 ? 'a' : 'p';
  let h12 = h % 12; if (h12 === 0) h12 = 12;
  return `${h12}:${String(m).padStart(2, '0')}${ap}`;
}
const ordinal = n => {                    // 1 -> "1st"
  const s = ['th', 'st', 'nd', 'rd'], v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
};

// ---------- lightweight schema guard ----------
function validate(inter) {
  if (!inter || typeof inter !== 'object') die('intermediate.json is not an object');
  for (const k of ['show', 'days']) if (!(k in inter)) die(`intermediate.json missing "${k}"`);
  if (!Array.isArray(inter.days)) die('intermediate.days must be an array');
  inter.days.forEach((d, i) => {
    for (const k of ['label', 'entries', 'groups']) if (!(k in d)) die(`day[${i}] missing "${k}"`);
    if (!Array.isArray(d.entries)) die(`day[${i}].entries must be an array`);
    d.entries.forEach((e, j) => {
      for (const k of ['breed', 'ring', 'slotMin', 'ahead', 'entryCount'])
        if (e[k] == null) die(`day[${i}].entry[${j}] (${e.breed || '?'}) missing "${k}"`);
    });
  });
}

// ---------- breed matching ----------
// config uses friendly singular names; the PDF prints plurals / parentheticals.
// match = entry name startsWith config name (after normalising), exact preferred.
function breedMatches(entryBreed, cfgBreed) {
  const a = norm(entryBreed), b = norm(cfgBreed);
  return a === b || a.startsWith(b + ' ') || a.startsWith(b + 's') || a.startsWith(b);
}
function findEntry(entries, cfgBreed) {
  // exact first, then prefix, longest cfg wins is caller's job
  let hit = entries.find(e => norm(e.breed) === norm(cfgBreed));
  if (hit) return hit;
  return entries.find(e => breedMatches(e.breed, cfgBreed));
}

// ---------- main ----------
function main() {
  const args = process.argv.slice(2);
  const opt = { inter: 'intermediate.json', weekend: 'weekend.json', template: 'template.html', out: 'out.html' };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--intermediate' || a === '-i') opt.inter = args[++i];
    else if (a === '--weekend' || a === '-w') opt.weekend = args[++i];
    else if (a === '--template' || a === '-t') opt.template = args[++i];
    else if (a === '--out' || a === '-o') opt.out = args[++i];
    else die(`unknown arg ${a}`);
  }
  const inter = JSON.parse(fs.readFileSync(opt.inter, 'utf8'));
  const cfg = JSON.parse(fs.readFileSync(opt.weekend, 'utf8'));
  validate(inter);

  const est = Object.assign({ minPerDog: 2.5, minPerGroup: 30 }, cfg.estimates || {});
  const my = (cfg.myBreeds || []).map(String);
  const other = (cfg.otherBreeds || []).map(String);
  const tracked = [...my, ...other];
  const isOther = name => other.some(o => breedMatches(name, o));

  // clash lanes: 1st clash -> 'a', 2nd -> 'b', then cycle
  const clashDefs = (cfg.clashes || []).map((c, idx) => ({
    id: c.id, label: c.label, breeds: c.breeds,
    lane: idx % 2 === 0 ? 'a' : 'b',
    requiresOthers: c.breeds.some(isOther)
  }));

  // ring palette: distinct rings (entries + groups) sorted -> rc0..rc7
  const ringSet = new Set();
  const R = [], G = {}, DAYS = [], crunch = {}, warnings = [];
  const dayWindows = {};   // d -> [{cfg, entry, start, end, ring}]

  inter.days.forEach((day, d) => {
    DAYS.push([day.label, day.date ? isoToShort(day.date) : '']);
    const windows = [];
    for (const cfgBreed of tracked) {
      const e = findEntry(day.entries, cfgBreed);
      if (!e) continue;
      const ahead = e.ahead || 0;
      const eM = e.slotMin + round(ahead * est.minPerDog);
      const eL = ahead > 0 ? '~' + fmt(eM) : fmt(e.slotMin);
      const win = { start: eM, end: eM + (e.entryCount || 0) * est.minPerDog, ring: e.ring };
      windows.push({ cfgBreed, e, eM });
      ringSet.add(e.ring);
      R.push({
        day: d, breed: e.breed, ring: e.ring, judge: e.judge || '',
        sL: e.slotTime || fmt(e.slotMin), sM: e.slotMin, eL, eM,
        ent: String(e.entryCount), sub: e.split || '',
        hide: isOther(e.breed) ? 1 : 0, clash: [],
        ahead, prev: e.prevBreed || null, prevN: e.prevN || null,
        pup: e.puppy ? 1 : 0, pupNote: e.puppyNote || '',
        flag: (e.flags && e.flags.length) ? e.flags[0] : null,
        _win: win
      });
    }
    dayWindows[d] = R.filter(r => r.day === d);
  });

  // clash detection: per day, per clash, if all breeds present and windows overlap
  inter.days.forEach((day, d) => {
    const rowsToday = R.filter(r => r.day === d);
    const rowFor = cfgBreed => rowsToday.find(r => breedMatches(r.breed, cfgBreed));
    const fired = [];
    for (const c of clashDefs) {
      const rows = c.breeds.map(rowFor);
      if (rows.some(r => !r)) continue;                     // a breed not entered that day
      const overlap = pairwiseOverlap(rows.map(r => r._win));
      if (!overlap) continue;
      fired.push({ c, rows });
      for (const r of rows) r.clash.push({ id: c.id, label: c.label, lane: c.lane, requiresOthers: c.requiresOthers });
    }
    // crunch label = span of all fired windows that day
    if (fired.length) {
      const wins = fired.flatMap(f => f.rows.map(r => r._win));
      const s = Math.min(...wins.map(w => w.start)), e = Math.max(...wins.map(w => w.end));
      crunch[d] = `Crunch ${fmt(s)}–${fmt(e)}`;
    } else crunch[d] = null;
    // per-day fired info stashed for warnings
    day._fired = fired;
  });

  // groups. Onofrio prints Regular and NOHS group blocks; on some days one is
  // missing or short. Merge them: take the running order from whichever block
  // has it (Regular preferred), append any group only the other block lists, and
  // show whatever judges we have for each. So a NOHS-only day (or a Regular block
  // missing one group) still renders a full, ordered panel.
  inter.days.forEach((day, d) => {
    const reg = (day.groups && day.groups.regular) || { start: null, order: [] };
    const nohs = (day.groups && day.groups.nohs) || { order: [] };
    const startMin = reg.startMin != null ? reg.startMin : parseClock(reg.start);
    const showGroups = cfg.groups && cfg.groups.length ? cfg.groups : null; // [{name,hideWithOthers,ring}]
    const regOrder = reg.order || [], nohsOrder = nohs.order || [];
    const judgeIn = (arr, name) => { const g = arr.find(x => norm(x.group) === norm(name)); return g ? g.judge : ''; };

    const order = [];
    const add = n => { if (!order.some(x => norm(x) === norm(n))) order.push(n); };
    (regOrder.length ? regOrder : nohsOrder).forEach(g => add(g.group)); // primary sequence
    nohsOrder.forEach(g => add(g.group));                                // fill gaps (e.g. a missing Non-Sporting)

    const groups = [];
    order.forEach((name, i) => {
      const conf = showGroups ? showGroups.find(s => norm(s.name) === norm(name)) : {};
      if (showGroups && !conf) return;                       // not a group we care about
      const estMin = startMin != null ? startMin + i * est.minPerGroup : null;
      groups.push({
        grp: name, ord: ordinal(i + 1), reg: judgeIn(regOrder, name), nohs: judgeIn(nohsOrder, name),
        estL: estMin != null ? '~' + fmt(estMin) : '—',
        ring: (conf && conf.ring) || pickGroupRing(name, R, d),
        work: !!(conf && conf.hideWithOthers),
        // "mine" = a tracked breed runs in this group this day, so the card can
        // collapse the panel to just the groups you're actually in.
        mine: !!R.find(r => r.day === d && groupOf(r.breed) === norm(name))
      });
    });
    G[d] = { start: reg.start || (startMin != null ? fmt(startMin) : '—'), groups };
    groups.forEach(g => ringSet.add(g.ring));
  });

  // assign palette after all rings known
  const rings = [...ringSet].filter(x => x != null).sort((a, b) => a - b);
  const ringClass = {};
  rings.forEach((r, i) => ringClass[r] = 'rc' + (i % 8));

  // legend — one entry per tracked breed, listing the ring(s) it runs in across
  // the weekend (coloured), so you can eyeball "which ring am I in" at a glance.
  const legend = cfg.legend && cfg.legend.length ? cfg.legend : perBreedLegend(R, my, other);

  // warnings (auto-generated, plain, hand-editable)
  buildWarnings(inter, R, crunch, clashDefs, isOther, warnings);

  // note footer
  const note = cfg.note || defaultNote();

  const DATA = {
    show: {
      eyebrow: cfg.show && cfg.show.eyebrow || `${inter.show.club} · Ring Card`,
      title: cfg.show && cfg.show.title || inter.show.dates || '',
      sub: cfg.show && cfg.show.sub || [inter.show.venue, inter.show.super].filter(Boolean).join(' · '),
    },
    legend, ringClass, days: DAYS,
    R: R.map(stripInternal), G, warnings, crunch, note
  };

  const template = fs.readFileSync(opt.template, 'utf8');
  const injected = template.replace(
    /\/\*__DATA__\*\/[\s\S]*?\/\*__END_DATA__\*\//,
    '/*__DATA__*/ ' + JSON.stringify(DATA) + ' /*__END_DATA__*/'
  );
  if (injected === template) die('template has no /*__DATA__*/ … /*__END_DATA__*/ marker');
  fs.writeFileSync(opt.out, injected);
  console.error(`render: wrote ${opt.out}  (${R.length} rows across ${DAYS.length} days, rings ${rings.join(',')})`);
}

// ---------- support ----------
function stripInternal(r) { const { _win, ...rest } = r; return rest; }

function pairwiseOverlap(wins) {           // any two windows overlap?
  for (let i = 0; i < wins.length; i++)
    for (let j = i + 1; j < wins.length; j++)
      if (wins[i].start < wins[j].end && wins[j].start < wins[i].end) return true;
  return false;
}
function parseClock(s) {                    // "2:45p" | "2:45 pm" -> minutes
  if (!s) return null;
  const m = String(s).match(/(\d{1,2}):(\d{2})\s*([ap])/i);
  if (!m) return null;
  let h = +m[1] % 12; if (/p/i.test(m[3])) h += 12;
  return h * 60 + +m[2];
}
function isoToShort(iso) {                  // 2026-07-10 -> "Jul 10"
  const M = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const m = /(\d{4})-(\d{2})-(\d{2})/.exec(iso);
  return m ? `${M[+m[2] - 1]} ${+m[3]}` : iso;
}
function pickGroupRing(name, R, d) {        // colour a group like a tracked ring if we can
  const r = R.find(x => x.day === d && groupOf(x.breed) === norm(name));
  return r ? r.ring : null;
}
const GROUP_MAP = {                          // minimal breed->group for colour hinting only
  'finnish spitz': 'non-sporting', 'norwegian buhund': 'herding',
  'black russian terrier': 'working', 'belgian tervuren': 'herding',
  'belgian sheepdog': 'herding'
};
const groupOf = breed => GROUP_MAP[norm(breed).replace(/s$/, '')] || GROUP_MAP[norm(breed)] || '';

function perBreedLegend(R, myBreeds, otherBreeds) {
  const out = [];
  for (const b of [...myBreeds, ...otherBreeds]) {
    const rows = R.filter(r => breedMatches(r.breed, b));
    if (!rows.length) continue;
    const rings = [...new Set(rows.map(r => r.ring))].filter(x => x != null).sort((a, z) => a - z);
    out.push({ breed: b, rings });
  }
  return out;
}
function buildWarnings(inter, R, crunch, clashDefs, isOther, out) {
  const short = { Friday: 'Fri', Saturday: 'Sat', Sunday: 'Sun', Monday: 'Mon', Thursday: 'Thu' };
  // one banner per fired clash per day
  inter.days.forEach((day, d) => {
    for (const f of (day._fired || [])) {
      const c = f.c, dl = short[day.label] || day.label;
      const parts = f.rows.map(r =>
        `<span class="k">${r.breed}</span> (Ring ${r.ring}, ${r.eL}${+r.ent > 1 ? `, ${r.ent} deep` : ''})`);
      out.push({
        cls: c.lane, requiresOthers: c.requiresOthers,
        title: `Clash ${c.id} · ${dl} — ${c.label}`,
        body: `<p>${parts.join(' overlaps ')}. Same handler can't cover both — split them or hand one off.</p>`,
        hiddenAlt: c.requiresOthers ? {
          title: `${c.label} hidden`,
          body: `Involves an "other" dog — turn on "Other dogs" to track this overlap.`
        } : null
      });
    }
  });
  // day-by-day summary
  const lines = inter.days.map((day, d) => {
    const dl = short[day.label] || day.label;
    const fired = (day._fired || []).length;
    const txt = fired
      ? `${crunch[d] ? crunch[d].replace('Crunch ', 'pinch ') : 'overlap'} — ${fired} clash${fired > 1 ? 'es' : ''}. See above.`
      : `Spread clear. No two-ring overlap among tracked breeds.`;
    return `<div class="dayline"><span class="dl">${dl}</span><p>${txt}</p></div>`;
  }).join('');
  out.push({ cls: 'flat', title: 'Day by day', body: lines });
  // bottom line
  const anyClash = inter.days.some(day => (day._fired || []).length);
  out.push({
    cls: 'ok', title: 'Bottom line',
    body: anyClash
      ? `<p>Overlaps are coverable with a second handler on the crunch day(s). Other days run solo.</p>`
      : `<p>No overlaps among tracked breeds this weekend — every breed clears the next. Runs solo.</p>`
  });
}
function defaultNote() {
  return `<b>Entry key:</b> counts read <span class="mono">dogs-bitches-specials(d)-specials(b)</span>.<br>` +
    `<b>N ahead</b> = dogs in that ring before you that day. <b>after [breed]</b> = the breed right in front of you — your cue to get ringside. ` +
    `<b>Slot</b> = printed timeslot; <b>Est.</b> = when it likely goes, from slot + ahead × est min/dog.<br>` +
    `<b>Reg</b> = Regular group. <b>NOHS</b> trails the Regular group of the same type with its own judge. Group times are the softest estimates on the sheet.`;
}

main();
