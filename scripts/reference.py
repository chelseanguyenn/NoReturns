#!/usr/bin/env python3
"""
Slow, simple reference implementation of rules.md, in plain Python.

Every step follows the rules text literally; nothing is clever.  Its only
purpose is to cross-check pipeline/pipeline.py on generated datasets.

Usage: DATA_DIR=... OUTPUT_DIR=... python3 scripts/reference.py
   or: python3 scripts/reference.py DATA_DIR OUTPUT_DIR
Writes OUTPUT_DIR/user_daily/part-0.parquet and OUTPUT_DIR/campaign_daily/part-0.parquet.
"""
import datetime as dt
import glob
import json
import os
import re
import shutil
import sys

import polars as pl

if len(sys.argv) >= 3:
    DATA, OUT = sys.argv[-2], sys.argv[-1]
else:
    DATA = os.environ.get("DATA_DIR", "/data")
    OUT = os.environ.get("OUTPUT_DIR", "/output")

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
GAP_MS = 1_800_000

# ---------------------------------------------------------------- CSV dialect


def parse_line(line, n_fields):
    """Return the list of values (None for empty) or None if the line is malformed."""
    if line == "":
        return None  # an empty line is malformed
    raw = line.split(",")  # every comma separates fields
    if len(raw) != n_fields:
        return None
    values = []
    for f in raw:
        if '"' in f:
            # A quote is allowed only as a pair wrapping the whole field.
            if len(f) < 2 or f[0] != '"' or f[-1] != '"' or '"' in f[1:-1]:
                return None
            f = f[1:-1]
        values.append(f if f != "" else None)
    return values


def read_csv_file(path, n_fields):
    with open(path, "rb") as fh:
        text = fh.read().decode("utf-8", errors="surrogateescape")
    lines = text.split("\n")  # only \n ends a line; \r is an ordinary character
    if text.endswith("\n"):
        lines.pop()  # the final \n terminates the last line, it does not start a new one
    rows = []
    for line in lines[1:]:  # skip the header
        v = parse_line(line, n_fields)
        if v is not None:
            rows.append(v)
    return rows


def read_dir(sub, n_fields):
    rows = []
    for path in sorted(glob.glob(os.path.join(DATA, sub, "*.csv"))):
        if os.path.isfile(path):
            rows.extend(read_csv_file(path, n_fields))
    return rows


# ---------------------------------------------------------------- timestamps

D = "[0-9]"  # ASCII digits only (\d would accept other scripts' digits)
F1 = re.compile(rf"({D}{{4}})-({D}{{2}})-({D}{{2}})T({D}{{2}}):({D}{{2}}):({D}{{2}})Z")
F2 = re.compile(rf"({D}{{4}})-({D}{{2}})-({D}{{2}})T({D}{{2}}):({D}{{2}}):({D}{{2}})([+-])({D}{{2}}):({D}{{2}})")
F3 = re.compile(rf"{D}+")
EPOCH_ORDINAL = dt.date(1970, 1, 1).toordinal()


def civil_ms(y, mo, d, h, mi, s):
    """Milliseconds since the epoch for a UTC wall time, or None if impossible."""
    if not (1 <= mo <= 12):
        return None
    try:
        days = dt.date(y, mo, d).toordinal() - EPOCH_ORDINAL  # rejects Feb 30, Feb 29 off-leap, ...
    except ValueError:
        return None
    if not (h <= 23 and mi <= 59 and s <= 59):
        return None
    return ((days * 24 + h) * 60 + mi) * 60_000 + s * 1000


def parse_ts(t):
    if t is None:
        return None
    m = F1.fullmatch(t)
    if m:
        return civil_ms(*map(int, m.groups()))
    m = F2.fullmatch(t)
    if m:
        y, mo, d, h, mi, s = map(int, m.groups()[:6])
        sign, oh, om = m.group(7), int(m.group(8)), int(m.group(9))
        if oh > 23 or om > 59:
            return None
        local = civil_ms(y, mo, d, h, mi, s)
        if local is None:
            return None
        off = (oh * 60 + om) * 60_000
        return local - off if sign == "+" else local + off
    if F3.fullmatch(t):
        return int(t)  # any length; out-of-window values are filtered by the caller
    return None


with open(os.path.join(DATA, "manifest.json")) as fh:
    manifest = json.load(fh)
WS = parse_ts(manifest["window_start"])
WE = parse_ts(manifest["window_end"])

# ---------------------------------------------------------------- lookups

products = {}
for pid, cat in read_csv_file(os.path.join(DATA, "products.csv"), 2):
    products[pid] = cat
campaigns = {}
for cid, ch in read_csv_file(os.path.join(DATA, "campaigns.csv"), 2):
    campaigns[cid] = ch

# ---------------------------------------------------------------- linked IDs

parent = {}


def find(x):
    root = x
    while parent.get(root, root) != root:
        root = parent[root]
    while parent.get(x, x) != root:  # path compression
        nxt = parent[x]
        parent[x] = root
        x = nxt
    return root


for a, b in read_dir("links", 2):
    if a is None or b is None:
        continue
    ra, rb = find(a), find(b)
    if ra != rb:
        # Keep the byte-wise smallest ID as the root, so the root is the name.
        # (Python compares str by code point, which is UTF-8 byte order.)
        if ra < rb:
            parent[rb] = ra
        else:
            parent[ra] = rb


def person(uid):
    return find(uid)  # an unlinked ID is its own root


# ---------------------------------------------------------------- events

kept = {}  # event_id -> (ingest_ms, row)
for row in read_dir("events", 7):
    event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents = row
    if event_id is None:
        continue
    ing = parse_ts(ingest_ts)  # always valid F1
    if event_id not in kept or ing < kept[event_id][0]:
        kept[event_id] = (ing, row)

events_by_person = {}
for ing, row in kept.values():
    event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents = row
    if user_id is None:
        continue
    t = parse_ts(ts)
    if t is None or not (WS <= t < WE):
        continue
    if event_type not in ("view", "cart", "purchase"):
        continue
    if product_id is None:
        continue
    price = 0
    if event_type == "purchase":
        if price_cents is None or not F3.fullmatch(price_cents):
            continue
        price = int(price_cents)
        if not (1 <= price <= 10_000_000):
            continue
    if ing - t > DAY_MS:
        continue
    events_by_person.setdefault(person(user_id), []).append(
        (t, event_type == "purchase", price, products.get(product_id, "unknown")))

# Sessionize: sort by ts; new session when the gap from the previous event is
# over 30 min.  Equal ts have gap 0, so ties always share a session.
sessions = []  # dicts: person, start, day, events, revenue, has_purchase, cat_rev
for p, evs in events_by_person.items():
    evs.sort(key=lambda e: e[0])
    cur = None
    prev_t = None
    for t, is_p, price, cat in evs:
        if cur is None or t - prev_t > GAP_MS:
            cur = {"person": p, "start": t, "events": 0, "revenue": 0, "has_purchase": False, "cat_rev": {}}
            sessions.append(cur)
        cur["events"] += 1
        if is_p:
            cur["revenue"] += price
            cur["has_purchase"] = True
            cur["cat_rev"][cat] = cur["cat_rev"].get(cat, 0) + price
        prev_t = t
for s in sessions:
    s["day"] = dt.date(1970, 1, 1) + dt.timedelta(days=s["start"] // DAY_MS)

# ---------------------------------------------------------------- clicks

clicks = {}  # click_id -> row; copies are identical, keep any
for row in read_dir("clicks", 4):
    if row[0] is None:
        continue
    clicks.setdefault(row[0], row)

clicks_by_person = {}
for click_id, user_id, campaign_id, ts in clicks.values():
    if user_id is None:
        continue
    t = parse_ts(ts)
    if t is None or not (WS - WEEK_MS <= t < WE):
        continue
    if campaign_id not in campaigns:
        continue
    clicks_by_person.setdefault(person(user_id), []).append((t, click_id, campaign_id))

for s in sessions:
    start = s["start"]
    best = None
    assists = set()
    for t, click_id, campaign_id in clicks_by_person.get(s["person"], []):
        if t <= start and start - t <= WEEK_MS:
            assists.add(campaign_id)
            if best is None or t > best[0] or (t == best[0] and click_id < best[1]):
                best = (t, click_id, campaign_id)
    s["credit"] = best[2] if best else "organic"
    s["assists"] = assists

# ---------------------------------------------------------------- outputs

ud = {}
for s in sessions:
    r = ud.setdefault((s["person"], s["day"]), {"sessions": 0, "events": 0, "revenue": 0, "cat_rev": {}})
    r["sessions"] += 1
    r["events"] += s["events"]
    r["revenue"] += s["revenue"]
    for c, v in s["cat_rev"].items():
        r["cat_rev"][c] = r["cat_rev"].get(c, 0) + v
ud_rows = []
for (p, day), r in ud.items():
    top = None
    for c, v in r["cat_rev"].items():
        if top is None or v > r["cat_rev"][top] or (v == r["cat_rev"][top] and c < top):
            top = c
    ud_rows.append((p, day, r["sessions"], r["events"], r["revenue"], top))

cd = {}


def cd_row(cid, day):
    return cd.setdefault((cid, day), {"sessions": 0, "purchasing": 0, "revenue": 0, "assisted": 0})


for s in sessions:
    r = cd_row(s["credit"], s["day"])
    r["sessions"] += 1
    r["purchasing"] += 1 if s["has_purchase"] else 0
    r["revenue"] += s["revenue"]
    for cid in s["assists"]:
        cd_row(cid, s["day"])["assisted"] += 1
cd_rows = [(cid, "organic" if cid == "organic" else campaigns[cid], day,
            r["sessions"], r["purchasing"], r["revenue"], r["assisted"]) for (cid, day), r in cd.items()]

for name, cols, data in (
    ("user_daily", {"user_id": pl.String, "day": pl.Date, "sessions": pl.Int64, "events": pl.Int64,
                    "revenue_cents": pl.Int64, "top_category": pl.String}, ud_rows),
    ("campaign_daily", {"campaign_id": pl.String, "channel": pl.String, "day": pl.Date, "sessions": pl.Int64,
                        "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64,
                        "assisted_sessions": pl.Int64}, cd_rows),
):
    d = os.path.join(OUT, name)
    if os.path.isdir(d):
        shutil.rmtree(d)
    os.makedirs(d)
    pl.DataFrame(data, schema=cols, orient="row").write_parquet(os.path.join(d, "part-0.parquet"))
