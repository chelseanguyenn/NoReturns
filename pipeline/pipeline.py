#!/usr/bin/env python3
"""
Shopping Sessions pipeline (DuckDB), implementing rules.md.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)

The CSV dialect is handled by hand: files are read as raw text, split on '\n'
only, and each line is checked against a strict regex before its fields are
unquoted, so DuckDB's CSV sniffer/repair logic never touches the data.
"""
import glob
import json
import os
import shutil
import sys
import time

import duckdb
import numpy as np

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))
VERBOSE = os.environ.get("VERBOSE", "") != ""

T0 = time.time()


def log(msg):
    if VERBOSE:
        print(f"[{time.time() - T0:6.2f}s] {msg}", file=sys.stderr, flush=True)


_last = [T0]


def stage(name):
    """Always print the time spent since the previous stage to stderr."""
    now = time.time()
    print(f"[timing] {name:<26} {now - _last[0]:7.2f}s   total {now - T0:7.2f}s", file=sys.stderr, flush=True)
    _last[0] = now


try:
    os.makedirs(SCRATCH, exist_ok=True)
except OSError:  # local runs without /scratch
    import tempfile
    SCRATCH = tempfile.gettempdir()
con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
GAP_MS = 1_800_000


# ---------------------------------------------------------------- timestamps
def parse_iso(s):
    """Python version of the F1 parser, for manifest values."""
    import datetime as dt
    return int(dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp() * 1000)


with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
WS = parse_iso(manifest["window_start"])
WE = parse_iso(manifest["window_end"])
log(f"window {manifest['window_start']} .. {manifest['window_end']}")

# F1/F2: the regex pins the exact shape (ASCII digits, 'T', 'Z' or a +HH:MM /
# -HH:MM offset); try_strptime then rejects impossible calendar values (month
# 13, Feb 30, Feb 29 outside leap years, hour 24, minute/second 60).
# F3: ASCII digits only; over 19 significant digits, or over the BIGINT range,
# is far outside any window, so NULL (bad) is the right answer.
con.execute(r"""
CREATE MACRO parse_ts(t) AS CASE
    WHEN substr(t, 5, 1) = '-' AND regexp_full_match(t, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})') THEN
        CASE WHEN length(t) = 20 THEN epoch_ms(try_strptime(substr(t, 1, 19), '%Y-%m-%dT%H:%M:%S'))
             WHEN substr(t, 21, 2) < '24' AND substr(t, 24, 2) < '60' THEN
                 epoch_ms(try_strptime(substr(t, 1, 19), '%Y-%m-%dT%H:%M:%S'))
                 - (CASE WHEN substr(t, 20, 1) = '+' THEN 1 ELSE -1 END)
                   * (TRY_CAST(substr(t, 21, 2) AS BIGINT) * 3600000 + TRY_CAST(substr(t, 24, 2) AS BIGINT) * 60000)
        END
    WHEN regexp_full_match(t, '[0-9]+') THEN
        CASE WHEN length(ltrim(t, '0')) <= 19 THEN TRY_CAST(t AS BIGINT) END
    END
""")


# ---------------------------------------------------------------- raw lines
def csv_files(sub):
    return sorted(p for p in glob.glob(os.path.join(DATA, sub, "*.csv")) if os.path.isfile(p))


def field_regex(n):
    f = '(?:"[^",]*"|[^",]*)'
    return f + "(?:," + f + "){" + str(n - 1) + "}"


# DuckDB's CSV reader is used only as a line splitter: one VARCHAR column, a
# delimiter, quote, escape and null string that never occur in the data.  It
# would still end a line at a lone '\r', which the rules treat as an ordinary
# character, so a file containing '\r' is copied to scratch with each '\r'
# replaced by a placeholder byte absent from that file, and the placeholder is
# turned back into '\r' before parsing.
#
# Finding those files needs a pass over every byte.  It runs in background
# threads (DuckDB releases the GIL while it works), started for every input
# file up front, so it overlaps the parsing instead of preceding it.  A table
# is parsed optimistically as if no file held '\r'; if one did, the table is
# dropped and parsed again with the placeholder copies.
RAW_DIR = os.path.join(SCRATCH, "raw_lines")
RAW_OPTS = ("columns={'line': 'VARCHAR'}, delim='\x01\x02\x03\x04', quote='', escape='', "
            "header=true, auto_detect=false, strict_mode=false, nullstr='\x02\x03\x04\x01'")
PLACEHOLDERS = [bytes([b]) for b in (0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x0e, 0x0f, 0x10)]


def has_cr(path):
    # 8 MB chunks: bounded memory (a big part file is ~360 MB).  (mmap was
    # slower through Docker's bind mounts.)
    with open(path, "rb", buffering=0) as fh:
        while True:
            chunk = fh.read(8 << 20)
            if not chunk:
                return False
            if b"\r" in chunk:
                return True


def cr_copy(i, path):
    with open(path, "rb") as fh:
        data = fh.read()
    ph = next(p for p in PLACEHOLDERS if p not in data)
    os.makedirs(RAW_DIR, exist_ok=True)
    dst = os.path.join(RAW_DIR, f"{i:06d}.csv")
    with open(dst, "wb") as fh:
        fh.write(data.replace(b"\r", ph))
    return dst, ph


def sql_list(paths):
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def load_table(name, files, cols, select=None, where="true"):
    """Parse files into table `name`.

    Lines are split on '\n' only; the header (first line of each file) is
    skipped; lines with the wrong field count or a stray quote are dropped
    (empty lines too: the reader skips them).  Each field becomes a VARCHAR
    named by `cols` (NULL for empty); `select` and `where`, if given, are
    applied to those fields in the same pass, so typed columns are computed
    once while the line is in hand instead of in a second pass over a table."""
    n = len(cols)
    sel = ", ".join(f"nullif(f[{i + 1}], '') AS {c}" for i, c in enumerate(cols))
    select = select or ", ".join(cols)

    def build(cr_files):
        sources = []
        plain = [p for p in files if p not in cr_files]
        if plain:
            sources.append(f"SELECT line FROM read_csv({sql_list(plain)}, {RAW_OPTS})")
        for i, p in enumerate(cr_files):
            dst, ph = cr_copy(i, p)
            sources.append(f"SELECT replace(line, chr({ph[0]}), chr(13)) AS line "
                           f"FROM read_csv({sql_list([dst])}, {RAW_OPTS})")
        if not sources:
            sources.append("SELECT NULL::VARCHAR AS line WHERE false")
        con.execute(f"""
            CREATE OR REPLACE TABLE {name} AS
            SELECT {select} FROM (
                SELECT {sel} FROM (
                    SELECT string_split(replace(line, '"', ''), ',') AS f
                    FROM ({" UNION ALL ".join(sources)})
                    WHERE regexp_full_match(line, '{field_regex(n)}')
                )
            ) WHERE {where}
        """)

    build([])
    cr_files = [p for p in files if CR_SCAN[p].result()]
    if cr_files:
        log(f"  {name}: {len(cr_files)} file(s) contain \\r, parsing again")
        build(cr_files)
        shutil.rmtree(RAW_DIR, ignore_errors=True)


# ---------------------------------------------------------------- inputs
PRODUCTS, CAMPAIGNS = os.path.join(DATA, "products.csv"), os.path.join(DATA, "campaigns.csv")
LINK_FILES, EVENT_FILES, CLICK_FILES = csv_files("links"), csv_files("events"), csv_files("clicks")
# Queued in the order the tables are loaded, so each table's scan is done (or
# nearly) by the time its parse finishes.  Few threads: it is I/O + memchr.
from concurrent.futures import ThreadPoolExecutor
_scan_pool = ThreadPoolExecutor(max_workers=3)
CR_SCAN = {p: _scan_pool.submit(has_cr, p)
           for p in [PRODUCTS, CAMPAIGNS] + LINK_FILES + EVENT_FILES + CLICK_FILES}
stage("setup")
load_table("products", [PRODUCTS], ["product_id", "category"])
load_table("campaigns", [CAMPAIGNS], ["campaign_id", "channel"])
load_table("links_raw", LINK_FILES, ["a", "b"])
stage("read/parse links+lookups")
# Events, typed in the parsing pass.  Every copy is typed, but nothing is
# filtered except an empty event_id: validation applies to the copy dedup
# keeps.  t: 0 = bad type or empty product_id, 1 = view/cart, 2 = purchase.
# prod is kept only for purchases (the only rows that need a category).
# price is NULL unless the row is a purchase with digits-only price_cents of
# at most 8 significant digits (anything longer is over 10,000,000 anyway).
load_table("ev_raw", EVENT_FILES,
           ["event_id", "user_id", "ts_s", "ingest_s", "etype", "product_id", "price_s"],
           select=f"""event_id, user_id, parse_ts(ts_s) AS ts,
                      epoch_ms(try_strptime(ingest_s, '%Y-%m-%dT%H:%M:%SZ')) AS ing,
                      (CASE WHEN product_id IS NULL THEN 0
                            WHEN etype IN ('view', 'cart') THEN 1
                            WHEN etype = 'purchase' THEN 2 ELSE 0 END)::UTINYINT AS t,
                      CASE WHEN etype = 'purchase' THEN product_id END AS prod,
                      CASE WHEN etype = 'purchase' AND regexp_full_match(price_s, '[0-9]+')
                                AND length(ltrim(price_s, '0')) <= 8
                           THEN TRY_CAST(price_s AS BIGINT) END AS price""",
           where="event_id IS NOT NULL")
stage("read/parse events")
# Clicks are validated in the parsing pass too (no dedup can pick a bad copy:
# copies are identical).  Duplicates are left in: the credit arg_min and the
# assist DISTINCT both give the same answer with or without them.
load_table("ck_raw", CLICK_FILES, ["click_id", "user_id", "campaign_id", "ts_s"],
           select="click_id, user_id, campaign_id, parse_ts(ts_s) AS ts",
           where="click_id IS NOT NULL AND user_id IS NOT NULL "
                 "AND campaign_id IN (SELECT campaign_id FROM campaigns)")
stage("read/parse clicks")

# ---------------------------------------------------------------- linked IDs
con.execute("""
CREATE TABLE lk AS SELECT DISTINCT a, b FROM links_raw WHERE a IS NOT NULL AND b IS NOT NULL AND a <> b
""")
con.execute("""
CREATE TABLE lid AS
SELECT id, (row_number() OVER (ORDER BY id) - 1)::INTEGER AS ix
FROM (SELECT a AS id FROM lk UNION SELECT b FROM lk)
""")
n_ids = con.execute("SELECT count(*) FROM lid").fetchone()[0]
log(f"lk/lid built: {n_ids} ids")
e = con.execute("""
SELECT la.ix AS u, lb.ix AS v FROM lk JOIN lid la ON lk.a = la.id JOIN lid lb ON lk.b = lb.id
""").fetchnumpy()
U = np.asarray(e["u"], dtype=np.int64)
V = np.asarray(e["v"], dtype=np.int64)
del e
log(f"fetched {len(U)} edges")

# Connected components, vectorized: hook + pointer jumping (Shiloach-Vishkin
# style).  Invariants: L[x] <= x and L[x] is in x's component.  Each round
# first jumps L to a fixpoint (every L[x] is a root, L[r] == r), then hooks
# every root touched by an edge onto the smaller of the edge's two roots.
# At the fixpoint every edge has equal labels, so each component carries one
# label; its smallest member m has L[m] == m, so that label is m: the
# byte-wise smallest ID, since ix follows ORDER BY id.  Long chains converge
# in O(log length) rounds because both steps move labels many links at once.
L = np.arange(n_ids, dtype=np.int64)
rounds = 0
while True:
    while True:
        L2 = L[L]
        if np.array_equal(L2, L):
            break
        L = L2
    lu, lv = L[U], L[V]
    diff = lu != lv
    if not diff.any():
        break
    lu, lv = lu[diff], lv[diff]
    m = np.minimum(lu, lv)
    np.minimum.at(L, lu, m)
    np.minimum.at(L, lv, m)
    rounds += 1
del U, V
log(f"components over {n_ids} linked ids in {rounds} rounds")

nonroot = np.nonzero(L != np.arange(n_ids, dtype=np.int64))[0]
lroot_np = {"ix": nonroot.astype(np.int32), "r": L[nonroot].astype(np.int32)}
con.register("lroot_np", lroot_np)
con.execute("CREATE TABLE lroot AS SELECT ix, r FROM lroot_np")
con.unregister("lroot_np")
del L, nonroot, lroot_np
con.execute("""
CREATE TABLE pmap AS
SELECT l.id, n.id AS person FROM lid l JOIN lroot USING (ix) JOIN lid n ON n.ix = lroot.r
""")
# Drop each intermediate table once nothing later reads it, so DuckDB frees
# its memory instead of spilling it to scratch.
con.execute("DROP TABLE links_raw; DROP TABLE lk; DROP TABLE lid; DROP TABLE lroot")
stage("links (components)")

# ---------------------------------------------------------------- events
# Dedup: only the few event_ids seen more than once need the arg_min; the rest
# pass straight through (a full GROUP BY with a struct payload is ~4x slower).
con.execute("""
CREATE TABLE dup_ids AS
SELECT event_id FROM ev_raw GROUP BY event_id HAVING count(*) > 1
""")
log("  dup_ids")
# ev: valid kept copies.  price is 0 for view/cart; prod IS NOT NULL marks a purchase.
con.execute(f"""
CREATE TABLE ev AS
WITH dedup AS (
    SELECT user_id, ts, ing, t, prod, price
    FROM ev_raw ANTI JOIN dup_ids USING (event_id)
    UNION ALL
    SELECT unnest(arg_min(struct_pack(user_id, ts, ing, t, prod, price), ing))
    FROM ev_raw SEMI JOIN dup_ids USING (event_id)
    GROUP BY event_id
)
SELECT coalesce(pm.person, d.user_id) AS person, d.ts,
       CASE WHEN d.t = 2 THEN d.price ELSE 0 END AS price, d.prod
FROM dedup d
LEFT JOIN pmap pm ON pm.id = d.user_id
WHERE d.user_id IS NOT NULL AND d.ts >= {WS} AND d.ts < {WE} AND d.t > 0
  AND (d.t = 1 OR d.price BETWEEN 1 AND 10000000)
  AND d.ing - d.ts <= {DAY_MS}
""")
stage("dedup/validate events")
con.execute("DROP TABLE ev_raw; DROP TABLE dup_ids")

# ---------------------------------------------------------------- clicks
con.execute(f"""
CREATE TABLE ck AS
SELECT coalesce(pm.person, c.user_id) AS person, c.ts, c.click_id, c.campaign_id
FROM ck_raw c
LEFT JOIN pmap pm ON pm.id = c.user_id
WHERE c.ts >= {WS - WEEK_MS} AND c.ts < {WE}
""")

stage("validate clicks")
con.execute("DROP TABLE ck_raw")

# Sessionize with one sort.  Ordered by (ts, rowid), only the first event of a
# group of equal ts can see a gap over 30 min (the rest see 0), so ties always
# share a session.  st holds each session's first event: its ts (the start),
# its row number rn and the person's event count; a session's events are the
# rows from its rn to the next start's rn.  Only purchases (~3%) need to be
# placed in their session explicitly: the latest start <= their ts.
con.execute(f"""
CREATE TABLE st AS
SELECT person, ts AS start, rn, cnt FROM (
    SELECT person, ts, lag(ts) OVER w AS prev, row_number() OVER w AS rn,
           count(*) OVER (PARTITION BY person) AS cnt
    FROM ev WINDOW w AS (PARTITION BY person ORDER BY ts, rowid)
) WHERE prev IS NULL OR ts - prev > {GAP_MS}
""")
log("  st (session starts)")
con.execute("""
CREATE TABLE pp AS
SELECT s.person, s.start, e.price, e.prod
FROM (SELECT person, ts, price, prod FROM ev WHERE prod IS NOT NULL) e
ASOF JOIN st s ON e.person = s.person AND e.ts >= s.start
""")
log("  pp (purchases in sessions)")
con.execute(f"""
CREATE TABLE sess AS
WITH n AS (
    SELECT person, start,
           coalesce(lead(rn) OVER (PARTITION BY person ORDER BY start), cnt + 1) - rn AS n_events
    FROM st
), r AS (
    SELECT person, start, sum(price) AS revenue FROM pp GROUP BY person, start
)
SELECT n.person, n.start, (DATE '1970-01-01' + (n.start // {DAY_MS})::INTEGER) AS day, n.n_events,
       coalesce(r.revenue, 0) AS revenue, r.revenue IS NOT NULL AS has_p
FROM n LEFT JOIN r USING (person, start)
""")
log("  sess")
stage("sessionize")
con.execute("DROP TABLE ev; DROP TABLE st")

con.execute(f"""
CREATE TABLE user_daily AS
WITH base AS (
    SELECT person AS user_id, day, count(*) AS sessions, sum(n_events) AS events,
           sum(revenue) AS revenue_cents
    FROM sess GROUP BY ALL
), cat AS (
    SELECT pp.person AS user_id, (DATE '1970-01-01' + (pp.start // {DAY_MS})::INTEGER) AS day,
           coalesce(p.category, 'unknown') AS category, sum(pp.price) AS rev
    FROM pp LEFT JOIN products p ON p.product_id = pp.prod
    GROUP BY ALL
), top AS (
    SELECT user_id, day, arg_min(category, (-rev, category)) AS top_category
    FROM cat GROUP BY ALL
)
SELECT b.user_id, b.day, b.sessions::BIGINT AS sessions, b.events::BIGINT AS events,
       b.revenue_cents::BIGINT AS revenue_cents, t.top_category
FROM base b LEFT JOIN top t USING (user_id, day)
""")
stage("aggregate user_daily")
con.execute("DROP TABLE pp")


# Credit: latest click at or before the start (ties: smallest click_id), within 7 days.
con.execute(f"""
CREATE TABLE credit AS
WITH best AS (
    SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id
    FROM ck GROUP BY person, ts
)
SELECT s.person, s.start, s.day, s.revenue, s.has_p,
       CASE WHEN b.ts IS NOT NULL AND s.start - b.ts <= {WEEK_MS} THEN b.campaign_id ELSE 'organic' END
           AS campaign_id
FROM sess s ASOF LEFT JOIN best b ON s.person = b.person AND s.start >= b.ts
""")
stage("attribution (credit)")

# Assists: per (person, campaign), merge click times into intervals [a, b + 7d];
# a session is assisted iff its start lies in one of them.  Locate the covered
# run of the person's sessions (by index) and count it per session day.
con.execute(f"""
CREATE TABLE assist AS
WITH cts AS (
    SELECT DISTINCT person, campaign_id, ts FROM ck
), grp AS (
    SELECT person, campaign_id, ts,
           sum(CASE WHEN prev IS NULL OR ts - prev > {WEEK_MS} THEN 1 ELSE 0 END)
               OVER (PARTITION BY person, campaign_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS g
    FROM (SELECT *, lag(ts) OVER (PARTITION BY person, campaign_id ORDER BY ts) AS prev FROM cts)
), iv AS (
    SELECT person, campaign_id, min(ts) AS a, max(ts) + {WEEK_MS} AS b FROM grp GROUP BY person, campaign_id, g
), sidx AS (
    SELECT person, start, day, row_number() OVER (PARTITION BY person ORDER BY start) AS k FROM sess
), runs AS (
    SELECT person, day, min(k) AS dlo, max(k) AS dhi FROM sidx GROUP BY person, day
), lo AS (
    SELECT iv.person, iv.campaign_id, iv.b, s.k AS lo
    FROM iv ASOF JOIN sidx s ON iv.person = s.person AND iv.a <= s.start
), rng AS (
    SELECT lo.person, lo.campaign_id, lo.lo, s.k AS hi
    FROM lo ASOF JOIN sidx s ON lo.person = s.person AND lo.b >= s.start
)
SELECT rng.campaign_id, r.day,
       sum(least(rng.hi, r.dhi) - greatest(rng.lo, r.dlo) + 1) AS assisted
FROM rng JOIN runs r ON rng.person = r.person AND r.dlo <= rng.hi AND r.dhi >= rng.lo
WHERE rng.lo <= rng.hi
GROUP BY ALL
""")
stage("assists")

con.execute("""
CREATE TABLE campaign_daily AS
WITH cr AS (
    SELECT campaign_id, day, count(*) AS sessions, count(*) FILTER (WHERE has_p) AS purchasing_sessions,
           sum(revenue) AS revenue_cents
    FROM credit GROUP BY ALL
)
SELECT campaign_id,
       CASE WHEN campaign_id = 'organic' THEN 'organic' ELSE c.channel END AS channel,
       day,
       coalesce(cr.sessions, 0)::BIGINT AS sessions,
       coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
       coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
       coalesce(a.assisted, 0)::BIGINT AS assisted_sessions
FROM cr FULL OUTER JOIN assist a USING (campaign_id, day)
LEFT JOIN campaigns c USING (campaign_id)
""")
stage("aggregate campaign_daily")

# ---------------------------------------------------------------- outputs
for table in ("user_daily", "campaign_daily"):
    d = os.path.join(OUT, table)
    if os.path.isdir(d):
        shutil.rmtree(d)
    os.makedirs(d, exist_ok=True)
    con.execute(f"COPY {table} TO '{os.path.join(d, 'part-0.parquet')}' (FORMAT parquet)")
stage("write parquet")
