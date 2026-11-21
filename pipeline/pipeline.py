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
import time

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))
VERBOSE = os.environ.get("VERBOSE", "") != ""

T0 = time.time()


def log(msg):
    if VERBOSE:
        print(f"[{time.time() - T0:6.2f}s] {msg}", flush=True)


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
# character, so any file containing '\r' is first copied to scratch with each
# '\r' replaced by a placeholder byte absent from that file, and the
# placeholder is turned back into '\r' before parsing.
RAW_DIR = os.path.join(SCRATCH, "raw_lines")
RAW_OPTS = ("columns={'line': 'VARCHAR'}, delim='\x01\x02\x03\x04', quote='', escape='', "
            "header=true, auto_detect=false, strict_mode=false, nullstr='\x02\x03\x04\x01'")
PLACEHOLDERS = [bytes([b]) for b in (0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x0e, 0x0f, 0x10)]


def _prepare(job):
    i, path = job
    with open(path, "rb") as fh:
        data = fh.read()
    if b"\r" not in data:
        return path, None
    ph = next(p for p in PLACEHOLDERS if p not in data)
    os.makedirs(RAW_DIR, exist_ok=True)
    dst = os.path.join(RAW_DIR, f"{i:06d}.csv")
    with open(dst, "wb") as fh:
        fh.write(data.replace(b"\r", ph))
    return dst, ph


_job_counter = [0]


def prepare(files):
    from concurrent.futures import ThreadPoolExecutor
    jobs = []
    for p in files:
        jobs.append((_job_counter[0], p))
        _job_counter[0] += 1
    with ThreadPoolExecutor(max_workers=max(1, NCPU)) as pool:
        return list(pool.map(_prepare, jobs))


def sql_list(paths):
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def load_table(name, files, cols):
    """Parse files into table `name` with VARCHAR columns `cols` (NULL for empty).

    Lines are split on '\n' only; the header (first line of each file) is
    skipped; lines with the wrong field count or a stray quote are dropped
    (empty lines too: the reader skips them)."""
    n = len(cols)
    sel = ", ".join(f"nullif(f[{i + 1}], '') AS {c}" for i, c in enumerate(cols))
    prepared = prepare(files)
    plain = [p for p, ph in prepared if ph is None]
    sources = []
    if plain:
        sources.append(f"SELECT line FROM read_csv({sql_list(plain)}, {RAW_OPTS})")
    for p, ph in prepared:
        if ph is not None:
            sources.append(f"SELECT replace(line, chr({ph[0]}), chr(13)) AS line "
                           f"FROM read_csv({sql_list([p])}, {RAW_OPTS})")
    if not sources:
        con.execute(f"CREATE TABLE {name} AS SELECT " +
                    ", ".join(f"NULL::VARCHAR AS {c}" for c in cols) + " WHERE false")
        return
    con.execute(f"""
        CREATE TABLE {name} AS
        SELECT {sel} FROM (
            SELECT string_split(replace(line, '"', ''), ',') AS f
            FROM ({" UNION ALL ".join(sources)})
            WHERE regexp_full_match(line, '{field_regex(n)}')
        )
    """)


# ---------------------------------------------------------------- inputs
load_table("products", [os.path.join(DATA, "products.csv")], ["product_id", "category"])
load_table("campaigns", [os.path.join(DATA, "campaigns.csv")], ["campaign_id", "channel"])
load_table("links_raw", csv_files("links"), ["a", "b"])
load_table("ev_raw", csv_files("events"),
           ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"])
load_table("ck_raw", csv_files("clicks"), ["click_id", "user_id", "campaign_id", "ts"])
log("loaded raw tables")

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
edges = con.execute("""
SELECT la.ix, lb.ix FROM lk JOIN lid la ON lk.a = la.id JOIN lid lb ON lk.b = lb.id
""").fetchall()

# Union-find; the root of each set is its smallest index = byte-wise smallest ID.
parent = list(range(n_ids))
for x, y in edges:
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    while parent[y] != y:
        parent[y] = parent[parent[y]]
        y = parent[y]
    if x < y:
        parent[y] = x
    elif y < x:
        parent[x] = y
for i in range(n_ids):
    parent[i] = parent[parent[i]]  # parents always have smaller index: one pass suffices
del edges
log(f"union-find over {n_ids} linked ids")

con.execute("CREATE TABLE lroot AS SELECT unnest(range(?::INTEGER)) AS ix, unnest(?::INTEGER[]) AS r",
            [n_ids, parent])
con.execute("""
CREATE TABLE pmap AS
SELECT l.id, n.id AS person FROM lid l JOIN lroot USING (ix) JOIN lid n ON n.ix = lroot.r
WHERE l.id <> n.id
""")
del parent

# ---------------------------------------------------------------- events
# Dedup: only the few event_ids seen more than once need the arg_min; the rest
# pass straight through (a full GROUP BY with a struct payload is ~4x slower).
con.execute("""
CREATE TABLE dup_ids AS
SELECT event_id FROM ev_raw WHERE event_id IS NOT NULL GROUP BY event_id HAVING count(*) > 1
""")
# Materialized so parse_ts runs once per row (the optimizer would otherwise
# copy the inlined macro into every filter that mentions ts).
con.execute(f"""
CREATE TABLE ev_typed AS
WITH dedup AS (
    SELECT user_id, ts, ingest_ts, event_type, product_id, price_cents
    FROM ev_raw ANTI JOIN dup_ids USING (event_id)
    WHERE event_id IS NOT NULL
    UNION ALL
    SELECT unnest(arg_min(struct_pack(user_id, ts, ingest_ts, event_type, product_id, price_cents),
                          epoch_ms(try_strptime(ingest_ts, '%Y-%m-%dT%H:%M:%SZ'))))
    FROM ev_raw SEMI JOIN dup_ids USING (event_id)
    GROUP BY event_id
), typed AS (
    SELECT user_id, parse_ts(ts) AS ts, epoch_ms(try_strptime(ingest_ts, '%Y-%m-%dT%H:%M:%SZ')) AS ing,
           event_type, product_id, price_cents AS price_s
    FROM dedup
)
SELECT * FROM typed
""")
con.execute(f"""
CREATE TABLE ev AS
WITH valid AS (
    SELECT user_id, ts, event_type, product_id,
           CASE WHEN event_type = 'purchase' THEN
               CASE WHEN regexp_full_match(price_s, '[0-9]+') AND length(ltrim(price_s, '0')) <= 8
                    THEN TRY_CAST(price_s AS BIGINT) END
           END AS price
    FROM ev_typed
    WHERE user_id IS NOT NULL AND ts >= {WS} AND ts < {WE}
      AND event_type IN ('view', 'cart', 'purchase') AND product_id IS NOT NULL
      AND ing - ts <= {DAY_MS}
)
SELECT coalesce(pm.person, v.user_id) AS person, v.ts, v.event_type = 'purchase' AS is_p,
       coalesce(v.price, 0) AS price, coalesce(p.category, 'unknown') AS category
FROM valid v
LEFT JOIN pmap pm ON pm.id = v.user_id
LEFT JOIN products p ON p.product_id = v.product_id
WHERE v.event_type <> 'purchase' OR v.price BETWEEN 1 AND 10000000
""")
log("events validated")

con.execute(f"""
CREATE TABLE evs AS
-- Both windows order by (ts, rowid): one total order, so equal timestamps get
-- the same treatment in each pass.  Only the first of a group of equal ts can
-- see a gap (the rest see 0), so ties always share a session.
SELECT person, is_p, price, category,
       max(CASE WHEN prev IS NULL OR ts - prev > {GAP_MS} THEN ts END)
           OVER (PARTITION BY person ORDER BY ts, rid ROWS UNBOUNDED PRECEDING) AS start
FROM (SELECT *, rowid AS rid, lag(ts) OVER (PARTITION BY person ORDER BY ts, rowid) AS prev FROM ev)
""")
con.execute(f"""
CREATE TABLE sess AS
SELECT person, start, (DATE '1970-01-01' + (start // {DAY_MS})::INTEGER) AS day,
       count(*) AS n_events, sum(price) AS revenue, bool_or(is_p) AS has_p
FROM evs GROUP BY person, start
""")
log("sessionized")

con.execute("""
CREATE TABLE user_daily AS
WITH base AS (
    SELECT person AS user_id, day, count(*) AS sessions, sum(n_events) AS events,
           sum(revenue) AS revenue_cents
    FROM sess GROUP BY ALL
), cat AS (
    SELECT e.person AS user_id, s.day, e.category, sum(e.price) AS rev
    FROM evs e JOIN sess s ON e.person = s.person AND e.start = s.start
    WHERE e.is_p GROUP BY ALL
), top AS (
    SELECT user_id, day, arg_min(category, (-rev, category)) AS top_category
    FROM cat GROUP BY ALL
)
SELECT b.user_id, b.day, b.sessions::BIGINT AS sessions, b.events::BIGINT AS events,
       b.revenue_cents::BIGINT AS revenue_cents, t.top_category
FROM base b LEFT JOIN top t USING (user_id, day)
""")
log("user_daily")

# ---------------------------------------------------------------- clicks
con.execute(f"""
CREATE TABLE ck AS
SELECT coalesce(pm.person, c.user_id) AS person, c.ts, c.click_id, c.campaign_id
FROM (
    SELECT click_id, user_id, campaign_id, parse_ts(ts) AS ts FROM ck_raw
    WHERE click_id IS NOT NULL AND user_id IS NOT NULL
      AND campaign_id IN (SELECT campaign_id FROM campaigns)
) c
LEFT JOIN pmap pm ON pm.id = c.user_id
WHERE c.ts >= {WS - WEEK_MS} AND c.ts < {WE}
""")

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
log("campaign_daily")

# ---------------------------------------------------------------- outputs
for table in ("user_daily", "campaign_daily"):
    d = os.path.join(OUT, table)
    if os.path.isdir(d):
        shutil.rmtree(d)
    os.makedirs(d, exist_ok=True)
    con.execute(f"COPY {table} TO '{os.path.join(d, 'part-0.parquet')}' (FORMAT parquet)")
log("written")
