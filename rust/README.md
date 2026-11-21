# Rust pipeline

A single binary implementing `../rules.md`; output is identical to `pipeline/pipeline.py` (DuckDB).

- Parse: every CSV is mmapped, cut into ~2 MB line-aligned chunks and parsed by hand on `NCPU` threads (default 8). No CSV library.
- IDs: each thread interns user IDs locally; one parallel sort turns them into global byte-wise ranks, so the smallest rank in a linked set is its name.
- Links: array-based union-find with the smallest rank as root.
- Events: dedup by hash-sharded sort on `event_id` (exact byte compare), keep the smallest `ingest_ts` copy if valid.
- Sessions: events sorted by packed `(person, ts)`; one scan builds sessions and user-days; credit is a binary search over the person's clicks sorted by `(ts, click_id)`; assists come from merged per-campaign click intervals.
- Output: Parquet via the `parquet` crate (low-level column writers).

Build and check (from the repo root; the top-level `Dockerfile` builds this crate with the root as context):

    make fixtures IMAGE=shopping-sessions-rust:latest
    make run-dev  IMAGE=shopping-sessions-rust:latest && make check-dev

Native: `cd rust && cargo build --release && target/release/sessions DATA_DIR OUTPUT_DIR`. Set `VERBOSE=1` for ID counts; stage timings always go to stderr.
