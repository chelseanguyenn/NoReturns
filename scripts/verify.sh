#!/bin/sh
# Correctness gate for the Rust pipeline. Prints PASS and exits 0 only if
# every check matches exactly; otherwise prints FAIL lines and exits 1.
#
#  1. all fixtures against the native binary (rust/target/release/sessions)
#  2. check-dev: Rust output on $DEV vs $ANSWER
#  3. seeds 2-13, 15 and 62 (200k events, 50k clicks; generated into $DATA_ROOT/sN when
#     missing) and $DATA_ROOT/big: Rust vs the DuckDB pipeline from `main`
#
# DuckDB outputs are cached in $CACHE/<blob of main:pipeline/pipeline.py>/,
# so each dataset is computed once per version of the reference pipeline.
#
# Env: DATA_ROOT (default ./data), DEV (./dev), ANSWER (./dev-answer),
#      REF (git ref holding the DuckDB pipeline, default main),
#      CACHE (default $DATA_ROOT/.duckdb-cache), SEEDS (default 2..13 15 62).
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DATA_ROOT=${DATA_ROOT:-$ROOT/data}
DEV=${DEV:-$ROOT/dev}
ANSWER=${ANSWER:-$ROOT/dev-answer}
REF=${REF:-main}
CACHE=${CACHE:-$DATA_ROOT/.duckdb-cache}
# Seeds 2-13 never reach Feb 29, 2028; 15 and 62 put it inside the event window.
SEEDS=${SEEDS:-"2 3 4 5 6 7 8 9 10 11 12 13 15 62"}
BIN=$ROOT/rust/target/release/sessions
FAILED=0
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT INT TERM
cd "$ROOT" || exit 1

ok()   { echo "ok    $*"; }
fail() { echo "FAIL  $*"; FAILED=1; }

# ---- build
if ! (cd rust && cargo build --release --quiet); then
  echo "FAIL  cargo build"; echo FAIL; exit 1
fi

# ---- 1. fixtures
if python3 scripts/run_fixtures.py fixtures -- "$BIN" > "$TMP/fixtures.log" 2>&1; then
  ok "fixtures: $(tail -1 "$TMP/fixtures.log")"
else
  grep -v '^ok' "$TMP/fixtures.log" | tail -20
  fail "fixtures: $(tail -1 "$TMP/fixtures.log")"
fi

# compare_dirs NAME RUST_OUT EXPECTED
compare_dirs() {
  if python3 scripts/compare.py "$2" "$3" > "$TMP/cmp.log" 2>&1; then
    ok "$1"
  else
    cat "$TMP/cmp.log"; fail "$1"
  fi
}

# run_rust NAME DATA -> $TMP/rs-NAME
run_rust() {
  rm -rf "$TMP/rs-$1"
  if ! "$BIN" "$2" "$TMP/rs-$1" > "$TMP/rs-$1.log" 2>&1; then
    tail -5 "$TMP/rs-$1.log"; fail "$1: rust exited non-zero"; return 1
  fi
}

# ---- 2. check-dev
if [ -f "$DEV/manifest.json" ] && [ -d "$ANSWER" ]; then
  run_rust dev "$DEV" && compare_dirs "check-dev" "$TMP/rs-dev" "$ANSWER"
else
  fail "check-dev: missing $DEV or $ANSWER"
fi

# ---- 3. DuckDB reference from $REF, cached per pipeline version
BLOB=$(git rev-parse "$REF:pipeline/pipeline.py" 2>/dev/null) || { fail "no pipeline/pipeline.py on $REF"; echo FAIL; exit 1; }
REFDIR=$CACHE/$BLOB
mkdir -p "$REFDIR" || { fail "cannot create $REFDIR"; echo FAIL; exit 1; }
[ -f "$REFDIR/pipeline.py" ] || git show "$REF:pipeline/pipeline.py" > "$REFDIR/pipeline.py"

# duck_ref NAME DATA -> prints the cached DuckDB output dir (computes it if needed)
duck_ref() {
  out=$REFDIR/$1
  if [ ! -f "$out/.done" ]; then
    rm -rf "$out" "$TMP/duckscratch"
    mkdir -p "$out" "$TMP/duckscratch"
    if ! DATA_DIR="$2" OUTPUT_DIR="$out" SCRATCH_DIR="$TMP/duckscratch" python3 "$REFDIR/pipeline.py" > "$TMP/duck-$1.log" 2>&1; then
      tail -5 "$TMP/duck-$1.log" >&2; rm -rf "$out"; return 1
    fi
    touch "$out/.done"
  fi
  echo "$out"
}

check_dataset() {  # NAME DATA
  if ! ref=$(duck_ref "$1" "$2"); then fail "$1: DuckDB reference failed"; return; fi
  run_rust "$1" "$2" && compare_dirs "$1 vs DuckDB" "$TMP/rs-$1" "$ref"
}

for n in $SEEDS; do
  d=$DATA_ROOT/s$n
  if [ ! -f "$d/manifest.json" ]; then
    # Generate into a temp dir and move it into place, so a failed run leaves nothing behind.
    rm -rf "$d.tmp"
    if generate --seed "$n" --events 200000 --clicks 50000 --output "$d.tmp" > "$TMP/gen.log" 2>&1 \
       && mv "$d.tmp" "$d"; then
      echo "      generated s$n"
    else
      tail -5 "$TMP/gen.log"; rm -rf "$d.tmp"; fail "s$n: generate failed"; continue
    fi
  fi
  check_dataset "s$n" "$d"
done

if [ -f "$DATA_ROOT/big/manifest.json" ]; then
  check_dataset big "$DATA_ROOT/big"
else
  fail "big: missing $DATA_ROOT/big"
fi

if [ "$FAILED" = 0 ]; then echo PASS; exit 0; else echo FAIL; exit 1; fi
