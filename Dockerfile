# Shopping Sessions: Rust pipeline (rust/). The build context is the repo root.
# The scorers are linux/amd64 and build this file natively, so no FROM line
# pins a platform. On amd64 the binary is built for x86-64-v3 (AVX2, BMI2).

# ---- build stage
FROM rust:1-slim-bookworm AS build
ARG TARGETARCH
WORKDIR /src
# Dependencies first, in their own layer, so source edits rebuild only the crate.
COPY rust/Cargo.toml rust/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
 && if [ "$TARGETARCH" = "amd64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi \
 && cargo build --release --locked \
 && rm -rf src target/release/deps/sessions-* target/release/.fingerprint/sessions-* target/release/sessions*
COPY rust/src ./src
# touch: COPY keeps the source's old mtime, which must not look older than the stub build.
RUN touch src/main.rs \
 && if [ "$TARGETARCH" = "amd64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi \
 && cargo build --release --locked \
 && strip target/release/sessions

# ---- runtime stage: glibc + libgcc only, no shell
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/sessions /app/sessions
# At run time the root filesystem is read-only and the user is 1000:1000; the
# binary only reads $DATA_DIR and writes $OUTPUT_DIR.
ENV DATA_DIR=/data OUTPUT_DIR=/output
USER 1000:1000
ENTRYPOINT ["/app/sessions"]
