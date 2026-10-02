##
# builder image
##
ARG RUST_VERSION
FROM rust:${RUST_VERSION}-alpine AS builder

ARG RUST_VERSION
ARG REPO_DIR=.

RUN apk --no-cache add musl-dev g++ linux-headers make perl git

WORKDIR /work

# Overrides the copied toolchain file, so rustup installs no clippy or rustfmt.
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION} \
  CARGO_NET_GIT_FETCH_WITH_CLI="true"

# Cache dependencies.
COPY "$REPO_DIR"/Cargo.toml "$REPO_DIR"/Cargo.lock ./
RUN mkdir -p src/bin && \
    echo "fn main() {}" > build.rs && \
    echo "" > src/lib.rs && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > src/bin/openapi.rs
RUN cargo fetch --locked
RUN cargo build --release --locked --offline

COPY "$REPO_DIR" .
RUN find . -type f \( -name build.rs -or -name lib.rs -or -name main.rs -or -name openapi.rs \) -exec touch {} +
RUN cargo build --release --locked --offline --bin dotk-indexer


##
# runtime image
##
FROM alpine:3.24

COPY --from=builder /work/target/release/dotk-indexer /usr/local/bin/
COPY --from=builder /work/LICENSE /work/NOTICE /work/src/fonts/OFL.txt /usr/share/licenses/dotk-indexer/

RUN install -d -o 13337 -g 13337 /data
WORKDIR /data
USER 13337:13337

EXPOSE 7799

ENTRYPOINT ["dotk-indexer"]
CMD ["--help"]
