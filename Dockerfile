ARG RUST_TAG=1.98-trixie

FROM rust:${RUST_TAG} AS planner
WORKDIR /app
RUN cargo install cargo-chef
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM rust:${RUST_TAG} AS builder
WORKDIR /app
RUN cargo install cargo-chef

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release

# hadolint ignore=DL3007
FROM gcr.io/distroless/cc-debian13:latest AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/arr-upgrade /app/arr-upgrade

ENV ARR_UPGRADE_CONFIG="/config"
ENV RUST_LOG=arr_upgrade=debug
CMD ["/app/arr-upgrade"]
