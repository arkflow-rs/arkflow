# syntax=docker/dockerfile:1.7

# ---- chef: 提供 cargo-chef 工具 ----
FROM rust:1.97-slim AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# ---- planner: 仅生成依赖 recipe（不编译） ----
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: 先装系统依赖 → 预编译依赖 → 再编译项目 ----
FROM chef AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang perl libfindbin-libs-perl make cmake gcc libssl-dev pkg-config \
    build-essential libsqlite3-dev protobuf-compiler python3 python3-dev \
    libcurl4-openssl-dev \
 && rm -rf /var/lib/apt/lists/*

# 关键：依赖层只依赖 recipe.json，业务代码改动不会让它失效
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# 现在才 COPY 真实源码，只触发业务代码编译
COPY . .
RUN cargo build --release

# ---- runtime ----
FROM debian:bookworm-slim AS arkflow

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 python3 python3-dev \
 && rm -rf /var/lib/apt/lists/*

# Copy compiled binary from builder stage
COPY --from=builder /app/target/release/arkflow /app/arkflow

# Set environment variables
ENV RUST_LOG=info

# Set startup command
CMD ["/app/arkflow", "--config", "/app/etc/config.yaml"]
