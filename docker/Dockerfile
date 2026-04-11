# Build stage
FROM rust:1.88-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && \
    apt-get install -y clang perl libfindbin-libs-perl make cmake gcc \
        libssl-dev pkg-config build-essential libsqlite3-dev \
        protobuf-compiler python3 python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy cargo files for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# Build project
RUN cargo build --release && \
    # Strip binary to reduce size
    strip /build/target/release/arkflow

# Runtime stage
FROM debian:bookworm-slim as arkflow

# Build arguments for metadata
ARG VERSION=dev
ARG BUILD_DATE
ARG VCS_REF

# Add metadata labels
LABEL org.opencontainers.image.title="ArkFlow Stream Processing Engine" \
      org.opencontainers.image.description="High-performance Rust stream processing engine" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="https://github.com/arkflow/arkflow" \
      org.opencontainers.image.licenses="Apache-2.0"

# Create non-root user
RUN groupadd -r arkflow && \
    useradd -r -g arkflow -s /sbin/nologin -c "ArkFlow user" arkflow && \
    mkdir -p /app/etc /app/logs /var/lib/arkflow && \
    chown -R arkflow:arkflow /app /var/lib/arkflow

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends libsqlite3-0 python3 ca-certificates && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Copy compiled binary from builder stage
COPY --from=builder /build/target/release/arkflow /app/arkflow

# Set ownership
RUN chown arkflow:arkflow /app/arkflow

# Switch to non-root user
USER arkflow

# Set environment variables
ENV RUST_LOG=info \
    PATH="/app:$PATH"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Expose ports
EXPOSE 8080 9090

# Set startup command
CMD ["/app/arkflow", "--config", "/app/etc/config.yaml"]
