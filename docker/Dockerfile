# Build stage
FROM rust:1.86-slim as builder

WORKDIR /app
COPY .. .
RUN apt-get update && \
    apt-get install -y clang perl libfindbin-libs-perl make cmake gcc libssl-dev pkg-config build-essential libsqlite3-dev protobuf-compiler python3 python3-dev

# Build project
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim as arkflow

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y libsqlite3-0 python3 python3-dev&& rm -rf /var/lib/apt/lists/*

# Copy compiled binary from builder stage
COPY --from=builder /app/target/release/arkflow /app/arkflow

 
# Set environment variables
ENV RUST_LOG=info


# Set startup command
CMD ["/app/arkflow", "--config", "/app/etc/config.yaml"]