# Build stage
FROM rust:1.85-slim as builder

WORKDIR /app
COPY .. .
RUN apt-get update && \
    apt-get install -y clang perl libfindbin-libs-perl make cmake gcc libssl-dev pkg-config build-essential libsqlite3-dev protobuf-compiler

# Build project
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim as arkflow

WORKDIR /app

# Copy compiled binary from builder stage
COPY --from=builder /app/target/release/arkflow /app/arkflow

 
# Set environment variables
ENV RUST_LOG=info


# Set startup command
CMD ["/app/arkflow", "--config", "/app/etc/config.yaml"]