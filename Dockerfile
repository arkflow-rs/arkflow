# Build stage
FROM rust:1.75-slim as builder

WORKDIR /app
COPY . .

# Build project
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy compiled binary from builder stage
COPY --from=builder /app/target/release/arkflow /app/arkflow

# Copy example configuration files
COPY examples /app/examples

# Set environment variables
ENV RUST_LOG=info

# Expose default port
EXPOSE 8000

# Set startup command
CMD ["/app/arkflow" "--config", "/app/examples/config.toml"]