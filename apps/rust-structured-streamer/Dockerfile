# Multi-Stage Dockerfile cho Rust Stream Processor Engine (Cloud-Native Non-Root Security)
# Tối ưu Docker BuildKit Cache: Cache Cargo crates registry và target build folder giữa các lần build

# ------------------------------------------
# Stage 1: Build Stage (Rust Release Binary)
# ------------------------------------------
FROM rust:1.87-slim-bookworm AS builder

# Cài đặt gói hệ thống cần thiết cho cargo build (pkg-config, libssl-dev, cmake, libzstd-dev cho rdkafka zstd C binding)
# Tối ưu cache cho apt-get package manager
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    libzstd-dev \
    cmake \
    build-essential

WORKDIR /app

# Copy Cargo metadata để cache dependencies
COPY Cargo.toml Cargo.lock ./

# Biên dịch trước các dependency crates với dummy main.rs và BuildKit Cargo cache mount
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    mkdir -p src \
    && echo 'fn main() { println!("dummy"); }' > src/main.rs \
    && cargo build --release \
    && rm -rf src/main.rs

# Copy source code thật và build release binary sử dụng lại Cargo build cache
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    touch src/main.rs \
    && cargo build --release \
    && cp /app/target/release/rust-structured-streamer /app/rust-structured-streamer-bin

# ------------------------------------------
# Stage 2: Minimal Runtime Stage (Debian Bookworm với OpenSSL + libzstd1 - 100% Native Rust)
# ------------------------------------------
FROM debian:bookworm-slim AS runner

# Cài đặt ca-certificates, libssl3, libzstd1 cho Native Rust Runtime
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libzstd1

WORKDIR /app

# Tạo non-root user bảo mật Cloud-Native (UID 10001 cố định)
RUN useradd -m -u 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Copy binary đã biên dịch từ Stage 1
COPY --from=builder /app/rust-structured-streamer-bin /app/rust-structured-streamer

# Cấu hình log level mặc định và entrypoint cho rust-structured-streamer
ENV RUST_LOG=info
ENTRYPOINT ["/app/rust-structured-streamer"]
