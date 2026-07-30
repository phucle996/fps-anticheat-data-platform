# Multi-Stage Dockerfile cho Rust Stream Processor Engine (Cloud-Native Non-Root Security)
# Tối ưu Docker BuildKit Cache: Cache Cargo crates registry và target build folder giữa các lần build

# ------------------------------------------
# Stage 1: Build Stage (Rust Release Binary)
# ------------------------------------------
FROM rust:1.87-slim-bookworm AS builder

# Cài đặt gói hệ thống cần thiết cho cargo build (pkg-config, libssl-dev, cmake cho rdkafka C binding)
# Tối ưu cache cho apt-get package manager
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
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
    && cp /app/target/release/rust-processor /app/rust-processor-bin

# ------------------------------------------
# Stage 2: Runtime Stage (Debian Bookworm với R 4.4 + Arrow + OpenSSL)
# ------------------------------------------
FROM debian:bookworm-slim AS runner

# Cài đặt R runtime, OpenSSL, ca-certificates và các thư viện hệ thống cần cho R arrow package
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    r-base \
    r-cran-jsonlite \
    libcurl4-openssl-dev \
    libxml2-dev \
    build-essential

# Cài đặt R package arrow trực tiếp trong build stage của image để đảm bảo runtime có sẵn (Fail-Close)
RUN Rscript -e 'install.packages("arrow", repos="https://cloud.r-project.org")' \
    && Rscript -e 'stopifnot(requireNamespace("arrow", quietly=TRUE))' \
    && Rscript -e 'stopifnot(requireNamespace("jsonlite", quietly=TRUE))'

WORKDIR /app

# Copy R processor subfolder từ build context (r-processor) sang /opt/r-processor
COPY r-processor /opt/r-processor

# Tạo non-root user bảo mật Cloud-Native (UID 10001 cố định)
RUN useradd -m -u 10001 appuser \
    && chown -R appuser:appuser /app /opt/r-processor
USER appuser

# Copy binary đã biên dịch từ Stage 1
COPY --from=builder /app/rust-processor-bin /app/rust-processor

# Cấu hình log level mặc định và entrypoint
ENV RUST_LOG=info
ENTRYPOINT ["/app/rust-processor"]
