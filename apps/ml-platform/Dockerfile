# =============================================================================
# Unified ML Platform 3-in-1 Container Dockerfile
# Python ML Worker + Rust ONNX Inference Engine + Go REST API Gateway
# Tối ưu BuildKit Cache Mounts cho Cargo, Go Modules và Pip Wheels
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Build Rust Inference Engine Binary
# -----------------------------------------------------------------------------
# Sử dụng rust:1.88-slim-trixie (Debian 13 Trixie glibc 2.39+) để đáp ứng MSRV (rustc >= 1.88) và đầy đủ C23 symbols (__isoc23_strtol) của ONNX Runtime prebuilt binaries
FROM rust:1.88-slim-trixie AS rust-builder
WORKDIR /app

# Cài đặt build tools và C dependencies (pkg-config, libssl-dev, cmake, build-essential) với Apt cache mount
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    cmake \
    build-essential

# Copy Cargo metadata trước để cache dependencies
COPY rust-inference/Cargo.toml rust-inference/Cargo.lock ./

# Biên dịch trước dependency crates với dummy main.rs và Cargo cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    mkdir -p src \
    && echo 'fn main() { println!("dummy"); }' > src/main.rs \
    && cargo build --release \
    && rm -rf src/main.rs

# Copy source code thật và build release binary
COPY rust-inference/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    touch src/main.rs \
    && cargo build --release \
    && cp /app/target/release/rust-inference /app/rust-inference-bin

# -----------------------------------------------------------------------------
# Stage 2: Build Go REST API Gateway Binary
# -----------------------------------------------------------------------------
FROM golang:1.26-alpine AS go-builder
WORKDIR /app

# Copy Go mod files để cache module download layer
COPY go-api/go.mod ./
COPY go-api/go.sum* ./

# Download dependencies và compile với BuildKit Go cache mounts
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY go-api/ ./
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/go-api ./cmd/server/main.go

# -----------------------------------------------------------------------------
# Stage 3: Unified ML Platform Composite Runtime Container
# -----------------------------------------------------------------------------
# Sử dụng python:3.13-slim-trixie (Debian 13 Trixie glibc 2.39+) để đồng bộ phiên bản glibc runtime với Stage 1
FROM python:3.13-slim-trixie
WORKDIR /app

# Cài đặt system packages với Apt cache mount
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl

# Cài đặt Python requirements với Pip wheel cache mount
COPY python-ml-worker/requirements.txt ./python-requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r python-requirements.txt

# Copy Binaries và Source code từ các Stage 1 & 2
COPY --from=rust-builder /app/rust-inference-bin /usr/local/bin/rust-inference
COPY --from=go-builder /app/go-api /usr/local/bin/go-api
COPY python-ml-worker/src ./python-ml-worker/src
COPY config ./config

EXPOSE 8081

# Entrypoint script khởi chạy đồng thời cả 3 thành phần trong container
CMD ["sh", "-c", "/usr/local/bin/rust-inference & /usr/local/bin/go-api & cd /app/python-ml-worker && python src/main.py"]
