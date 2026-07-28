# =============================================================================
# Unified ML Platform 3-in-1 Container Dockerfile
# Python ML Worker + Rust ONNX Inference Engine + Go REST API Gateway
# =============================================================================

# Stage 1: Build Rust Inference Engine Binary
FROM rust:1.87-alpine AS rust-builder
WORKDIR /app
RUN apk add --no-cache musl-dev g++ make pkgconfig
COPY rust-inference/Cargo.toml rust-inference/Cargo.lock ./
COPY rust-inference/src ./src
RUN cargo build --release

# Stage 2: Build Go REST API Gateway Binary
FROM golang:1.26-alpine AS go-builder
WORKDIR /app
COPY go-api/go.mod ./
COPY go-api/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o go-api ./main.go

# Stage 3: Unified ML Platform Composite Runtime Container
FROM python:3.13-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY python-ml-worker/requirements.txt ./python-requirements.txt
RUN pip install --no-cache-dir -r python-requirements.txt

# Copy Binaries and Sources from Stage 1 & 2
COPY --from=rust-builder /app/target/release/rust-inference /usr/local/bin/rust-inference
COPY --from=go-builder /app/go-api /usr/local/bin/go-api
COPY python-ml-worker/src ./python-ml-worker/src

EXPOSE 8081

# Entrypoint script to run all 3 components in single container
CMD ["sh", "-c", "/usr/local/bin/rust-inference & /usr/local/bin/go-api & cd /app/python-ml-worker && python src/main.py"]
