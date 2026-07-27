# Multi-Stage Dockerfile cho Rust Stream Processor Engine (Cloud-Native Non-Root Security)
# Stage 1: Build binary bằng Rust Toolchain chính thức
FROM rust:1.85-slim-bookworm AS builder

# Cài đặt các gói phụ thuộc hệ thống cần thiết (pkg-config, libssl-dev, cmake cho rdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy tệp định nghĩa Cargo
COPY Cargo.toml Cargo.lock ./

# Copy mã nguồn
COPY src ./src

# Biên dịch sản phẩm Release Binary với tối ưu hóa
RUN cargo build --release

# Stage 2: Container Runtime siêu nhẹ dựa trên Debian Bookworm Slim
FROM debian:bookworm-slim AS runner

# Cài đặt OpenSSL runtime & ca-certificates cho kết nối SSL/TLS an toàn
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Tạo non-root user cho an toàn bảo mật Cloud-Native
RUN useradd -m -u 10001 appuser
USER appuser

# Copy file binary biên dịch từ Stage 1
COPY --from=builder /app/target/release/rust-processor /app/rust-processor

# Cấu hình biến môi trường và điểm chạy
ENV RUST_LOG=info
ENTRYPOINT ["/app/rust-processor"]
