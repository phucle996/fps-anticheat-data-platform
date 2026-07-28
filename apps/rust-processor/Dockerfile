# Multi-Stage Dockerfile cho Rust Stream Processor Engine (Cloud-Native Non-Root Security)
# Tối ưu Docker Layer Caching: Dependencies compile 1 lần, chỉ rebuild source khi src thay đổi

# ------------------------------------------
# Stage 1: Build Stage (Rust Release Binary)
# ------------------------------------------
FROM rust:1.87-slim-bookworm AS builder

# Cài đặt gói hệ thống cần thiết (pkg-config, libssl-dev, cmake cho rdkafka C binding)
# Layer này cache ổn định — chỉ thay đổi khi cập nhật base image
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- LAYER CACHE TỐI ƯU: Compile toàn bộ dependencies trước ---
# Chỉ copy manifest files, chưa copy source code thật
COPY Cargo.toml Cargo.lock ./

# Tạo dummy main.rs để cargo fetch & compile tất cả dependencies vào cache
# Layer này chỉ rebuild khi Cargo.toml hoặc Cargo.lock thay đổi (thêm/xóa dependency)
RUN mkdir src \
    && echo 'fn main() { println!("dummy"); }' > src/main.rs \
    && cargo build --release \
    && rm -rf src/main.rs

# --- COPY SOURCE CODE THẬT ---
# Layer này thay đổi mỗi khi src/ thay đổi, nhưng dependencies đã cache xong ở trên
# Touch file binary để buộc cargo nhận ra binary cần rebuild (tránh incremental skip)
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release

# ------------------------------------------
# Stage 2: Runtime Stage (Debian Bookworm Slim — không có build tools)
# ------------------------------------------
FROM debian:bookworm-slim AS runner

# Cài đặt OpenSSL runtime & ca-certificates cho SSL/TLS
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Tạo non-root user bảo mật Cloud-Native (UID 10001 cố định)
RUN useradd -m -u 10001 appuser
USER appuser

# Copy binary đã biên dịch từ Stage 1
COPY --from=builder /app/target/release/rust-processor /app/rust-processor

# Cấu hình log level mặc định và entrypoint
ENV RUST_LOG=info
ENTRYPOINT ["/app/rust-processor"]
