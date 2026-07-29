# ==========================================
# Dockerfile — Go Ingestor (dataset-sync + replay)
# PUBG Anti-Cheat Data Platform (Production Hardened)
# Build 2 binary trong 1 image: dataset-sync & replay
# ==========================================

# ------------------------------------------
# Stage 1: Builder — Biên dịch cả 2 Go Binary
# ------------------------------------------
FROM golang:1.26-alpine AS builder

# Cài đặt ca-certificates để HTTPS hoạt động an toàn và git cho VCS stamping
RUN apk add --no-cache ca-certificates git

WORKDIR /app

# --- LAYER CACHE TỐI ƯU: Copy go.mod/go.sum trước source code ---
# Layer này chỉ rebuild khi dependency thay đổi — không phải mỗi lần sửa code
COPY go.mod go.sum ./

# Download dependencies — BuildKit mount cache tránh download lại mỗi build
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Copy toàn bộ source code
COPY . .

# Build binary dataset-sync — CGO_ENABLED=0 để binary tĩnh không phụ thuộc libc
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/dataset-sync ./cmd/dataset-sync

# Build binary replay — cùng cờ build, tĩnh và nhỏ
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/replay ./cmd/replay

# ------------------------------------------
# Stage 2: Runtime — Alpine nhỏ gọn, có shell để hỗ trợ cả 2 entrypoint
# ------------------------------------------
FROM alpine:3.20

# ca-certificates cần thiết cho HTTPS calls (MinIO S3, Kaggle API)
RUN apk add --no-cache ca-certificates

# Chạy dưới user không có quyền root — tăng cường bảo mật
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy cả 2 binary đã build từ Stage 1
COPY --from=builder /app/bin/dataset-sync /app/bin/dataset-sync
COPY --from=builder /app/bin/replay       /app/bin/replay

# Chuyển sang user không phải root
USER appuser

# Không set ENTRYPOINT cố định — để docker-compose override bằng command:
# - service go-ingestor-sync  → command: ["/app/bin/dataset-sync"]
# - service go-ingestor-replay → command: ["/app/bin/replay", "-limit=0", ...]
