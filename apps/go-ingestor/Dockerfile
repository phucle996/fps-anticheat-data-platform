# ==========================================
# Dockerfile — Go Ingestor (dataset-sync)
# PUBG Anti-Cheat Data Platform (Production Hardened)
# Tối ưu Docker Layer Caching với BuildKit mount cache
# ==========================================

# ------------------------------------------
# Stage 1: Build Stage (Biên dịch Go Binary)
# ------------------------------------------
FROM golang:1.23-alpine AS builder

# Cài đặt ca-certificates để tải HTTPS an toàn
# Layer này cache ổn định — chỉ đổi khi update base image
RUN apk add --no-cache ca-certificates git

WORKDIR /app

# --- LAYER CACHE TỐI ƯU: Download dependencies trước ---
# Copy go.mod và go.sum TRƯỚC KHI copy source code
# Layer này chỉ rebuild khi go.mod/go.sum thay đổi (thêm/xóa dependency)
COPY go.mod go.sum ./

# Dùng BuildKit mount cache để cache Go module download vào /root/go/pkg/mod
# Lần sau build sẽ reuse cache thay vì download lại từ internet
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# --- COPY SOURCE CODE THẬT ---
# Layer này thay đổi mỗi khi source thay đổi, nhưng dependencies đã cache ở trên
COPY . .

# Biên dịch binary tĩnh (CGO_ENABLED=0) tối ưu cho Distroless
# Dùng BuildKit mount cache cho Go build cache (~/.cache/go-build)
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/dataset-sync ./cmd/dataset-sync

# ------------------------------------------
# Stage 2: Runtime Stage (Google Distroless Static Debian12)
# ------------------------------------------
# Sử dụng Google Distroless siêu bảo mật, không có shell/bash
FROM gcr.io/distroless/static-debian12:nonroot

# Chạy dưới tài khoản không có quyền root (nonroot UID 65532)
USER 65532:65532

# Thư mục làm việc của runtime
WORKDIR /app

# Copy binary đã biên dịch từ Stage 1
COPY --from=builder /app/bin/dataset-sync /app/dataset-sync

# Entrypoint thực thi command dataset-sync
ENTRYPOINT ["/app/dataset-sync"]
