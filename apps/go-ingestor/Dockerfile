# ==========================================
# Dockerfile — Go Ingestor (dataset-sync)
# PUBG Anti-Cheat Data Platform (Production Hardened)
# ==========================================

# ------------------------------------------
# Stage 1: Build Stage (Biên dịch Go Binary)
# ------------------------------------------
FROM golang:1.22-alpine AS builder

# Cài đặt ca-certificates để tải HTTPS an toàn
RUN apk add --no-cache ca-certificates git

# Thiếp lập thư mục làm việc trong container build
WORKDIR /app

# Copy go.mod và go.sum để tận dụng Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy toàn bộ mã nguồn Go vào container
COPY . .

# Biên dịch binary tĩnh (CGO_ENABLED=0) tối ưu cho Distroless
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/dataset-sync ./cmd/dataset-sync

# ------------------------------------------
# Stage 2: Runtime Stage (Google Distroless Static Debian12)
# ------------------------------------------
# Sử dụng Google Distroless siêu bảo mật, không có shell/bash, không có utilities dư thừa
FROM gcr.io/distroless/static-debian12:nonroot

# Chạy dưới tài khoản không có quyền root (nonroot UID 65532)
USER 65532:65532

# Thư mục làm việc của runtime
WORKDIR /app

# Copy binary đã biên dịch từ Stage 1
COPY --from=builder /app/bin/dataset-sync /app/dataset-sync

# Entrypoint thực thi command dataset-sync
ENTRYPOINT ["/app/dataset-sync"]
