# ==========================================
# Dockerfile — Go Ingestor (dataset-sync)
# PUBG Anti-Cheat Data Platform
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

# Biên dịch binary tĩnh (CGO_ENABLED=0) tối ưu cho container nhỏ nhẹ
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/dataset-sync ./cmd/dataset-sync

# ------------------------------------------
# Stage 2: Runtime Stage (Minimal Alpine Base)
# ------------------------------------------
FROM alpine:3.19

# Cài đặt SSL Certificates cho kết nối HTTPS Kaggle & MinIO
RUN apk add --no-cache ca-certificates tzdata

# Thêm user không có quyền root (non-root user) để bảo mật
RUN adduser -D -u 10001 appuser
USER appuser

# Thư mục làm việc của runtime
WORKDIR /app

# Copy binary đã biên dịch từ Stage 1
COPY --from=builder /app/bin/dataset-sync /app/dataset-sync

# Entrypoint thực thi command dataset-sync
ENTRYPOINT ["/app/dataset-sync"]
