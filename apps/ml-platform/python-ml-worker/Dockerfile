# Dockerfile cho Python Machine Learning Worker
# ==============================================================================

FROM python:3.13-slim

WORKDIR /app

# Cài đặt system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy và cài đặt Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn ứng dụng
COPY src/ ./src/

# Command chạy ứng dụng
CMD ["python3", "-m", "src.main"]
