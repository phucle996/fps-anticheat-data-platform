# Sử dụng Python 3.11 Slim cho container gọn nhẹ
FROM python:3.11-slim

WORKDIR /app

# Sao chép và cài đặt thư viện dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn ứng dụng Streamlit
COPY . .

# Expose cổng 8501
EXPOSE 8501

# Khởi chạy Streamlit Server
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
