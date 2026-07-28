# PUBG Anti-Cheat Exploratory Data Analysis (EDA) Report

**Thời gian khởi tạo**: `2026-07-28 12:46:15 UTC`

---

## 📊 1. Thống kê Tổng quan (Dataset Summary)
- **Tổng số bản ghi (Total Records)**: `5`
- **Số lượng Người chơi (Unique Players)**: `5`
- **Số lượng Trận đấu (Unique Matches)**: `3`

---

## 📈 2. Thống kê Mô tả Đặc trưng ML (Descriptive Statistics)
| Feature Name | Mean | Median | Min | Max | SD | P95 | P99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `kills` | 8.60 | 5.00 | 0.00 | 25.00 | 10.31 | 22.40 | 24.48 |
| `damage_dealt` | 966.00 | 520.00 | 0.00 | 2800.00 | 1163.69 | 2520.00 | 2744.00 |
| `headshot_ratio` | 0.43 | 0.40 | 0.00 | 0.92 | 0.44 | 0.90 | 0.92 |
| `total_distance` | 2150.00 | 1800.00 | 50.00 | 5000.00 | 2094.04 | 4700.00 | 4940.00 |
| `kills_per_minute` | 0.45 | 0.30 | 0.00 | 1.25 | 0.50 | 1.12 | 1.22 |
| `damage_per_minute` | 50.44 | 31.20 | 0.00 | 140.00 | 56.73 | 126.00 | 137.20 |
| `damage_per_kill` | 88.52 | 110.00 | 0.00 | 116.60 | 49.69 | 115.68 | 116.42 |
| `movement_per_minute` | 115.60 | 108.00 | 5.00 | 250.00 | 99.50 | 235.00 | 247.00 |
| `performance_versus_lobby` | 0.00 | 0.00 | -645.00 | 645.00 | 491.74 | 568.00 | 629.60 |

---

## ⚠️ 3. Phát hiện Outliers & Extreme Values Nghi vấn Gian lận
- **Số lượng bản ghi vi phạm ngưỡng cao (Headshot Ratio > 85% & Kills >= 10)**: `1` mẫu

---

## 🎯 4. Danh sách Đặc trưng được Chọn cho Mô hình ML (Isolation Forest)
- `kills_per_minute` (Tốc độ hạ gục)
- `damage_per_minute` (Tốc độ gây sát thương)
- `headshot_ratio` (Tỷ lệ headshot)
- `damage_per_kill` (Sát thương trung bình / mạng)
- `movement_per_minute` (Tốc độ di chuyển)
- `performance_versus_lobby` (Chênh lệch với Lobby)
