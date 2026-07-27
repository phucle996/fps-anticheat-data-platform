# Data Dictionary — Extracted Features (Gold Layer)

Tài liệu giải thích các đặc trưng nâng cao (Engineered Features) được trích xuất ở tầng Gold để phục vụ mô hình Machine Learning phát hiện bất thường (Isolation Forest).

---

## 1. Danh sách các Features

| Feature Name | Data Type | Formula / Logic | Description |
| :--- | :--- | :--- | :--- |
| `kills_per_minute` | Float | `kills / (survival_duration / 60.0)` | Tần suất hạ gục trên mỗi phút tồn tại. |
| `damage_per_minute` | Float | `damage_dealt / (survival_duration / 60.0)` | Lượng sát thương gây ra trung bình mỗi phút. |
| `headshot_ratio` | Float | `headshot_kills / max(kills, 1)` | Tỷ lệ bắn trúng đầu trên tổng số mạng hạ gục. |
| `total_distance` | Float | `walk_distance + ride_distance + swim_distance` | Tổng quãng đường di chuyển trong trận. |
| `damage_per_kill` | Float | `damage_dealt / max(kills, 1)` | Lượng sát thương trung bình để có 1 mạng hạ gục. |
| `movement_per_minute` | Float | `total_distance / (survival_duration / 60.0)` | Vận tốc di chuyển trung bình mỗi phút. |
| `performance_vs_lobby` | Float | `(damage_dealt - lobby_median_damage) / lobby_sd_damage` | Điểm lệch sát thương so với trung vị của toàn phòng chơi (Lobby). |

---

## 2. Xử lý Trường hợp Đặc biệt (Edge Cases & Safety)

1. **Chia cho 0 (Division by Zero)**:
   - Nếu `survival_duration == 0`, gán thời lượng tối thiểu bằng `1.0` giây trước khi tính rate.
   - Nếu `kills == 0`, gán `headshot_ratio = 0.0`.
2. **Missing & Infinite Values (`NA` / `Inf`)**:
   - Tất cả giá trị `Inf` hoặc `-Inf` do phép chia sinh ra sẽ được ép về `0.0` hoặc giá trị Max/Min hợp lệ trước khi đẩy vào mô hình Isolation Forest.
