# Data Dictionary — Player Statistics (Raw CSV Record)

Tài liệu từ điển dữ liệu giải thích ý nghĩa các cột dữ liệu thô từ PUBG Kaggle Dataset (`train_V2.csv`) và các quy tắc chuẩn hóa, kiểm tra hợp lệ.

---

## 1. Danh sách các Trường dữ liệu (Fields List)

| Column Name (CSV) | Internal Field | Data Type | Nullable | Description & Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `Id` | `player_id` | String | No | Mã định danh duy nhất của người chơi trong trận đấu. |
| `groupId` | `group_id` | String | No | Mã định danh nhóm/đội của người chơi trong trận đấu. |
| `matchId` | `match_id` | String | No | Mã định danh duy nhất của trận đấu (Dùng làm Partition Key). |
| `kills` | `kills` | Integer | No | Số mạng hạ gục. Ràng buộc: `kills >= 0`. |
| `damageDealt` | `damage_dealt` | Float | No | Tổng lượng sát thương gây ra. Ràng buộc: `damage_dealt >= 0.0`. |
| `headshotKills` | `headshot_kills` | Integer | No | Số mạng hạ gục bằng headshot. Ràng buộc: `0 <= headshot_kills <= kills`. |
| `walkDistance` | `walk_distance` | Float | No | Khoảng cách di chuyển bằng chân (mét). Ràng buộc: `walk_distance >= 0.0`. |
| `rideDistance` | `ride_distance` | Float | No | Khoảng cách di chuyển bằng phương tiện (mét). Ràng buộc: `ride_distance >= 0.0`. |
| `swimDistance` | `swim_distance` | Float | No | Khoảng cách bơi lội (mét). Ràng buộc: `swim_distance >= 0.0`. |
| `matchDuration` | `survival_duration` | Float | No | Thời gian tồn tại trong trận đấu (giây). Ràng buộc: `survival_duration >= 0.0`. |
| `winPlacePerc` | `win_place_perc` | Float | Yes | Tỷ lệ phần trăm thứ hạng thắng (0.0 đến 1.0). |

---

## 2. Quy tắc Semantic Validation (Game Logic Integrity)

1. **Ràng buộc Mạng hạ gục**:
   - `headshot_kills` tuyệt đối không được lớn hơn `kills`. Nếu `headshot_kills > kills` $\rightarrow$ Bản ghi bất hợp lệ (Invalid record).
2. **Ràng buộc Khoảng cách & Sát thương**:
   - Khoảng cách di chuyển (`walk_distance`, `ride_distance`, `swim_distance`) không được âm.
   - Sát thương (`damage_dealt`) không được âm.
3. **Ràng buộc Tốc độ di chuyển bất thường**:
   - Nếu `walk_distance > 10000` mét trong thời gian `survival_duration < 100` giây $\rightarrow$ Cảnh báo tốc độ di chuyển siêu nhiên (Speedhack / Teleport anomaly).
