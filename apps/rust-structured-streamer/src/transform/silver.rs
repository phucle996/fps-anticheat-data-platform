use crate::error::{AppError, Result};
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Native Silver Preprocessor chịu trách nhiệm chuyển đổi Bronze RecordBatch sang Silver Entities
pub struct NativeSilverPreprocessor;

impl NativeSilverPreprocessor {
    /// Schema định nghĩa cho Silver Kill Events layer
    pub fn kill_events_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("match_id", DataType::Utf8, false),
            Field::new("event_time", DataType::Utf8, true),
            Field::new("event_time_seconds", DataType::Float64, true),
            Field::new("killer_name", DataType::Utf8, true),
            Field::new("victim_name", DataType::Utf8, true),
            Field::new("killer_position_x", DataType::Float64, true),
            Field::new("killer_position_y", DataType::Float64, true),
            Field::new("victim_position_x", DataType::Float64, true),
            Field::new("victim_position_y", DataType::Float64, true),
            Field::new("weapon", DataType::Utf8, true),
            Field::new("damage_dealt", DataType::Float64, true),
        ]))
    }

    /// Schema định nghĩa cho Silver Player Match layer
    pub fn player_match_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("match_id", DataType::Utf8, false),
            Field::new("player_id", DataType::Utf8, false),
            Field::new("kills", DataType::Int64, true),
            Field::new("headshot_kills", DataType::Int64, true),
            Field::new("damage_dealt", DataType::Float64, true),
            Field::new("walk_distance", DataType::Float64, true),
            Field::new("ride_distance", DataType::Float64, true),
            Field::new("swim_distance", DataType::Float64, true),
            Field::new("survival_duration", DataType::Float64, true),
            Field::new("win_place_perc", DataType::Float64, true),
        ]))
    }

    /// Thực hiện chuyển đổi Bronze RecordBatches thành (silver_kill_events, silver_player_match)
    pub fn process_silver(batches: &[RecordBatch]) -> Result<(RecordBatch, RecordBatch)> {
        // 1. Tập hợp và deduplicate các bản ghi theo event_id
        let mut seen_event_ids = HashSet::new();

        // Data structures chứa dữ liệu đã chuẩn hóa
        let mut ke_event_ids = Vec::new();
        let mut ke_match_ids = Vec::new();
        let mut ke_event_times = Vec::new();
        let mut ke_event_time_secs = Vec::new();
        let mut ke_killer_names = Vec::new();
        let mut ke_victim_names = Vec::new();
        let mut ke_killer_pos_x = Vec::new();
        let mut ke_killer_pos_y = Vec::new();
        let mut ke_victim_pos_x = Vec::new();
        let mut ke_victim_pos_y = Vec::new();
        let mut ke_weapons = Vec::new();
        let mut ke_damage_dealt = Vec::new();

        // Player match stats aggregator: (match_id, player_id) -> PlayerStats
        #[derive(Default)]
        struct PlayerStats {
            kills: i64,
            headshot_kills: i64,
            damage_dealt: f64,
            walk_distance: f64,
            ride_distance: f64,
            swim_distance: f64,
            survival_duration: f64,
            win_place_perc: f64,
        }
        let mut player_stats_map: HashMap<(String, String), PlayerStats> = HashMap::new();

        for batch in batches {
            let num_rows = batch.num_rows();

            // Trích xuất các cột từ Bronze RecordBatch
            let get_string_col = |name: &str| -> Option<&StringArray> {
                batch
                    .column_by_name(name)?
                    .as_any()
                    .downcast_ref::<StringArray>()
            };
            let get_f64_col = |name: &str| -> Option<&Float64Array> {
                batch
                    .column_by_name(name)?
                    .as_any()
                    .downcast_ref::<Float64Array>()
            };
            let get_i64_col = |name: &str| -> Option<&Int64Array> {
                batch
                    .column_by_name(name)?
                    .as_any()
                    .downcast_ref::<Int64Array>()
            };

            let event_id_arr = get_string_col("event_id");
            let match_id_arr = get_string_col("match_id");
            let player_id_arr = get_string_col("player_id");
            let event_time_arr = get_string_col("event_time");
            let event_time_secs_arr = get_f64_col("event_time_seconds");
            let killer_name_arr = get_string_col("killer_name");
            let victim_name_arr = get_string_col("victim_name");
            let killer_pos_x_arr = get_f64_col("killer_position_x");
            let killer_pos_y_arr = get_f64_col("killer_position_y");
            let victim_pos_x_arr = get_f64_col("victim_position_x");
            let victim_pos_y_arr = get_f64_col("victim_position_y");
            let weapon_arr = get_string_col("weapon");
            let damage_arr = get_f64_col("damage_dealt");
            let kills_arr = get_i64_col("kills");
            let headshots_arr = get_i64_col("headshot_kills");
            let walk_dist_arr = get_f64_col("walk_distance");
            let ride_dist_arr = get_f64_col("ride_distance");
            let swim_dist_arr = get_f64_col("swim_distance");
            let survival_arr = get_f64_col("survival_duration");
            let win_place_arr = get_f64_col("win_place_perc");

            for row in 0..num_rows {
                let event_id = event_id_arr
                    .map(|a| a.value(row).to_string())
                    .unwrap_or_default();

                // Lọc trùng bản ghi theo event_id
                if !event_id.is_empty() && !seen_event_ids.insert(event_id.clone()) {
                    continue;
                }

                let match_id = match_id_arr
                    .map(|a| a.value(row).to_string())
                    .unwrap_or_default();
                let player_id = player_id_arr
                    .map(|a| a.value(row).to_string())
                    .unwrap_or_default();
                let killer_name = killer_name_arr
                    .filter(|a| !a.is_null(row))
                    .map(|a| a.value(row).to_string())
                    .unwrap_or_else(|| player_id.clone());
                let victim_name = victim_name_arr
                    .filter(|a| !a.is_null(row))
                    .map(|a| a.value(row).to_string());

                // Ghi nhận Kill Event
                ke_event_ids.push(event_id);
                ke_match_ids.push(match_id.clone());
                ke_event_times.push(
                    event_time_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row).to_string()),
                );
                ke_event_time_secs.push(
                    event_time_secs_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row)),
                );
                ke_killer_names.push(Some(killer_name.clone()));
                ke_victim_names.push(victim_name.clone());
                ke_killer_pos_x.push(
                    killer_pos_x_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row)),
                );
                ke_killer_pos_y.push(
                    killer_pos_y_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row)),
                );
                ke_victim_pos_x.push(
                    victim_pos_x_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row)),
                );
                ke_victim_pos_y.push(
                    victim_pos_y_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row)),
                );
                ke_weapons.push(
                    weapon_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row).to_string()),
                );
                ke_damage_dealt.push(damage_arr.filter(|a| !a.is_null(row)).map(|a| a.value(row)));

                // Cập nhật Player Match Aggregations
                if !match_id.is_empty() && !player_id.is_empty() {
                    let entry = player_stats_map
                        .entry((match_id.clone(), player_id.clone()))
                        .or_default();
                    if let Some(kills) = kills_arr.filter(|a| !a.is_null(row)).map(|a| a.value(row))
                    {
                        entry.kills = entry.kills.max(kills);
                    }
                    if let Some(hs) = headshots_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row))
                    {
                        entry.headshot_kills = entry.headshot_kills.max(hs);
                    }
                    if let Some(dmg) = damage_arr.filter(|a| !a.is_null(row)).map(|a| a.value(row))
                    {
                        entry.damage_dealt = entry.damage_dealt.max(dmg);
                    }
                    if let Some(w) = walk_dist_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row))
                    {
                        entry.walk_distance = entry.walk_distance.max(w);
                    }
                    if let Some(r) = ride_dist_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row))
                    {
                        entry.ride_distance = entry.ride_distance.max(r);
                    }
                    if let Some(s) = swim_dist_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row))
                    {
                        entry.swim_distance = entry.swim_distance.max(s);
                    }
                    if let Some(dur) = survival_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row))
                    {
                        entry.survival_duration = entry.survival_duration.max(dur);
                    }
                    if let Some(wp) = win_place_arr
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row))
                    {
                        entry.win_place_perc = entry.win_place_perc.max(wp);
                    }
                }

                // Cập nhật thông tin cho nạn nhân nếu có
                if let (Some(vic), true) = (victim_name.as_ref(), !match_id.is_empty()) {
                    let entry = player_stats_map
                        .entry((match_id.clone(), vic.clone()))
                        .or_default();
                    let _ = entry; // Đảm bảo nạn nhân cũng xuất hiện trong player_match ngay cả khi 0 kill
                }
            }
        }

        // Build RecordBatch cho silver_kill_events
        let ke_batch = RecordBatch::try_new(
            Self::kill_events_schema(),
            vec![
                Arc::new(StringArray::from(
                    ke_event_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    ke_match_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(ke_event_times)),
                Arc::new(Float64Array::from(ke_event_time_secs)),
                Arc::new(StringArray::from(ke_killer_names)),
                Arc::new(StringArray::from(ke_victim_names)),
                Arc::new(Float64Array::from(ke_killer_pos_x)),
                Arc::new(Float64Array::from(ke_killer_pos_y)),
                Arc::new(Float64Array::from(ke_victim_pos_x)),
                Arc::new(Float64Array::from(ke_victim_pos_y)),
                Arc::new(StringArray::from(ke_weapons)),
                Arc::new(Float64Array::from(ke_damage_dealt)),
            ],
        )
        .map_err(|e| {
            AppError::Arrow(format!("Khởi tạo silver_kill_events RecordBatch thất bại: {}", e))
        })?;

        // Build RecordBatch cho silver_player_match
        let mut pm_match_ids = Vec::with_capacity(player_stats_map.len());
        let mut pm_player_ids = Vec::with_capacity(player_stats_map.len());
        let mut pm_kills = Vec::with_capacity(player_stats_map.len());
        let mut pm_headshots = Vec::with_capacity(player_stats_map.len());
        let mut pm_damage = Vec::with_capacity(player_stats_map.len());
        let mut pm_walk = Vec::with_capacity(player_stats_map.len());
        let mut pm_ride = Vec::with_capacity(player_stats_map.len());
        let mut pm_swim = Vec::with_capacity(player_stats_map.len());
        let mut pm_survival = Vec::with_capacity(player_stats_map.len());
        let mut pm_win_place = Vec::with_capacity(player_stats_map.len());

        for ((m_id, p_id), stats) in player_stats_map {
            pm_match_ids.push(m_id);
            pm_player_ids.push(p_id);
            pm_kills.push(stats.kills);
            pm_headshots.push(stats.headshot_kills);
            pm_damage.push(stats.damage_dealt);
            pm_walk.push(stats.walk_distance);
            pm_ride.push(stats.ride_distance);
            pm_swim.push(stats.swim_distance);
            pm_survival.push(stats.survival_duration);
            pm_win_place.push(stats.win_place_perc);
        }

        let pm_batch = RecordBatch::try_new(
            Self::player_match_schema(),
            vec![
                Arc::new(StringArray::from(
                    pm_match_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    pm_player_ids
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(pm_kills)),
                Arc::new(Int64Array::from(pm_headshots)),
                Arc::new(Float64Array::from(pm_damage)),
                Arc::new(Float64Array::from(pm_walk)),
                Arc::new(Float64Array::from(pm_ride)),
                Arc::new(Float64Array::from(pm_swim)),
                Arc::new(Float64Array::from(pm_survival)),
                Arc::new(Float64Array::from(pm_win_place)),
            ],
        )
        .map_err(|e| {
            AppError::Arrow(format!(
                "Khởi tạo silver_player_match RecordBatch thất bại: {}",
                e
            ))
        })?;

        Ok((ke_batch, pm_batch))
    }
}
