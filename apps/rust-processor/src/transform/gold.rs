use crate::error::{AppError, Result};
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Native Gold Feature Generator chịu trách nhiệm tính toán ma trận đặc trưng ML Anti-Cheat
pub struct NativeGoldFeatureGenerator;

impl NativeGoldFeatureGenerator {
    /// Schema định nghĩa cho Gold Layer Feature Matrix
    pub fn gold_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("match_id", DataType::Utf8, false),
            Field::new("player_id", DataType::Utf8, false),
            Field::new("kills", DataType::Int64, true),
            Field::new("minimum_kill_interval_seconds", DataType::Float64, true),
            Field::new("median_kill_distance_coordinate_units", DataType::Float64, true),
            Field::new("short_kill_interval_count", DataType::Int64, true),
            Field::new("unique_weapons_used", DataType::Int64, true),
            Field::new("feature_version", DataType::Utf8, false),
            Field::new("created_at", DataType::Utf8, false),
        ]))
    }

    /// Trích xuất Gold Features từ Silver Kill Events và Silver Player Match RecordBatches
    pub fn generate_gold(
        silver_ke: &RecordBatch,
        silver_pm: &RecordBatch,
    ) -> Result<RecordBatch> {
        let pm_num_rows = silver_pm.num_rows();

        let pm_match_id_arr = silver_pm
            .column_by_name("match_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let pm_player_id_arr = silver_pm
            .column_by_name("player_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let pm_kills_arr = silver_pm
            .column_by_name("kills")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        // Phân nhóm kill events theo (match_id, killer_name)
        struct KillRecord {
            event_time_secs: Option<f64>,
            killer_pos_x: Option<f64>,
            killer_pos_y: Option<f64>,
            victim_pos_x: Option<f64>,
            victim_pos_y: Option<f64>,
            weapon: Option<String>,
        }

        let mut kills_by_player: HashMap<(String, String), Vec<KillRecord>> = HashMap::new();

        let ke_num_rows = silver_ke.num_rows();
        let ke_match_id_arr = silver_ke
            .column_by_name("match_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let ke_killer_name_arr = silver_ke
            .column_by_name("killer_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let ke_time_secs_arr = silver_ke
            .column_by_name("event_time_seconds")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ke_kp_x = silver_ke
            .column_by_name("killer_position_x")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ke_kp_y = silver_ke
            .column_by_name("killer_position_y")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ke_vp_x = silver_ke
            .column_by_name("victim_position_x")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ke_vp_y = silver_ke
            .column_by_name("victim_position_y")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ke_weapon_arr = silver_ke
            .column_by_name("weapon")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        for r in 0..ke_num_rows {
            let m_id = ke_match_id_arr
                .map(|a| a.value(r).to_string())
                .unwrap_or_default();
            let k_name = ke_killer_name_arr
                .filter(|a| !a.is_null(r))
                .map(|a| a.value(r).to_string())
                .unwrap_or_default();

            if !m_id.is_empty() && !k_name.is_empty() {
                kills_by_player
                    .entry((m_id, k_name))
                    .or_default()
                    .push(KillRecord {
                        event_time_secs: ke_time_secs_arr.filter(|a| !a.is_null(r)).map(|a| a.value(r)),
                        killer_pos_x: ke_kp_x.filter(|a| !a.is_null(r)).map(|a| a.value(r)),
                        killer_pos_y: ke_kp_y.filter(|a| !a.is_null(r)).map(|a| a.value(r)),
                        victim_pos_x: ke_vp_x.filter(|a| !a.is_null(r)).map(|a| a.value(r)),
                        victim_pos_y: ke_vp_y.filter(|a| !a.is_null(r)).map(|a| a.value(r)),
                        weapon: ke_weapon_arr.filter(|a| !a.is_null(r)).map(|a| a.value(r).to_string()),
                    });
            }
        }

        let mut g_match_ids = Vec::with_capacity(pm_num_rows);
        let mut g_player_ids = Vec::with_capacity(pm_num_rows);
        let mut g_kills = Vec::with_capacity(pm_num_rows);
        let mut g_min_intervals = Vec::with_capacity(pm_num_rows);
        let mut g_median_distances = Vec::with_capacity(pm_num_rows);
        let mut g_short_interval_counts = Vec::with_capacity(pm_num_rows);
        let mut g_unique_weapons = Vec::with_capacity(pm_num_rows);
        let mut g_feature_versions = Vec::with_capacity(pm_num_rows);
        let mut g_created_ats = Vec::with_capacity(pm_num_rows);

        let created_at_iso = Utc::now().to_rfc3339();

        for r in 0..pm_num_rows {
            let m_id = pm_match_id_arr
                .map(|a| a.value(r).to_string())
                .unwrap_or_default();
            let p_id = pm_player_id_arr
                .map(|a| a.value(r).to_string())
                .unwrap_or_default();
            let kills_count = pm_kills_arr
                .filter(|a| !a.is_null(r))
                .map(|a| a.value(r))
                .unwrap_or(0);

            g_match_ids.push(m_id.clone());
            g_player_ids.push(p_id.clone());

            let player_kills = kills_by_player.get(&(m_id, p_id));

            if let Some(records) = player_kills {
                g_kills.push(records.len() as i64);

                // 1. Tính toán khoảng thời gian giữa các lần kill (Kill Intervals)
                let mut times: Vec<f64> = records
                    .iter()
                    .filter_map(|rec| rec.event_time_secs)
                    .collect();
                times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

                let mut intervals = Vec::new();
                for i in 1..times.len() {
                    let diff = times[i] - times[i - 1];
                    if diff >= 0.0 {
                        intervals.push(diff);
                    }
                }

                let min_interval = if intervals.is_empty() {
                    None
                } else {
                    intervals.iter().cloned().fold(f64::INFINITY, f64::min).into()
                };
                g_min_intervals.push(min_interval);

                let short_count = intervals.iter().filter(|&&dt| dt <= 10.0).count() as i64;
                g_short_interval_counts.push(short_count);

                // 2. Tính toán khoảng cách Euclidean đến nạn nhân
                let mut distances = Vec::new();
                for rec in records {
                    if let (Some(kx), Some(ky), Some(vx), Some(vy)) = (
                        rec.killer_pos_x,
                        rec.killer_pos_y,
                        rec.victim_pos_x,
                        rec.victim_pos_y,
                    ) {
                        let dist = ((kx - vx).powi(2) + (ky - vy).powi(2)).sqrt();
                        if !dist.is_nan() && !dist.is_infinite() {
                            distances.push(dist);
                        }
                    }
                }

                let median_dist = if distances.is_empty() {
                    None
                } else {
                    distances.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let mid = distances.len() / 2;
                    let med = if distances.len() % 2 == 0 {
                        (distances[mid - 1] + distances[mid]) / 2.0
                    } else {
                        distances[mid]
                    };
                    Some(med)
                };
                g_median_distances.push(median_dist);

                // 3. Đếm số loại vũ khí độc nhất đã dùng
                let unique_w: HashSet<&str> = records
                    .iter()
                    .filter_map(|rec| rec.weapon.as_deref())
                    .filter(|w| !w.is_empty())
                    .collect();
                g_unique_weapons.push(unique_w.len() as i64);
            } else {
                g_kills.push(kills_count);
                g_min_intervals.push(None);
                g_median_distances.push(None);
                g_short_interval_counts.push(0);
                g_unique_weapons.push(0);
            }

            g_feature_versions.push("v1.0.0".to_string());
            g_created_ats.push(created_at_iso.clone());
        }

        RecordBatch::try_new(
            Self::gold_schema(),
            vec![
                Arc::new(StringArray::from(
                    g_match_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    g_player_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(g_kills)),
                Arc::new(Float64Array::from(g_min_intervals)),
                Arc::new(Float64Array::from(g_median_distances)),
                Arc::new(Int64Array::from(g_short_interval_counts)),
                Arc::new(Int64Array::from(g_unique_weapons)),
                Arc::new(StringArray::from(
                    g_feature_versions.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    g_created_ats.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| AppError::Arrow(format!("Khởi tạo gold RecordBatch thất bại: {}", e)))
    }
}
