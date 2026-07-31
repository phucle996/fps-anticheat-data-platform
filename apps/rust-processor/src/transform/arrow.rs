use crate::domain::AnyEnvelope;
use crate::error::{AppError, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// ArrowConverter quản lý khởi tạo Arrow Schema và chuyển đổi AnyEnvelope sang Apache Arrow RecordBatch
pub struct ArrowConverter;

impl ArrowConverter {
    /// Build_schema tạo định nghĩa Arrow Schema với đầy đủ trường kill telemetry từ match_deaths
    pub fn build_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("schema_version", DataType::Utf8, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("event_time", DataType::Utf8, true),
            Field::new("ingest_time", DataType::Utf8, false),
            Field::new("match_id", DataType::Utf8, false),
            Field::new("player_id", DataType::Utf8, false),
            Field::new("source_provider", DataType::Utf8, false),
            Field::new("source_dataset_id", DataType::Utf8, false),
            Field::new("source_file", DataType::Utf8, false),
            Field::new("source_record_index", DataType::Int64, false),
            Field::new("killer_name", DataType::Utf8, true),
            Field::new("victim_name", DataType::Utf8, true),
            Field::new("killer_placement", DataType::Int64, true),
            Field::new("victim_placement", DataType::Int64, true),
            Field::new("killer_position_x", DataType::Float64, true),
            Field::new("killer_position_y", DataType::Float64, true),
            Field::new("victim_position_x", DataType::Float64, true),
            Field::new("victim_position_y", DataType::Float64, true),
            Field::new("event_time_seconds", DataType::Float64, true),
            Field::new("weapon", DataType::Utf8, true),
            Field::new("kills", DataType::Int64, true),
            Field::new("damage_dealt", DataType::Float64, true),
            Field::new("headshot_kills", DataType::Int64, true),
            Field::new("walk_distance", DataType::Float64, true),
            Field::new("ride_distance", DataType::Float64, true),
            Field::new("swim_distance", DataType::Float64, true),
            Field::new("survival_duration", DataType::Float64, true),
            Field::new("win_place_perc", DataType::Float64, true),
        ]))
    }

    /// Events_to_record_batch ánh xạ mảng AnyEnvelope thành Apache Arrow RecordBatch dạng cột
    pub fn events_to_record_batch(events: &[AnyEnvelope]) -> Result<RecordBatch> {
        let schema = Self::build_schema();
        let len = events.len();

        let mut event_id_vec = Vec::with_capacity(len);
        let mut schema_ver_vec = Vec::with_capacity(len);
        let mut op_vec = Vec::with_capacity(len);
        let mut event_time_vec = Vec::with_capacity(len);
        let mut ingest_time_vec = Vec::with_capacity(len);
        let mut match_id_vec = Vec::with_capacity(len);
        let mut player_id_vec = Vec::with_capacity(len);
        let mut src_provider_vec = Vec::with_capacity(len);
        let mut src_dataset_id_vec = Vec::with_capacity(len);
        let mut src_file_vec = Vec::with_capacity(len);
        let mut src_rec_idx_vec = Vec::with_capacity(len);

        let mut killer_name_vec = Vec::with_capacity(len);
        let mut victim_name_vec = Vec::with_capacity(len);
        let mut killer_place_vec = Vec::with_capacity(len);
        let mut victim_place_vec = Vec::with_capacity(len);
        let mut killer_x_vec = Vec::with_capacity(len);
        let mut killer_y_vec = Vec::with_capacity(len);
        let mut victim_x_vec = Vec::with_capacity(len);
        let mut victim_y_vec = Vec::with_capacity(len);
        let mut event_time_sec_vec = Vec::with_capacity(len);
        let mut weapon_vec = Vec::with_capacity(len);
        let mut kills_vec = Vec::with_capacity(len);
        let mut damage_dealt_vec = Vec::with_capacity(len);
        let mut headshot_kills_vec = Vec::with_capacity(len);
        let mut walk_distance_vec = Vec::with_capacity(len);
        let mut ride_distance_vec = Vec::with_capacity(len);
        let mut swim_distance_vec = Vec::with_capacity(len);
        let mut survival_duration_vec = Vec::with_capacity(len);
        let mut win_place_perc_vec = Vec::with_capacity(len);

        for e in events {
            match e {
                AnyEnvelope::Kill(k) => {
                    event_id_vec.push(k.event_id.as_str());
                    schema_ver_vec.push(k.schema_version.as_str());
                    op_vec.push(k.op.as_str());
                    event_time_vec.push(k.event_time.as_deref());
                    ingest_time_vec.push(k.ingest_time.as_str());
                    match_id_vec.push(k.match_id.as_str());
                    player_id_vec.push(k.player_id.as_str());
                    src_provider_vec.push(k.source.provider.as_str());
                    src_dataset_id_vec.push(k.source.dataset_id.as_str());
                    src_file_vec.push(k.source.source_file.as_str());
                    src_rec_idx_vec.push(k.source.record_index);

                    killer_name_vec.push(k.payload.killer_name.as_deref());
                    victim_name_vec.push(k.payload.victim_name.as_deref());
                    killer_place_vec.push(k.payload.killer_placement.map(|v| v as i64));
                    victim_place_vec.push(k.payload.victim_placement.map(|v| v as i64));
                    killer_x_vec.push(k.payload.killer_position_x);
                    killer_y_vec.push(k.payload.killer_position_y);
                    victim_x_vec.push(k.payload.victim_position_x);
                    victim_y_vec.push(k.payload.victim_position_y);
                    event_time_sec_vec.push(k.payload.event_time_seconds);
                    weapon_vec.push(k.payload.weapon.as_deref());

                    kills_vec.push(None);
                    damage_dealt_vec.push(None);
                    headshot_kills_vec.push(None);
                    walk_distance_vec.push(None);
                    ride_distance_vec.push(None);
                    swim_distance_vec.push(None);
                    survival_duration_vec.push(None);
                    win_place_perc_vec.push(None);
                }
                AnyEnvelope::PlayerStat(p) => {
                    event_id_vec.push(p.event_id.as_str());
                    schema_ver_vec.push(p.schema_version.as_str());
                    op_vec.push(p.op.as_str());
                    event_time_vec.push(p.event_time.as_deref());
                    ingest_time_vec.push(p.ingest_time.as_str());
                    match_id_vec.push(p.match_id.as_str());
                    player_id_vec.push(p.player_id.as_str());
                    src_provider_vec.push(p.source.provider.as_str());
                    src_dataset_id_vec.push(p.source.dataset_id.as_str());
                    src_file_vec.push(p.source.source_file.as_str());
                    src_rec_idx_vec.push(p.source.record_index);

                    killer_name_vec.push(Some(p.player_id.as_str()));
                    victim_name_vec.push(None);
                    killer_place_vec.push(None);
                    victim_place_vec.push(None);
                    killer_x_vec.push(None);
                    killer_y_vec.push(None);
                    victim_x_vec.push(None);
                    victim_y_vec.push(None);
                    event_time_sec_vec.push(None);
                    weapon_vec.push(None);

                    // Hai envelope dùng chung Bronze schema; nullable columns giữ nguyên
                    // ranh giới contract thay vì giả lập player-stat thành kill telemetry.
                    kills_vec.push(Some(p.payload.kills));
                    damage_dealt_vec.push(Some(p.payload.damage_dealt));
                    headshot_kills_vec.push(Some(p.payload.headshot_kills));
                    walk_distance_vec.push(Some(p.payload.walk_distance));
                    ride_distance_vec.push(Some(p.payload.ride_distance));
                    swim_distance_vec.push(Some(p.payload.swim_distance));
                    survival_duration_vec.push(Some(p.payload.survival_duration));
                    win_place_perc_vec.push(p.payload.win_place_perc);
                }
            }
        }

        let arr_event_id = StringArray::from(event_id_vec);
        let arr_schema_ver = StringArray::from(schema_ver_vec);
        let arr_op = StringArray::from(op_vec);
        let arr_event_time = StringArray::from(event_time_vec);
        let arr_ingest_time = StringArray::from(ingest_time_vec);
        let arr_match_id = StringArray::from(match_id_vec);
        let arr_player_id = StringArray::from(player_id_vec);
        let arr_src_provider = StringArray::from(src_provider_vec);
        let arr_src_dataset_id = StringArray::from(src_dataset_id_vec);
        let arr_src_file = StringArray::from(src_file_vec);
        let arr_src_rec_idx = Int64Array::from(src_rec_idx_vec);

        let arr_killer_name = StringArray::from(killer_name_vec);
        let arr_victim_name = StringArray::from(victim_name_vec);
        let arr_killer_place = Int64Array::from(killer_place_vec);
        let arr_victim_place = Int64Array::from(victim_place_vec);
        let arr_killer_x = Float64Array::from(killer_x_vec);
        let arr_killer_y = Float64Array::from(killer_y_vec);
        let arr_victim_x = Float64Array::from(victim_x_vec);
        let arr_victim_y = Float64Array::from(victim_y_vec);
        let arr_event_time_sec = Float64Array::from(event_time_sec_vec);
        let arr_weapon = StringArray::from(weapon_vec);
        let arr_kills = Int64Array::from(kills_vec);
        let arr_damage_dealt = Float64Array::from(damage_dealt_vec);
        let arr_headshot_kills = Int64Array::from(headshot_kills_vec);
        let arr_walk_distance = Float64Array::from(walk_distance_vec);
        let arr_ride_distance = Float64Array::from(ride_distance_vec);
        let arr_swim_distance = Float64Array::from(swim_distance_vec);
        let arr_survival_duration = Float64Array::from(survival_duration_vec);
        let arr_win_place_perc = Float64Array::from(win_place_perc_vec);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arr_event_id),
                Arc::new(arr_schema_ver),
                Arc::new(arr_op),
                Arc::new(arr_event_time),
                Arc::new(arr_ingest_time),
                Arc::new(arr_match_id),
                Arc::new(arr_player_id),
                Arc::new(arr_src_provider),
                Arc::new(arr_src_dataset_id),
                Arc::new(arr_src_file),
                Arc::new(arr_src_rec_idx),
                Arc::new(arr_killer_name),
                Arc::new(arr_victim_name),
                Arc::new(arr_killer_place),
                Arc::new(arr_victim_place),
                Arc::new(arr_killer_x),
                Arc::new(arr_killer_y),
                Arc::new(arr_victim_x),
                Arc::new(arr_victim_y),
                Arc::new(arr_event_time_sec),
                Arc::new(arr_weapon),
                Arc::new(arr_kills),
                Arc::new(arr_damage_dealt),
                Arc::new(arr_headshot_kills),
                Arc::new(arr_walk_distance),
                Arc::new(arr_ride_distance),
                Arc::new(arr_swim_distance),
                Arc::new(arr_survival_duration),
                Arc::new(arr_win_place_perc),
            ],
        )
        .map_err(|e| AppError::Arrow(format!("Khởi tạo Arrow RecordBatch thất bại: {}", e)))
    }
}
