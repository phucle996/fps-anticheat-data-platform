use crate::domain::EventEnvelope;
use crate::error::{AppError, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// ArrowConverter quản lý khởi tạo Arrow Schema và chuyển đổi EventEnvelope/KillEventEnvelope sang Apache Arrow RecordBatch
pub struct ArrowConverter;

impl ArrowConverter {
    /// Build_schema tạo định nghĩa Arrow Schema 19 cột chuẩn hóa với đầy đủ thông tin telemetry
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
            Field::new("kills", DataType::Int64, false),
            Field::new("damage_dealt", DataType::Float64, false),
            Field::new("headshot_kills", DataType::Int64, false),
            Field::new("walk_distance", DataType::Float64, false),
            Field::new("ride_distance", DataType::Float64, false),
            Field::new("swim_distance", DataType::Float64, false),
            Field::new("survival_duration", DataType::Float64, false),
            Field::new("win_place_perc", DataType::Float64, true),
        ]))
    }

    /// Events_to_record_batch ánh xạ danh sách EventEnvelope thành Apache Arrow RecordBatch dạng cột
    pub fn events_to_record_batch(events: &[EventEnvelope]) -> Result<RecordBatch> {
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

        let mut kills_vec = Vec::with_capacity(len);
        let mut damage_vec = Vec::with_capacity(len);
        let mut headshot_vec = Vec::with_capacity(len);
        let mut walk_vec = Vec::with_capacity(len);
        let mut ride_vec = Vec::with_capacity(len);
        let mut swim_vec = Vec::with_capacity(len);
        let mut survival_vec = Vec::with_capacity(len);
        let mut win_place_vec = Vec::with_capacity(len);

        for e in events {
            event_id_vec.push(e.event_id.as_str());
            schema_ver_vec.push(e.schema_version.as_str());
            op_vec.push(e.op.as_str());
            event_time_vec.push(e.event_time.as_deref());
            ingest_time_vec.push(e.ingest_time.as_str());
            match_id_vec.push(e.match_id.as_str());
            player_id_vec.push(e.player_id.as_str());
            src_provider_vec.push(e.source.provider.as_str());
            src_dataset_id_vec.push(e.source.dataset_id.as_str());
            src_file_vec.push(e.source.source_file.as_str());
            src_rec_idx_vec.push(e.source.record_index);

            kills_vec.push(e.payload.kills);
            damage_vec.push(e.payload.damage_dealt);
            headshot_vec.push(e.payload.headshot_kills);
            walk_vec.push(e.payload.walk_distance);
            ride_vec.push(e.payload.ride_distance);
            swim_vec.push(e.payload.swim_distance);
            survival_vec.push(e.payload.survival_duration);
            win_place_vec.push(e.payload.win_place_perc);
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

        let arr_kills = Int64Array::from(kills_vec);
        let arr_damage = Float64Array::from(damage_vec);
        let arr_headshot = Int64Array::from(headshot_vec);
        let arr_walk = Float64Array::from(walk_vec);
        let arr_ride = Float64Array::from(ride_vec);
        let arr_swim = Float64Array::from(swim_vec);
        let arr_survival = Float64Array::from(survival_vec);
        let arr_win_place = Float64Array::from(win_place_vec);

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
                Arc::new(arr_kills),
                Arc::new(arr_damage),
                Arc::new(arr_headshot),
                Arc::new(arr_walk),
                Arc::new(arr_ride),
                Arc::new(arr_swim),
                Arc::new(arr_survival),
                Arc::new(arr_win_place),
            ],
        )
        .map_err(|e| AppError::Arrow(format!("Khởi tạo Arrow RecordBatch thất bại: {}", e)))
    }
}
