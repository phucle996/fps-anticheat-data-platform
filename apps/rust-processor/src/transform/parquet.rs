use crate::error::{AppError, Result};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// ParquetSerializer chịu trách nhiệm nạp nén Zstandard và ghi/đọc định dạng Parquet
pub struct ParquetSerializer;

impl ParquetSerializer {
    /// Record_batch_to_parquet_bytes mã hóa RecordBatch thành chuỗi byte Parquet nén Zstandard
    pub fn record_batch_to_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // 1. Cấu hình WriterProperties với nén Zstandard (ZSTD) mặc định
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        // 2. Khởi tạo Parquet ArrowWriter
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).map_err(|e| {
                AppError::Parquet(format!("Khởi tạo Parquet ArrowWriter thất bại: {}", e))
            })?;

        // 3. Ghi RecordBatch
        writer.write(batch).map_err(|e| {
            AppError::Parquet(format!("Ghi RecordBatch sang Parquet thất bại: {}", e))
        })?;

        // 4. Đóng Writer để hoàn tất metadata footer Parquet
        writer
            .close()
            .map_err(|e| AppError::Parquet(format!("Đóng Parquet ArrowWriter thất bại: {}", e)))?;

        Ok(buffer)
    }

    /// Read_parquet_bytes đọc lại chuỗi byte Parquet phục vụ xác minh dữ liệu round-trip 100%
    pub fn read_parquet_bytes(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
        let bytes_data = Bytes::copy_from_slice(bytes);

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes_data).map_err(|e| {
            AppError::Parquet(format!(
                "Tạo ParquetRecordBatchReaderBuilder thất bại: {}",
                e
            ))
        })?;

        let reader = builder.build().map_err(|e| {
            AppError::Parquet(format!("Tạo Parquet RecordBatchReader thất bại: {}", e))
        })?;

        let mut batches = Vec::new();
        for batch_res in reader {
            let batch = batch_res.map_err(|e| {
                AppError::Parquet(format!(
                    "Đọc RecordBatch từ Parquet byte stream thất bại: {}",
                    e
                ))
            })?;
            batches.push(batch);
        }

        Ok(batches)
    }
}
