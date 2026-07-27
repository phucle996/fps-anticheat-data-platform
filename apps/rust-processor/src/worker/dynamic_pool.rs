use super::r_spawner::RWorkerSpawner;
use tracing::info;

/// RDynamicWorkerPool quản lý tập hợp các R Workers động theo mô hình Spark Dynamic Allocation
#[derive(Clone)]
pub struct RDynamicWorkerPool {
    spawner: RWorkerSpawner, // Bộ điều phối async subprocess spawner
    max_workers: usize,      // Ngưỡng tối đa workers song song (Scale từ CPU Cores)
}

impl RDynamicWorkerPool {
    /// New khởi tạo RDynamicWorkerPool với cấu hình sức chứa tối đa
    pub fn new(max_workers: usize) -> Self {
        let workers = if max_workers == 0 { 4 } else { max_workers };
        info!(
            max_workers = workers,
            "Đã khởi tạo Spark-Style RDynamicWorkerPool (Automatic CPU Core Scaling Active)"
        );

        Self {
            spawner: RWorkerSpawner::new(workers),
            max_workers: workers,
        }
    }

    /// Dispatch_manifest điều phối một batch manifest tới R Worker đang rảnh hoặc mở rộng worker mới
    pub fn dispatch_manifest(&self, manifest_path: String) {
        info!(
            manifest = %manifest_path,
            max_capacity = self.max_workers,
            "Dispatching manifest task tới Dynamic R Worker Pool..."
        );
        self.spawner.spawn_worker(manifest_path);
    }

    /// Max_workers trả về giới hạn worker tối đa của pool
    pub fn max_workers(&self) -> usize {
        self.max_workers
    }
}
