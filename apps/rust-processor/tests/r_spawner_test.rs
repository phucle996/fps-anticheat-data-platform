use rust_processor::worker::RWorkerSpawner;

#[test]
fn test_r_worker_runtime_paths_are_resolvable() {
    let (work_dir, script_path) = RWorkerSpawner::resolve_runtime_paths();
    assert!(
        work_dir.is_dir(),
        "R worker workdir phải tồn tại: {}",
        work_dir.display()
    );
    assert!(
        script_path.is_file(),
        "R worker script phải tồn tại: {}",
        script_path.display()
    );
}
