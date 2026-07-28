# R Config Loader Module - Nạp cấu hình biến môi trường Fail-Close cho R Engine
# ==============================================================================

load_config <- function() {
  get_required_env <- function(key) {
    val <- Sys.getenv(key)
    if (nchar(trimws(val)) == 0) {
      stop(sprintf("[ERROR] Thiếu biến môi trường bắt buộc '%s' (Fail-Close Triggered)", key))
    }
    return(val)
  }

  list(
    minio_endpoint   = get_required_env("MINIO_ENDPOINT"),
    minio_bucket     = get_required_env("MINIO_BUCKET"),
    minio_access_key = get_required_env("MINIO_ACCESS_KEY"),
    minio_secret_key = get_required_env("MINIO_SECRET_KEY")
  )
}
