import hashlib

# Validation logic cho Gold Event & Model Versioning (Fail-Close 100%)

def validate_gold_event(payload: dict) -> None:
    """Kiểm tra tính hợp lệ của Kafka Gold Event Payload theo nguyên tắc Fail-Close 100%"""
    if payload.get("schema_version") != "1.0":
        raise ValueError("[FAIL-CLOSE] Gold event schema_version không được hỗ trợ!")
    if payload.get("op") != "data.dataset.gold.ready":
        raise ValueError("[FAIL-CLOSE] Gold event op không hợp lệ!")
    
    event_id = payload.get("event_id", "")
    checksum = payload.get("checksum_sha256", "")
    object_uri = payload.get("object_uri", "")

    # Kiểm tra định dạng hex SHA-256 (64 ký tự hex)
    if len(event_id) != 64 or any(char not in "0123456789abcdef" for char in event_id):
        raise ValueError("[FAIL-CLOSE] Gold event_id phải là SHA-256 hex 64 ký tự!")
    if len(checksum) != 64 or any(char not in "0123456789abcdef" for char in checksum):
        raise ValueError("[FAIL-CLOSE] Gold checksum_sha256 phải là SHA-256 hex 64 ký tự!")
    if not object_uri.startswith("s3://"):
        raise ValueError("[FAIL-CLOSE] Gold object_uri phải có dạng s3:// URI!")

def compute_model_version(payload: dict) -> str:
    """Tạo phiên bản Model ID ổn định và duy nhất từ event_id và checksum_sha256 để đảm bảo tính Idempotent"""
    event_id = payload["event_id"]
    checksum = payload["checksum_sha256"]
    # Băm kết hợp event_id và checksum để tạo model version id 16 ký tự hex
    return f"v-{hashlib.sha256(f'{event_id}|{checksum}'.encode()).hexdigest()[:16]}"
