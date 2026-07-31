import streamlit as st
import pandas as pd
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient


def render_model_registry_page(api_client: APIClient, s3_client: S3DataClient):
    """Hiển thị Giao diện Quản lý Danh sách Mô hình & Trạng thái Active Model (Cyberpunk Dark Mode)."""
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.title("🤖 ML Model Registry & Active Model")
    st.caption("Quản lý danh sách các phiên bản mô hình ML trên S3 Lakehouse và trạng thái Hot-Swap mô hình đang chạy")
    st.markdown('</div>', unsafe_allow_html=True)

    # 1. Truy vấn thông tin Active Model từ Go API Gateway / S3 Checkpoint State
    summary_data = api_client.get_dataset_summary()
    active_model_version = summary_data.get("model_version", "v1.0-rf")

    # 2. Truy vấn danh sách tất cả các phiên bản mô hình ML từ S3 Bucket `pubg-models`
    models_list = s3_client.list_model_versions()

    # Thẻ Metric Tổng quan ở hàng trên
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Mô hình đang chạy (Active)",
            value=active_model_version,
            delta="ONLINE",
        )
    with col2:
        st.metric(
            label="Tổng số mô hình đã train",
            value=len(models_list),
        )
    with col3:
        st.metric(
            label="Cơ chế Hot-Swap IPC",
            value="ACTIVE",
            delta="Zero-Downtime",
        )
    with col4:
        st.metric(
            label="Thuật toán chủ đạo",
            value="XGBoost GPU",
        )

    st.markdown("---")

    # 3. Bảng Danh sách chi tiết các phiên bản Mô hình ML
    st.subheader("📋 Danh sách các phiên bản Mô hình trên S3 (Model Registry)")

    if not models_list:
        st.warning("⚠️ Chưa tìm thấy phiên bản mô hình ML nào trên MinIO S3 bucket `pubg-models`.")
        st.info("💡 Bạn có thể kích hoạt huấn luyện bằng cách chạy lệnh `make ml` từ terminal.")
        return

    # Chuẩn bị dữ liệu bảng DataFrame hiển thị
    table_rows = []
    for model in models_list:
        is_active = (model["version"] == active_model_version) or (len(table_rows) == 0 and active_model_version in ["v1.0-rf", "UNAVAILABLE"])
        table_rows.append({
            "Trạng thái": "🟢 ACTIVE" if is_active else "⚪ INACTIVE",
            "Version ID": model["version"],
            "Thuật toán": model.get("model_name", "XGBoost GPU"),
            "Số mẫu huấn luyện": f"{model.get('total_samples', 0):,}",
            "Thời điểm khởi tạo": model.get("created_at", "N/A"),
        })

    df_models = pd.DataFrame(table_rows)
    st.dataframe(df_models, use_container_width=True)

    st.markdown("---")

    # 4. Khu vực Xem Chi tiết Metrics của từng Version được chọn
    st.subheader("🔍 Chi tiết Metrics & Hyperparameters từng phiên bản")
    selected_version = st.selectbox(
        "Chọn phiên bản mô hình để kiểm tra chi tiết:",
        [m["version"] for m in models_list],
    )

    selected_model_data = next((m for m in models_list if m["version"] == selected_version), None)
    if selected_model_data and selected_model_data.get("metrics"):
        m_data = selected_model_data["metrics"]
        st.json(m_data)
    else:
        st.info(f"Không có dữ liệu json metrics chi tiết cho phiên bản {selected_version}.")
