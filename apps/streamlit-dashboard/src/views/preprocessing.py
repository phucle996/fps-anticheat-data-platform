import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient

def render_preprocessing_page(api_client: APIClient, s3_client: S3DataClient):
    """Render_preprocessing_page hiển thị trình so sánh màng lọc dữ liệu 1-1 Trước vs Sau tiền xử lý từ dữ liệu thực tế"""
    st.title("🧹 Preprocessing Before vs After Explorer")
    st.markdown(
        "<div class='glass-card'>Trình so sánh chứng minh hiệu quả quá trình làm sạch dữ liệu: Xử lý Ô trống (Missing Values), Khử trùng (Duplicates), Phân tích Lý do Loại bỏ (Invalid Reasons) và So sánh 1-1 Thông lượng.</div>",
        unsafe_allow_html=True
    )

    # 1. Truy vấn danh sách Batch Manifests và Invalid Records thực tế từ MinIO S3
    manifests = s3_client.list_manifests()
    invalid_records = s3_client.load_invalid_records()

    if not manifests:
        st.info("ℹ️ Đường ống Data Lake hiện chưa có dữ liệu tiền xử lý. Hãy thực thi lệnh `make run` để stream dữ liệu real-time từ Kaggle dataset!")
        return

    # Tính toán thông số thực tế từ manifests
    total_read = sum(m.get("total_records_read", 0) for m in manifests)
    total_valid = sum(m.get("valid_records_count", 0) for m in manifests)
    total_invalid = sum(m.get("invalid_records_count", 0) for m in manifests)
    total_duplicate = sum(m.get("duplicate_records_count", 0) for m in manifests)
    retention_rate = (total_valid / total_read * 100) if total_read > 0 else 0.0

    # 2. Bộ Lọc Tương Tác (Interactive Filters)
    st.subheader("🔍 Bộ Lọc Dữ Liệu Tương Tác")
    f_col1, f_col2 = st.columns(2)
    selected_source = f_col1.selectbox("Chọn File Nguồn (Source File):", ["Tất cả (All Sources)"])
    batch_ids = [m.get("batch_id", "") for m in manifests if m.get("batch_id")]
    selected_batch = f_col2.selectbox("Chọn Batch ID:", ["Tất cả (All Batches)"] + batch_ids)

    st.markdown("---")

    # 3. Thống kê Ô trống & Duplicates thực tế
    st.subheader("1. Ô Trống (Missing Values) & Khử Trùng Lặp (Deduplication)")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Tổng Bản Ghi Tiêu Thụ", f"{total_read:,} bản ghi")
    m2.metric("Bản Ghi Hợp Lệ (Silver)", f"{total_valid:,} bản ghi", delta="100% Clean")
    m3.metric("Bản Ghi Trùng Lặp (Duplicates)", f"{total_duplicate:,} bản ghi", delta_color="inverse")
    m4.metric("Tỷ Lệ Giữ Lại Dữ Liệu (Retention)", f"{retention_rate:.2f}%")

    st.markdown("<br>", unsafe_allow_html=True)

    # 4. Phân tích Invalid Records thực tế từ S3
    st.subheader("2. Phân Tích Lý Do Loại Bỏ Bản Ghi Bất Hợp Lệ (Invalid Reasons Breakdown)")
    inv_col1, inv_col2 = st.columns([1, 1])

    if not invalid_records:
        st.success("🟢 Không phát hiện bản ghi bị loại bỏ (Invalid Record) nào trong Data Lake S3.")
    else:
        # Gom nhóm lý do loại bỏ thực tế từ invalid_records
        reasons_map = {}
        for rec in invalid_records:
            reason = rec.get("error_reason", rec.get("reason", "Lỗi Schema / Validation không xác định"))
            reasons_map[reason] = reasons_map.get(reason, 0) + 1
        
        invalid_reasons_data = []
        for r_name, r_count in reasons_map.items():
            invalid_reasons_data.append({
                "Lý do loại bỏ (Invalid Reason)": r_name,
                "Số bản ghi": r_count,
                "Tỷ lệ": f"{(r_count / len(invalid_records) * 100):.1f}%"
            })
        df_invalid = pd.DataFrame(invalid_reasons_data)

        with inv_col1:
            st.markdown("##### 📋 Bảng Thống Kế Lỗi Schema & Validation Thật")
            st.dataframe(df_invalid, use_container_width=True, hide_index=True)

        with inv_col2:
            fig_pie = px.pie(
                df_invalid,
                names="Lý do loại bỏ (Invalid Reason)",
                values="Số bản ghi",
                title="Biểu đồ tỷ lệ lý do bản ghi bất hợp lệ",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_pie.update_layout(
                paper_bgcolor="rgba(15, 23, 42, 0.75)",
                font=dict(color="#e2e8f0")
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown("---")

    # 5. Biểu đồ So sánh 1-1 Thông lượng Bản ghi theo Batch thực tế
    st.subheader("3. Biểu Đồ So Sánh 1-1 Số Lượng Bản Ghi Theo Batch (Raw vs Silver/Gold)")
    
    batch_comp_data = []
    for m in manifests:
        b_id = m.get("batch_id", "Unknown")
        r_cnt = m.get("total_records_read", 0)
        c_cnt = m.get("valid_records_count", 0)
        batch_comp_data.append({"Batch ID": b_id, "Trạng thái": "Bản ghi thô (Before)", "Số lượng": r_cnt})
        batch_comp_data.append({"Batch ID": b_id, "Trạng thái": "Dữ liệu sạch (After)", "Số lượng": c_cnt})

    df_comp = pd.DataFrame(batch_comp_data)
    if selected_batch != "Tất cả (All Batches)":
        df_comp = df_comp[df_comp["Batch ID"] == selected_batch]

    fig_comp = px.bar(
        df_comp,
        x="Batch ID",
        y="Số lượng",
        color="Trạng thái",
        barmode="group",
        title="So sánh 1-1 Số lượng Bản ghi Ban đầu vs Sau Tiền xử lý theo từng Batch",
        color_discrete_map={"Bản ghi thô (Before)": "#94a3b8", "Dữ liệu sạch (After)": "#38bdf8"},
        template="plotly_dark"
    )
    fig_comp.update_layout(
        paper_bgcolor="rgba(15, 23, 42, 0.75)",
        plot_bgcolor="rgba(15, 23, 42, 0.75)",
        font=dict(color="#e2e8f0")
    )
    st.plotly_chart(fig_comp, use_container_width=True)

    st.markdown("---")

    # 6. Trực quan hóa Telemetry Kill Events & Bản Đồ Bản Bắn 2D (Spatial Kill Map)
    st.subheader("4. Telemetry Kill Events & Bản Đồ Tọa Độ Hạ Gục 2D (Spatial 2D Kill Map)")
    df_kill = s3_client.load_kill_events_dataframe()

    if df_kill is None or df_kill.empty:
        st.info("ℹ️ Chưa có nhật ký Telemetry Kill Events trong S3 Data Lake `silver/kill-events/`. Hãy nạp dữ liệu telemetry để trực quan hóa sơ đồ bắn!")
    else:
        k_col1, k_col2, k_col3 = st.columns(3)
        total_kills = len(df_kill)
        anomaly_kills = len(df_kill[df_kill["telemetry_anomaly"] == True]) if "telemetry_anomaly" in df_kill.columns else 0
        avg_dist = df_kill["distance_euclidean"].mean() if "distance_euclidean" in df_kill.columns else 0.0

        k_col1.metric("Tổng Số Pha Hạ Gục (Kill Events)", f"{total_kills:,}")
        k_col2.metric("Pha Bắn Bất Thường Telemetry", f"{anomaly_kills:,}", delta="Dị thường Euclid" if anomaly_kills > 0 else "An toàn", delta_color="inverse")
        k_col3.metric("Khoảng Cách Euclid TB", f"{avg_dist:.1f} m")

        st.markdown("##### 🗺️ Sơ Đồ Vị Trí Bắn 2D (Killer Position vs Victim Position)")
        
        # Tạo đồ thị 2D Scatter Plot vị trí người bắn và nạn nhân
        fig_map = go.Figure()

        # Thêm điểm người bắn (Killer)
        if "killer_x" in df_kill.columns and "killer_y" in df_kill.columns:
            fig_map.add_trace(go.Scatter(
                x=df_kill["killer_x"],
                y=df_kill["killer_y"],
                mode="markers",
                name="Vị Trí Người Bắn (Killer)",
                marker=dict(size=8, color="#38bdf8", symbol="circle"),
                hovertext=df_kill["killer_name"] if "killer_name" in df_kill.columns else None
            ))

        # Thêm điểm nạn nhân (Victim)
        if "victim_x" in df_kill.columns and "victim_y" in df_kill.columns:
            fig_map.add_trace(go.Scatter(
                x=df_kill["victim_x"],
                y=df_kill["victim_y"],
                mode="markers",
                name="Vị Trí Nạn Nhân (Victim)",
                marker=dict(size=8, color="#f43f5e", symbol="x"),
                hovertext=df_kill["victim_name"] if "victim_name" in df_kill.columns else None
            ))

        fig_map.update_layout(
            title="Bản Đồ Phân Bổ Tọa Độ Euclid 2D Hạ Gục (PUBG Map Space)",
            xaxis_title="Tọa độ X (Meters / Pixels)",
            yaxis_title="Tọa độ Y (Meters / Pixels)",
            paper_bgcolor="rgba(15, 23, 42, 0.75)",
            plot_bgcolor="rgba(15, 23, 42, 0.75)",
            font=dict(color="#e2e8f0"),
            height=500
        )
        st.plotly_chart(fig_map, use_container_width=True)

        st.markdown("##### 📋 Bảng Chi Tiết Nhật Ký Hạ Gục Telemetry Silver")
        st.dataframe(df_kill.head(100), use_container_width=True)
