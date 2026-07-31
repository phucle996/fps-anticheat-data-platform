import streamlit as st
import pandas as pd
import plotly.express as px
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient

def render_risk_analysis_page(api_client: APIClient, s3_client: S3DataClient):
    """Render_risk_analysis_page hiển thị bảng kết quả dự báo suy luận thực tế 100% từ MinIO S3 Data Lake (Zero Fake Data)"""
    st.title("🎯 Risk Analysis & Prediction Explorer")
    st.markdown(
        "<div class='glass-card'>Trình quản trị và phân tích điểm nguy cơ Anomaly Risk Score: Tìm kiếm, lọc theo cấp độ rủi ro, kiểm tra biểu đồ phân bổ score và trích xuất chi tiết bằng chứng gian lận Evidence Matrix từ dữ liệu S3 Lakehouse thực tế.</div>",
        unsafe_allow_html=True
    )

    # 1. Đọc dữ liệu Gold Features thực tế từ MinIO S3
    df_gold = s3_client.load_gold_features_dataframe()

    if df_gold is None or len(df_gold) == 0:
        st.info("ℹ️ Đường ống Data Lake hiện chưa có dữ liệu Gold Features. Hãy thực thi lệnh `make run` để stream dữ liệu real-time từ Kaggle dataset!")
        return

    # Xây dựng danh sách predictions thực tế bằng cách duyệt qua các bản ghi trong S3 Gold Data Lake
    player_col = "player_id" if "player_id" in df_gold.columns else df_gold.columns[0]
    match_col = "match_id" if "match_id" in df_gold.columns else "match_id"

    predictions_data = []
    # Giới hạn 50 mẫu tiêu biểu để tối ưu tốc độ render UI
    sample_rows = df_gold.head(50)

    for idx, row in sample_rows.iterrows():
        p_id = str(row.get(player_col, f"player_{idx}"))
        m_id = str(row.get(match_col, f"match_{idx}"))

        kills = float(row.get("kills", 0.0))
        min_interval = float(row.get("minimum_kill_interval_seconds", 0.0))
        median_dist = float(row.get("median_kill_distance_coordinate_units", 0.0))
        short_cnt = float(row.get("short_kill_interval_count", 0.0))
        unique_weapons = float(row.get("unique_weapons_used", 0.0))

        features = [kills, min_interval, median_dist, short_cnt, unique_weapons]

        # Gọi API Gateway lấy Risk Score và Evidence Matrix thực tế cho từng mẫu
        pred_res = api_client.predict_realtime(m_id, p_id, features)
        r_score = float(pred_res.get("risk_score", 0.0))
        r_level = str(pred_res.get("risk_level", "NORMAL"))
        m_ver = str(pred_res.get("model_version", "ONNX"))
        ev_items = pred_res.get("evidence_matrix", {}).get("top_evidence_features", [])
        top_ev = ev_items[0].get("reason", "Chỉ số bình thường") if ev_items else "Chỉ số bình thường"

        predictions_data.append({
            "Match ID": m_id[:12],
            "Player ID": p_id,
            "Risk Score": round(r_score, 3),
            "Risk Level": r_level,
            "Model Version": m_ver,
            "Top Evidence": top_ev
        })

    if not predictions_data:
        st.info("ℹ️ Chưa có dữ liệu dự báo suy luận nào được ghi nhận.")
        return

    df_pred = pd.DataFrame(predictions_data)

    # 2. Bộ Lọc Tương Tác
    st.subheader("🔍 Bộ Lọc & Sắp Xếp Dự Báo (Filters & Sorting)")
    f_col1, f_col2, f_col3 = st.columns(3)

    available_levels = ["Tất cả (All Levels)"] + list(df_pred["Risk Level"].unique())
    available_models = ["Tất cả (All Models)"] + list(df_pred["Model Version"].unique())

    selected_level = f_col1.selectbox("Lọc theo Risk Level:", available_levels)
    selected_model = f_col2.selectbox("Lọc theo Model Version:", available_models)
    sort_order = f_col3.selectbox("Sắp xếp theo Risk Score:", ["Giảm dần (Descending)", "Tăng dần (Ascending)"])

    st.markdown("---")

    # 3. Áp dụng Bộ Lọc và Sắp Xếp
    if selected_level != "Tất cả (All Levels)":
        df_pred = df_pred[df_pred["Risk Level"] == selected_level]

    if selected_model != "Tất cả (All Models)":
        df_pred = df_pred[df_pred["Model Version"] == selected_model]

    ascending = (sort_order == "Tăng dần (Ascending)")
    df_pred = df_pred.sort_values(by="Risk Score", ascending=ascending)

    # 4. Biểu đồ Phân Bổ Điểm Risk Score Thật
    st.subheader("📊 Biểu Đồ Phân Bổ Điểm Anomaly Risk Score Thực Tế (S3 Lakehouse Data)")
    d_col1, d_col2 = st.columns([1, 1])

    with d_col1:
        fig_hist = px.histogram(
            df_pred,
            x="Risk Score",
            nbins=10,
            title="Phân bổ tần suất điểm Risk Score (0.0 đến 1.0)",
            color="Risk Level",
            color_discrete_map={"CRITICAL": "#f43f5e", "HIGH": "#fb923c", "MEDIUM": "#facc15", "LOW": "#38bdf8", "NORMAL": "#38bdf8"},
            template="plotly_dark"
        )
        fig_hist.update_layout(
            paper_bgcolor="rgba(15, 23, 42, 0.75)",
            plot_bgcolor="rgba(15, 23, 42, 0.75)",
            font=dict(color="#e2e8f0")
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    with d_col2:
        fig_donut = px.pie(
            df_pred,
            names="Risk Level",
            title="Tỷ lệ phân bổ theo cấp độ rủi ro Risk Level",
            hole=0.4,
            color="Risk Level",
            color_discrete_map={"CRITICAL": "#f43f5e", "HIGH": "#fb923c", "MEDIUM": "#facc15", "LOW": "#38bdf8", "NORMAL": "#38bdf8"}
        )
        fig_donut.update_layout(
            paper_bgcolor="rgba(15, 23, 42, 0.75)",
            font=dict(color="#e2e8f0")
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    st.markdown("---")

    # 5. Bảng Kết Quả Dự Báo Suy Luận Thực Tế
    st.subheader(f"📋 Bảng Kết Quả Suy Luận AI/ML ({len(df_pred)} bản ghi thực tế)")
    st.dataframe(df_pred, use_container_width=True, hide_index=True)

    st.markdown("---")

    # 6. Live Telemetry Sandbox Test Platform
    st.subheader("⚡ Live Telemetry Test Playground (Thử Nghiệm Rust Policy Engine Real-time)")
    st.markdown(
        "<div class='glass-card'>Nhập thông số Telemetry người chơi để gửi trực tiếp qua Go API Gateway tới Rust Decision Engine và kiểm chứng tốc độ ra quyết định Instant Ban!</div>",
        unsafe_allow_html=True
    )

    sb_c1, sb_c2, sb_c3, sb_c4 = st.columns(4)
    sim_kills = sb_c1.number_input("Số Kills (Kills)", min_value=0, max_value=50, value=8)
    sim_dist = sb_c2.number_input("Max Kill Distance (m)", min_value=0.0, max_value=2000.0, value=150.0)
    sim_burst = sb_c3.number_input("Burst Kill Interval (ms)", min_value=0.0, max_value=10000.0, value=120.0)
    sim_teleport = sb_c4.number_input("Spatial Teleport Score", min_value=0.0, max_value=1.0, value=0.95, step=0.05)

    sb_c5, sb_c6 = st.columns(2)
    sim_hs_streak = sb_c5.number_input("Headshot Streak Count", min_value=0, max_value=30, value=6)
    sim_hs_ratio = sb_c6.slider("Headshot Ratio %", min_value=0.0, max_value=1.0, value=0.85)

    if st.button("🚀 Gửi Request Dự Báo Realtime Tới Rust Engine", type="primary", use_container_width=True):
        sim_features = [
            float(sim_kills), 500.0, float(sim_hs_ratio), 100.0, 10.0, 500.0,
            float(sim_dist), float(sim_dist), float(sim_burst), float(sim_teleport), float(sim_hs_streak)
        ]

        res = api_client.predict_realtime("match_sim", "player_sandbox_01", sim_features)

        st.markdown("##### 📌 Kết Quả Phán Quyết Từ Rust Inference Engine:")
        outcome = res.get("decision_outcome", {})
        if outcome:
            act = outcome.get("action", "UNKNOWN")
            prio = outcome.get("priority", "UNKNOWN")
            reason = outcome.get("reason", "")
            rule = outcome.get("policy_rule", "")

            if act == "PERMANENT_BAN":
                st.error(f"🔴 **HÀNH ĐỘNG: {act}** | **Mức Độ: {prio}**\n\n- **Quy Tắc Match:** `{rule}`\n- **Chi Tiết Lý Do:** {reason}")
            elif act == "SUSPEND_ACCOUNT":
                st.warning(f"🟠 **HÀNH ĐỘNG: {act}** | **Mức Độ: {prio}**\n\n- **Quy Tắc Match:** `{rule}`\n- **Chi Tiết Lý Do:** {reason}")
            else:
                st.success(f"🟢 **HÀNH ĐỘNG: {act}** | **Mức Độ: {prio}**\n\n- **Quy Tắc Match:** `{rule}`\n- **Chi Tiết Lý Do:** {reason}")
        else:
            st.info(f"Phản hồi từ Go Gateway: Risk Score = {res.get('risk_score', 0.0):.2f}")
