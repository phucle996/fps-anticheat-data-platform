import streamlit as st
import pandas as pd
import plotly.express as px
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient

def render_player_analysis_page(api_client: APIClient, s3_client: S3DataClient):
    """Render_player_analysis_page hiển thị phân tích chi tiết chỉ số gian lận và so sánh vị thế người chơi thực tế 100% từ MinIO S3 (Zero Fake Data)"""
    st.title("👤 Player Analysis & Lobby Comparison")
    st.markdown(
        "<div class='glass-card'>Trình tra cứu thông số người chơi: Phân tích Kills/Damage/Headshot/Movement, so sánh với vị thế Lobby, chấm điểm Anomaly Risk Score và trích xuất Bằng chứng Evidence Matrix từ dữ liệu S3 Lakehouse thực tế.</div>",
        unsafe_allow_html=True
    )

    # 1. Đọc dữ liệu Gold Features thực tế từ MinIO S3 Lakehouse
    df_gold = s3_client.load_gold_features_dataframe()

    if df_gold is None or len(df_gold) == 0:
        st.info("ℹ️ Đường ống S3 Data Lake hiện chưa có dữ liệu Gold Features. Hãy thực thi lệnh `make run` để stream dữ liệu real-time từ Kaggle dataset!")
        return

    # Thu thập danh sách Player ID thực tế từ Gold DataFrame
    player_col = "player_id" if "player_id" in df_gold.columns else df_gold.columns[0]
    match_col = "match_id" if "match_id" in df_gold.columns else "match_id"

    unique_players = df_gold[player_col].dropna().unique().tolist()
    if not unique_players:
        st.info("ℹ️ Chưa phát hiện bản ghi người chơi nào trong các file Parquet S3.")
        return

    # 2. Bộ Tra Cứu Người Chơi (Player Selector)
    st.subheader("🔍 Chọn Người Chơi Để Phân Tích (Player Selector)")
    selected_player = st.selectbox("Chọn Mã Người Chơi (Player ID):", unique_players[:100])

    # Lấy bản ghi thực tế của người chơi được chọn
    p_row = df_gold[df_gold[player_col] == selected_player].iloc[0]
    match_id = str(p_row.get(match_col, "match_s3"))

    # Extract 5 Gold Feature Contract metrics thực tế
    kills = float(p_row.get("kills", 0.0))
    min_interval = float(p_row.get("minimum_kill_interval_seconds", 0.0))
    median_dist = float(p_row.get("median_kill_distance_coordinate_units", 0.0))
    short_cnt = float(p_row.get("short_kill_interval_count", 0.0))
    unique_weapons = float(p_row.get("unique_weapons_used", 0.0))

    features = [kills, min_interval, median_dist, short_cnt, unique_weapons]

    st.markdown("---")

    # 3. Hiển thị Chỉ Số Kỹ Thuật Thực Tế từ MinIO S3
    st.subheader("📌 Chỉ Số Thi Đấu Thật (S3 Gold Lakehouse)")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Tổng Kills", f"{int(kills)} kills")
    m2.metric("Khoảng Cách Hạ Gục Trung Bình", f"{median_dist:.1f} m")
    m3.metric("Min Kill Interval", f"{min_interval:.2f} s")
    m4.metric("Short Kill Intervals", f"{int(short_cnt)}")
    m5.metric("Số Loại Vũ Khí Dùng", f"{int(unique_weapons)}")

    st.markdown("<br>", unsafe_allow_html=True)

    # 4. Gọi REST API Gateway lấy Anomaly Risk Score & Evidence Matrix thực tế
    pred_res = api_client.predict_realtime(match_id, selected_player, features)
    risk_score = float(pred_res.get("risk_score", 0.0))
    risk_level = str(pred_res.get("risk_level", "NORMAL"))
    model_version = str(pred_res.get("model_version", "ONNX"))
    evidence_matrix = pred_res.get("evidence_matrix", {}).get("top_evidence_features", [])

    st.subheader("🎯 Anomaly Risk Score & Prediction Badge")
    r1, r2 = st.columns([1, 2])

    with r1:
        if risk_level == "CRITICAL":
            st.error(f"🚨 **RISK LEVEL: {risk_level}**\n\nAnomaly Score: **{risk_score:.2f} / 1.00**\n\nModel Ver: `{model_version}`")
        elif risk_level == "HIGH":
            st.warning(f"⚠️ **RISK LEVEL: {risk_level}**\n\nAnomaly Score: **{risk_score:.2f} / 1.00**\n\nModel Ver: `{model_version}`")
        else:
            st.success(f"🟢 **RISK LEVEL: {risk_level}**\n\nAnomaly Score: **{risk_score:.2f} / 1.00**\n\nModel Ver: `{model_version}`")

    with r2:
        st.markdown("##### 🔍 Bằng Chứng Gian Lận Đóng Gói (Prediction Evidence Matrix)")
        if evidence_matrix:
            df_ev = pd.DataFrame(evidence_matrix)
            st.dataframe(df_ev, use_container_width=True, hide_index=True)
        else:
            st.info("Không phát hiện bất thường nào vượt ngưỡng nghi vấn (Chỉ số người chơi hợp lệ).")
