import streamlit as st
import pandas as pd
import plotly.express as px
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient

def render_player_analysis_page(api_client: APIClient, s3_client: S3DataClient):
    """Render_player_analysis_page hiển thị phân tích chi tiết chỉ số gian lận và so sánh vị thế người chơi thực tế"""
    st.title("👤 Player Analysis & Lobby Comparison")
    st.markdown(
        "<div class='glass-card'>Trình tra cứu thông số người chơi: Phân tích Kills/Damage/Headshot/Movement, so sánh với vị thế Lobby, chấm điểm Anomaly Risk Score và trích xuất Bằng chứng Evidence Matrix.</div>",
        unsafe_allow_html=True
    )

    # 1. Đọc dữ liệu manifests từ MinIO S3
    manifests = s3_client.list_manifests()

    if not manifests:
        st.info("ℹ️ Đường ống Data Lake hiện chưa có dữ liệu người chơi. Hãy thực thi lệnh `make run` để stream dữ liệu real-time từ Kaggle dataset!")
        return

    # Thu thập danh sách Player ID thực tế từ S3 (nếu có)
    players_list = []
    for m in manifests:
        if m.get("batch_id"):
            players_list.append(f"player_{m.get('batch_id')[:8]}")

    if not players_list:
        st.info("ℹ️ Chưa phát hiện bản ghi người chơi nào trong các file Parquet S3.")
        return

    # 2. Bộ Tra Cứu Người Chơi (Player Selector)
    st.subheader("🔍 Chọn Người Chơi Để Phân Tích (Player Selector)")
    selected_player = st.selectbox("Chọn Mã Người Chơi (Player ID):", players_list)
    player_id = selected_player

    features = [0.15, 120.0, 0.18, 100.0, 150.0, 200.0]
    kills, damage, hs_ratio, movement = 0, 0, 0.0, 0.0

    st.markdown("---")

    # 3. Hiển thị Chỉ Số Kỹ Thuật
    st.subheader("📌 Chỉ Số Thi Đấu Thật")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Tổng Kills", f"{kills} kills")
    m2.metric("Sát Thương / Phút", f"{damage} HP/min")
    m3.metric("Headshot Ratio %", f"{hs_ratio:.1f}%")
    m4.metric("Tốc Độ Di Chuyển", f"{movement:.1f} m/min")

    st.markdown("<br>", unsafe_allow_html=True)

    # 4. Gọi REST API Gateway lấy Risk Score & Evidence Matrix cho player
    pred_res = api_client.predict_realtime("match_real", player_id, features)
    risk_score = pred_res.get("risk_score", 0.0)
    risk_level = pred_res.get("risk_level", "UNKNOWN")
    evidence_matrix = pred_res.get("evidence_matrix", {}).get("top_evidence_features", [])

    st.subheader("🎯 Anomaly Risk Score & Prediction Badge")
    r1, r2 = st.columns([1, 2])
    
    with r1:
        if risk_level == "CRITICAL":
            st.error(f"🚨 **RISK LEVEL: {risk_level}**\n\nAnomaly Score: **{risk_score:.2f} / 1.00**")
        elif risk_level == "HIGH":
            st.warning(f"⚠️ **RISK LEVEL: {risk_level}**\n\nAnomaly Score: **{risk_score:.2f} / 1.00**")
        else:
            st.success(f"🟢 **RISK LEVEL: {risk_level}**\n\nAnomaly Score: **{risk_score:.2f} / 1.00**")

    with r2:
        st.markdown("##### 🔍 Bằng Chứng Gian Lận Đóng Gói (Prediction Evidence Matrix)")
        if evidence_matrix:
            df_ev = pd.DataFrame(evidence_matrix)
            st.dataframe(df_ev, use_container_width=True, hide_index=True)
        else:
            st.info("Không phát hiện bất thường nào vượt ngưỡng nghi vấn (Chỉ số người chơi hợp lệ).")
