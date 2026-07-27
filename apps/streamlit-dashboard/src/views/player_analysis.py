import streamlit as st
import pandas as pd
import plotly.express as px
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient

def render_player_analysis_page(api_client: APIClient, s3_client: S3DataClient):
    """Render_player_analysis_page hiển thị phân tích chi tiết chỉ số gian lận và so sánh vị thế người chơi với Lobby"""
    st.title("👤 Player Analysis & Lobby Comparison")
    st.markdown(
        "<div class='glass-card'>Trình tra cứu thông số người chơi: Phân tích Kills/Damage/Headshot/Movement, so sánh với vị thế Lobby, chấm điểm Anomaly Risk Score và trích xuất Bằng chứng Evidence Matrix.</div>",
        unsafe_allow_html=True
    )

    # 1. Bộ Tra Cứu Người Chơi (Player Selector)
    st.subheader("🔍 Chọn Người Chơi Để Phân Tích (Player Selector)")
    players_list = ["player_suspect_99 (Nghi vấn cao)", "player_alpha_01 (Thường)", "player_veteran_05 (Thường)"]
    selected_player = st.selectbox("Chọn Mã Người Chơi (Player ID):", players_list)

    player_id = selected_player.split(" ")[0]

    # Giả lập tập đặc trưng Gold features dựa trên người chơi được chọn
    if "suspect" in player_id:
        features = [1.50, 750.0, 0.833, 500.0, 250.0, 800.0]
        kills, damage, hs_ratio, movement = 18, 1450, 83.3, 250.0
    else:
        features = [0.15, 120.0, 0.18, 100.0, 150.0, 200.0]
        kills, damage, hs_ratio, movement = 2, 240, 18.0, 150.0

    st.markdown("---")

    # 2. Hiển thị Chỉ Số Kỹ Thuật (Metrics Cards)
    st.subheader("📌 Chỉ Số Thi Đấu Trung Bình")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Tổng Kills", f"{kills} kills")
    m2.metric("Sát Thương / Phút", f"{damage} HP/min")
    m3.metric("Headshot Ratio %", f"{hs_ratio:.1f}%")
    m4.metric("Tốc Độ Di Chuyển", f"{movement:.1f} m/min")

    st.markdown("<br>", unsafe_allow_html=True)

    # 3. Gọi REST API Gateway lấy Risk Score & Evidence Matrix
    pred_res = api_client.predict_realtime("match_100", player_id, features)
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

    st.markdown("---")

    # 4. Biểu đồ So sánh Chỉ số Với Lobby (Lobby Comparison Chart)
    st.subheader("📊 So Sánh 6 Đặc Trưng Gold Với Trung Bình Lobby (Lobby Comparison)")
    
    gold_feature_names = [
        "Kills / min", "Damage / min", "Headshot Ratio", 
        "Damage / kill", "Movement / min", "Perf vs Lobby"
    ]
    lobby_avgs = [0.15, 120.0, 0.18, 100.0, 150.0, 200.0]

    comp_data = []
    for idx, f_name in enumerate(gold_feature_names):
        comp_data.append({"Đặc trưng ML": f_name, "Đối tượng": "Người chơi", "Giá trị": features[idx]})
        comp_data.append({"Đặc trưng ML": f_name, "Đối tượng": "Lobby Average", "Giá trị": lobby_avgs[idx]})

    df_lobby = pd.DataFrame(comp_data)
    fig_lobby = px.bar(
        df_lobby,
        x="Đặc trưng ML",
        y="Giá trị",
        color="Đối tượng",
        barmode="group",
        title="So sánh chi tiết 6 đặc trưng Gold Features với trung bình cả trận đấu",
        color_discrete_map={"Người chơi": "#f43f5e" if risk_level == "CRITICAL" else "#38bdf8", "Lobby Average": "#94a3b8"},
        template="plotly_dark"
    )
    fig_lobby.update_layout(
        paper_bgcolor="rgba(15, 23, 42, 0.75)",
        plot_bgcolor="rgba(15, 23, 42, 0.75)",
        font=dict(color="#e2e8f0")
    )
    st.plotly_chart(fig_lobby, use_container_width=True)

    st.markdown("---")

    # 5. Bảng Lịch Sử Trận Đấu (Match History Table)
    st.subheader("📜 Lịch Sử Trận Đấu Gần Đây (Match History)")
    history_data = [
        {"Match ID": "match_100", "Kills": kills, "Damage": damage, "Headshot %": f"{hs_ratio}%", "Risk Score": risk_score, "Risk Level": risk_level},
        {"Match ID": "match_099", "Kills": max(1, kills - 2), "Damage": max(100, damage - 150), "Headshot %": f"{hs_ratio - 2.0:.1f}%", "Risk Score": risk_score, "Risk Level": risk_level},
        {"Match ID": "match_098", "Kills": max(1, kills - 4), "Damage": max(100, damage - 300), "Headshot %": f"{hs_ratio - 5.0:.1f}%", "Risk Score": max(0.1, risk_score - 0.1), "Risk Level": risk_level},
    ]
    st.dataframe(pd.DataFrame(history_data), use_container_width=True, hide_index=True)
