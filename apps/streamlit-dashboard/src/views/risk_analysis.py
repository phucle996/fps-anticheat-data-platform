import streamlit as st
import pandas as pd
import plotly.express as px
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient

def render_risk_analysis_page(api_client: APIClient, s3_client: S3DataClient):
    """Render_risk_analysis_page hiển thị bảng kết quả dự báo suy luận, bộ lọc tương tác và biểu đồ phân bổ rủi ro"""
    st.title("🎯 Risk Analysis & Prediction Explorer")
    st.markdown(
        "<div class='glass-card'>Trình quản trị và phân tích điểm nguy cơ Anomaly Risk Score: Tìm kiếm, lọc theo cấp độ rủi ro, kiểm tra biểu đồ phân bổ score và trích xuất chi tiết bằng chứng gian lận Evidence Matrix.</div>",
        unsafe_allow_html=True
    )

    # 1. Bộ Lọc Tương Tác (Interactive Filters)
    st.subheader("🔍 Bộ Lọc & Sắp Xếp Dự Báo (Filters & Sorting)")
    f_col1, f_col2, f_col3 = st.columns(3)
    
    selected_level = f_col1.selectbox("Lọc theo Risk Level:", ["Tất cả (All Levels)", "CRITICAL", "HIGH", "MEDIUM", "LOW"])
    selected_model = f_col2.selectbox("Lọc theo Model Version:", ["Tất cả (All Models)", "v1.0-rf (Random Forest)", "v1.0-hgb (HistGradient)", "v1.0-iso (Isolation Forest)"])
    sort_order = f_col3.selectbox("Sắp xếp theo Risk Score:", ["Giảm dần (Descending)", "Tăng dần (Ascending)"])

    st.markdown("---")

    # Giả lập tập danh sách dữ liệu dự báo mẫu (Predictions Dataset)
    predictions_data = [
        {"Match ID": "match_100", "Player ID": "player_suspect_99", "Risk Score": 0.95, "Risk Level": "CRITICAL", "Model Version": "v1.0-rf", "Top Evidence": "Headshot Ratio 83.3% vs Lobby Avg 18.0%"},
        {"Match ID": "match_100", "Player ID": "player_hacker_88", "Risk Score": 0.88, "Risk Level": "CRITICAL", "Model Version": "v1.0-rf", "Top Evidence": "Damage/min 750.0 HP vs Lobby Avg 120.0"},
        {"Match ID": "match_099", "Player ID": "player_aimbot_77", "Risk Score": 0.82, "Risk Level": "CRITICAL", "Model Version": "v1.0-rf", "Top Evidence": "Headshot Ratio 78.5% (Z-score: +4.2)"},
        {"Match ID": "match_101", "Player ID": "player_suspect_66", "Risk Score": 0.74, "Risk Level": "HIGH", "Model Version": "v1.0-rf", "Top Evidence": "Kills/min 1.20 vs Lobby Avg 0.15"},
        {"Match ID": "match_102", "Player ID": "player_speed_55", "Risk Score": 0.68, "Risk Level": "HIGH", "Model Version": "v1.0-rf", "Top Evidence": "Movement 280m/min vs Lobby Avg 150m/min"},
        {"Match ID": "match_101", "Player ID": "player_pro_44", "Risk Score": 0.45, "Risk Level": "MEDIUM", "Model Version": "v1.0-rf", "Top Evidence": "Damage/min 320.0 HP vs Lobby Avg 120.0"},
        {"Match ID": "match_103", "Player ID": "player_alpha_01", "Risk Score": 0.15, "Risk Level": "LOW", "Model Version": "v1.0-rf", "Top Evidence": "Chỉ số bình thường"},
        {"Match ID": "match_103", "Player ID": "player_veteran_05", "Risk Score": 0.12, "Risk Level": "LOW", "Model Version": "v1.0-rf", "Top Evidence": "Chỉ số bình thường"},
    ]
    df_pred = pd.DataFrame(predictions_data)

    # 2. Áp dụng Bộ Lọc và Sắp Xếp
    if selected_level != "Tất cả (All Levels)":
        df_pred = df_pred[df_pred["Risk Level"] == selected_level]

    if selected_model != "Tất cả (All Models)":
        model_tag = selected_model.split(" ")[0]
        df_pred = df_pred[df_pred["Model Version"] == model_tag]

    ascending = (sort_order == "Tăng dần (Ascending)")
    df_pred = df_pred.sort_values(by="Risk Score", ascending=ascending)

    # 3. Biểu đồ Phân Bổ Điểm Risk Score (Distribution Chart)
    st.subheader("📊 Biểu Đồ Phân Bổ Điểm Anomaly Risk Score (Score Distribution)")
    d_col1, d_col2 = st.columns([1, 1])

    with d_col1:
        fig_hist = px.histogram(
            df_pred,
            x="Risk Score",
            nbins=10,
            title="Phân bổ tần suất điểm Risk Score (0.0 đến 1.0)",
            color="Risk Level",
            color_discrete_map={"CRITICAL": "#f43f5e", "HIGH": "#fb923c", "MEDIUM": "#facc15", "LOW": "#38bdf8"},
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
            color_discrete_map={"CRITICAL": "#f43f5e", "HIGH": "#fb923c", "MEDIUM": "#facc15", "LOW": "#38bdf8"}
        )
        fig_donut.update_layout(
            paper_bgcolor="rgba(15, 23, 42, 0.75)",
            font=dict(color="#e2e8f0")
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    st.markdown("---")

    # 4. Bảng Kết Quả Dự Báo Suy Luận (Predictions Table)
    st.subheader(f"📋 Bảng Kết Quả Suy Luận AI/ML ({len(df_pred)} bản ghi)")
    st.dataframe(df_pred, use_container_width=True, hide_index=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # 5. Trình Xem Chi Tiết Bằng Chứng (Prediction Details & Evidence Inspector)
    st.subheader("🔍 Chi Tiết Bằng Chứng Gian Lận Theo Người Chơi")
    selected_row_player = st.selectbox("Chọn người chơi để xem bằng chứng chi tiết:", df_pred["Player ID"].tolist())

    if selected_row_player:
        player_row = df_pred[df_pred["Player ID"] == selected_row_player].iloc[0]
        with st.expander(f"📌 Chi tiết bằng chứng cho {player_row['Player ID']} (Match: {player_row['Match ID']})", expanded=True):
            st.markdown(f"**Risk Level:** `{player_row['Risk Level']}` | **Risk Score:** `{player_row['Risk Score']}` | **Model:** `{player_row['Model Version']}`")
            st.warning(f"**Bằng chứng bất thường nổi bật:** {player_row['Top Evidence']}")
