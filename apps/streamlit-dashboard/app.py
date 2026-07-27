import os
import streamlit as st
from src.config import DashboardConfig
from src.services.api_client import APIClient

# Cấu hình Trang Streamlit & Theme Cyberpunk Dark Mode
st.set_page_config(
    page_title="PUBG PC Anti-Cheat Data Platform",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS Inject - Glassmorphism & Vibrant Dark Theme
st.markdown("""
<style>
    .main {
        background-color: #0b0f19;
        color: #e2e8f0;
        font-family: 'Inter', sans-serif;
    }
    .stMetric {
        background: rgba(30, 41, 59, 0.7);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 15px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        backdrop-filter: blur(8px);
    }
    .glass-card {
        background: rgba(15, 23, 42, 0.75);
        border: 1px solid rgba(56, 189, 248, 0.2);
        border-radius: 16px;
        padding: 24px;
        backdrop-filter: blur(12px);
        margin-bottom: 20px;
    }
    h1, h2, h3 {
        color: #38bdf8 !important;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

# 1. Nạp cấu hình Fail-Close 100%
@st.cache_resource
def get_dashboard_config() -> DashboardConfig:
    try:
        return DashboardConfig.from_env()
    except Exception as e:
        st.error(f"🚨 FAIL-CLOSE TRIGGERED: {e}")
        st.stop()

config = get_dashboard_config()
api_client = APIClient(config)

# 2. Navigation Sidebar
st.sidebar.title("🛡️ PUBG Anti-Cheat")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Danh mục hệ thống:",
    [
        "📊 Overview & Pipeline Health",
        "🧹 Preprocessing Before vs After",
        "👤 Player Analysis & Lobby Comparison",
        "🎯 Risk Analysis & Prediction Explorer"
    ]
)

# Health Status Badge ở Sidebar
health_data = api_client.check_health()
if health_data.get("status") == "UP":
    st.sidebar.success("🟢 Go API Gateway: ONLINE")
else:
    st.sidebar.error(f"🔴 Go API Gateway: OFFLINE ({health_data.get('error', '')})")

st.sidebar.markdown("---")
st.sidebar.caption("⚡ FPS Anti-Cheat Data Platform v1.0")

# 3. Router nội dung trang
if page == "📊 Overview & Pipeline Health":
    st.title("📊 Overview & Pipeline Health Monitor")
    st.markdown("<div class='glass-card'>Trang tổng quan đo lường hiệu năng và sức khỏe hệ thống đường ống dữ liệu (Pipeline Health).</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    summary = api_client.get_dataset_summary()
    
    col1.metric("Tổng bản ghi thô (Raw)", f"{summary.get('total_raw_records', 0):,}")
    col2.metric("Bản ghi sạch (Silver)", f"{summary.get('clean_silver_records', 0):,}")
    col3.metric("Bản ghi không hợp lệ (Invalid)", f"{summary.get('invalid_records', 0):,}", delta="-1.5%", delta_color="inverse")
    col4.metric("Gold Features Version", summary.get("feature_version", "v1.0"))

elif page == "🧹 Preprocessing Before vs After":
    st.title("🧹 Preprocessing Before vs After Explorer")
    st.info("Trình khám phá và so sánh chất lượng dữ liệu màng lọc 1-1 Trước vs Sau khi tiền xử lý.")

elif page == "👤 Player Analysis & Lobby Comparison":
    st.title("👤 Player Analysis & Lobby Comparison")
    st.info("Phân tích chi tiết chỉ số gian lận người chơi và so sánh vị thế với Lobby.")

elif page == "🎯 Risk Analysis & Prediction Explorer":
    st.title("🎯 Risk Analysis & Prediction Explorer")
    st.info("Trích xuất danh sách dự báo Anomaly Risk Score và Bằng chứng nghi vấn gian lận Evidence Matrix.")
