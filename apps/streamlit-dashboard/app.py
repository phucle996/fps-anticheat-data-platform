import streamlit as st
from src.config import DashboardConfig
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient
from src.views.overview import render_overview_page
from src.views.preprocessing import render_preprocessing_page
from src.views.player_analysis import render_player_analysis_page
from src.views.risk_analysis import render_risk_analysis_page
from src.views.model_registry import render_model_registry_page

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
s3_client = S3DataClient(config)

# 2. Navigation Sidebar
st.sidebar.title("🛡️ PUBG Anti-Cheat")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Danh mục hệ thống:",
    [
        "📊 Overview & Pipeline Health",
        "🧹 Preprocessing Before vs After",
        "👤 Player Analysis & Lobby Comparison",
        "🎯 Risk Analysis & Prediction Explorer",
        "🤖 ML Model Registry & Active Model",
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
    render_overview_page(api_client, s3_client)

elif page == "🧹 Preprocessing Before vs After":
    render_preprocessing_page(api_client, s3_client)

elif page == "👤 Player Analysis & Lobby Comparison":
    render_player_analysis_page(api_client, s3_client)

elif page == "🎯 Risk Analysis & Prediction Explorer":
    render_risk_analysis_page(api_client, s3_client)

elif page == "🤖 ML Model Registry & Active Model":
    render_model_registry_page(api_client, s3_client)
