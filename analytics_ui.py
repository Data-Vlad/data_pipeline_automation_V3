import streamlit as st
import os
from dotenv import load_dotenv
import time
from itsdangerous import URLSafeTimedSerializer

# Import views
from views.dashboard import page_dashboard
from views.ai_analysts import page_conversational_analytics, page_agentic_analyst, page_ai_auto_dashboards
from views.deep_dive import page_predictive_insights, page_root_cause_analysis, page_clustering_segmentation, page_prescriptive_optimization, page_what_if_simulator
from views.advanced_ai import page_semantic_search, page_multi_modal_analysis
from views.data_operations import page_data_explorer, page_data_steward, page_data_observability, page_autonomous_data_repair, page_configuration_manager

# Load environment variables
load_dotenv()

# Page Config
<<<<<<< HEAD
st.set_page_config(page_title="Data and Analytics Launchpad", page_icon="📈", layout="wide")
=======
st.set_page_config(page_title="Analytics Hub", page_icon="📈", layout="wide")

# Database Connection
@st.cache_resource
def get_connection():
    db_connection_str = (
        f"mssql+pyodbc://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_SERVER')}/{os.getenv('DB_DATABASE')}?"
        f"driver={os.getenv('DB_DRIVER')}&TrustServerCertificate={os.getenv('DB_TRUST_SERVER_CERTIFICATE')}"
    )
    return create_engine(db_connection_str)

engine = get_connection()

# --- Caching Helper ---
@st.cache_data(ttl=600) # Cache data for 10 minutes
def run_query(query_str):
    with engine.connect() as conn:
        return pd.read_sql(query_str, conn)
>>>>>>> f85118260c4daa8917ea6a49d8b6583231ad7fc3
 
# --- Authentication & RBAC System ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.user_role = None

# --- SSO Token Validation ---
# Check for a token from simple_ui before showing the login screen.
if not st.session_state.authenticated and "token" in st.query_params:
    token = st.query_params["token"]
    secret_key = os.getenv("SECRET_KEY")
    if secret_key:
        s = URLSafeTimedSerializer(secret_key)
        try:
            # The token is valid for 60 seconds to prevent reuse.
            data = s.loads(token, max_age=60)
            if data.get("user"):
                st.session_state.authenticated = True
                st.session_state.user_role = "Admin"  # Assign role based on token if needed
                # Clear the token from the URL to prevent reuse/bookmarking
                st.query_params.clear()
                st.rerun()
        except Exception:
            st.warning("Invalid or expired access token. Please launch from the main application.")
            time.sleep(2) # Give user time to read the message

if not st.session_state.authenticated:
    st.error("Please sign in via the main Data and Analytics Launchpad application.")
    st.stop() # Block access to the rest of the app

<<<<<<< HEAD
# --- Navigation Setup ---
st.sidebar.title("Data and Analytics Launchpad")
=======
# --- Sidebar ---
st.sidebar.title("Analytics Hub")
>>>>>>> f85118260c4daa8917ea6a49d8b6583231ad7fc3
st.sidebar.caption(f"👤 Role: **{st.session_state.user_role}**")

pg = st.navigation({
    "📊 Overview": [st.Page(page_dashboard, title="Dashboard", icon="🚀")],
    "🤖 AI Analysts": [
        st.Page(page_conversational_analytics, title="Conversational Analytics", icon="💬"),
        st.Page(page_agentic_analyst, title="Agentic Analyst", icon="🕵️"),
        st.Page(page_ai_auto_dashboards, title="AI Auto-Dashboards", icon="✨")
    ],
    "📉 Deep Dive Analytics": [
        st.Page(page_predictive_insights, title="Predictive Insights", icon="🤖"),
        st.Page(page_root_cause_analysis, title="Root Cause Analysis", icon="📉"),
        st.Page(page_clustering_segmentation, title="Clustering & Segmentation", icon="🧩"),
        st.Page(page_prescriptive_optimization, title="Prescriptive Optimization", icon="🎯"),
        st.Page(page_what_if_simulator, title="What-If Simulator", icon="🎛️")
    ],
    "🧠 Advanced AI": [
        st.Page(page_semantic_search, title="Semantic Search", icon="🧠"),
        st.Page(page_multi_modal_analysis, title="Multi-Modal Analysis", icon="📷")
    ],
    "🛠️ Data Operations": [
        st.Page(page_data_explorer, title="Data Explorer", icon="🔎"),
        st.Page(page_data_steward, title="Data Steward", icon="🛡️"),
        st.Page(page_data_observability, title="Data Observability", icon="🩺"),
        st.Page(page_autonomous_data_repair, title="Autonomous Data Repair", icon="🔧"),
        st.Page(page_configuration_manager, title="Configuration Manager", icon="⚙️")
    ]
})

pg.run()