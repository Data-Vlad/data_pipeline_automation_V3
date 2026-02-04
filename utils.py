import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv

# Add project root to path to import core modules
sys.path.append(os.path.dirname(__file__))
from elt_project.core.ml_engine import MLEngine

# Load environment variables
load_dotenv()

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

# Export common objects
__all__ = ['engine', 'run_query', 'MLEngine']