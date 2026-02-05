import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
from faker import Faker

# Load environment variables from the project root
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_db_engine():
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server").replace(' ', '+')
    
    if not all([server, database, username, password]):
        raise ValueError("Missing DB credentials in .env file")

    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes"
    return create_engine(conn_str)

def create_tables(engine):
    print("🔨 Creating Database Tables...")
    with engine.begin() as conn:
        # 1. Analytics Config Table
        conn.execute(text("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='analytics_config' AND xtype='U')
            CREATE TABLE analytics_config (
                id INT IDENTITY(1,1) PRIMARY KEY,
                target_table NVARCHAR(255) NOT NULL,
                date_column NVARCHAR(255) NOT NULL,
                value_column NVARCHAR(255) NOT NULL,
                model_type NVARCHAR(50) NOT NULL,
                alert_webhook_url NVARCHAR(MAX) NULL,
                is_active BIT DEFAULT 1,
                created_at DATETIME DEFAULT GETUTCDATE()
            );
        """))

        # 2. Predictions Table
        conn.execute(text("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='analytics_predictions' AND xtype='U')
            CREATE TABLE analytics_predictions (
                id INT IDENTITY(1,1) PRIMARY KEY,
                run_id NVARCHAR(255),
                target_table NVARCHAR(255),
                model_type NVARCHAR(50),
                prediction_date DATETIME,
                actual_value FLOAT NULL,
                predicted_value FLOAT NULL,
                is_anomaly BIT DEFAULT 0,
                anomaly_score FLOAT NULL,
                created_at DATETIME DEFAULT GETUTCDATE()
            );
        """))

        # 3. Demo Data Table (Fact Table)
        conn.execute(text("""
            IF OBJECT_ID('fact_retail_sales', 'U') IS NOT NULL DROP TABLE fact_retail_sales;
            CREATE TABLE fact_retail_sales (
                TransactionID INT IDENTITY(1,1) PRIMARY KEY,
                TransactionDate DATE,
                Region NVARCHAR(50),
                Category NVARCHAR(50),
                SalesAmount FLOAT,
                MarketingSpend FLOAT,
                DiscountRate FLOAT,
                CustomerFeedback NVARCHAR(MAX)
            );
        """))
        
        # 4. Data Governance Tables (Quality & Enrichment)
        conn.execute(text("""
            IF OBJECT_ID('data_quality_rules', 'U') IS NULL
            CREATE TABLE data_quality_rules (
                rule_id INT IDENTITY(1,1) PRIMARY KEY,
                rule_name NVARCHAR(255),
                description NVARCHAR(MAX),
                target_table NVARCHAR(255),
                target_column NVARCHAR(255),
                check_type NVARCHAR(50),
                severity NVARCHAR(50),
                is_active BIT DEFAULT 1
            );
        """))

        conn.execute(text("""
            IF OBJECT_ID('data_quality_run_logs', 'U') IS NULL
            CREATE TABLE data_quality_run_logs (
                log_id INT IDENTITY(1,1) PRIMARY KEY,
                run_id NVARCHAR(255),
                rule_name NVARCHAR(255),
                status NVARCHAR(50),
                failing_row_count INT,
                details NVARCHAR(MAX),
                check_timestamp DATETIME DEFAULT GETUTCDATE()
            );
        """))

        conn.execute(text("""
            IF OBJECT_ID('data_enrichment_rules', 'U') IS NULL
            CREATE TABLE data_enrichment_rules (
                rule_id INT IDENTITY(1,1) PRIMARY KEY,
                rule_name NVARCHAR(255),
                target_table NVARCHAR(255),
                lookup_table NVARCHAR(255),
                description NVARCHAR(MAX)
            );
        """))


def generate_demo_data(engine):
    print("📊 Generating Synthetic Retail Data (365 Days)...")
    fake = Faker()
    
    data = []
    start_date = datetime.now() - timedelta(days=365)
    
    regions = ['North', 'South', 'East', 'West']
    categories = ['Electronics', 'Clothing', 'Home', 'Beauty']
    
    # Feedback templates for Semantic Search demo
    positive_feedback = ["Great service, really appreciated the quick delivery.", "Loved the product, exactly as described on the website.", "Fast shipping and excellent packaging.", "Excellent quality, will definitely buy again from this vendor."]
    negative_feedback = ["Delivery was late and the driver was rude.", "Item arrived damaged and the box was crushed.", "Rude support when I called to complain about the delay.", "Too expensive for the quality provided, very disappointed.", "Not as described, the color is completely off."]
    
    for i in range(365):
        current_date = start_date + timedelta(days=i)
        is_weekend = current_date.weekday() >= 5
        
        # Create multiple transactions per day
        daily_tx_count = random.randint(20, 50)
        
        for _ in range(daily_tx_count):
            region = random.choice(regions)
            category = random.choice(categories)
            
            # --- 1. Base Logic for Optimization Demo ---
            # Sales is correlated with MarketingSpend and DiscountRate
            marketing_spend = random.uniform(100, 1000)
            discount_rate = random.uniform(0, 0.30)
            
            # Formula: Base + (Spend * Efficiency) + (Discount * Lift) + Noise
            base_sales = 500 if is_weekend else 300
            sales = base_sales + (marketing_spend * 0.5) + (discount_rate * 1000) + random.normalvariate(0, 50)
            
            # --- 2. Inject Anomaly for Root Cause Demo ---
            # Scenario: 3 days ago, "North" region had a server outage affecting "Electronics"
            days_ago = (datetime.now() - current_date).days
            feedback = random.choice(positive_feedback) if random.random() > 0.1 else random.choice(negative_feedback)

            if days_ago == 3: 
                if region == 'North' and category == 'Electronics':
                    sales = sales * 0.2  # 80% drop
                    feedback = "I tried to buy this multiple times but kept getting a 500 server error during checkout. Frustrating!" # Specific text for search
                elif region == 'North':
                    sales = sales * 0.8 # Slight dip elsewhere in North
            
            # --- 3. Inject Text Patterns for Semantic Search ---
            if "late" in feedback or "damaged" in feedback:
                # Correlate bad feedback with lower sales slightly
                sales = sales * 0.9

            data.append({
                "TransactionDate": current_date.date(),
                "Region": region,
                "Category": category,
                "SalesAmount": round(sales, 2),
                "MarketingSpend": round(marketing_spend, 2),
                "DiscountRate": round(discount_rate, 2),
                "CustomerFeedback": feedback
            })
            
    df = pd.DataFrame(data)
    
    # Bulk insert
    df.to_sql('fact_retail_sales', engine, if_exists='append', index=False)
    print(f"✅ Inserted {len(df)} rows into 'fact_retail_sales'.")
    
    # --- Generate Governance Metadata ---
    print("🛡️ Generating Governance Rules & Logs...")
    with engine.begin() as conn:
        # Clear old data
        conn.execute(text("DELETE FROM data_quality_rules"))
        conn.execute(text("DELETE FROM data_quality_run_logs"))
        conn.execute(text("DELETE FROM data_enrichment_rules"))

        # 1. Quality Rules
        conn.execute(text("""
            INSERT INTO data_quality_rules (rule_name, description, target_table, target_column, check_type, severity)
            VALUES 
            ('Sales_Not_Negative', 'SalesAmount must be >= 0', 'fact_retail_sales', 'SalesAmount', 'MIN_VALUE', 'FAIL'),
            ('Valid_Region', 'Region must be North, South, East, or West', 'fact_retail_sales', 'Region', 'IS_IN_SET', 'WARN'),
            ('No_Orphan_Transactions', 'TransactionID must be unique', 'fact_retail_sales', 'TransactionID', 'UNIQUE', 'FAIL')
        """))

        # 2. Enrichment Rules
        conn.execute(text("""
            INSERT INTO data_enrichment_rules (rule_name, target_table, lookup_table, description)
            VALUES 
            ('Enrich_Product_Category', 'stg_sales_upload', 'dim_products', 'Auto-fill Category from SKU if missing'),
            ('Enrich_Customer_Segment', 'stg_sales_upload', 'dim_customers', 'Lookup Segment based on CustomerID')
        """))

        # 3. Simulate Past Quality Logs (Show that the system is working)
        conn.execute(text("""
            INSERT INTO data_quality_run_logs (run_id, rule_name, status, failing_row_count, details, check_timestamp)
            VALUES 
            ('run_abc_123', 'Sales_Not_Negative', 'PASS', 0, 'Check passed.', DATEADD(day, -1, GETUTCDATE())),
            ('run_abc_123', 'Valid_Region', 'PASS', 0, 'Check passed.', DATEADD(day, -1, GETUTCDATE())),
            ('run_xyz_999', 'Sales_Not_Negative', 'FAIL', 12, 'Check failed for 12 rows. Pipeline Halted.', DATEADD(day, -5, GETUTCDATE())),
            ('run_xyz_999', 'Valid_Region', 'FAIL', 45, 'Found 45 rows with invalid regions (e.g. "Narth").', DATEADD(day, -5, GETUTCDATE()))
        """))

def configure_analytics(engine):
    print("⚙️ Configuring AI Hub...")
    with engine.begin() as conn:
        # Clear old config for clean slate
        conn.execute(text("DELETE FROM analytics_config WHERE target_table = 'fact_retail_sales'"))
        
        # Add Anomaly Detection Rule
        conn.execute(text("""
            INSERT INTO analytics_config (target_table, date_column, value_column, model_type, is_active)
            VALUES ('fact_retail_sales', 'TransactionDate', 'SalesAmount', 'anomaly_detection', 1)
        """))
        
        # Add Forecasting Rule
        conn.execute(text("""
            INSERT INTO analytics_config (target_table, date_column, value_column, model_type, is_active)
            VALUES ('fact_retail_sales', 'TransactionDate', 'SalesAmount', 'forecast', 1)
        """))
    print("✅ Analytics configured for 'fact_retail_sales'.")

if __name__ == "__main__":
    try:
        engine = get_db_engine()
        create_tables(engine)
        generate_demo_data(engine)
        configure_analytics(engine)
        print("\n🎉 Demo Environment Setup Complete!")
        print("Next Steps:")
        print("1. Run 'dagster dev' and materialize 'run_predictive_analytics' to generate insights.")
        print("2. Run 'streamlit run analytics_ui.py' to view the dashboard.")
    except Exception as e:
        print(f"❌ Error: {e}")