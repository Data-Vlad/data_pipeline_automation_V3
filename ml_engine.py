import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression

class MLEngine:
    """
    A lightweight ML engine for automated pipeline analytics.
    """

    @staticmethod
    def detect_anomalies(df: pd.DataFrame, value_col: str, contamination: float = 0.05) -> pd.DataFrame:
        """
        Uses Isolation Forest to flag anomalous rows in the dataset.
        """
        # Robustness: Check for empty or insufficient data (Isolation Forest needs samples)
        if df.empty or len(df) < 5:
            return df

        # Robustness: Handle missing values to prevent fitting errors
        df = df.dropna(subset=[value_col]).copy()

        model = IsolationForest(contamination=contamination, random_state=42)
        df['anomaly_score'] = model.fit_predict(df[[value_col]])
        
        # -1 indicates anomaly, 1 indicates normal. Convert to boolean.
        df['is_anomaly'] = df['anomaly_score'].apply(lambda x: 1 if x == -1 else 0)
        
        return df

    @staticmethod
    def generate_forecast(df: pd.DataFrame, date_col: str, value_col: str, periods: int = 30) -> pd.DataFrame:
        """
        Generates a simple linear trend forecast for the next 'periods' days.
        """
        # Robustness: Check for empty or insufficient data (Regression needs >1 point)
        if df.empty or len(df) < 2:
            return pd.DataFrame()

        # Robustness: Handle missing values
        df = df.dropna(subset=[date_col, value_col]).copy()

        # Prepare data for linear regression
        df = df.sort_values(by=date_col)
        df['timestamp_numeric'] = pd.to_datetime(df[date_col]).map(pd.Timestamp.timestamp)
        
        X = df[['timestamp_numeric']].values
        y = df[value_col].values

        model = LinearRegression()
        model.fit(X, y)

        # Create future dates
        last_date = pd.to_datetime(df[date_col].max())
        future_dates = [last_date + pd.Timedelta(days=x) for x in range(1, periods + 1)]
        
        future_df = pd.DataFrame({date_col: future_dates})
        future_df['timestamp_numeric'] = future_df[date_col].map(pd.Timestamp.timestamp)
        
        # Predict
        future_df['predicted_value'] = model.predict(future_df[['timestamp_numeric']].values)
        future_df['model_type'] = 'forecast'
        
        return future_df[[date_col, 'predicted_value', 'model_type']]

    @staticmethod
    def generate_insights(df: pd.DataFrame, value_col: str) -> str:
        """
        Generates a natural language summary of the data (Mock AI).
        """
        mean_val = df[value_col].mean()
        max_val = df[value_col].max()
        trend = "increasing" if df[value_col].iloc[-1] > df[value_col].iloc[0] else "decreasing"
        
        return f"The average value is {mean_val:.2f} with a peak of {max_val:.2f}. The overall trend appears to be {trend}."