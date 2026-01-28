import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
try:
    from prophet import Prophet
    PROPHET_INSTALLED = True
except ImportError:
    PROPHET_INSTALLED = False
    
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
    def generate_forecast(df: pd.DataFrame, date_col: str, value_col: str, periods: int = 30, context=None) -> pd.DataFrame:
        """
        Generates a forecast using Prophet if available, otherwise falls back to Linear Regression.
        """
        # Robustness: Check for empty or insufficient data
        if df.empty or len(df) < 2:
            return pd.DataFrame()

        # Robustness: Handle missing values
        df = df.dropna(subset=[date_col, value_col]).copy()

        # Rename columns for Prophet compatibility
        df.rename(columns={date_col: 'ds', value_col: 'y'}, inplace=True)
        df['ds'] = pd.to_datetime(df['ds'])

        if PROPHET_INSTALLED:
            if context: context.log.info("Using Prophet for forecasting.")
            model = Prophet()
            model.fit(df)
            future = model.make_future_dataframe(periods=periods)
            forecast = model.predict(future)
            # Return only future predictions
            forecast_df = forecast.iloc[-periods:].copy()
            forecast_df.rename(columns={'ds': 'prediction_date', 'yhat': 'predicted_value'}, inplace=True)
            forecast_df['model_type'] = 'forecast'
            return forecast_df[['prediction_date', 'predicted_value', 'model_type']]
        else:
            # Fallback to Linear Regression if Prophet is not installed
            if context: context.log.warning("Prophet library not found. Falling back to simple Linear Regression.")
            result_df = MLEngine._linear_regression_forecast(df.rename(columns={'ds': date_col, 'y': value_col}), date_col, value_col, periods)
            result_df['model_type'] = 'forecast'
            return result_df
        
    
    @staticmethod
    def _linear_regression_forecast(df: pd.DataFrame, date_col: str, value_col: str, periods: int = 30) -> pd.DataFrame:
        """Internal method for simple linear trend forecast."""
        df = df.sort_values(by=date_col).copy()
        df['timestamp_numeric'] = pd.to_datetime(df[date_col]).map(pd.Timestamp.timestamp)
        X = df[['timestamp_numeric']].values
        y = df[value_col].values
        model = LinearRegression()
        model.fit(X, y)
        last_date = pd.to_datetime(df[date_col].max())
        future_dates = [last_date + pd.Timedelta(days=x) for x in range(1, periods + 1)]
        future_df = pd.DataFrame({'prediction_date': future_dates})
        future_df['timestamp_numeric'] = future_df['prediction_date'].map(pd.Timestamp.timestamp)
        future_df['predicted_value'] = model.predict(future_df[['timestamp_numeric']].values)
        return future_df

    @staticmethod
    def generate_insights(df: pd.DataFrame, value_col: str) -> str:
        """
        Generates a natural language summary of the data (Mock AI).
        """
        mean_val = df[value_col].mean()
        max_val = df[value_col].max()
        trend = "increasing" if df[value_col].iloc[-1] > df[value_col].iloc[0] else "decreasing"
        
        return f"The average value is {mean_val:.2f} with a peak of {max_val:.2f}. The overall trend appears to be {trend}."

    @staticmethod
    def recommend_visualization(df: pd.DataFrame) -> dict:
        """
        Analyzes the dataframe structure and content to recommend the best visualization.
        Returns a dictionary with 'type', 'x', 'y', 'title', and 'reasoning'.
        """
        if df.empty:
            return {"type": "none", "reasoning": "Dataset is empty."}

        # Identify column types
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        # Heuristic for datetime: check dtypes or try converting object cols
        datetime_cols = df.select_dtypes(include=['datetime', 'datetimetz']).columns.tolist()
        if not datetime_cols:
            for col in df.select_dtypes(include=['object']).columns:
                try:
                    # Check first few non-null rows to see if they look like dates
                    sample = df[col].dropna().iloc[:10]
                    if not sample.empty and pd.to_datetime(sample, errors='coerce').notna().all():
                        datetime_cols.append(col)
                except:
                    pass
        
        cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        cat_cols = [c for c in cat_cols if c not in datetime_cols] # Exclude dates from categories

        # --- Decision Logic ---
        if datetime_cols and numeric_cols:
            return {"type": "time_series", "x": datetime_cols[0], "y": numeric_cols, "title": "Time Series Trends", "reasoning": "Detected date column and numeric metrics."}
        
        elif len(numeric_cols) >= 3:
            return {"type": "correlation_matrix", "cols": numeric_cols, "title": "Correlation Heatmap", "reasoning": "3+ numeric variables detected; correlation analysis is best."}
        
        elif len(numeric_cols) == 2:
            return {"type": "scatter", "x": numeric_cols[0], "y": numeric_cols[1], "title": f"{numeric_cols[0]} vs {numeric_cols[1]}", "reasoning": "Two numeric variables detected; scatter plot shows relationship."}
        
        elif cat_cols and numeric_cols:
            return {"type": "bar", "x": cat_cols[0], "y": numeric_cols[0], "title": f"{numeric_cols[0]} by {cat_cols[0]}", "reasoning": "Categorical data with metrics detected."}
        
        elif numeric_cols:
            return {"type": "histogram", "x": numeric_cols[0], "title": f"Distribution of {numeric_cols[0]}", "reasoning": "Single numeric variable; showing distribution."}
        
        else:
            return {"type": "table", "title": "Raw Data View", "reasoning": "Could not detect clear patterns for visualization."}