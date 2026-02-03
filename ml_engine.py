import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
import difflib
import re
import json
import base64
import io
try:
    from prophet import Prophet
    PROPHET_INSTALLED = True
except ImportError:
    PROPHET_INSTALLED = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    STATSMODELS_INSTALLED = True
except ImportError:
    STATSMODELS_INSTALLED = False

try:
    import openai
    OPENAI_INSTALLED = True
except ImportError:
    OPENAI_INSTALLED = False

try:
    from scipy.optimize import minimize
    SCIPY_INSTALLED = True
except ImportError:
    SCIPY_INSTALLED = False
    
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
    def analyze_root_cause(df: pd.DataFrame, date_col: str, value_col: str, target_date, compare_date) -> list:
        """
        Identifies which categorical segments contributed most to the change in value_col between two dates.
        Returns a list of insights (dictionaries).
        """
        insights = []
        
        # Ensure dates are comparable
        df[date_col] = pd.to_datetime(df[date_col])
        target_date = pd.to_datetime(target_date)
        compare_date = pd.to_datetime(compare_date)
        
        # Filter data for the two periods
        target_data = df[df[date_col] == target_date]
        compare_data = df[df[date_col] == compare_date]
        
        if target_data.empty or compare_data.empty:
            return [{"dimension": "N/A", "segment": "N/A", "impact": 0, "reason": "Insufficient data for comparison."}]

        total_target = target_data[value_col].sum()
        total_compare = compare_data[value_col].sum()
        total_delta = total_target - total_compare

        # Identify categorical columns (Dimensions)
        cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        for dim in cat_cols:
            # Group by dimension to find drivers
            t_grp = target_data.groupby(dim)[value_col].sum()
            c_grp = compare_data.groupby(dim)[value_col].sum()
            
            # Align indexes
            all_segments = t_grp.index.union(c_grp.index)
            t_grp = t_grp.reindex(all_segments, fill_value=0)
            c_grp = c_grp.reindex(all_segments, fill_value=0)
            
            deltas = t_grp - c_grp
            
            # Find top contributors (absolute impact)
            sorted_deltas = deltas.abs().sort_values(ascending=False).head(3)
            
            for segment, _ in sorted_deltas.items():
                delta = deltas[segment]
                # Only report significant drivers (> 5% of total change or if total change is small)
                if abs(total_delta) < 1e-9 or abs(delta) / abs(total_delta) > 0.05:
                    insights.append({
                        "dimension": dim,
                        "segment": segment,
                        "impact": delta,
                        "prev_value": c_grp[segment],
                        "curr_value": t_grp[segment],
                        "contribution_pct": (delta / total_delta) * 100 if total_delta != 0 else 0
                    })
        
        # Sort by absolute impact
        insights.sort(key=lambda x: abs(x['impact']), reverse=True)
        return insights

    @staticmethod
    def perform_clustering(df: pd.DataFrame, features: list, n_clusters: int = 3) -> pd.DataFrame:
        """
        Performs K-Means clustering on selected features.
        """
        if df.empty or len(features) < 1:
            return df
            
        # Drop rows with missing values in selected features
        data = df[features].dropna()
        
        model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        data['cluster_id'] = model.fit_predict(data[features])
        
        return data

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
            # Fallback to Holt-Winters or Linear Regression if Prophet is not installed
            if context: context.log.warning("Prophet library not found. Falling back to heuristic forecasting.")
            result_df = MLEngine._heuristic_forecast(df.rename(columns={'ds': date_col, 'y': value_col}), date_col, value_col, periods)
            result_df['model_type'] = 'forecast'
            return result_df
        
    
    @staticmethod
    def _heuristic_forecast(df: pd.DataFrame, date_col: str, value_col: str, periods: int = 30) -> pd.DataFrame:
        """Internal method for forecasting using Holt-Winters (if available) or Linear Regression."""
        df = df.sort_values(by=date_col).copy()
        
        # Try Holt-Winters first (Better for seasonality)
        if STATSMODELS_INSTALLED and len(df) > 10:
            try:
                # Infer frequency
                df = df.set_index(date_col)
                model = ExponentialSmoothing(df[value_col], trend='add', seasonal='add', seasonal_periods=min(len(df)//2, 12)).fit()
                future_predictions = model.forecast(periods)
                future_df = pd.DataFrame({'prediction_date': future_predictions.index, 'predicted_value': future_predictions.values})
                return future_df
            except Exception:
                pass # Fallback to Linear Regression on failure

        # Fallback: Linear Regression
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
    def generate_data_story(metrics_dict: dict, context_str: str = "") -> str:
        """
        Generates a natural language summary of the data using OpenAI if available, 
        otherwise falls back to a heuristic template.
        """
        if OPENAI_INSTALLED and os.getenv("OPENAI_API_KEY"):
            try:
                client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
                prompt = f"""
                You are a Data Analyst. Write a brief, professional executive summary based on these metrics:
                {metrics_dict}
                Context: {context_str}
                Keep it under 4 bullet points. Focus on the 'So What?'.
                """
                completion = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}]
                )
                return completion.choices[0].message.content.strip()
            except Exception as e:
                return f"AI Generation Failed: {e}. Falling back to standard report."
        
        # Fallback Heuristic
        return f"""
        **Executive Summary (Automated)**:
        *   **Success Rate**: {metrics_dict.get('success_rate', 0):.1f}%
        *   **Anomalies**: {metrics_dict.get('anomalies', 0)} detected events requiring attention.
        *   **Volume**: Processed {metrics_dict.get('total_runs', 0)} pipeline runs.
        """

    @staticmethod
    def validate_sql_safety(sql: str) -> bool:
        """Checks SQL for forbidden DDL/DML keywords to prevent injection/accidental deletion."""
        forbidden_patterns = [r"\bDROP\b", r"\bDELETE\b", r"\bUPDATE\b", r"\bINSERT\b", r"\bALTER\b", r"\bTRUNCATE\b", r"\bEXEC\b", r"\bGRANT\b", r"\bREVOKE\b"]
        for pattern in forbidden_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return False
        return True

    @staticmethod
    def generate_sql_from_question(question: str, schema_context: str) -> str:
        """Translates natural language to SQL using OpenAI."""
        if not OPENAI_INSTALLED or not os.getenv("OPENAI_API_KEY"):
            raise ImportError("OpenAI library or API Key is missing.")
            
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": f"You are a SQL Server expert. Return ONLY a valid SQL Server query (no markdown) to answer the question based on this schema:\n{schema_context}"},
                {"role": "user", "content": question}
            ]
        )
        
        sql = completion.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
        
        if not MLEngine.validate_sql_safety(sql):
            raise ValueError("Security Alert: The generated SQL contained forbidden keywords (e.g., DROP, DELETE). Execution blocked.")
            
        return sql

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

    @staticmethod
    def optimize_business_objective(df: pd.DataFrame, target_col: str, input_cols: list, constraints: dict = None) -> dict:
        """
        Prescriptive Analytics: Recommends optimal values for input columns to maximize the target column.
        Uses Linear Regression to model relationships and Scipy to optimize.
        
        :param constraints: dict of {col_name: (min_val, max_val)}
        """
        if not SCIPY_INSTALLED:
            return {"error": "Scipy not installed. Cannot perform optimization."}
        
        if df.empty or len(df) < 10:
            return {"error": "Insufficient data to model relationships."}

        # 1. Train a lightweight model to understand relationships (Coefficients)
        # We assume linear relationships for this basic implementation
        clean_df = df.dropna(subset=[target_col] + input_cols)
        X = clean_df[input_cols].values
        y = clean_df[target_col].values
        
        model = LinearRegression()
        model.fit(X, y)
        
        # 2. Define the Objective Function (Maximize Target -> Minimize Negative Target)
        def objective_function(inputs):
            # Predict target based on inputs
            prediction = model.predict([inputs])[0]
            return -prediction # Negate because we want to maximize

        # 3. Define Bounds (Constraints)
        # If no constraints provided, use min/max from historical data +/- 20%
        bounds = []
        for i, col in enumerate(input_cols):
            if constraints and col in constraints:
                bounds.append(constraints[col])
            else:
                col_min = clean_df[col].min() * 0.8
                col_max = clean_df[col].max() * 1.2
                bounds.append((col_min, col_max))

        # 4. Run Optimization
        initial_guess = [clean_df[col].mean() for col in input_cols]
        result = minimize(objective_function, initial_guess, bounds=bounds, method='SLSQP')
        
        return {
            "optimized_target": -result.fun,
            "recommendations": dict(zip(input_cols, result.x)),
            "success": result.success,
            "message": result.message
        }

    @staticmethod
    def perform_semantic_search(df: pd.DataFrame, query: str, text_col: str, top_k: int = 5) -> pd.DataFrame:
        """
        Performs semantic search (RAG) on a dataframe column using OpenAI embeddings.
        """
        if not OPENAI_INSTALLED or not os.getenv("OPENAI_API_KEY"):
             return pd.DataFrame()

        try:
            client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

            # 1. Get query embedding
            query_resp = client.embeddings.create(input=[query], model="text-embedding-3-small")
            query_embedding = query_resp.data[0].embedding

            # 2. Get text embeddings (Optimization: Only embed unique values to save API tokens)
            unique_texts = df[text_col].dropna().astype(str).unique().tolist()[:50] # Limit to 50 for demo
            
            if not unique_texts:
                return pd.DataFrame()

            emb_resp = client.embeddings.create(input=unique_texts, model="text-embedding-3-small")
            embeddings = [d.embedding for d in emb_resp.data]
            text_emb_map = dict(zip(unique_texts, embeddings))

            # 3. Calculate Cosine Similarity
            def get_similarity(text):
                text = str(text)
                if text not in text_emb_map: return -1
                emb = text_emb_map[text]
                return np.dot(query_embedding, emb) / (np.linalg.norm(query_embedding) * np.linalg.norm(emb))

            df_copy = df.copy()
            df_copy['similarity_score'] = df_copy[text_col].apply(get_similarity)
            return df_copy.sort_values('similarity_score', ascending=False).head(top_k)
        except Exception as e:
            return pd.DataFrame()

    @staticmethod
    def suggest_data_repairs(df: pd.DataFrame) -> list:
        """
        Scans categorical columns for inconsistencies (e.g. 'CA' vs 'Californi') using fuzzy matching.
        """
        suggestions = []
        cat_cols = df.select_dtypes(include=['object']).columns
        
        for col in cat_cols:
            counts = df[col].value_counts()
            if len(counts) < 3: continue 
            
            values = counts.index.tolist()
            for val in values:
                # Skip if this is a very frequent value (likely correct)
                if counts[val] > len(df) * 0.05: continue
                
                # Look for matches in values that are MORE frequent than the current one
                potential_matches = [v for v in values if counts[v] > counts[val]]
                matches = difflib.get_close_matches(str(val), [str(v) for v in potential_matches], n=1, cutoff=0.85)
                
                if matches:
                    better_val = matches[0]
                    suggestions.append({
                        "column": col,
                        "issue": f"Potential typo: '{val}'",
                        "suggestion": better_val,
                        "confidence": "High",
                        "original_value": val,
                        "suggested_value": better_val,
                        "affected_rows": int(counts[val])
                    })
        return suggestions

    @staticmethod
    def extract_structured_data(file_bytes: bytes, file_type: str, extraction_schema: str) -> dict:
        """
        Uses Multimodal LLMs to extract structured data from Images or PDFs.
        """
        if not OPENAI_INSTALLED or not os.getenv("OPENAI_API_KEY"):
            return {"error": "OpenAI API Key missing or library not installed."}

        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        # Handle Images
        if file_type.lower() in ['png', 'jpg', 'jpeg']:
            try:
                base64_image = base64.b64encode(file_bytes).decode('utf-8')
                
                response = client.chat.completions.create(
                    model="gpt-4o", 
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": f"Extract the following fields from this image as JSON: {extraction_schema}"},
                                {"type": "image_url", "image_url": {"url": f"data:image/{file_type};base64,{base64_image}"}},
                            ],
                        }
                    ],
                    response_format={ "type": "json_object" }
                )
                return json.loads(response.choices[0].message.content)
            except Exception as e:
                return {"error": f"Image processing failed: {str(e)}"}
        
        # Handle PDF
        elif file_type.lower() == 'pdf':
            try:
                import pypdf
                reader = pypdf.PdfReader(io.BytesIO(file_bytes))
                text_content = "\n".join([page.extract_text() for page in reader.pages])
                
                response = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a data extraction assistant. Output JSON."},
                        {"role": "user", "content": f"Extract these fields: {extraction_schema}\n\nFrom this text:\n{text_content[:15000]}"} # Truncate for limits
                    ],
                    response_format={ "type": "json_object" }
                )
                return json.loads(response.choices[0].message.content)
            except ImportError:
                return {"error": "pypdf library not installed. Cannot process PDFs."}
            except Exception as e:
                return {"error": f"PDF processing failed: {str(e)}"}

        return {"error": "Unsupported file type."}