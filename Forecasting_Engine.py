# Forecasting_Engine.py
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

def generate_forecast(df, start_date=None, end_date=None, periods=3):
    try:
       
        df.columns = df.columns.str.strip().str.lower()

        
        aliases = {
            "price": ["sales", "revenue", "total", "amount", "price"],
            "date": ["date", "order_date"]
        }

        def detect(possible_aliases):
            for name in possible_aliases:
                for col in df.columns:
                    if name == col: return col
            for name in possible_aliases:
                for col in df.columns:
                    if "percentage" in col or "%" in col or "rate" in col: continue
                    if name in col: return col
            return None

        date_col = detect(aliases["date"])
        revenue_col = detect(aliases["price"])

        if not date_col or not revenue_col:
            return {"status": "error", "message": "Required columns for forecasting not found."}

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        if start_date: df = df[df[date_col] >= pd.to_datetime(start_date)]
        if end_date: df = df[df[date_col] <= pd.to_datetime(end_date)]

        monthly_data = df.groupby(df[date_col].dt.to_period("M"))[revenue_col].sum().reset_index()
        
        if len(monthly_data) < 2:
            return {"status": "error", "message": "Insufficient data historical velocity. Need at least 2 months."}
        
        monthly_data['time_index'] = np.arange(len(monthly_data))
        
        X = monthly_data[['time_index']]
        y = monthly_data[revenue_col]
        
        model = LinearRegression()
        model.fit(X, y)
        
        last_index = monthly_data['time_index'].iloc[-1]
        future_indices = np.arange(last_index + 1, last_index + 1 + periods).reshape(-1, 1)
        predictions = model.predict(future_indices)
        
        last_date = monthly_data[date_col].iloc[-1]
        forecast_output = []
        
        for i, pred in enumerate(predictions):
            # التعديل هنا: استخدام DateOffset لضمان الانتقال للشهر التالي بدقة دايماً
            next_month = (last_date + pd.DateOffset(months=i + 1)).strftime("%Y-%m")
            forecast_output.append({
                "month": next_month,
                "predicted_revenue": round(float(max(0, pred)), 2) # لضمان عدم ظهور أرقام سالبة
            })
            
        return {"status": "success", "data": forecast_output}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}
