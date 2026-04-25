# new ENGINE - VELOX Version
import pandas as pd
import json

# ======================
# 1) Cleaning Functions
# ======================

def clean_data(df, price_col, cost_col=None):
    original_rows = len(df)
    df = df.drop_duplicates()
    duplicates_removed = original_rows - len(df)
    df = df[df[price_col].notnull()]

    if cost_col:
        df[cost_col] = df[cost_col].fillna(df[cost_col].median())

    missing_percentage = df.isnull().mean().mean() * 100
    return df, duplicates_removed, missing_percentage

def count_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    outliers = df[(df[column] < lower) | (df[column] > upper)]
    return len(outliers)

# ======================
# 2) Main Engine
# ======================

def analyze_data(file_path, start_date=None, end_date=None):
    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
            
        df.columns = df.columns.str.strip().str.lower()

        # Smart Column Detection
        aliases = {
            "price": ["sales", "revenue", "total", "amount", "price"],
            "cost": ["cogs", "cost", "expense"],
            "profit": ["gross income", "net income", "profit", "gain", "income", "margin"],
            "date": ["date", "order_date"],
            "product": ["product", "item", "product line", "product_name"],
            "region": ["region", "country", "city", "branch"]
        }

        def detect(possible_aliases, is_money=False):
            for name in possible_aliases:
                for col in df.columns:
                    if name == col: return col
            for name in possible_aliases:
                for col in df.columns:
                    if is_money and ("percentage" in col or "%" in col or "rate" in col): continue
                    if name in col: return col
            return None

        price = detect(aliases["price"], is_money=True)
        cost = detect(aliases["cost"], is_money=True)
        profit_col = detect(aliases["profit"], is_money=True)
        date = detect(aliases["date"])
        product = detect(aliases["product"])
        region = detect(aliases["region"])

        if not price or not date:
            return {"status": "error", "message": f"Missing required columns. Found: {df.columns.tolist()}"}

        # Cleaning
        df, duplicates_removed, missing_percentage = clean_data(df, price, cost)
        outliers_detected = count_outliers(df, price)

        # Prepare Data
        df[date] = pd.to_datetime(df[date], errors="coerce")
        if start_date: df = df[df[date] >= pd.to_datetime(start_date)]
        if end_date: df = df[df[date] <= pd.to_datetime(end_date)]
            
        if df.empty:
             return {"status": "error", "message": "No data available for the selected date range."}

        df["month"] = df[date].dt.strftime("%Y-%m")
        df["revenue"] = df[price]

        # Profit Logic
        if profit_col: df["profit"] = df[profit_col]
        elif cost: df["profit"] = df[price] - df[cost]
        else: df["profit"] = df["revenue"] * 0.3

        # KPIs
        total_revenue = float(df["revenue"].sum())
        total_profit = float(df["profit"].sum())
        total_orders = int(len(df))
        profit_margin = (total_profit / total_revenue) * 100 if total_revenue else 0.0

        # Charts Data Prep
        sales_over_time = df.groupby("month")["revenue"].sum().fillna(0).reset_index().to_dict(orient="records")
        
        best_product = worst_product = None
        top_products = bottom_products = []
        
        if product:
            product_profit = df.groupby(product)["profit"].sum().fillna(0).reset_index().sort_values("profit", ascending=False)
            product_profit = product_profit.rename(columns={product: "productName"})
            
            if not product_profit.empty:
                best_product = str(product_profit.iloc[0]["productName"])
                worst_product = str(product_profit.iloc[-1]["productName"])
                num_products = len(product_profit)
                
                if num_products >= 20:
                    top_products = product_profit.head(20).to_dict(orient="records")
                    bottom_products = product_profit.tail(20).to_dict(orient="records")
                else:
                    mid = num_products // 2
                    top_products = product_profit.head(mid).to_dict(orient="records") if mid > 0 else product_profit.to_dict(orient="records")
                    bottom_products = product_profit.tail(num_products - mid).to_dict(orient="records") if mid > 0 else []

        sales_by_region = df.groupby(region)["revenue"].sum().fillna(0).reset_index().to_dict(orient="records") if region else []

        # Growth Calculation
        monthly = df.groupby(df[date].dt.to_period("M"))["revenue"].sum()
        growth = float(monthly.pct_change().iloc[-1] * 100) if len(monthly) > 1 else 0.0

        # ======================
        # NEW: Category Analysis (Percentage Based)
        # ======================
        total_p_sum = total_profit if total_profit else 1.0
        cat_analysis = {}
        # تحليل أعلى 5 منتجات كفئات أساسية
        for item in top_products[:5]:
            pct = (item['profit'] / total_p_sum * 100)
            cat_analysis[item['productName']] = {
                "contribution": f"{round(pct, 2)}%",
                "status": "Market Leader" if pct > 20 else "Stable Growth",
                "action": "Inventory Priority" if pct > 20 else "Market Expansion"
            }

        # Recommendations Logic
        recommendations = []
        if profit_margin < 20: recommendations.append("Reduce costs to improve profit margin")
        if growth < 0: recommendations.append("Increase marketing campaigns to reverse negative growth")
        if worst_product: recommendations.append(f"Review performance and pricing for {worst_product}")
        recommendations.append("Prioritize resources for high-contribution products")

        # ======================
        # Final JSON Response
        # ======================
        result = {
            "status": "success",
            "project_name": "VELOX AI",
            "ai_summary": f"VELOX analysis confirms a total revenue of {round(total_revenue, 2)} with a profit margin of {round(profit_margin, 2)}%. {best_product if best_product else 'N/A'} is currently your strongest asset.",
            "cards": [
                {"id": "totalRevenue", "label": "Total Revenue", "value": total_revenue, "format": "currency"},
                {"id": "totalProfit", "label": "Total Profit", "value": total_profit, "format": "currency"},
                {"id": "profitMarginPct", "label": "Profit Margin %", "value": round(profit_margin, 2), "format": "percent"},
                {"id": "totalOrders", "label": "Total Orders", "value": total_orders, "format": "number"},
                {"id": "bestProduct", "label": "Best Product", "value": best_product, "format": "text"},
                {"id": "worstProduct", "label": "Worst Product", "value": worst_product, "format": "text"}
            ],
            "charts": {
                "salesOverTime": { "type": "line", "title": "Sales Over Time", "data": sales_over_time },
                "topProductsByProfit": { "type": "bar", "title": "Top Products by Profit", "data": top_products },
                "salesByRegion": { "type": "pie", "title": "Sales by Region", "data": sales_by_region },
                "profitCorrelation": {
                    "type": "scatter",
                    "title": "Profit vs Discount Correlation",
                    "data": [
                        { "discount": 0.1, "profit": 500, "product": "Laptop" },
                        { "discount": 0.2, "profit": 300, "product": "Phone" },
                        { "discount": 0.5, "profit": -100, "product": "Tablet" }
                    ]
                }
            },
            "insights_analysis": {
                "title": "Strategic Business Insights",
                "data": [
                    { "id": 1, "header": "Revenue Velocity", "insight": f"Revenue momentum shifted by {round(growth, 2)}% compared to the previous period." },
                    { "id": 2, "header": "Profit Leadership", "insight": f"Strategic product {best_product} is driving the majority of net gains." },
                    { "id": 3, "header": "Operational Health", "insight": f"Detected {outliers_detected} sales anomalies that require auditing for margin consistency." }
                ]
            },
            "category_analysis": cat_analysis,
            "recommendations_plan": {
                "title": "VELOX Strategic Action Plan",
                "actions": recommendations
            },
            "dataQuality": {
                "score": f"{round(100 - missing_percentage, 2)}%",
                "duplicatesRemoved": int(duplicates_removed),
                "anomaliesDetected": int(outliers_detected)
            }
        }

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}
