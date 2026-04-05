# @title Default title text
import pandas as pd
import json

# ======================
# 1) Cleaning Functions
# ======================

def clean_data(df, price_col, cost_col=None):
    original_rows = len(df)

    # Remove duplicates
    df = df.drop_duplicates()
    duplicates_removed = original_rows - len(df)

    # Handle missing price
    df = df[df[price_col].notnull()]

    # Fill missing cost
    if cost_col:
        df[cost_col] = df[cost_col].fillna(df[cost_col].median())

    missing_percentage = df.isnull().mean().mean() * 100

    return df, duplicates_removed, missing_percentage


def remove_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)

    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    before = len(df)
    df = df[(df[column] >= lower) & (df[column] <= upper)]
    after = len(df)

    removed = before - after

    return df, removed


# ======================
# 2) Main Engine
# ======================

def analyze_data(file_path):
    try:
        # ======================
        # Load Data
        # ======================
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.lower()

        # ======================
        # Column Detection
        # ======================
        aliases = {
            "price": ["price", "revenue", "amount", "sales"],
            "cost": ["cost", "expense"],
            "profit": ["profit", "gain"],
            "date": ["date", "order_date","order date"],
            "product": ["product", "item"],
            "region": ["region", "country"]
        }

        def detect(possible):
            for col in df.columns:
                for name in possible:
                    if name in col:
                        return col
            return None

        price = detect(aliases["price"])
        cost = detect(aliases["cost"])
        profit_col = detect(aliases["profit"])
        date = detect(aliases["date"])
        product = detect(aliases["product"])
        region = detect(aliases["region"])

        if not price or not date:
            return {
                "status": "error",
                "message": f"Missing required columns. Found: {df.columns.tolist()}"
            }

        # ======================
        # Cleaning
        # ======================
        df, duplicates_removed, missing_percentage = clean_data(df, price, cost)
        df, outliers_removed = remove_outliers(df, price)

        # ======================
        # Prepare Data
        # ======================
        df[date] = pd.to_datetime(df[date], errors="coerce")
        # تعديل 1: استخدام السنة-الشهر لمنع تداخل تواريخ السنوات المختلفة
        df["month"] = df[date].dt.strftime("%Y-%m")

        df["revenue"] = df[price]

        # ======================
        # Profit Logic
        # ======================
        if profit_col:
            df["profit"] = df[profit_col]
        elif cost:
            df["profit"] = df[price] - df[cost]
        else:
            df["profit"] = df["revenue"] * 0.3

        # ======================
        # KPIs
        # ======================
        total_revenue = float(df["revenue"].sum())
        total_profit = float(df["profit"].sum())
        total_orders = int(len(df))

        profit_margin = (total_profit / total_revenue) * 100 if total_revenue else 0.0

        # ======================
        # Charts (مع تعديل fillna لحماية الـ JSON من الـ NaN)
        # ======================
        sales_over_time = (
            df.groupby("month")["revenue"]
            .sum()
            .fillna(0)
            .reset_index()
            .to_dict(orient="records")
        )

        profit_over_time = (
            df.groupby("month")["profit"]
            .sum()
            .fillna(0)
            .reset_index()
            .to_dict(orient="records")
        )

        if product:
            product_profit = (
                df.groupby(product)["profit"]
                .sum()
                .fillna(0)
                .reset_index()
                .sort_values("profit", ascending=False)
            )

            profit_by_product = product_profit.to_dict(orient="records")

            # Amendment 2: Protection against collapse if the table is complete after cleaning
            if not product_profit.empty:
                best_product = str(product_profit.iloc[0][product])
                worst_product = str(product_profit.iloc[-1][product])
            else:
                best_product = None
                worst_product = None
        else:
            profit_by_product = []
            best_product = None
            worst_product = None

        sales_by_region = (
            df.groupby(region)["revenue"]
            .sum()
            .fillna(0)
            .reset_index()
            .to_dict(orient="records")
        ) if region else []

        # ======================
        # Growth
        # ======================
        monthly = df.groupby(df[date].dt.to_period("M"))["revenue"].sum()

        if len(monthly) > 1:
            growth = float(monthly.pct_change().iloc[-1] * 100)
        else:
            growth = None

        # ======================
        # Insights
        # ======================
        insights = []

        if growth is not None:
            insights.append(f"Revenue changed by {round(growth, 2)}% last month")

        insights.append(f"Total profit is {round(total_profit, 2)}")
        insights.append(f"Profit margin is {round(profit_margin, 2)}%")

        if best_product:
            insights.append(f"{best_product} is the most profitable product")

        if worst_product:
            insights.append(f"{worst_product} is the least profitable product")

        if missing_percentage > 10:
            insights.append("High missing values detected")

        if outliers_removed > 0:
            insights.append(f"{outliers_removed} outliers were removed")

        # ======================
        # Recommendations
        # ======================
        recommendations = []

        if profit_margin < 20:
            recommendations.append("Reduce costs to improve profit margin")

        if growth is not None and growth < 0:
            recommendations.append("Increase marketing campaigns")

        if worst_product:
            recommendations.append(f"Review {worst_product} performance")

        recommendations.append("Focus on high-profit products")

        # ======================
        # Final JSON
        # ======================
        result = {
            "status": "success",
            "dashboard": {
                "cards": [
                    {"title": "Total Revenue", "value": total_revenue},
                    {"title": "Total Profit", "value": total_profit},
                    {"title": "Profit Margin %", "value": round(profit_margin, 2)},
                    {"title": "Total Orders", "value": total_orders},
                    {"title": "Best Product", "value": best_product},
                    {"title": "Worst Product", "value": worst_product}
                ],
                "charts": [
                    {"type": "line", "title": "Sales Over Time", "data": sales_over_time},
                    {"type": "line", "title": "Profit Over Time", "data": profit_over_time},
                    {"type": "bar", "title": "Profit by Product", "data": profit_by_product},
                    {"type": "pie", "title": "Sales by Region", "data": sales_by_region}
                ]
            },
            "insights": insights,
            "recommendations": recommendations,
            "data_quality": {
                "missing_percentage": round(float(missing_percentage), 2),
                "duplicates_removed": int(duplicates_removed),
                "outliers_removed": int(outliers_removed)
            }
        }

        return result

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }