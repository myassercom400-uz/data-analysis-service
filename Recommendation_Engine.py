# Recommendation_Engine.py
import pandas as pd

def generate_recommendations(df):
    try:
        df.columns = df.columns.str.strip().str.lower()

        aliases = {
            "price": ["sales", "revenue", "total", "amount", "price"],
            "profit": ["gross income", "net income", "profit", "gain", "income", "margin"],
            "product": ["product", "item", "product line", "product_name"],
            "quantity": ["quantity", "qty", "count"]
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

        product_col = detect(aliases["product"])
        revenue_col = detect(aliases["price"])
        profit_col = detect(aliases["profit"])
        qty_col = detect(aliases["quantity"])

        if not product_col or not revenue_col:
            return {"status": "error", "message": "Required columns for recommendations not found."}

        recommendations_list = []

        agg_dict = {revenue_col: "sum"}
        if profit_col: agg_dict[profit_col] = "sum"
        if qty_col: agg_dict[qty_col] = "sum"
        
        product_perf = df.groupby(product_col).agg(agg_dict).reset_index()

        
        top_product = product_perf.sort_values(by=revenue_col, ascending=False).iloc[0][product_col]
        recommendations_list.append({
            "type": "strategy",
            "title": "ركز على الحصان الرابح",
            "body": f"المنتج '{top_product}' هو الأعلى تحقيقاً للإيرادات. ننصح بزيادة الحملات الإعلانية له وتأمين مخزونه دايماً."
        })

        if profit_col:
            low_profit_products = product_perf.sort_values(by=profit_col, ascending=True).head(2)
            for _, row in low_profit_products.iterrows():
                if row[profit_col] <= 0:
                    recommendations_list.append({
                        "type": "warning",
                        "title": "إيقاف نزيف الخسائر",
                        "body": f"المنتج '{row[product_col]}' بيحقق هوامش ربح سالبة أو صفرية. راجع سعر الشراء من المورد أو ارفع سعر البيع."
                    })
                else:
                    recommendations_list.append({
                        "type": "action",
                        "title": "تنشيط أصناف راكدة",
                        "body": f"المنتج '{row[product_col]}' أرباحه ضعيفة جداً. جرب تعمل عليه عرض ترويجي (خصم لفترة محدودة) لتسييل البضاعة."
                    })

        if qty_col:
            low_qty_products = product_perf.sort_values(by=qty_col, ascending=True).head(2)
            for _, row in low_qty_products.iterrows():
                recommendations_list.append({
                    "type": "inventory",
                    "title": "تنبيه نقص مخزون",
                    "body": f"معدل سحب المنتج '{row[product_col]}' عالي ومخزونه قليل. اطلب كمية جديدة فوراً عشان مبيعاتك متوقفش."
                })

        return {
            "status": "success",
            "recommendations": recommendations_list
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}
