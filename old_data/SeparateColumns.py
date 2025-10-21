import pandas as pd
import json

df = pd.read_csv("invoices_extracted.csv")

lines = []

for _, row in df.iterrows():
    items = json.loads(row["line_items_json"])
    for item in items:
        lines.append({
            "vendor": row["vendor"],
            "invoice_no": row["invoice_no"],
            "po_no": row["po_no"],
            "sku": item["sku"],
            "desc": item["desc"],
            "qty_billed": item["qty_billed"],
            "unit_price_invoice": item["unit_price_invoice"],
            "line_total": item["line_total"]
        })

lines_df = pd.DataFrame(lines)

lines_df.to_csv("invoice_lines.csv", index=False, encoding="utf-8-sig")
