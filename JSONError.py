import pandas as pd
import json

df = pd.read_csv("invoices_extracted.csv")

lines = []
log_file = open("log.txt", "w", encoding="utf-8")

for idx, row in df.iterrows():
    invoice_id = row.get("invoice_id")
    vendor = row.get("vendor")
    raw_json = row.get("line_items_json")

    try:
        items = json.loads(raw_json)

        if isinstance(items, dict):
            items = [items]

        for item in items:
            lines.append({
                "invoice_id": invoice_id,
                "vendor": vendor,
                "sku": item.get("sku"),
                "desc": item.get("desc"),
                "qty": item.get("qty"),
                "unit_price": item.get("price")
            })

    except Exception as e:
        log_file.write(f"[ERROR] Invoice {invoice_id}: {e}\n")
        log_file.write(f"Raw data: {raw_json}\n\n")

log_file.close()

df_lines = pd.DataFrame(lines)
df_lines.to_csv("invoice_lines.csv", index=False, encoding="utf-8")

print("Ghi log loi JSON vao log.txt")
