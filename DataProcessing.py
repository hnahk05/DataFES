import pandas as pd
import numpy as np
import traceback

PRICE_VARIANCE_THRESHOLD = 0.05

try:
    po = pd.read_csv("po_clean.csv")
    grn = pd.read_csv("grn_clean.csv")
    inv = pd.read_csv("invoice_lines.csv")
except Exception as e:
    with open("log.txt", "a", encoding="utf-8") as log:
        log.write("[ERROR] Lỗi đọc file dữ liệu\n")
        log.write(str(e) + "\n")
        log.write(traceback.format_exc())
    raise

# Tạo nhóm trùng lặp dòng
inv["dup_group"] = inv.groupby(
    ["vendor", "invoice_no", "po_no", "sku", "unit_price_invoice", "qty_billed"]
).ngroup()

inv["dup_rank"] = inv.groupby("dup_group").cumcount() + 1
inv["is_dup_line"] = inv.duplicated(
    subset=["vendor", "invoice_no", "po_no", "sku", "unit_price_invoice", "qty_billed"],
    keep="first"
)

# Lấy ngày gốc từ dòng đầu tiên trong nhóm trùng
if "invoice_date" in inv.columns:
    inv["invoice_date"] = pd.to_datetime(inv["invoice_date"], errors="coerce")
    dup_reference = inv.groupby("dup_group").first()[["invoice_date"]].rename(columns={"invoice_date": "ref_date"})
    dup_reference["ref_date_str"] = dup_reference["ref_date"].dt.strftime("%Y-%m-%d")
    inv = inv.merge(dup_reference[["ref_date_str"]], left_on="dup_group", right_index=True, how="left")
else:
    inv["ref_date_str"] = "không rõ"

# Tiền xử lý PO và GRN
po["vat_rate"] = po["vat_code"].str.extract(r'(\d+)').astype(float)
grn_agg = grn.groupby(["po_no", "sku"])["qty"].sum().reset_index().rename(columns={"qty": "qty_received"})
po = po.rename(columns={"qty": "qty_ordered", "price": "unit_price_po"})
inv["line_total_calc"] = inv["qty_billed"] * inv["unit_price_invoice"]

# Gộp dữ liệu
merged = inv.merge(po, on=["po_no", "vendor", "sku"], how="left")
merged = merged.merge(grn_agg, on=["po_no", "sku"], how="left")

# Hàm kiểm tra từng dòng
def check_line(row):
    flags = []
    notes = []

    if row["is_dup_line"]:
        flags.append("DUPLICATE")
        notes.append(f"Trùng vendor+invoice_no+total với {row['ref_date_str']}")

    if pd.notnull(row["unit_price_po"]):
        price_diff = abs(row["unit_price_invoice"] - row["unit_price_po"]) / row["unit_price_po"]
        if price_diff > PRICE_VARIANCE_THRESHOLD:
            flags.append("PRICE_VARIANCE")
            notes.append(f"Price diff {price_diff:.2%}")
    else:
        notes.append("PO not found")

    if pd.notnull(row["qty_ordered"]) and row["qty_billed"] > row["qty_ordered"]:
        flags.append("QTY_VARIANCE")
        notes.append(f"Billed {row['qty_billed']} > ordered {row['qty_ordered']}")
    if pd.notnull(row["qty_received"]) and row["qty_billed"] > row["qty_received"]:
        flags.append("QTY_VARIANCE")
        notes.append(f"Billed {row['qty_billed']} > received {row['qty_received']}")
    elif pd.isnull(row["qty_received"]):
        notes.append("Missing GRN data")

    status = "MATCH" if not flags else "REVIEW"
    return pd.Series([";".join(flags) if flags else "OK", "; ".join(notes), status])

merged[["line_flag", "notes", "status"]] = merged.apply(check_line, axis=1)

# Xuất file results.csv
results = merged[[
    "vendor", "invoice_no", "po_no", "sku",
    "qty_billed", "qty_ordered", "qty_received",
    "unit_price_invoice", "unit_price_po",
    "line_flag", "status", "notes"
]]
results.to_csv("results.csv", index=False)

# Xuất thêm file lines_check.csv
lines_check = merged[[
    "invoice_no", "po_no", "sku", "qty_billed", "qty_ordered", "qty_received",
    "unit_price_invoice", "unit_price_po", "line_flag", "notes", "status", "ref_date_str"
]]
lines_check.to_csv("lines_check.csv", index=False)

# Ghi log dòng trùng
with open("log.txt", "a", encoding="utf-8") as log:
    log.write("=== DUPLICATE LINE LOG ===\n")
    dups = merged[merged["is_dup_line"]]
    for _, row in dups.iterrows():
        log.write(f"{row['vendor']} | {row['invoice_no']} | PO: {row['po_no']} | SKU: {row['sku']} | DUPLICATE | Ngày bị trùng: {row['ref_date_str']}\n")