import pandas as pd
import numpy as np
import time

PRICE_VARIANCE_THRESHOLD = 0.05  

po = pd.read_csv("po_clean.csv")
grn = pd.read_csv("grn_clean.csv")
inv = pd.read_csv("invoice_lines.csv")

po["vat_rate"] = po["vat_code"].str.extract(r'(\d+)').astype(float)
grn_agg = grn.groupby(["po_no", "sku"])["qty"].sum().reset_index().rename(columns={"qty": "qty_received"})
po = po.rename(columns={"qty": "qty_ordered", "price": "unit_price_po"})
inv["line_total_calc"] = inv["qty_billed"] * inv["unit_price_invoice"]

merged = inv.merge(po, on=["po_no", "vendor", "sku"], how="left")
merged = merged.merge(grn_agg, on=["po_no", "sku"], how="left")

def check_line(row):
    flags = []
    notes = []

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

    if not flags:
        return pd.Series(["OK", "Khớp PO và GRN"])
    return pd.Series([";".join(set(flags)), "; ".join(notes)])

merged[["line_flag", "notes"]] = merged.apply(check_line, axis=1)

lines_check = merged[[
    "invoice_no", "po_no", "sku", "qty_billed", "qty_ordered", "qty_received",
    "unit_price_invoice", "unit_price_po", "line_flag", "notes"
]]
lines_check.to_csv("lines_check.csv", index=False)

invoice_summary = merged.groupby(["vendor", "invoice_no", "po_no"]).agg({
    "line_total_calc": "sum",
    "vat_rate": "first",
    "line_flag": lambda x: list(x),
    "notes": lambda x: list(x)
}).reset_index()

invoice_summary["tax"] = invoice_summary["line_total_calc"] * invoice_summary["vat_rate"] / 100
invoice_summary["total"] = invoice_summary["line_total_calc"] + invoice_summary["tax"]

invoice_summary["dup_key"] = invoice_summary["vendor"] + "|" + invoice_summary["invoice_no"] + "|" + invoice_summary["total"].astype(str)
invoice_summary["is_duplicate"] = invoice_summary.duplicated("dup_key", keep=False)

def invoice_status(row):
    flags = set()
    explanations = []

    if row["is_duplicate"]:
        flags.add("DUPLICATE")
        explanations.append("Trùng vendor+invoice_no+total")

    for flag_list, note_list in zip(row["line_flag"], row["notes"]):
        if flag_list != "OK":
            for f in flag_list.split(";"):
                flags.add(f)
            explanations.append(note_list)

    status = "MATCH" if not flags else "REVIEW"
    return pd.Series([status, ";".join(sorted(flags)), " | ".join(explanations)])

invoice_summary[["status", "flags", "explanation"]] = invoice_summary.apply(invoice_status, axis=1)

results = invoice_summary[["vendor", "invoice_no", "po_no", "status", "flags", "explanation"]]
results.to_csv("results.csv", index=False)

total_invoices = len(invoice_summary)
flagged_invoices = (invoice_summary["status"] == "REVIEW").sum()
three_way_pass = (invoice_summary["status"] == "MATCH").sum()
duplicate_detected = invoice_summary["is_duplicate"].sum()

print("KPI")
print(f"Total invoices: {total_invoices}")
print(f"3-way pass rate: {three_way_pass / total_invoices:.2%}")
print(f"Flagged invoices: {flagged_invoices} ({flagged_invoices / total_invoices:.2%})")
print(f"Duplicate invoices detected: {duplicate_detected}")