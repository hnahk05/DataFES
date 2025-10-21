import pandas as pd

po = pd.read_csv("po.csv")

po = po.rename(columns={
    "PO_no": "po_no",
    "vendor": "vendor",
    "vat_code": "vat_code",
    "sku": "sku",
    "desc": "desc",
    "qty_ordered": "qty",
    "unit_price_po": "price"
})

po["po_no"] = po["po_no"].astype(str).str.strip()
po["vendor"] = po["vendor"].astype(str).str.strip()
po["sku"] = po["sku"].astype(str).str.strip()

po["qty"] = pd.to_numeric(po["qty"], errors="coerce")
po["price"] = pd.to_numeric(po["price"], errors="coerce")

po.to_csv("po_clean.csv", index=False)

grn = pd.read_csv("grn.csv")

grn = grn.rename(columns={
    "PO_no": "po_no",
    "vendor": "vendor",
    "sku": "sku",
    "qty_received": "qty",
    "grn_date": "date"
})

grn["po_no"] = grn["po_no"].astype(str).str.strip()
grn["vendor"] = grn["vendor"].astype(str).str.strip()
grn["sku"] = grn["sku"].astype(str).str.strip()
grn["qty"] = pd.to_numeric(grn["qty"], errors="coerce")
grn["date"] = pd.to_datetime(grn["date"], errors="coerce")

grn.to_csv("grn_clean.csv", index=False)
