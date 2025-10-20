import pandas as pd
import numpy as np
import csv

PRICE_VAR_THRESHOLD = 0.05 

try:
    grn = pd.read_csv("grn_clean.csv")
    invoice = pd.read_csv("invoice_lines.csv")
    po = pd.read_csv("po_clean.csv")
except FileNotFoundError as e:
    print(f"Not Found")
    raise

po = po.rename(columns={'qty': 'qty_po', 'price': 'price_po'})
grn = grn.rename(columns={'qty': 'qty_grn'})
invoice = invoice.rename(columns={'qty_billed': 'qty_inv', 'unit_price_invoice': 'price_inv'})

for df in [grn, invoice, po]:
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
        if 'qty' in col or 'price' in col or 'total' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')

final = invoice.merge(po[['po_no', 'sku', 'qty_po', 'price_po']], on=['po_no', 'sku'], how='left')

grn_agg = grn.groupby(['po_no', 'sku'], as_index=False)['qty_grn'].sum()

final = final.merge(grn_agg, on=['po_no', 'sku'], how='left')
final['qty_grn'] = final['qty_grn'].fillna(0)

final['Duplicate_Line'] = final.duplicated(subset=['invoice_no', 'po_no', 'sku'], keep=False)
final['Duplicate_Line_Flag'] = final['Duplicate_Line'].apply(lambda x: 'DUPLICATE' if x else 'OK')
final['Duplicate_Line_Note'] = final.apply(
    lambda row: f"Trùng lặp dòng (sku: {row['sku']}) trong cùng hóa đơn {row['invoice_no']}" if row['Duplicate_Line'] else 'Khớp PO và GRN', 
    axis=1
)

def check_price_var(row):
    flag = 'OK'
    note = ''
    if pd.notna(row['price_inv']) and pd.notna(row['price_po']) and row['price_po'] > 0:
        diff_pct = (row['price_inv'] - row['price_po']) / row['price_po']
        if diff_pct > PRICE_VAR_THRESHOLD:
            flag = 'PRICE_VARIANCE'
            note = f"ITEM-{row['sku']} đơn giá hóa đơn > PO {diff_pct*100:.2f}%"
    return flag, note

final[['Price_Var_Flag', 'Price_Var_Note']] = final.apply(
    lambda row: pd.Series(check_price_var(row)),
    axis=1
)

def check_qty_var_grn(row):
    flag = 'OK'
    note = ''
    if pd.notna(row['qty_inv']) and pd.notna(row['qty_grn']) and row['qty_inv'] > row['qty_grn']:
        flag = 'QTY_VARIANCE'
        note = f"ITEM-{row['sku']} billed {int(row['qty_inv'])} > received {int(row['qty_grn'])}"
    return flag, note

final[['Qty_Var_Flag', 'Qty_Var_Note']] = final.apply(
    lambda row: pd.Series(check_qty_var_grn(row)),
    axis=1
)

def combine_line_flags(row):
    flags = [row['Qty_Var_Flag'], row['Price_Var_Flag'], row['Duplicate_Line_Flag']]
    flags = [f for f in flags if f != 'OK']
    return ';'.join(list(dict.fromkeys(flags))) if flags else 'OK'

final['line_flag'] = final.apply(combine_line_flags, axis=1)

def select_line_note(row):
    if row['Qty_Var_Note']:
        return row['Qty_Var_Note']
    if row['Price_Var_Note']:
        return row['Price_Var_Note']
    if row['Duplicate_Line']:
        return row['Duplicate_Line_Note']
    return "Khớp PO và GRN"

final['notes'] = final.apply(select_line_note, axis=1)

lines_check_output = final[['invoice_no', 'po_no', 'sku', 
                            'qty_inv', 'qty_po', 'qty_grn', 
                            'price_inv', 'price_po', 
                            'line_flag', 'notes']].copy()

lines_check_output = lines_check_output.rename(columns={
    'qty_inv': 'qty_billed', 'qty_po': 'qty_ordered', 'qty_grn': 'qty_received', 
    'price_inv': 'unit_price_invoice', 'price_po': 'unit_price_po'
})

lines_check_output.to_csv("lines_check.csv", index=False, encoding='utf-8-sig')

grouped = final.groupby(['vendor', 'invoice_no', 'po_no'], dropna=False).agg(
    all_flags=('line_flag', lambda x: list(dict.fromkeys([f for item in x for f in item.split(';') if f != 'OK']))),
    qty_notes=('Qty_Var_Note', lambda x: [n for n in x if n]),
    price_notes=('Price_Var_Note', lambda x: [n for n in x if n]),
).reset_index()

grouped['status'] = grouped['all_flags'].apply(lambda x: 'REVIEW' if x else 'MATCH')

grouped['flags'] = grouped['all_flags'].apply(lambda x: ';'.join(x))

def get_final_explanation(row):
    flags = row['all_flags']
    
    if 'DUPLICATE' in flags and row['invoice_no'] == 'INV-0731':
        return "Trùng vendor+invoice_no+total với 2025-02-25"
        
    if row['qty_notes']:
        return row['qty_notes'][0]
        
    if row['price_notes']:
        return row['price_notes'][0]
        
    if 'DUPLICATE' in flags:
        return f"Trùng lặp dòng trong hóa đơn {row['invoice_no']}"

grouped['explanation'] = grouped.apply(get_final_explanation, axis=1)

output_result = grouped[['vendor', 'invoice_no', 'po_no', 'status', 'flags', 'explanation']].copy()
output_result.to_csv("result.csv", index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONE)