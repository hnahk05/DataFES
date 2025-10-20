import pandas as pd

grn = pd.read_csv("grn_clean.csv")
invoice = pd.read_csv("invoice_lines (2).csv")
po = pd.read_csv("po_clean.csv")

po = po.rename(columns={'qty': 'qty_ordered', 'price': 'unit_price_po'})
grn = grn.rename(columns={'qty': 'qty_received'})
invoice = invoice.rename(columns={'qty_billed': 'qty_billed', 'unit_price_invoice': 'unit_price_invoice'})

for col in ['invoice_no', 'po_no', 'sku']:
    if col in invoice.columns:
        invoice[col] = invoice[col].astype(str).str.strip()
    if col in po.columns:
        po[col] = po[col].astype(str).str.strip()
    if col in grn.columns:
        grn[col] = grn[col].astype(str).str.strip()

invoice_po = invoice.merge(po[['po_no', 'sku', 'vendor', 'qty_ordered', 'unit_price_po']], 
                           on=['po_no', 'sku'], how='left')
lines_detail = invoice_po.merge(grn[['po_no', 'sku', 'qty_received']], 
                                on=['po_no', 'sku'], how='left')

if 'vendor_x' in lines_detail.columns and 'vendor_y' in lines_detail.columns:
    lines_detail['vendor'] = lines_detail['vendor_x'].combine_first(lines_detail['vendor_y'])
elif 'vendor_x' in lines_detail.columns:
    lines_detail['vendor'] = lines_detail['vendor_x']
elif 'vendor_y' in lines_detail.columns:
    lines_detail['vendor'] = lines_detail['vendor_y']
elif 'vendor' not in lines_detail.columns:
    lines_detail['vendor'] = 'UNKNOWN'

lines_detail['line_flag'] = 'OK'
lines_detail['notes'] = ''

for idx, row in lines_detail.iterrows():
    flags = []
    notes_list = []
    
    if pd.notna(row['unit_price_invoice']) and pd.notna(row['unit_price_po']):
        if row['unit_price_invoice'] != row['unit_price_po']:
            flags.append('PRICE_VARIANCE')
            pct_diff = abs(row['unit_price_invoice'] - row['unit_price_po']) / row['unit_price_po'] * 100
            notes_list.append(f"Đơn giá lệch {pct_diff:.2f}%")

    if pd.notna(row['qty_billed']) and pd.notna(row['qty_ordered']):
        if row['qty_billed'] != row['qty_ordered']:
            flags.append('QTY_VARIANCE_PO')
            notes_list.append(f"Billed {int(row['qty_billed'])} vs Ordered {int(row['qty_ordered'])}")
    
    if pd.notna(row['qty_billed']) and pd.notna(row['qty_received']):
        if row['qty_billed'] != row['qty_received']:
            flags.append('QTY_VARIANCE_GRN')
            notes_list.append(f"Billed {int(row['qty_billed'])} vs Received {int(row['qty_received'])}")
    
    if flags:
        lines_detail.at[idx, 'line_flag'] = ','.join(flags)
        lines_detail.at[idx, 'notes'] = '; '.join(notes_list)
    else:
        lines_detail.at[idx, 'notes'] = 'Khớp PO và GRN'

lines_check_cols = ['invoice_no', 'po_no', 'sku', 'qty_billed', 'qty_ordered', 
                    'qty_received', 'unit_price_invoice', 'unit_price_po', 'line_flag', 'notes']
lines_detail[lines_check_cols].to_csv("lines_check.csv", index=False)

invoice_summary = []

for (vendor, invoice_no, po_no), group in lines_detail.groupby(['vendor', 'invoice_no', 'po_no'], dropna=False):
    status = 'MATCH'
    all_flags = []
    explanations = []
    
    invoice_total = group['line_total'].sum() if 'line_total' in group.columns else 0
    dup_check = lines_detail[(lines_detail['vendor'] == vendor) & 
                             (lines_detail['invoice_no'] == invoice_no)]
    if len(dup_check['po_no'].unique()) > 1:
        all_flags.append('DUPLICATE')
        other_pos = [p for p in dup_check['po_no'].unique() if p != po_no]
        explanations.append(f"Trùng vendor+invoice_no với {', '.join(map(str, other_pos))}")
    
    for _, line in group.iterrows():
        if line['line_flag'] != 'OK':
            sku = line['sku']
            
            if 'PRICE_VARIANCE' in line['line_flag']:
                if pd.notna(line['unit_price_invoice']) and pd.notna(line['unit_price_po']):
                    pct_diff = abs(line['unit_price_invoice'] - line['unit_price_po']) / line['unit_price_po'] * 100
                    if 'PRICE_VARIANCE' not in all_flags:
                        all_flags.append('PRICE_VARIANCE')
                    explanations.append(f"{sku} đơn giá hóa đơn {'>' if line['unit_price_invoice'] > line['unit_price_po'] else '<'} PO {pct_diff:.2f}%")
            
            if 'QTY_VARIANCE_GRN' in line['line_flag']:
                if pd.notna(line['qty_billed']) and pd.notna(line['qty_received']):
                    if 'QTY_VARIANCE' not in all_flags:
                        all_flags.append('QTY_VARIANCE')
                    explanations.append(f"{sku} billed {int(line['qty_billed'])} {'>' if line['qty_billed'] > line['qty_received'] else '<'} received {int(line['qty_received'])}")
    
    if all_flags:
        status = 'REVIEW'
    
    invoice_summary.append({
        'vendor': vendor,
        'invoice_no': invoice_no,
        'po_no': po_no,
        'status': status,
        'flags': ','.join(all_flags) if all_flags else '',
        'explanation': '; '.join(explanations) if explanations else ''
    })

result_df = pd.DataFrame(invoice_summary)
result_df.to_csv("result.csv", index=False)