
DATA PACK — CASE STUDY 2: AP Smart Intake
Version: 1.0 | Generated: 2025-10-17 09:04:17

Mục đích
--------
Gói dữ liệu mô phỏng để thí sinh thiết kế pipeline AP: so khớp hóa đơn (đã được trích sẵn từ OCR) với PO/GRN,
phát hiện trùng lặp/đáng ngờ và thực hiện "3-way check" (PO–GRN–Invoice).

Tệp & Schema
------------
1) invoices_extracted.csv  (kết quả trích rút từ hóa đơn, thay cho thư mục PDF + OCR)
   - vendor (text)
   - invoice_no (text)
   - invoice_date (YYYY-MM-DD)
   - po_no (text)
   - vat_rate (int, %): 8 hoặc 10
   - currency (text): VND
   - subtotal (int)
   - tax (int)
   - total (int)
   - line_items_json (json array): mỗi phần tử gồm:
       - sku (text)
       - desc (text)
       - qty_billed (int)
       - unit_price_invoice (int)
       - line_total (int)

2) po.csv  (đơn đặt hàng)
   - PO_no, vendor, vat_code (VAT8/VAT10), sku, desc, qty_ordered, unit_price_po

3) grn.csv (ghi nhận hàng về)
   - PO_no, vendor, sku, qty_received, grn_date

4) answer_key_sample.csv  (nhãn mẫu để kiểm thử)
   - vendor, invoice_no, po_no, status (MATCH/REVIEW), flags (DUPLICATE;PRICE_VARIANCE;QTY_VARIANCE…)

Gợi ý đánh giá
--------------
• "3-way check": so khớp đơn giá × số lượng theo PO, đối chiếu với GRN (số lượng nhận) và hóa đơn.
• Phát hiện:
  - DUPLICATE: trùng vendor+invoice_no+total hoặc các tiêu chí gần tương đương
  - PRICE_VARIANCE: đơn giá lệch so với PO theo % ngưỡng
  - QTY_VARIANCE: qty_billed > qty_ordered hoặc > qty_received
• KPI gợi ý: F1 trích trường (nếu đội có bổ sung trích rút), recall phát hiện duplicate, tỉ lệ 3-way pass, thời gian xử lý batch.

Lưu ý
-----
• Gói dữ liệu có chủ đích tạo vài trường hợp trùng lặp & chênh lệch giá/số lượng để kiểm thử rule/AI của đội.
• Thí sinh không cần làm OCR; tập trung vào đối chiếu & phát hiện rủi ro.
• Có thể mở rộng pipeline bằng embedding/LLM để tăng chất lượng phát hiện, nhưng không bắt buộc ở vòng sơ khảo.
