import pandas as pd

aks = pd.read_csv("answer_key_sample.csv")  
res = pd.read_csv("results.csv")          

common_cols = list(set(aks.columns) & set(res.columns))

aks["row_key"] = aks[common_cols].astype(str).agg("|".join, axis=1)
res["row_key"] = res[common_cols].astype(str).agg("|".join, axis=1)

missing_rows = aks[~aks["row_key"].isin(res["row_key"])]

if missing_rows.empty:
    print("PASS 100%")
else:
    print("Các dòng sau KHÔNG tìm thấy trong results.csv:")
    print(missing_rows[common_cols].to_string(index=False))
