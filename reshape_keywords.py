import pandas as pd

df = pd.read_csv("merged_data.csv")

rows = []
pending_num = None  # holds a numeric-only keyword to prepend

for idx, row in df.iterrows():
    article_id = idx
    title = row.get("Title", "")

    # Split on commas but repair numeric-only splits
    for raw_kw in str(row["Keywords"]).split(","):
        kw = raw_kw.strip()
        if not kw:
            continue

        # If the keyword is only digits, mark as pending and join with the next
        if kw.isdigit():
            pending_num = kw
            continue

        # If we have a pending number, prepend it to this keyword
        if pending_num:
            kw = f"{pending_num},{kw}"
            pending_num = None

        rows.append({"Article Index": article_id, "Title": title, "Keyword": kw})

df_normalized = pd.DataFrame(rows)

output_path = "articles_by_keyword.csv"
df_normalized.to_csv(output_path, index=False)
print(f"Normalized data saved to '{output_path}'.")
