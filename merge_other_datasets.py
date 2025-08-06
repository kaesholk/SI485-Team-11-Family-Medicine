import pandas as pd

# File paths
original_csv = "merged_data.csv"
scrape_csv = "combine_orcid_scopus_final.csv"
altmetric_csv = "Altmetric_2024-07-03.csv"
output_csv = "merged_data_new.csv"

# Columns for refresh and propagation
refresh_cols = ["Article Affiliation", "ORCID", "Scopus ID", "Abstract"]
propagate_cols = ["ORCID", "Scopus ID"]
alt_cols = ["Title", "Authors at my Institution", "Publication Date", "DOI", 
            "Journal/Collection Title", "News mentions", "X mentions", 
            "Number of Mendeley readers", "Number of Dimensions citations", 
            "Altmetric Attention Score"]

# Load original and scraped data
df_orig = pd.read_csv(original_csv)
df_scrape = pd.read_csv(scrape_csv)

# Drop refresh columns from original
df_orig = df_orig.drop(columns=refresh_cols, errors='ignore')
# Keep only Title + refresh fields in scraped
keep_cols = ["Title"] + refresh_cols
df_scrape = df_scrape[keep_cols]

# Determine dynamic merge keys for first join 
merge_keys1 = sorted(set(df_orig.columns) & set(df_scrape.columns) - set(refresh_cols))
if not merge_keys1:
    raise ValueError("No common keys found between original and scrape data")
print(f"Merging scrape on columns: {merge_keys1}")

# Deduplicate scraped data on merge keys
df_scrape = df_scrape.drop_duplicates(subset=merge_keys1, keep='first')

# First merge
df_merged = df_orig.merge(df_scrape, on=merge_keys1, how="left")

# Propagate ORCID and Scopus ID across each author’s rows
for col in propagate_cols:
    df_merged[col] = (
        df_merged
        .groupby("Name")[col]
        .transform(lambda grp: grp.fillna(method="ffill").fillna(method="bfill"))
    )

# Load Altmetric data
df_alt = pd.read_csv(altmetric_csv, encoding="cp1252")[alt_cols]

# Determine dynamic merge keys for Altmetric join 
merge_keys2 = sorted(set(df_merged.columns) & set(df_alt.columns) - {"Journal/Collection Title"})
if not merge_keys2:
    raise ValueError("No common keys found between merged and Altmetric data")
print(f"Merging Altmetric on columns: {merge_keys2}")

# Deduplicate Altmetric data
df_alt = df_alt.drop_duplicates(subset=merge_keys2, keep='first')

# Second merge
df_merged = df_merged.merge(df_alt, on=merge_keys2, how="left")

# Fill Journal from Altmetric where empty
df_merged["Journal"] = df_merged["Journal"].fillna(
    df_merged["Journal/Collection Title"]
)
df_merged = df_merged.drop(columns=["Journal/Collection Title"])

# final dedupe just for fun
df_merged = df_merged.drop_duplicates(keep='first')

# Output
df_merged.to_csv(output_csv, index=False)
print(f"Merged {len(df_merged)} rows; output written to '{output_csv}'")
