import argparse
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge original, scrape, and Altmetric CSV files into one output CSV."
    )
    parser.add_argument(
        "--original_csv", required=True,
        help="Path to the original merged_data CSV file"
    )
    parser.add_argument(
        "--elements_csv", required=False,
        help="Path to the Elements CSV file"
    )
    parser.add_argument(
        "--scrape_csv", required=False,
        help="Path to extra scraped data"
    )
    parser.add_argument(
        "--altmetric_csv", required=False,
        help="Path to the Altmetric CSV file"
    )
    parser.add_argument(
        "--output_csv", required=True,
        help="Path where the merged output CSV should be written"
    )
    return parser.parse_args()

def merge_elements(df_orig, df_elements):
    """
    Merge new elements data into orig by filtering out rows whose author and title already exist in orig.
    """
    # Combine Title and Subtitle
    def merge_title(row):
        title = str(row['Title']).strip()
        subtitle = str(row.get('Subtitle', '')).strip()
        if subtitle and subtitle.lower() not in ('nan', ''):
            return f"{title}: {subtitle}"
        return title

    df_elements['Title'] = df_elements.apply(merge_title, axis=1)
    if 'Subtitle' in df_elements.columns:
        df_elements = df_elements.drop(columns=['Subtitle'])

    # Ensure author column exists
    author_col = 'Name'
    if author_col not in df_orig.columns or author_col not in df_elements.columns:
        raise KeyError(f"Author column '{author_col}' must be present in both DataFrames")

    # Identify existing (author, title) pairs
    existing = df_orig[[author_col, 'Title']].drop_duplicates()

    # Left merge with indicator to filter out existing rows
    df_new = df_elements.merge(
        existing,
        on=[author_col, 'Title'],
        how='left',
        indicator=True
    )
    df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge'])

    return df_new

def merge_scrape(df_orig, df_scrape, refresh_cols):
    # Drop refresh columns, retain only Title + refresh in scrape
    df_orig = df_orig.drop(columns=refresh_cols, errors='ignore')
    df_scrape = df_scrape[["Title"] + refresh_cols]

    # Determine merge keys and dedupe
    merge_keys = sorted(
        set(df_orig.columns) & set(df_scrape.columns) - set(refresh_cols)
    )
    if not merge_keys:
        raise ValueError("No common keys found between original and scrape data")
    print(f"Merging scrape on columns: {merge_keys}")

    df_scrape = df_scrape.drop_duplicates(subset=merge_keys, keep='first')
    return df_orig.merge(df_scrape, on=merge_keys, how="left")


def propagate_ids(df, name_col, propagate_cols):
    for col in propagate_cols:
        df[col] = (
            df
            .groupby(name_col)[col]
            .transform(lambda grp: grp.fillna(method="ffill").fillna(method="bfill"))
        )
    return df


def merge_altmetric(df, df_altmetric, altmetric_cols):
    df_altmetric = df_altmetric[altmetric_cols]
    merge_keys = sorted(
        set(df.columns) & set(df_altmetric.columns) - {"Journal/Collection Title"}
    )
    if not merge_keys:
        raise ValueError("No common keys found between merged and Altmetric data")
    print(f"Merging Altmetric on columns: {merge_keys}")

    df_altmetric = df_altmetric.drop_duplicates(subset=merge_keys, keep='first')
    return df.merge(df_altmetric, on=merge_keys, how="left"), merge_keys


def dedupe_metadata(df, merge_keys, meta_cols):
    df['_meta_null'] = df[meta_cols].isna().all(axis=1)
    df = df.sort_values(by=merge_keys + ['_meta_null'])
    df = df.drop_duplicates(subset=merge_keys, keep='first')
    return df.drop(columns=['_meta_null'])


def fill_journal(df):
    df["Journal"] = df["Journal"].fillna(df["Journal/Collection Title"])
    return df.drop(columns=["Journal/Collection Title"])


def main():
    args = parse_args()

    refresh_cols = ["Article Affiliation", "ORCID", "Scopus ID", "Abstract"]
    propagate_cols = ["ORCID", "Scopus ID"]
    altmetric_cols = [
        "Title", "Authors at my Institution", "Publication Date", "DOI",
        "Journal/Collection Title", "News mentions", "X mentions",
        "Number of Mendeley readers", "Number of Dimensions citations",
        "Altmetric Attention Score"
    ]

    # Load data
    df_merged = pd.read_csv(args.original_csv)

    if args.elements_csv:
        df_elements = pd.read_csv(args.elements_csv, encoding="cp1252")
        df_new = merge_elements(df_merged, df_elements)   # returns only the new rows
        # keep original rows and append new ones (concat will do nothing if df_new is empty)
        df_merged = pd.concat([df_merged, df_new], ignore_index=True, sort=False)


    if (args.scrape_csv):
        df_scrape = pd.read_csv(args.scrape_csv)
        df_merged = merge_scrape(df_merged, df_scrape, refresh_cols)
        df_merged = propagate_ids(df_merged, name_col="Name", propagate_cols=propagate_cols)
    

    if (args.altmetric_csv):
        df_altmetric = pd.read_csv(args.altmetric_csv, encoding="cp1252")
        df_merged, merge_keys2 = merge_altmetric(df_merged, df_altmetric, altmetric_cols)
        df_merged = fill_journal(df_merged)

    # Deduplicate metadata
    df_merged = dedupe_metadata(df_merged, merge_keys2, meta_cols=["DOI"])

    # Output
    df_merged.to_csv(args.output_csv, index=False)
    print(f"Merged {len(df_merged)} rows; output written to '{args.output_csv}'")


if __name__ == "__main__":
    main()
