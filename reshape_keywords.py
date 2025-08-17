import argparse
import pandas as pd

def parse_args():
    parser = argparse.ArgumentParser(
        description="Normalize keywords from a merged CSV into one row per keyword."
    )
    parser.add_argument(
        "--input_csv",
        required=True,
        help="Path to the input CSV (contains Title and Keywords columns)"
    )
    parser.add_argument(
        "--output_csv",
        required=True,
        help="Path to write the normalized CSV (Article Index, Title, Keyword)"
    )
    return parser.parse_args()

def normalize_keywords(input_path: str, output_path: str):
    df = pd.read_csv(input_path)

    rows = []
    pending_num = None  # holds a numeric-only keyword to prepend

    # Ensure Keywords column exists to avoid KeyError
    if 'Keywords' not in df.columns:
        df['Keywords'] = None

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

    # If file ends with a dangling numeric-only keyword, keep it as-is (or drop — here we keep)
    if pending_num:
        rows.append({"Article Index": len(df), "Title": "", "Keyword": pending_num})

    df_normalized = pd.DataFrame(rows)

    df_normalized.to_csv(output_path, index=False)
    print(f"Normalized data saved to '{output_path}'.")

def main():
    args = parse_args()
    normalize_keywords(args.input_csv, args.output_csv)

if __name__ == "__main__":
    main()
