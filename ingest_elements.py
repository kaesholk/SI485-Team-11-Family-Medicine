import pandas as pd
from typing import List

def load_and_process(input_csv_path: str, output_csv_path: str):
    df = pd.read_csv(input_csv_path, encoding='cp1252')
    
    # merge title and subtitle (note: some elements records have weird nonsense, urls, etc. in this field)
    def merge_title(row):
        title = str(row['Title']).strip()
        subtitle = str(row.get('Subtitle', '')).strip()
        if subtitle and subtitle.lower() not in ('nan', ''):
            return f"{title}: {subtitle}"
        return title

    df['Title'] = df.apply(merge_title, axis=1)
    if 'Subtitle' in df.columns:
        df = df.drop(columns=['Subtitle'])

    df.to_csv(output_csv_path, index=False)
    print(f"Done. CSV saved to {output_csv_path}")
    df.to_csv(output_csv_path, index=False)

if __name__ == "__main__":
    # Example usage
    input_path = "Elements_final_dataset.csv"
    output_path = "elements_ingested.csv"
    load_and_process(input_path, output_path)
