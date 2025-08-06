import pandas as pd
import requests
import re
from typing import List
from tqdm import tqdm
import time

tqdm.pandas()

def request_mesh_ondemand(query: str, session_cookie: str = None, retries: int = 1, backoff: float = 2.0) -> requests.Response:
    url = "https://meshb.nlm.nih.gov/api/MOD"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/json",
        "Origin": "https://meshb.nlm.nih.gov",
        "Referer": "https://meshb.nlm.nih.gov/MeSHonDemand",
        "Connection": "keep-alive",
        # "Cookie": f"ncbi_sid={session_cookie}",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=0"
    }

    payload = {
        "input": query
    }

    attempt = 0
    while attempt < retries:
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            attempt += 1
            wait_time = backoff * attempt
            print(f"[Retry {attempt}/{retries}] Error: {e}. Retrying in {wait_time:.1f} seconds...")
            time.sleep(wait_time)

    raise RuntimeError(f"Failed to fetch MeSH terms after {retries} attempts.")

def extract_mesh_terms(response: requests.Response) -> List[str]:
    response_text = response.text
    # Find the block between "-- MeSH Terms --" and the next "--"
    pattern = r"-- MeSH Terms --\s*(.*?)\s*--"
    match = re.search(pattern, response_text, re.DOTALL)
    if not match:
        return []

    block = match.group(1)
    # Split into lines, strip whitespace, and drop any empty lines
    terms = [line.strip() for line in block.splitlines() if line.strip()]
    return terms

def generate_keywords(title: str, abstract: str) -> str:
    text = str(title) + "\n" + str(abstract)
    response = request_mesh_ondemand(text)
    return ", ".join(extract_mesh_terms(response))

def update_keywords(input_csv_path: str, output_csv_path: str):
    df = pd.read_csv(input_csv_path)

    if 'Keywords' not in df.columns:
        df['Keywords'] = ""

    # Iterate with progress bar and save after each keyword generation
    try:
        for idx in tqdm(df.index, desc="Generating keywords"):
            if pd.notna(df.at[idx, 'Keywords']) and str(df.at[idx, 'Keywords']).strip():
                continue  # Skip if already has keywords

            title = df.at[idx, 'Title']
            abstract = df.at[idx, 'Abstract']
            try:
                df.at[idx, 'Keywords'] = generate_keywords(title, abstract)
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                df.at[idx, 'Keywords'] = "!ERROR"
                continue

            # Save progress after each row
            df.to_csv(output_csv_path, index=False)

    except KeyboardInterrupt:
        print("\nInterrupted by user. Partial results saved.")

    df.to_csv(output_csv_path, index=False)
    print(f"Done. CSV saved to {output_csv_path}")
    df.to_csv(output_csv_path, index=False)

if __name__ == "__main__":
    path = "elements_ingested.csv"
    update_keywords(path, path)
