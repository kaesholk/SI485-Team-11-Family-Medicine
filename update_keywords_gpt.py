import os
import argparse
import pandas as pd
from dotenv import load_dotenv
from openai import AzureOpenAI
from tqdm import tqdm

def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate MeSH keywords for articles in a CSV via Azure OpenAI"
    )
    parser.add_argument(
        "--input_csv", required=True,
        help="Path to the input CSV file"
    )
    parser.add_argument(
        "--output_csv", required=True,
        help="Path where the output CSV with Keywords will be written"
    )
    return parser.parse_args()


def needs_processing(val):
    return pd.isna(val) or str(val).strip() == ''


def generate_keywords(input_csv: str, output_csv: str, batch_size: int = 50):
    # Ensure working directory is script location
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Load environment
    if not load_dotenv('.env'):
        print('Unable to load .env file.')
        return

    # Init OpenAI client
    client = AzureOpenAI(
        api_key=os.environ['OPENAI_API_KEY'],
        api_version=os.environ['API_VERSION'],
        azure_endpoint=os.environ['OPENAI_API_BASE'],
        organization=os.environ.get('OPENAI_ORGANIZATION', None)
    )

    # Read DataFrame
    df = pd.read_csv(input_csv)

    # Add Keywords column if missing
    if 'Keywords' not in df.columns:
        df['Keywords'] = pd.NA

    # Find first index needing processing
    start_idx = next(
        (i for i, v in enumerate(df['Keywords']) if needs_processing(v)),
        len(df)
    )
    print(f'Skipped to row {start_idx}.\n')
    if start_idx >= len(df):
        print('No rows to process. All keywords are already generated.')
        return

    total = len(df)
    progress_range = range(start_idx, total)

    # Loop and generate keywords
    for count, idx in enumerate(tqdm(progress_range, desc='Generating MeSH keywords', unit='row'), start=1):
        existing = df.at[idx, 'Keywords']
        if needs_processing(existing) or str(existing).strip() == '!ERROR':
            title = df.at[idx, 'Title']
            abstract = df.at[idx, 'Abstract']
            messages = [
                {"role": "system", "content": (
                    "You are an AI model that identifies MeSH (Medical Subject Heading) keywords "
                    "based on article titles and abstracts. Only generate keywords that can be "
                    "reasonably inferred from the given text. Answer in the form of a comma separated "
                    "list of keywords, e.g. Pregnancy, Causality, Risk Factors, Abortion. If no keywords can be identified, answer NA"
                )},
                {"role": "user", "content": f"Identify MeSH keywords from the following text:\nTitle: {title}\nAbstract: {abstract}"}
            ]
            try:
                resp = client.chat.completions.create(
                    model=os.environ['MODEL'],
                    messages=messages,
                    temperature=0
                )
                keywords = resp.choices[0].message.content.strip()
            except Exception as e:
                print(f"Error at row {idx}: {e}")
                keywords = '!ERROR'
            df.at[idx, 'Keywords'] = keywords
        # Save at batch boundaries
        if count % batch_size == 0 or idx == total - 1:
            df.to_csv(output_csv, index=False)
            tqdm.write(f"Saved progress up to row {idx}")

    print(f"Processing started at row {start_idx}. All rows processed. Final file written to {output_csv}")


def main():
    args = parse_args()
    generate_keywords(args.input_csv, args.output_csv)


if __name__ == '__main__':
    main()
