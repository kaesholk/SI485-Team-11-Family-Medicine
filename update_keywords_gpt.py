import os
import pandas as pd
from dotenv import load_dotenv
from openai import AzureOpenAI
from tqdm import tqdm

# Set working directory to script location
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
if not load_dotenv('.env'):
    print('Unable to load .env file.')
    quit()

# Initialize Azure OpenAI client
client = AzureOpenAI(
    api_key=os.environ['OPENAI_API_KEY'],
    api_version=os.environ['API_VERSION'],
    azure_endpoint=os.environ['OPENAI_API_BASE'],
    organization=os.environ.get('OPENAI_ORGANIZATION', None)
)

input_csv = 'merged_data.csv'
output_csv = 'merged_data.csv'
df = pd.read_csv(input_csv)
batch_size = 50

df = pd.read_csv(input_csv)

if 'Keywords' not in df.columns:
    df['Keywords'] = None

# Find first index that needs processing (empty or '!ERROR')
def needs_processing(val):
    return pd.isna(val) or str(val).strip() == ''

start_idx = next((i for i, v in enumerate(df['Keywords']) if needs_processing(v)), len(df))
print(f'Skipped to row {start_idx}.\n')
if start_idx >= len(df):
    print('No rows to process. All keywords are already generated.')
    exit(0)

# Process all needed rows with a single progress bar, saving at each batch boundary
total = len(df)
progress_range = range(start_idx, total)

tqdm_bar = tqdm(progress_range, desc='Generating MeSH keywords', unit='row')
for count, idx in enumerate(tqdm_bar, start=1):
    row = df.loc[idx]
    existing = row.get('Keywords', '')
    if pd.isna(existing) or existing.strip() == '' or existing.strip() == '!ERROR':
        title = row.get('Title', '')
        abstract = row.get('Abstract', '')
        messages = [
            {"role": "system", "content": (
                "You are an AI model that identifies MeSH (Medical Subject Heading) keywords "
                "based on article titles and abstracts. Only generate keywords that can be "
                "reasonably inferred from the given text. Answer in the form of a comma separated "
                "list of keywords, e.g. Pregnancy, Causality, Risk Factors, Abortion. If no keywords can be identified, answer NA")},
            {"role": "user", "content": f"Identify MeSH keywords from the following text:\nTitle: {title}\nAbstract: {abstract}"}
        ]
        try:
            response = client.chat.completions.create(
                model=os.environ['MODEL'],
                messages=messages,
                temperature=0
            )
            keywords = response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error at row {idx}: {e}")
            keywords = '!ERROR'
        df.at[idx, 'Keywords'] = keywords
    else:
        df.at[idx, 'Keywords'] = existing

    # Save progress every batch_size rows or at end
    if count % batch_size == 0 or idx == total - 1:
        df.to_csv(output_csv, index=False)
        tqdm_bar.set_postfix(saved=f"row {idx}")
        #print(f"Saved progress up to row {idx} to {output_csv}")

print(f"Processing started at row {start_idx}. All rows processed. Final file written to {output_csv}")
