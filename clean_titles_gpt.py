import os
import pandas as pd
from dotenv import load_dotenv
from openai import AzureOpenAI
from tqdm import tqdm

# Change working directory to the script's location
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from .env
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

# File paths
input_csv = 'merged_data.csv'
output_csv = 'merged_data.csv'

# Read in the data
df = pd.read_csv(input_csv)
batch_size = 50

# Ensure a column for cleaned titles
if 'CLEAN_DATA_FLAG' not in df.columns:
    df['CLEAN_DATA_FLAG'] = pd.NA

if 'CleanTitle' not in df.columns:
    df['CleanTitle'] = pd.NA

# Determine which rows need cleaning

def needs_cleaning(val):
    return pd.isna(val) or str(val).strip() == '0'

start_idx = next((i for i, v in enumerate(df['CLEAN_DATA_FLAG']) if needs_cleaning(v)), len(df))
print(f'Starting clean-up at row {start_idx}...')
if start_idx >= len(df):
    print('All titles already cleaned.')
    exit(0)

# Iterate and clean titles
progress_iter = range(start_idx, len(df))
tqdm_bar = tqdm(progress_iter, desc='Cleaning Titles', unit='row')
for count, idx in enumerate(tqdm_bar, start=1):
    existing = df.at[idx, 'Title']
    if needs_cleaning(df.at[idx, 'CLEAN_DATA_FLAG']):
        df.at[idx, 'CLEAN_DATA_FLAG'] = 0
        original_title = df.at[idx, 'Title']
        messages = [
            {"role": "system", "content": (
                "You are a text-cleaning AI. Given an article title that may include author names, "
                "journal names, dates, or other extra information, extract and return only the "
                "core article title. Do not include any author names, journal titles, dates, or other "
                "non-title information. Normalize the title to proper title case. If the title is already clean, return it unchanged."
            )},
            {"role": "user", "content": f"Clean this title:\n{original_title}"}
        ]
        try:
            response = client.chat.completions.create(
                model=os.environ['MODEL'],
                messages=messages,
                temperature=0
            )
            cleaned = response.choices[0].message.content.strip()
            df.at[idx, 'CleanTitle'] = cleaned
            df.at[idx, 'CLEAN_DATA_FLAG'] = 1
        except Exception as e:
            print(f"Error at row {idx}: {e}")
        
    # Save at batch boundaries
    if count % batch_size == 0 or idx == len(df) - 1:
        df.to_csv(output_csv, index=False)
        tqdm_bar.set_postfix(saved=f"row {idx}")

print(f"Cleaning completed from row {start_idx}. Output saved to {output_csv}.")