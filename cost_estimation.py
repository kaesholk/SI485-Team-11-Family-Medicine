import pandas as pd

file_path = "elements_ingested.csv"

prompt_cost_per_word = 0.10 / 1000000
completion_cost_per_word = 0.40 / 1000000

df = pd.read_csv(file_path)

title_abstract_word_counts = (
    df["Title"].fillna("").astype(str) + " " + df["Abstract"].fillna("").astype(str)
).str.split().apply(len)
total_title_abstract_words = title_abstract_word_counts.sum()

non_empty_keywords = df["Keywords"].fillna("").astype(str).str.strip()
keyword_mask = non_empty_keywords != ""
keyword_word_counts = non_empty_keywords[keyword_mask].str.split().apply(len)
average_keyword_words = keyword_word_counts.mean()
num_non_empty_keywords = keyword_mask.sum()

prompt_cost = prompt_cost_per_word * total_title_abstract_words
completion_cost = completion_cost_per_word * average_keyword_words * len(df)
total_cost = prompt_cost + completion_cost

print(f"Total Title+Abstract Word Count: {total_title_abstract_words}")
print(f"Average Keyword Word Count: {average_keyword_words:.2f}")
print(f"Prompt Cost: ${prompt_cost:.4f}")
print(f"Completion Cost: ${completion_cost:.4f}")
print(f"Total Estimated Cost: ${total_cost:.4f}")