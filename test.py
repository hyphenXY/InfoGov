import pandas as pd
import numpy as np
from transformers import AutoTokenizer, AutoModel
from sklearn.metrics.pairwise import cosine_similarity
import torch

# Step 1: Create Sample Datasets
# Dataset 1
data1 = {
    "full_name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "location": ["New York", "Los Angeles", "Chicago"],
}
df1 = pd.DataFrame(data1)

# Dataset 2
data2 = {
    "name": ["Alice", "Bob", "Charlie"],
    "years_old": [25, 30, 35],
    "city": ["New York", "Los Angeles", "Chicago"],
}
df2 = pd.DataFrame(data2)

# Display datasets
print("Dataset 1:")
print(df1)
print("\nDataset 2:")
print(df2)

# Step 2: Load the Hugging Face Embedding Model
model_name = "sentence-transformers/all-MiniLM-L6-v2"  # A lightweight embedding model
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

# Step 3: Define Function to Compute Embeddings
def get_embedding(text, tokenizer, model):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
    with torch.no_grad():
        outputs = model(**inputs)
        embeddings = outputs.last_hidden_state.mean(dim=1)  # Mean pooling
    return embeddings

# Step 4: Compute Embeddings for Column Names
columns1 = list(df1.columns)
columns2 = list(df2.columns)

# Create dictionaries to store embeddings
embeddings1 = {col: get_embedding(col, tokenizer, model) for col in columns1}
embeddings2 = {col: get_embedding(col, tokenizer, model) for col in columns2}

# Step 5: Perform Schema Matching
matches = []
for col1, embed1 in embeddings1.items():
    for col2, embed2 in embeddings2.items():
        similarity = cosine_similarity(embed1.numpy(), embed2.numpy())[0][0]
        if similarity > 0.8:  # Threshold for matching
            matches.append((col1, col2, similarity))

# Step 6: Display Results
print("\nSchema Matching Results:")
for match in matches:
    print(f"Match: {match[0]} <-> {match[1]} with similarity: {match[2]:.2f}")


