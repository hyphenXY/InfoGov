import pandas as pd
import numpy as np
from transformers import AutoTokenizer, AutoModel
from sklearn.metrics.pairwise import cosine_similarity
import torch

# Step 1: Create Sample Datasets
# Dataset 1
model_name = "sentence-transformers/all-MiniLM-L6-v2"  # A lightweight embedding model
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

def get_embedding(text, tokenizer, model):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
    with torch.no_grad():
        outputs = model(**inputs)
        embeddings = outputs.last_hidden_state.mean(dim=1)  # Mean pooling
    return embeddings

def matcher(col1,col2):
    e1=get_embedding(col1,tokenizer,model)
    e2=get_embedding(col2,tokenizer,model)
    sim=cosine_similarity(e1.numpy(), e2.numpy())[0][0]
    if sim>0.5:
        return True
    return False




# columns1 = list(df1.columns)  'goods and service tax ) payers registered before due date', 'registered_payer'
# columns2 = list(df2.columns)

# # Create dictionaries to store embeddings
# embeddings1 = {col: get_embedding(col, tokenizer, model) for col in columns1}
# embeddings2 = {col: get_embedding(col, tokenizer, model) for col in columns2}

# # Step 5: Perform Schema Matching
# matches = []
# for col1, embed1 in embeddings1.items():
#     for col2, embed2 in embeddings2.items():
#         similarity = cosine_similarity(embed1.numpy(), embed2.numpy())[0][0]
#         if similarity > 0.6:  # Threshold for matching
#             matches.append((col1, col2, similarity))

# # Step 6: Display Results
# print("\nSchema Matching Results:")
# for match in matches:
#     print(f"Match: {match[0]} <-> {match[1]} with similarity: {match[2]:.2f}")


