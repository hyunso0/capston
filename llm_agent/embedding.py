import os
import glob
import re
import pickle
import pandas as pd
import numpy as np
import faiss
import torch
from transformers import AutoTokenizer, AutoModel

# 설정 경로
CSV_DIR = os.path.abspath("../data/csv_data")
FAISS_INDEX_PATH = os.path.abspath("../data/faiss/faiss_index.idx")
META_PATH = os.path.abspath("../data/faiss/faiss_meta.pkl")
MODEL_NAME = "nlpai-lab/KURE-v1"

# 문자열 정규화 함수
def normalize_token(text):
    text = text.lower()
    text = re.sub(r"[·_\-/]", "", text)
    text = re.sub(r"\s+", "", text)
    return text.strip()

# 텍스트 임베딩 함수
@torch.no_grad()
def encode_texts(texts, tokenizer, model, device, batch_size=32):
    vecs = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        enc = tokenizer(batch, padding=True, truncation=True, return_tensors="pt", max_length=128).to(device)
        out = model(**enc).last_hidden_state[:, 0]
        out = torch.nn.functional.normalize(out, dim=1)
        vecs.append(out.cpu().numpy())
    return np.vstack(vecs)

# 임베딩 + 메타데이터 생성
def embed_csv_files(csv_dir, tokenizer, model, device):
    file_word_embeddings = {}
    file_token_index = {}

    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    print("발견된 CSV 파일 수:", len(csv_files))
    for csv_path in csv_files:
        table_name = os.path.basename(csv_path).replace(".csv", "")
        df = pd.read_csv(csv_path)

        norm_name = normalize_token(table_name)
        file_token_index[table_name] = [norm_name]

        words = set()
        words.add(table_name)
        words.update(df.columns)

        text_cols = df.select_dtypes(include='object').columns
        for col in text_cols:
            values = df[col].dropna().unique().tolist()
            words.update(str(v) for v in values)

        words = list(words)
        embeddings = encode_texts(words, tokenizer, model, device)
        word_embeddings = {w: emb for w, emb in zip(words, embeddings)}
        file_word_embeddings[table_name] = word_embeddings

    return file_word_embeddings, file_token_index

# FAISS 인덱스 및 메타 저장
def build_and_save_faiss_index(file_word_embeddings, faiss_path, meta_path):
    meta = []
    all_vectors = []
    for file_name, word_dict in file_word_embeddings.items():
        for word_raw, emb in word_dict.items():
            word_norm = normalize_token(word_raw)
            all_vectors.append(emb)
            meta.append((file_name, word_norm, word_raw))

    vec_matrix = np.vstack(all_vectors).astype(np.float32)
    index = faiss.IndexFlatIP(vec_matrix.shape[1])
    index.add(vec_matrix)

    faiss.write_index(index, faiss_path)
    with open(meta_path, "wb") as f:
        pickle.dump(meta, f)

    return index, meta

# 메인 실행
if __name__ == "__main__":
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME).to(device)
    model.eval()

    # 1. CSV → 임베딩
    file_word_embeddings, file_token_index = embed_csv_files(CSV_DIR, tokenizer, model, device)

    # 2. FAISS 인덱스 생성 및 저장
    index, meta = build_and_save_faiss_index(file_word_embeddings, FAISS_INDEX_PATH, META_PATH)
