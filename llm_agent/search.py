import os
import pickle
import faiss
from sentence_transformers import SentenceTransformer
import numpy as np
import re


# 경로 설정
FAISS_INDEX_PATH = os.path.abspath("./data/faiss/faiss_index.idx")
META_PATH = os.path.abspath("./data/faiss/faiss_meta.pkl")
SBERT_PATH = os.path.abspath("./llm_agent/KURE-v1")


# 정규화 함수
def normalize_token(text):
    text = text.lower()
    text = re.sub(r"[·_\-\/]", "", text)
    text = re.sub(r"\s+", "", text)
    return text.strip()


# 검색 함수
def search_faiss_with_partial_and_similarity(query_word, model, index, meta, file_token_index, thres1=0.4, thres2=0.5):
    query_norm = normalize_token(query_word)
    query_vec = model.encode(query_norm, convert_to_numpy=True, normalize_embeddings=True).astype(np.float32)
    print("🔍 query_vec.shape:", np.array([query_vec]).shape)
    print("🔍 index.d (expected):", index.d)
    candidate_files = {}
    partial_hits = {}

    for file_name, norm_tokens in file_token_index.items():
        for token in norm_tokens:
            if query_norm in token and query_norm != token:
                partial_hits[file_name] = {
                    "file": file_name,
                    "word": file_name,
                    "score": 1.0,
                    "match_type": "부분 포함"
                }

    D, I = index.search(np.array([query_vec]), index.ntotal)

    for dist, idx in zip(D[0], I[0]):
        if dist < thres1:
            continue
        file_name, word_norm, word_raw = meta[idx]
        if file_name in partial_hits:
            continue
        if query_norm in word_norm and query_norm != word_norm:
            partial_hits[file_name] = {
                "file": file_name,
                "word": word_raw,
                "score": 1.0,
                "match_type": "부분 포함"
            }
            candidate_files.pop(file_name, None)
            continue
        if file_name not in candidate_files and dist >= thres2:
            candidate_files[file_name] = {
                "file": file_name,
                "word": word_raw,
                "score": float(dist),
                "match_type": "유사도"
            }

    results = list(partial_hits.values()) + list(candidate_files.values())
    return sorted(results, key=lambda x: x["score"], reverse=True)

# 모델, 인덱스, 메타, 토큰 인덱스 로딩 함수 추가
def load_components():
    model = SentenceTransformer(SBERT_PATH, device="cpu")  # cuda도 가능
    index = faiss.read_index(FAISS_INDEX_PATH)
    with open(META_PATH, "rb") as f:
        meta = pickle.load(f)
    file_token_index = {file: [normalize_token(file)] for file, _, _ in meta}
    return model, index, meta, file_token_index