# LLM 기반 통계 분석 및 보고서 생성 서비스 
2025년 4학년 1학기 산학협력프로젝트


## 📌기능
- 통합 데이터 DB 기반 데이터 검색 제공
- 사용자 데이터 파일 업로드 및 분석 지원
- 사용자 업로드 데이터 파일 자동 전처리 지원
- 데이터 파일 미리보기 기능 제공
- 사용자가 선택한 파일에 기반한 챗봇 기능 지원
- LLM 응답에 기반한 한글보고서 자동 생성

## 📌개발환경
- Language : Python


- Framework : Streamlit, Flask


- Server : Docker


- Model : KURE-v1 (Embedding model), Qwen3 (LLM model)

## 📌시스템 아키텍처
<img src="https://github.com/user-attachments/assets/64406086-f9aa-4d5a-8716-6f6e5fc1d9b6" width="650"/>
<img src="https://github.com/user-attachments/assets/2857e364-731e-4d01-976b-df635220b8f0" width="650"/>

## 📌Web UI
<img src="https://github.com/user-attachments/assets/73d8f93e-1d40-4c30-b4b8-91594b37fc29" width="900"/>


## 📌파일구조
- **/app/streamlit_app.py** : Frontend Streamlit templates


- **/app/server.py** : Backend server logic


- **/app/data/** : DB 데이터 저장 폴더


- **/app/hwp_report/** : 한글보고서 생성 양식 및 코드


- **/app/llm_agent/** : LLM 관련 코드


<br>
<details> <summary>📁 전체 프로젝트 폴더 구조</summary>
  
```text
app/
├── .streamlit/
├── data/
│   ├── csv_data/ 전처리 이후 데이터
│   ├── faiss/
│   ├── graph/ 생성된 그래프 저장
│   ├── xlsx_data/ 전처리 이전 데이터
│   ├── database.db
│   └── debug_report.txt
├── graph/
├── hwp_report/
│   ├── __pycache__/
│   ├── hwp_file/
│   ├── json_file/
│   ├── template/
│   ├── hwp_pydantic.py
│   ├── hwp_struct.py
│   ├── hwp_xml.py
│   ├── jbnu_note.xml
│   ├── jbnu_pydantic_file.py
│   ├── jbnu_report.py
│   ├── model_json.py
│   └── note.xml
├── llm_agent/
│   ├── __pycache__/
│   ├── kpf-sbert-128d-v1/
│   ├── KURE-v1/
│   ├── csv_2_db.py
│   ├── embedding.py
│   ├── graph.py 그래프 생성
│   ├── preprocess.py
│   ├── search.py 데이터 검색
│   └── sql_report.py SQL문 생성 및 응답 생성
├── Dockerfile
├── logo.png
├── logo1.png
├── requirements.txt
├── server.py
├── streamlit_app.py
└── temp.py
```
</details>


