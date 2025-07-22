import os
import re
import difflib
import glob
import sqlite3
from io import StringIO
import pandas as pd
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase


# 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # == /app

# 정확한 DB 경로 설정
DB_PATH = os.path.join(BASE_DIR, "data", "database.db")
CSV_DIR = os.path.join(BASE_DIR, "data", "csv_data")
BASE_URL = ""
MODEL_NAME = "Qwen3-14B"


# DB 초기화 및 CSV 파일 읽기 (원하는 테이블만)
include_tables = ["전라북도_대학교_면적", "전라북도_대학교_인원현황"]  # 원하는 테이블명

conn = sqlite3.connect(DB_PATH)
csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))
for cp in csv_files:
    table_name = os.path.basename(cp)[:-4]
    if table_name in include_tables:
        df = pd.read_csv(cp)
        df.to_sql(table_name, conn, if_exists="replace", index=False)

cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
table_names = [row[0] for row in cursor.fetchall() if row[0] in include_tables]


# LLM 연결
llm = ChatOpenAI(
    base_url=BASE_URL,
    api_key="not-needed",
    model=MODEL_NAME,
    max_tokens=5000,
)

db = SQLDatabase.from_uri(f"sqlite:///{DB_PATH}")


# 테이블 스키마 정보 생성
def generate_table_info_with_full_values(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    schema_rows = cursor.fetchall()
    df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)

    table_info = f'Table Name: "{table_name}"\nColumns:'
    for row in schema_rows:
        col_name = row[1]
        col_type = row[2].upper()

        if col_type == "TEXT":
            unique_values = df[col_name].dropna().unique().tolist()
            unique_values = sorted(str(val) for val in unique_values)
            examples = ", ".join(f'"{v}"' for v in unique_values)
            table_info += f'\n- "{col_name}" (TEXT) -- 가능한 값: [{examples}]'
        elif col_type in ["INTEGER", "REAL", "FLOAT", "NUMERIC", "DOUBLE"]:
            if not df[col_name].dropna().empty:
                min_val = df[col_name].min()
                max_val = df[col_name].max()
                table_info += f'\n- "{col_name}" ({col_type}) -- 범위: [{min_val} ~ {max_val}]'
            else:
                table_info += f'\n- "{col_name}" ({col_type})'
        else:
            table_info += f'\n- "{col_name}" ({col_type})'

    return table_info

final_table_info = "\n\n\n".join(
    generate_table_info_with_full_values(conn, table) for table in table_names
)

# SQL 프롬프트
sql_prompt = ChatPromptTemplate.from_messages([
    ("system", 
     """You are an expert SQL query generator.

You MUST strictly follow all the following rules without exception:

1. Read the {user_question} carefully and understand the user's intent.

2. Use ONLY the exact table names and column names that appear in {table_info}.
   - DO NOT create, guess, hallucinate, abbreviate, translate, transliterate, or otherwise alter any names.
   - Language, spacing, special characters, and casing MUST match exactly as shown in {table_info}.
   - You MUST NOT mix languages. Use ONLY the original Korean text from {table_info} without replacing with Chinese characters, English letters, or other forms.
   
3. If an exact match is not possible, select the most semantically similar column strictly FROM {table_info} ONLY.

4. ALWAYS produce a valid SELECT SQL query starting with SELECT and ending with ;.

5. NEVER use JOINs, UNIONs, CTEs (WITH), subqueries, or any other query combination techniques.
   - Absolutely NO JOIN, UNION, WITH, CTE, or nested SELECT statements allowed.
   
6. Each SELECT query MUST use exactly ONE table.
   - In the WHERE clause, you MUST refer to columns from only the table declared in the FROM clause.
   - NEVER mix multiple tables inside a WHERE clause.
   
7. If multiple queries are needed, output multiple completely separate SELECT queries.

8. Wrap table names and column names in double quotes (") if they contain Korean characters, spaces, or parentheses.

9. DO NOT quote SQL reserved keywords (SELECT, FROM, WHERE, GROUP BY, etc.).

10. NEVER output INVALID_REQUEST.

11. Output ONLY pure SQL statements, with no explanations, descriptions, or comments.

12. You MUST reflect **ALL aspects** of the user's question in your query result.
   - If the question asks for multiple attributes, time ranges, or groups, your queries MUST include ALL of them.
   - If any part of the user's intent is missing, your output is INVALID.

13. Try to achieve the user's goal with the **minimum number of SELECT queries** possible.
   - Do NOT split queries unnecessarily.
   - Group multiple conditions into one query if they refer to the same table and context.

14. If the user's question requires a single aggregated number (e.g., total population, overall count, sum by region or year),
    you MUST include appropriate filter conditions to avoid ambiguity. 
    For example:
    - If data is broken down by age group and the question refers to overall population,
      you MUST add: WHERE "구역별" = '합계'
    - If data includes multiple subcategories, always filter for the correct aggregation level.

15. Even if not explicitly mentioned in the user's question, 
    you MUST include any categorical columns (e.g., "구역", "성별", "구분", etc.) 
    that are essential to understanding the meaning of the result values.

    - This applies even if the same value appears repeatedly across multiple rows.
    - If a column represents a category, grouping, or dimension that contextualizes the result (e.g., age group, gender, region), it MUST be included.
    - This ensures that each row in the result can be interpreted independently and unambiguously.

16. When performing division, be aware that SQL integer division (e.g., INTEGER / INTEGER) will discard all decimal points.
    To avoid incorrect results, you MUST force decimal division by explicitly casting at least one operand as REAL (e.g., CAST(column AS REAL) / other_column).
    
    - You MUST always format all ratio or division results to show **4 decimal places** of precision.
    - Use either CAST(... AS REAL), 1.0 * column, or ROUND(..., 4) depending on the database.
    
    Example:
    SELECT ROUND(CAST("A" AS REAL) / "B", 4) FROM "table";


Example:
  SELECT "년도", "서울특별시_총인구" FROM "전국_인구_통계" WHERE "구분" = '합계' ORDER BY "년도";

Provided Table Schema:
{table_info}

User Question:
{user_question}

Output ONLY the final executable SQL query or queries. Any deviation from these rules will be considered a critical error."""
    ),
    ("human", 
     """Based on the above table_info and user_question, generate ONLY raw SQL queries.
- Follow the rules strictly without any deviation.
- You MUST cover 100% of the user's informational needs.
- Use the **fewest number of queries** necessary to do so.
- Output pure SQL text only, with no explanations."""
    )
])

sql_chain = sql_prompt | llm


# 분석 보고서
response_prompt = ChatPromptTemplate.from_template("""
You are a professional analyst responsible for writing formal reports based on statistical data. Below is a user question and the result of an SQL query presented in CSV format.

[User Question]
{question}

[SQL Result Table (CSV Format)]
{table}

Write a **formal, structured, and richly detailed data analysis report in Korean** following the instructions below.

---

**Report Structure**:

1. Title: Bold and clear at the top.

2. Introduction:
   - The introduction must be structured into **three clearly labeled subsections** with the following titles:
    분석 주제, 분석 목적, 필요성.
   - Each subsection must consist of **one short paragraph of approximately 150 characters** in formal Korean.
   - Do NOT combine multiple ideas in a single sentence. Each paragraph must stay strictly within its theme.
   - Detailed guidance for each subsection:
     
    **분석 주제**: Clearly state what the report is about — the main subject and what kind of data or indicator is being examined.
     
    **분석 목적**: Explain why this analysis is being conducted — what question it aims to answer or what problem it addresses.
     
    **필요성**: Justify the importance of this analysis — explain why it matters socially, economically, or institutionally.

   - Avoid overly general phrases. Be precise and concise in each subsection.
   - Do NOT exceed 2 sentences per subsection. Each must remain around 150 characters in total.
   
3. Body:
   - Use numbered section headings.
   - In each section:
     - Write a natural flowing paragraph explaining numerical changes.
     - Insert **only one markdown table** per section. Do not use charts or visualizations.
     - The table must follow the exact syntax and layout described below.
     - When constructing the table:
       - Prefer a **vertical layout** (few columns, more rows) rather than a wide horizontal format.
       - **Exclude any columns or rows that are unnecessary** for understanding the key points.
     - After the table:
       - Leave **two blank lines**
       - On a new line, insert the table title using **this exact format**:
         
         ![짧고 간결한 표 제목]

       - Then write a paragraph interpreting the table’s significance (e.g., 증감, 추세, 비교, 시사점).
   - Use tables consistently throughout the report to support your analysis.

4. Conclusion: Summarize all key findings and suggest societal or policy implications.
   - Absolutely no extra notes or reminders after the Conclusion.

---

**Table Insertion Rules**:

You MUST follow the EXACT syntax and layout below when inserting a table.

1. Tables must be written using **valid markdown table syntax**:

   | 연도 | 서울시 | 부산시 |
   |------|--------|--------|
   | 2020 | 2,345  | 1,234  |
   | 2021 | 5,678  | 3,456  |

2. When constructing the table:
   - Exclude any columns or rows that are unnecessary for understanding the key point.
   - **If a column contains only null, empty, or missing values (e.g., None, NaN, ""), you MUST omit that column entirely from the table.**

3. After the table:
   - Insert **two blank lines**
   - Then insert the table title on its own line using this exact format:
     
     ![짧고 간결한 표 제목]

4. Strict rules for the table title:
   - DO NOT write titles like `**표 1: 서울시 인구**`, `표 1:`, or any bold/numbered form.
   - DO NOT use `[[...]]`.
   - Use ONLY the `![표 제목]` syntax — no numbering, no formatting, no alternatives.

5. Then write a paragraph explaining what the table shows (e.g., trends, causes, changes).

6. Only one table per section. Do not include any charts, graphs, or visual elements.

---

**Writing Style**:

- Use formal, academic, and professional Korean.
- Write in complete paragraphs. Do not use bullet points or lists.
- Do **not** list values or attributes using commas (예: “2020년, 2021년, 2022년” → 사용 금지).
- Each paragraph must consist of **2~3 well-structured sentences** that logically develop a single idea.
- **Do not write overly long sentences.** Each sentence should be concise and express only one core point.
- Maintain a smooth logical flow between sentences within each paragraph.
- Explain numerical changes precisely, including 증가율, 감소율, 증감량 등.
- Discuss observed trends, underlying causes, and their implications clearly and naturally.

---

**Markdown Syntax Reminder**:

- Always put spaces around tilde (~) in numeric ranges.
  - Correct: 2020 ~ 2022, 12세 ~ 21세
  - Incorrect: 2020~2022, 12세~21세

---

Start writing the report now, strictly following all these instructions.
""")


response_chain = response_prompt | llm

# 유틸 함수
def contains_chinese(text):
    return bool(re.search(r'[\u4e00-\u9fff]', text))


def correct_sql_table_names(sql_raw):
    def correct_table_name(name):
        return difflib.get_close_matches(name, table_names, n=1, cutoff=0.7)[0] if difflib.get_close_matches(name, table_names, n=1, cutoff=0.7) else name
    for match in re.findall(r'FROM\s+\"([^\"]+)\"|JOIN\s+\"([^\"]+)\"', sql_raw):
        table_name = match[0] or match[1]
        if contains_chinese(table_name) or (table_name not in table_names):
            corrected = correct_table_name(table_name)
            sql_raw = sql_raw.replace(f'"{table_name}"', f'"{corrected}"')
    return sql_raw


def extract_select_queries(text):
    return re.findall(r"(SELECT[\s\S]*?;)", text, flags=re.IGNORECASE)


def normalize_tilde_spacing(text: str) -> str:
    return re.sub(r'\s*~\s*', ' ~ ', text)


def extract_all_markdown_tables(text):
    lines = text.splitlines()
    tables = []
    current_table = []
    in_table = False

    for line in lines:
        stripped = line.strip()
        if re.match(r'^\|.*\|$', stripped):
            if re.match(r'^\|\s*-+', stripped):  # 구분선 제외
                continue
            # 숫자 사이 공백 제거
            stripped = re.sub(r'(?<=\d)\s+(?=\d)', '', stripped)
            current_table.append(stripped)
            in_table = True
        elif in_table:
            if current_table:
                df = convert_table(current_table)
                tables.append(df)
                current_table = []
            in_table = False

    if current_table:
        df = convert_table(current_table)
        tables.append(df)

    return tables


def convert_table(lines):
    table_str = '\n'.join(lines)
    df = pd.read_csv(StringIO(table_str), sep='|', engine='python', skipinitialspace=True)

    # 인덱스 열 제거
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # 열 이름 및 셀 값 좌우 공백 제거
    df.columns = df.columns.str.strip()
    df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)

    # 쉼표 제거
    df.replace(',', '', regex=True, inplace=True)

    # 숫자형 변환 시도
    for col in df.columns[1:]:
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception:
            pass

    return df.reset_index(drop=True)


def run_sql_analysis(user_query):
    global table_name, df_table  # streamlit에서 가져가기 위함

    sql_max_retry = 3
    sql_retry = 0
    sql_success = False

    while not sql_success and sql_retry < sql_max_retry:
        try:
            sql_response = sql_chain.invoke({
                "table_info": final_table_info,
                "top_k": 1000,
                "user_question": user_query
            })

            # sql_queries = extract_select_queries(sql_response.content.split('</think>')[-1])
            sql_queries = extract_select_queries(sql_response.content)
            df_result = []

            for i, sql_raw in enumerate(sql_queries):
                print(f"Trying SQL Query {i + 1}...")
                sql_corrected = correct_sql_table_names(sql_raw)
                print(sql_corrected)
                df = pd.read_sql(sql_corrected, db._engine)

                if df.empty:
                    raise ValueError("쿼리 실행 결과가 비어있습니다.")

                df_result.append({
                    "query": sql_corrected,
                    "dataframe": df
                })

            sql_success = True

        except Exception as e:
            print(f"에러 발생: {e}")
            sql_retry += 1
            print(f"재시도 {sql_retry}/{sql_max_retry}")

    if not sql_success:
        raise RuntimeError("SQL 쿼리 생성 및 실행에 실패했습니다.")


    response_max_retry = 3
    response_retry = 0
    response_success = False

    while not response_success and response_retry < response_max_retry:
        try:
            response = response_chain.invoke({"question": user_query, "table": df_result})
            if not response.content.strip():
                raise ValueError("응답이 비어 있음 (response.content가 없음)")
            response_success = True
            response_print = normalize_tilde_spacing(response.content)
            print(response_print)
        except Exception as e:
            print(f"자연어 응답 생성 오류: {e}")
            response_retry += 1
            print(f"자연어 응답 재시도 {response_retry}/{response_max_retry}")

    if not response_success:
        raise RuntimeError("자연어 응답 생성에 실패했습니다.")
    
    tables = extract_all_markdown_tables(response_print)
    df_table = [df for df in tables]
    table_name = re.findall(r'!\[(.*?)\]', response_print)

    print(response_print)
    
    return response_print, df_table, table_name
