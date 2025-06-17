import pandas as pd
import glob
import os
import warnings

warnings.filterwarnings("ignore")


def infer_header_rows(df):
    df_copy = df.copy()
    max_depth = len(df_copy)
    depth = 1  # 규칙 1: 최소 1개는 헤더로 간주

    # 규칙 2: 첫 번째 행 확인
    row0 = df_copy.iloc[0].ffill().to_list()
    df_copy.iloc[0] = row0
    for i in range(len(row0) - 1):
        val1, val2 = row0[i], row0[i + 1]
        if pd.notna(val1) and pd.notna(val2) and val1 == val2 and val1 not in ["", "-"]:
            depth = 2
            break  # 최소 2개로 설정하고 나머지 행 검사로 넘어감

    # 규칙 3: 두 번째 행부터는 위와 다르면 추가
    for row_idx in range(1, max_depth):
        df_copy.iloc[:row_idx] = df_copy.iloc[:row_idx].ffill()
        row = df_copy.iloc[row_idx].ffill().to_list()
        prev_row = df_copy.iloc[row_idx - 1].ffill().to_list()
        df_copy.iloc[row_idx] = row

        repeated_with_change = False
        for i in range(len(row) - 1):
            val1, val2 = row[i], row[i + 1]
            prev_val1, prev_val2 = prev_row[i], prev_row[i + 1]

            if (
                pd.notna(val1) and pd.notna(val2) and val1 == val2 and val1 not in ["", "-"] and
                pd.notna(prev_val1) and pd.notna(prev_val2) and prev_val1 == prev_val2
            ):
                repeated_with_change = True
                break

        if repeated_with_change:
            depth += 1
        else:
            break  # 조건에 맞지 않으면 헤더 확장 중지

    return depth


def preprocess_excel_with_variable_header(file_path):
    # Step 1: 파일 전체 불러오기
    df_raw = pd.read_excel(file_path, header=None)

    # Step 1.5: 모든 값이 NA인 열 제거
    df_raw = df_raw.dropna(axis=1, how="all")

    # Step 2: 병합된 셀 보정
    df_raw = df_raw.ffill()

    # Step 3: 헤더 줄 수 추론
    inferred_header_rows = infer_header_rows(df_raw)

    df_raw.iloc[:inferred_header_rows] = df_raw.iloc[:inferred_header_rows].ffill(axis=1)

    # Step 4: 헤더 생성
    data_start_row = inferred_header_rows
    if inferred_header_rows == 1:
        # 단일 헤더
        headers = df_raw.iloc[0]
    else:
        # 다중 헤더 → 문자열 결합
        header_df = df_raw.iloc[0:inferred_header_rows]
        combined = header_df.astype(str).apply(lambda x: "_".join(x), axis=0)

        def remove_redundant_prefix(header):
            parts = header.split("_")
            seen = set()
            unique_parts = []
            for part in parts:
                if part not in seen:
                    seen.add(part)
                    unique_parts.append(part)
            return "_".join(unique_parts)

        headers = combined.apply(remove_redundant_prefix)

    # Step 5: 데이터 정리
    df_data = df_raw.iloc[data_start_row:, :].copy()
    df_data.columns = headers
    df_data.reset_index(drop=True, inplace=True)
    df_data.replace("-", pd.NA, inplace=True)
    df_data.replace("X", pd.NA, inplace=True)

    return df_data


def data_save(df, load_path, save_path='./data/csv_data'):
    data_save_path = save_path + '/' + load_path.split('/')[-1][:-5] + '.csv'
    df.to_csv(data_save_path, index = False)



# def preprocess_run(file_path):
#     load_path = os.path.abspath('../data/xlsx_data')
#     save_path = os.path.abspath('../data/csv_data')
#     xlsx_path = glob.glob(os.path.join(load_path, "*.xlsx"))

#     print(f"[DEBUG] 대상 엑셀 파일: {xlsx_path}")

#     # a = 0
#     # for xp in xlsx_path:
#     #     df = preprocess_excel_with_variable_header(xp)
#     #     data_save(df, xp, save_path)
#     #     a += 1
#     # print(a)

#     count = 0
#     for xp in xlsx_path:
#         try:
#             df = preprocess_excel_with_variable_header(xp)
#             data_save(df, xp, save_path)
#             count += 1
#         except Exception as e:
#             print(f"[ERROR] 파일 처리 실패: {xp}, 이유: {e}")

#     print(f"[INFO] 총 {count}개 파일 저장 완료")

def preprocess_run(file_path):
    save_path = os.path.abspath('./data/csv_data')
    os.makedirs(save_path, exist_ok=True)

    try:
        df = preprocess_excel_with_variable_header(file_path)
        data_save(df, file_path, save_path)
        print(f"[INFO] 파일 처리 및 저장 완료")
    except Exception as e:
        print(f"[ERROR] 파일 처리 실패: {file_path}, 이유: {e}")
        raise