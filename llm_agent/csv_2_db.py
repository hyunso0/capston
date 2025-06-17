import os
import glob
import pandas as pd
import sqlite3


if __name__ == "__main__":
    # DB 경로 설정
    db_path = "../data/database.db"

    # 기존 DB 파일 삭제 (초기화)
    if os.path.exists(db_path):
        os.remove(db_path)

    # CSV 파일 목록 수집
    csv_path = glob.glob(os.path.join("./data/csv_data", "*.csv"))

    # SQLite 연결
    conn = sqlite3.connect(db_path)

    # 각 CSV 파일을 SQLite에 테이블로 저장
    for cp in csv_path:
        file_name = os.path.basename(cp).replace(".csv", "")
        df = pd.read_csv(cp)
        df.to_sql(file_name, conn, if_exists="replace", index=False)

    # 연결 종료
    conn.close()