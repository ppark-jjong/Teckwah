import os
import pandas as pd
import openpyxl
import shutil
from mysql_connection import MySQLConnection

# 다운로드 폴더 경로 설정
download_folder = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
xlsx_complete_folder = os.path.join("C:\\MyMain\\Teckwah\\download\\", "xlsx_files_complete")

# 폴더가 존재하지 않으면 생성
os.makedirs(xlsx_complete_folder, exist_ok=True)

# MySQL 연결 설정
mysql_connection_params = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,  # LOCAL INFILE 옵션 활성화
}

def sanitize_column_names(df):
    # 공백을 밑줄로 대체
    df.columns = [col.strip().replace(" ", "_") for col in df.columns]
    # 중복 열 이름에 고유 식별자 추가
    seen = {}
    new_columns = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_columns.append(col)
    df.columns = new_columns
    return df

def create_table_from_df(cursor, table_name, df):
    columns_with_types = ", ".join([f"`{col}` TEXT" for col in df.columns])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {columns_with_types}
    );
    """
    cursor.execute(create_table_sql)

def insert_data_from_df(cursor, table_name, df):
    for _, row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(row))
        columns = ", ".join([f"`{col}`" for col in df.columns])
        insert_sql = f"""
        INSERT INTO `{table_name}` ({columns})
        VALUES ({placeholders});
        """
        cursor.execute(
            insert_sql, tuple(str(value) if pd.notna(value) else None for value in row)
        )

def process_xlsx_file(xlsx_file, mysql_connection_params):
    xlsx_path = os.path.join(download_folder, xlsx_file)
    table_name = os.path.splitext(xlsx_file)[0]

    # 파일 읽기
    df = pd.read_excel(xlsx_path)

    # 컬럼 이름 정리
    df = sanitize_column_names(df)

    # MySQL 연결
    with MySQLConnection(**mysql_connection_params) as db:
        cursor = db.cursor

        # 테이블 생성
        create_table_from_df(cursor, table_name, df)

        # 데이터 삽입
        insert_data_from_df(cursor, table_name, df)

        db.connection.commit()

    # 파일을 완료 폴더로 이동
    shutil.move(xlsx_path, os.path.join(xlsx_complete_folder, xlsx_file))
    print(f"파일 처리 완료: {xlsx_file}")

# 메인 실행 부분
if __name__ == "__main__":
    # 콘솔로부터 파일명 입력받기
    xlsx_file_name = input("처리할 xlsx 파일명을 입력하세요: ")
    
    if os.path.exists(os.path.join(download_folder, xlsx_file_name)):
        process_xlsx_file(xlsx_file_name, mysql_connection_params)
    else:
        print(f"파일이 존재하지 않습니다: {xlsx_file_name}")
