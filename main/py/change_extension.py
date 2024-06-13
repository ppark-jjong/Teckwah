import os
import pandas as pd
import openpyxl
from mysql_connection import MySQLConnection

# 다운로드 폴더  경로 설정
download_folder = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
xlsx_complete_folder = os.path.join(
    "C:\\MyMain\\Teckwah\\download\\", "xlsx_files_complete"
)

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

    try:
        # 엑셀 파일의 E1 셀을 "from_country_2"로 변경
        workbook = openpyxl.load_workbook(xlsx_path)
        sheet = workbook.active
        sheet["E1"] = "from_country_2"
        workbook.save(xlsx_path)

        # 엑셀 파일을 읽어서 DataFrame으로 변환
        df = pd.read_excel(xlsx_path)

        # 모든 값이 NaN인 열을 제거
        df.dropna(axis=1, how="all", inplace=True)

        # from_Country가 KR인 데이터만 필터링
        df = df[df["from_Country"] == "KR"]

        # 열 이름을 정리 (중복 제거 및 공백을 밑줄로 대체)
        df = sanitize_column_names(df)

        # MySQL 데이터베이스에 연결
        db_connection = MySQLConnection(**mysql_connection_params)
        db_connection.connect()

        # 테이블이 존재하지 않는 경우 생성
        create_table_from_df(db_connection.cursor, table_name, df)

        # 데이터 삽입
        insert_data_from_df(db_connection.cursor, table_name, df)

        # 커밋
        db_connection.connection.commit()

        # 변환된 .xlsx 파일을 xlsx_files_complete 폴더로 이동
        complete_path = os.path.join(xlsx_complete_folder, xlsx_file)
        os.rename(xlsx_path, complete_path)

        print(
            f"파일 변환 완료: {xlsx_file}를 {table_name} 테이블로 변환하고 {xlsx_complete_folder}로 이동했습니다."
        )
    except Exception as e:
        print(f"파일 변환 중 오류 발생: {xlsx_file}, 오류: {e}")
    finally:
        db_connection.close()


# 특정 파일명 입력 받기
target_file = input("처리할 .xlsx 파일명을 입력하세요 (확장자 포함): ")

if target_file in os.listdir(download_folder):
    process_xlsx_file(target_file, mysql_connection_params)
    print("선택된 파일을 MySQL로 변환했습니다.")
else:
    print("해당 파일이 다운로드 폴더에 존재하지 않습니다.")
