import pandas as pd
import mysql.connector

# MySQL 연결 설정
db_config = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
}

def get_database_data(query):
    """데이터베이스에서 데이터를 조회합니다."""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data

def export_to_csv(data, file_path):
    """데이터를 CSV 파일로 내보냅니다."""
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print(f"데이터가 성공적으로 {file_path}에 내보내졌습니다.")

if __name__ == "__main__":
    query = "SELECT * FROM Receiving_TAT_Report"
    db_data = get_database_data(query)
    
    csv_file_path = "C:\\MyMain\\Teckwah\\download\\exported_data.csv"
    export_to_csv(db_data, csv_file_path)
