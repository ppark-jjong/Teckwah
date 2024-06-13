import mysql.connector
from mysql.connector import Error


class MySQLConnection:
    def __init__(self, host, database, user, password, allow_local_infile=True):
        self.connection_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "allow_local_infile": allow_local_infile,
        }
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.connection_params)
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                print("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
        except Error as e:
            print(f"데이터베이스 연결 실패: {e}")
            self.connection = None
            self.cursor = None

    def close(self):
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            print("MySQL 연결이 닫혔습니다.")

    def execute_query(self, query, data=None):
        try:
            if data:
                self.cursor.execute(query, data)
            else:
                self.cursor.execute(query)
        except Error as e:
            print(f"쿼리 실행 실패: {e}")
