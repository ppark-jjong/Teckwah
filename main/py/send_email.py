import os
import pandas as pd
import mysql.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import logging
from typing import Dict, List, Any

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 설정 정보 (실제로는 config.py나 환경 변수에서 가져와야 합니다)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "teckwah_test"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "1234"),
}

EMAIL_CONFIG = {
    'from_email': os.getenv("EMAIL_FROM", "parkjonghyeok2000@gmail.com"),
    'to_email': os.getenv("EMAIL_TO", "jonghp1357@naver.com"),
    'subject': '이번주 유지 데이터입니다.',
    'body': 'Here is the exported data from the database.',
    'smtp_server': os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    'smtp_port': int(os.getenv("SMTP_PORT", 587)),
    'password': os.getenv("EMAIL_PASSWORD", "egih cqix vpdl ixzh")
}

def get_database_connection():
    """데이터베이스 연결을 생성합니다."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logger.info("데이터베이스 연결 성공")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"데이터베이스 연결 실패: {err}")
        raise

def get_database_data(query: str) -> List[Dict[str, Any]]:
    """데이터베이스에서 데이터를 조회합니다."""
    try:
        with get_database_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
        logger.info(f"{len(data)}개의 레코드를 성공적으로 조회했습니다.")
        return data
    except mysql.connector.Error as err:
        logger.error(f"데이터 조회 실패: {err}")
        raise

def export_to_csv(data: List[Dict[str, Any]], file_path: str) -> None:
    """데이터를 CSV 파일로 내보냅니다."""
    try:
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        logger.info(f"데이터가 성공적으로 {file_path}에 내보내졌습니다.")
    except Exception as e:
        logger.error(f"CSV 파일 내보내기 실패: {e}")
        raise

def create_email_message(email_config: Dict[str, str], file_path: str) -> MIMEMultipart:
    """이메일 메시지를 생성합니다."""
    msg = MIMEMultipart()
    msg['From'] = email_config['from_email']
    msg['To'] = email_config['to_email']
    msg['Subject'] = email_config['subject']

    msg.attach(MIMEText(email_config['body'], 'plain'))

    with open(file_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
    
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(file_path)}")
    msg.attach(part)

    return msg

def send_email(email_config: Dict[str, str], msg: MIMEMultipart) -> None:
    """이메일을 전송합니다."""
    try:
        with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
            server.starttls()
            server.login(email_config['from_email'], email_config['password'])
            server.send_message(msg)
        logger.info(f"이메일이 성공적으로 {email_config['to_email']}로 전송되었습니다.")
    except smtplib.SMTPException as e:
        logger.error(f"이메일 전송 실패: {e}")
        raise

def main():
    """메인 실행 함수"""
    try:
        query = "SELECT * FROM Receiving_TAT_Report"
        db_data = get_database_data(query)
        
        csv_file_path = os.path.join(os.getcwd(), "exported_data.csv")
        export_to_csv(db_data, csv_file_path)
        
        email_msg = create_email_message(EMAIL_CONFIG, csv_file_path)
        send_email(EMAIL_CONFIG, email_msg)

    except Exception as e:
        logger.error(f"프로세스 실행 중 오류 발생: {e}")
    finally:
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
            logger.info(f"임시 파일 {csv_file_path} 삭제됨")

if __name__ == "__main__":
    main()