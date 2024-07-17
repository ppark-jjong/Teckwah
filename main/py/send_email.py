import pandas as pd
import mysql.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

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

def send_email_with_attachment(email_config, file_path):
    """첨부 파일과 함께 이메일을 보냅니다."""
    msg = MIMEMultipart()
    msg['From'] = email_config['from_email']
    msg['To'] = email_config['to_email']
    msg['Subject'] = email_config['subject']

    body = email_config['body']
    msg.attach(MIMEText(body, 'plain'))

    attachment = open(file_path, 'rb')
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f"attachment; filename= {file_path}")

    msg.attach(part)

    server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
    server.starttls()
    server.login(email_config['from_email'], email_config['password'])
    text = msg.as_string()
    server.sendmail(email_config['from_email'], email_config['to_email'], text)
    server.quit()

    print(f"이메일이 성공적으로 {email_config['to_email']}로 보내졌습니다.")

if __name__ == "__main__":
    query = "SELECT * FROM Receiving_TAT_Report"
    db_data = get_database_data(query)
    
    csv_file_path = "C:\\MyMain\\Teckwah\\download\\exported_data.csv"
    
    email_config = {
        'from_email': 'parkjonghyeok2000@gmail.com',        # 보내는 사람 이메일 주소
        'to_email': 'jonghp1357@naver.com',      # 받는 사람 이메일 주소
        'subject': '이번주 유지 데이터 입니다.',
        'body': 'Here is the exported data from the database.',
        'smtp_server': 'smtp.gmail.com', #gmail smtp 공식 서버 주소 (교체 x)         
        'smtp_port': 587,                             
        'password': 'egih cqix vpdl ixzh' #앱 비밀번호 생성 후 앱 비밀번호 사용           
    }
    
    send_email_with_attachment(email_config, csv_file_path)
