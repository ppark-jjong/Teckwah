import os
from datetime import datetime
from get_data_by_date import database.py


def save_to_csv(df, start_date, end_date):
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    filename = f"data_{start_date}_{end_date}.csv"
    file_path = os.path.join(output_dir, filename)
    df.to_csv(file_path, index=False)
    print(f"데이터가 {file_path}에 저장되었습니다.")
