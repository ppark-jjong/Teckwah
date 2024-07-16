import pandas as pd
import unittest
from sqlalchemy import create_engine
from config import DB_CONFIG
from datetime import datetime

class TestDataIntegrity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 사용자로부터 날짜 입력 받기
        cls.start_date = input("시작 날짜를 입력하세요 (YYYY-MM-DD 형식): ")
        cls.end_date = input("종료 날짜를 입력하세요 (YYYY-MM-DD 형식): ")
        
        # 입력된 날짜를 datetime 객체로 변환
        cls.start_date = datetime.strptime(cls.start_date, "%Y-%m-%d").date()
        cls.end_date = datetime.strptime(cls.end_date, "%Y-%m-%d").date()

        # Raw 데이터 로드 (Excel 파일)
        cls.raw_data_file = "C:/MyMain/Teckwah/download/xlsx_files/240706_240712_ReceivingTAT_report.xlsx"
        cls.raw_data = pd.read_excel(cls.raw_data_file, sheet_name="CS Receiving TAT")
        
        # DB 연결 설정
        connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        cls.engine = create_engine(connection_string)
        
        # DB에서 데이터 로드
        cls.db_data = pd.read_sql_table('receiving_tat_report', cls.engine)

        # InventoryDate를 datetime 형식으로 변환
        cls.db_data['InventoryDate'] = pd.to_datetime(cls.db_data['InventoryDate'])
        cls.raw_data['InventoryDate'] = pd.to_datetime(cls.raw_data['PutAwayDate']).dt.date

        # 입력받은 날짜 범위로 데이터 필터링
        cls.db_filtered = cls.db_data[(cls.db_data['InventoryDate'].dt.date >= cls.start_date) & 
                                      (cls.db_data['InventoryDate'].dt.date <= cls.end_date)]
        cls.raw_filtered = cls.raw_data[(cls.raw_data['InventoryDate'] >= cls.start_date) & 
                                        (cls.raw_data['InventoryDate'] <= cls.end_date)]

    def print_result(self, test_name, result, details=''):
        print(f"\n{'=' * 50}")
        print(f"테스트: {test_name}")
        print(f"결과: {'성공' if result else '실패'}")
        if details:
            print(f"상세 정보:\n{details}")
        print(f"{'=' * 50}\n")

    def test_row_count_and_duplicates(self):
        raw_count = len(self.raw_filtered)
        db_count = len(self.db_filtered)
        duplicate_count = raw_count - len(self.raw_filtered.drop_duplicates())

        details = (f"필터링된 Raw 데이터 행 수: {raw_count}\n"
                   f"필터링된 DB 데이터 행 수: {db_count}\n"
                   f"필터링된 Raw 데이터 중복 행 수: {duplicate_count}")

        result = raw_count >= db_count
        self.print_result("행 수 및 중복 확인", result, details)
        self.assertTrue(result, "Raw 데이터의 행 수가 DB 데이터보다 많아야 합니다.")

    def test_data_integrity(self):
        # Raw 데이터 그룹화 (Replen_Balance_Order와 Allocated Part 모두 고려)
        raw_grouped = self.raw_filtered.groupby(['Replen/Balance Order#', 'Allocated Part#']).agg({
            'Quantity': 'sum',
            'Cust Sys No': 'first'
        }).reset_index()

        # CountPO 계산 (Cust Sys No 기준)
        count_po = self.raw_filtered.groupby('Cust Sys No').size().reset_index(name='CountPO')
        raw_grouped = raw_grouped.merge(count_po, on='Cust Sys No', how='left')

        mismatches = []
        for index, db_row in self.db_filtered.iterrows():
            raw_row = raw_grouped[
                (raw_grouped['Replen/Balance Order#'] == db_row['Replen_Balance_Order']) &
                (raw_grouped['Allocated Part#'] == db_row['Allocated_Part'])
            ]
            if not raw_row.empty:
                if db_row['Quantity'] != raw_row['Quantity'].iloc[0] or db_row['Count_PO'] != raw_row['CountPO'].iloc[0]:
                    mismatches.append(f"Replen_Balance_Order: {db_row['Replen_Balance_Order']}, "
                                      f"Allocated_Part: {db_row['Allocated_Part']}, "
                                      f"DB Quantity: {db_row['Quantity']}, Raw Quantity: {raw_row['Quantity'].iloc[0]}, "
                                      f"DB Count_PO: {db_row['Count_PO']}, Raw CountPO: {raw_row['CountPO'].iloc[0]}")
            else:
                mismatches.append(f"Replen_Balance_Order {db_row['Replen_Balance_Order']} "
                                  f"with Allocated_Part {db_row['Allocated_Part']} not found in raw data")

        result = len(mismatches) == 0
        details = "\n".join(mismatches) if mismatches else "모든 데이터가 일치합니다."
        self.print_result("데이터 정합성 확인", result, details)
        self.assertTrue(result, "DB 데이터와 Raw 데이터 간 불일치가 있습니다.")

    def test_all_db_rows_in_raw(self):
        raw_orders = set(zip(self.raw_filtered['Replen/Balance Order#'], self.raw_filtered['Allocated Part#']))
        db_orders = set(zip(self.db_filtered['Replen_Balance_Order'], self.db_filtered['Allocated_Part']))

        missing_orders = db_orders - raw_orders
        result = len(missing_orders) == 0
        details = f"Raw 데이터에 없는 DB 행: {missing_orders}" if missing_orders else "모든 DB 행이 Raw 데이터에 존재합니다."
        self.print_result("DB 행 존재 여부 확인", result, details)
        self.assertTrue(result, "일부 DB 행이 Raw 데이터에 존재하지 않습니다.")

    def test_data_transformation(self):
        mismatches = []
        for index, db_row in self.db_filtered.iterrows():
            raw_rows = self.raw_filtered[
                (self.raw_filtered['Replen/Balance Order#'] == db_row['Replen_Balance_Order']) &
                (self.raw_filtered['Allocated Part#'] == db_row['Allocated_Part'])
            ]
            
            if not raw_rows.empty:
                if db_row['EDI_Order_Type'] != raw_rows['EDI Order Type'].iloc[0]:
                    mismatches.append(f"EDI_Order_Type mismatch for Replen_Balance_Order {db_row['Replen_Balance_Order']}, "
                                      f"Allocated_Part {db_row['Allocated_Part']}: "
                                      f"DB: {db_row['EDI_Order_Type']}, Raw: {raw_rows['EDI Order Type'].iloc[0]}")
                
                expected_order_type = self.get_expected_order_type(db_row['EDI_Order_Type'])
                if db_row['OrderType'] != expected_order_type:
                    mismatches.append(f"OrderType mismatch for Replen_Balance_Order {db_row['Replen_Balance_Order']}, "
                                      f"Allocated_Part {db_row['Allocated_Part']}: "
                                      f"DB: {db_row['OrderType']}, Expected: {expected_order_type}")
            else:
                mismatches.append(f"Replen_Balance_Order {db_row['Replen_Balance_Order']} "
                                  f"with Allocated_Part {db_row['Allocated_Part']} not found in raw data")

        result = len(mismatches) == 0
        details = "\n".join(mismatches) if mismatches else "모든 데이터 변환이 올바르게 수행되었습니다."
        self.print_result("데이터 변환 확인", result, details)
        self.assertTrue(result, "데이터 변환 과정에서 불일치가 발생했습니다.")

    @staticmethod
    def get_expected_order_type(edi_order_type):
        if edi_order_type in ['BALANCE-IN', 'REPLEN-IN']:
            return 'P3'
        elif edi_order_type == 'PURGE-IN':
            return 'Purge'
        elif edi_order_type in ['PNAE-IN', 'PNAC-IN']:
            return 'P1'
        else:
            return 'P6'  # DISPOSE-IN 또는 기타 케이스

if __name__ == '__main__':
    unittest.main(verbosity=2)