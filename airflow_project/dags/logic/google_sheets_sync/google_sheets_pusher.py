import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from sqlalchemy import create_engine

def push_mart_to_google_sheets(mart_table_name, sheet_url, target_tab_name):
    """
    Hàm này đọc 1 bảng Mart từ Postgres (gold)
    và đẩy (push) nó lên 1 Google Sheet, vào 1 TAB (Sheet) cụ thể.
    """
    print(f"Bắt đầu đẩy {mart_table_name} vào tab {target_tab_name}...")
    
    # 1. Kết nối Google
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    # Airflow chạy từ /opt/airflow/
    creds_path = "/opt/airflow/dags/logic/google_sheets_sync/credentials.json" 
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)

    # 2. Kết nối CSDL (Postgres)
    db_url = "postgresql://careerbi_user:careerbi_password@postgres:5432/careerbi"
    engine = create_engine(db_url)
    
    # 3. Đọc dữ liệu từ Mart (schema 'gold')
    df = pd.read_sql(f"SELECT * FROM gold.{mart_table_name}", engine)

    # 4. Đẩy (push) lên Google Sheets
    try:
        spreadsheet = client.open_by_url(sheet_url)
    except Exception as e:
        print(f"Lỗi: Không thể mở Google Sheet. URL đúng chưa? Bot đã được share chưa? {e}")
        raise

    try:
        # 4a. Thử tìm tab (sheet)
        worksheet = spreadsheet.worksheet(target_tab_name)
        worksheet.clear() # Xóa dữ liệu cũ
        print(f"Đã tìm thấy tab '{target_tab_name}', đang xóa dữ liệu cũ...")
    except gspread.WorksheetNotFound:
        # 4b. Nếu không thấy, tạo tab mới
        print(f"Không tìm thấy tab '{target_tab_name}', đang tạo tab mới...")
        rows, cols = df.shape
        worksheet = spreadsheet.add_worksheet(title=target_tab_name, rows=rows+1, cols=cols)
    
    # 5. Ghi đè (Write)
    set_with_dataframe(worksheet, df)
    print(f"Đã đẩy thành công {mart_table_name} vào tab '{target_tab_name}'.")