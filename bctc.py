import os
import time
import sqlite3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from vnstock import Vnstock
import pandas as pd
from tqdm import tqdm

# Hàm tải báo cáo với cơ chế retry
def download_report(func, *args, **kwargs):
    for attempt in range(5):
        try:
            df = func(*args, **kwargs)
            return df
        except Exception as e:
            print(f"Thử lần {attempt+1} thất bại: {e}")
            time.sleep(10)
    print("Tất cả các lần thử đều thất bại.")
    return None

# Hàm xử lý một mã cổ phiếu
def process_stock(symbol, exchange):
    # Đường dẫn cơ sở dữ liệu
    year_db_path = f"data/{exchange}/year/{symbol}.db"
    quarter_db_path = f"data/{exchange}/quarter/{symbol}.db"
    
    # Kết nối tới cơ sở dữ liệu
    conn_year = sqlite3.connect(year_db_path)
    conn_quarter = sqlite3.connect(quarter_db_path)
    
    # Tải dữ liệu cổ tức (lưu vào thư mục year)
    stock_tcbs = Vnstock().stock(symbol=symbol, source='TCBS')
    company = stock_tcbs.company
    print(f"Đang tải cổ tức cho {symbol}")
    df_dividends = download_report(company.dividends)
    if df_dividends is not None:
        df_dividends.to_sql('dividends', conn_year, if_exists='replace', index=False)
        print(f"Hoàn thành cổ tức cho {symbol}")
    else:
        print(f"Thất bại khi tải cổ tức cho {symbol}")
    time.sleep(30)
    
    # Tải các báo cáo tài chính
    stock_vci = Vnstock().stock(symbol=symbol, source='VCI')
    reports = [
        # Báo cáo cho thư mục year
        ('balance_sheet_year', stock_vci.finance.balance_sheet, {'period': 'year', 'lang': 'vi', 'dropna': True}, conn_year),
        ('income_statement_year', stock_vci.finance.income_statement, {'period': 'year', 'lang': 'vi', 'dropna': True}, conn_year),
        ('cash_flow_year', stock_vci.finance.cash_flow, {'period': 'year', 'dropna': True}, conn_year),
        ('ratios_year', stock_vci.finance.ratio, {'period': 'year', 'lang': 'vi', 'dropna': True}, conn_year),
        # Báo cáo cho thư mục quarter
        ('balance_sheet_quarter', stock_vci.finance.balance_sheet, {'period': 'quarter', 'lang': 'vi', 'dropna': True}, conn_quarter),
        ('income_statement_quarter', stock_vci.finance.income_statement, {'period': 'quarter', 'lang': 'vi', 'dropna': True}, conn_quarter),
        ('cash_flow_quarter', stock_vci.finance.cash_flow, {'period': 'quarter', 'dropna': True}, conn_quarter),
        ('ratios_quarter', stock_vci.finance.ratio, {'period': 'quarter', 'lang': 'vi', 'dropna': True}, conn_quarter),
    ]
    
    # Tải từng báo cáo với thanh tiến trình riêng
    for table_name, func, kwargs, conn in tqdm(reports, desc=f"Báo cáo cho {symbol}", leave=False):
        print(f"Đang tải {table_name} cho {symbol}")
        df = download_report(func, **kwargs)
        if df is not None:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Hoàn thành {table_name} cho {symbol}")
        else:
            print(f"Thất bại khi tải {table_name} cho {symbol}")
        time.sleep(30)
    
    # Đóng kết nối
    conn_year.close()
    conn_quarter.close()

# Hàm chính
def main():
    # Kiểm tra thời gian cập nhật cuối cùng
    if os.path.exists('last_update.txt'):
        with open('last_update.txt', 'r') as f:
            last_update = f.read()
        print(f"Thời gian cập nhật cuối cùng: {last_update}")
    else:
        last_update = "Chưa từng cập nhật"
        print("Chưa từng cập nhật dữ liệu.")
    
    update = input("Bạn có muốn cập nhật lại dữ liệu không? (Y/N): ").strip().upper()
    if update != 'Y':
        print("Thoát mà không cập nhật.")
        return
    
    # Tạo cấu trúc thư mục
    os.makedirs('data/HOSE/year', exist_ok=True)
    os.makedirs('data/HOSE/quarter', exist_ok=True)
    os.makedirs('data/HNX/year', exist_ok=True)
    os.makedirs('data/HNX/quarter', exist_ok=True)
    os.makedirs('stock_lists', exist_ok=True)
    
    # Tải danh sách cổ phiếu
    stock = Vnstock().stock(symbol='ACB', source='VCI')
    hose_symbols = stock.listing.symbols_by_group('HOSE')
    hnx_symbols = stock.listing.symbols_by_group('HNX')
    
    with open('stock_lists/hose_symbols.txt', 'w') as f:
        for symbol in hose_symbols:
            f.write(symbol + '\n')
    with open('stock_lists/hnx_symbols.txt', 'w') as f:
        for symbol in hnx_symbols:
            f.write(symbol + '\n')
    
    # Xử lý từng sàn
    for exchange, symbols in [('HOSE', hose_symbols), ('HNX', hnx_symbols)]:
        # Lọc mã có <= 3 ký tự
        filtered_symbols = [s for s in symbols if len(s) <= 3]
        batches = [filtered_symbols[i:i+40] for i in range(0, len(filtered_symbols), 40)]
        
        # Xử lý từng batch với thanh tiến trình chung
        for batch_idx, batch in enumerate(tqdm(batches, desc=f"Xử lý batch cho {exchange}")):
            print(f"Đang xử lý batch {batch_idx+1}/{len(batches)} cho {exchange}")
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = {executor.submit(process_stock, symbol, exchange): symbol for symbol in batch}
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Lỗi khi xử lý {symbol}: {e}")
            time.sleep(65)  # Nghỉ giữa các batch
    
    # Tạo tệp README.txt
    sample_stock = 'ACB'
    sample_exchange = 'HOSE'
    year_db_path = f"data/{sample_exchange}/year/{sample_stock}.db"
    quarter_db_path = f"data/{sample_exchange}/quarter/{sample_stock}.db"
    
    with open('README.txt', 'w', encoding='utf-8') as f:
        f.write(f"Thời gian cập nhật cuối cùng: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Cấu trúc thư mục year
        conn_year = sqlite3.connect(year_db_path)
        cursor_year = conn_year.cursor()
        year_tables = ['dividends', 'balance_sheet_year', 'income_statement_year', 'cash_flow_year', 'ratios_year']
        f.write("Thư mục Year:\n")
        for table in year_tables:
            cursor_year.execute(f"PRAGMA table_info({table})")
            columns = cursor_year.fetchall()
            if columns:
                f.write(f"Bảng: {table}\n")
                for col in columns:
                    f.write(f"Cột {col[0]}: {col[1]}\n")
                f.write("\n")
        conn_year.close()
        
        # Cấu trúc thư mục quarter
        conn_quarter = sqlite3.connect(quarter_db_path)
        cursor_quarter = conn_quarter.cursor()
        quarter_tables = ['balance_sheet_quarter', 'income_statement_quarter', 'cash_flow_quarter', 'ratios_quarter']
        f.write("Thư mục Quarter:\n")
        for table in quarter_tables:
            cursor_quarter.execute(f"PRAGMA table_info({table})")
            columns = cursor_quarter.fetchall()
            if columns:
                f.write(f"Bảng: {table}\n")
                for col in columns:
                    f.write(f"Cột {col[0]}: {col[1]}\n")
                f.write("\n")
        conn_quarter.close()
    
    # Ghi thời gian cập nhật cuối cùng
    with open('last_update.txt', 'w') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == "__main__":
    main()