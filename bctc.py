import os
import time
import sqlite3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from vnstock import Vnstock
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

# Danh sách các loại báo cáo và chu kỳ
REPORT_TYPES = ['balance_sheet', 'income_statement', 'cash_flow', 'ratios', 'dividends']
PERIODS = ['Year', 'Quarter']

# Ánh xạ các tiêu chí sang tiếng Việt để hiển thị cho người dùng
REPORT_TYPES_VN = {
    'balance_sheet': 'Bảng cân đối kế toán',
    'income_statement': 'Báo cáo kết quả kinh doanh',
    'cash_flow': 'Báo cáo lưu chuyển tiền tệ',
    'ratios': 'Chỉ số tài chính',
    'dividends': 'Cổ tức'
}
PERIODS_VN = {
    'Year': 'Năm',
    'Quarter': 'Quý'
}

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
    year_db_path = f"data/{exchange}/year/{symbol}.db"
    quarter_db_path = f"data/{exchange}/quarter/{symbol}.db"
    
    conn_year = sqlite3.connect(year_db_path)
    conn_quarter = sqlite3.connect(quarter_db_path)
    
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
    
    stock_vci = Vnstock().stock(symbol=symbol, source='VCI')
    reports = [
        ('balance_sheet_year', stock_vci.finance.balance_sheet, {'period': 'year', 'lang': 'vi', 'dropna': True}, conn_year),
        ('income_statement_year', stock_vci.finance.income_statement, {'period': 'year', 'lang': 'vi', 'dropna': True}, conn_year),
        ('cash_flow_year', stock_vci.finance.cash_flow, {'period': 'year', 'dropna': True}, conn_year),
        ('ratios_year', stock_vci.finance.ratio, {'period': 'year', 'lang': 'vi', 'dropna': True}, conn_year),
        ('balance_sheet_quarter', stock_vci.finance.balance_sheet, {'period': 'quarter', 'lang': 'vi', 'dropna': True}, conn_quarter),
        ('income_statement_quarter', stock_vci.finance.income_statement, {'period': 'quarter', 'lang': 'vi', 'dropna': True}, conn_quarter),
        ('cash_flow_quarter', stock_vci.finance.cash_flow, {'period': 'quarter', 'dropna': True}, conn_quarter),
        ('ratios_quarter', stock_vci.finance.ratio, {'period': 'quarter', 'lang': 'vi', 'dropna': True}, conn_quarter),
    ]
    
    for table_name, func, kwargs, conn in tqdm(reports, desc=f"Báo cáo cho {symbol}", leave=False):
        print(f"Đang tải {table_name} cho {symbol}")
        df = download_report(func, **kwargs)
        if df is not None:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Hoàn thành {table_name} cho {symbol}")
        else:
            print(f"Thất bại khi tải {table_name} cho {symbol}")
        time.sleep(30)
    
    conn_year.close()
    conn_quarter.close()

# Hàm vẽ biểu đồ
def plot_indicator(symbol, report_type, period, num_years, conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    if not columns:
        print(f"Bảng {table_name} không tồn tại hoặc không có cột nào.")
        return
    
    print("\nDanh sách các chỉ tiêu có thể chọn:")
    for idx, col in enumerate(columns, 1):
        print(f"{idx}. {col[1]}")
    
    while True:
        try:
            choice = int(input("Nhập số tương ứng với chỉ tiêu muốn xem: "))
            if 1 <= choice <= len(columns):
                indicator = columns[choice - 1][1]
                break
            else:
                print(f"Vui lòng nhập số từ 1 đến {len(columns)}.")
        except ValueError:
            print("Vui lòng nhập một số hợp lệ.")
    
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Lỗi khi truy xuất bảng {table_name}: {e}")
        return
    
    # Xử lý cột thời gian linh hoạt
    if period.lower() == 'year':
        year_cols = [col for col in df.columns if 'năm' in col.lower() or 'year' in col.lower()]
        if year_cols:
            time_col = year_cols[0]
        else:
            print(f"Lỗi: Không tìm thấy cột chứa thông tin năm trong bảng dữ liệu.")
            return
        df = df.sort_values(by=time_col, ascending=False).head(num_years)
        x = df[time_col]
    else:  # quarter
        year_cols = [col for col in df.columns if 'năm' in col.lower() or 'year' in col.lower()]
        if year_cols:
            time_col = year_cols[0]
        else:
            print(f"Lỗi: Không tìm thấy cột chứa thông tin năm trong bảng dữ liệu.")
            return
        
        quarter_cols = [col for col in df.columns if 'kỳ' in col.lower() or 'quarter' in col.lower() or 'length' in col.lower()]
        if quarter_cols:
            quarter_col = quarter_cols[0]
        else:
            print(f"Lỗi: Không tìm thấy cột chứa thông tin quý trong bảng dữ liệu.")
            return
        
        if df[time_col].isnull().all() or df[quarter_col].isnull().all():
            print(f"Lỗi: Cột '{time_col}' hoặc '{quarter_col}' không có dữ liệu.")
            return
        
        df['Thời gian'] = df[time_col].astype(str) + 'Q' + df[quarter_col].astype(str)
        df = df.sort_values(by=[time_col, quarter_col], ascending=False).head(num_years * 4)
        x = df['Thời gian']
    
    y = df[indicator]
    
    plt.figure(figsize=(10, 6))
    plt.plot(x, y, marker='o')
    plt.title(f"{indicator} của {symbol} ({PERIODS_VN[period]})")
    plt.xlabel('Thời gian')
    plt.ylabel(indicator)
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Hàm chính
def main():
    if os.path.exists('last_update.txt'):
        with open('last_update.txt', 'r') as f:
            last_update = f.read()
        print(f"Thời gian cập nhật cuối cùng: {last_update}")
    else:
        print("Chưa từng cập nhật dữ liệu.")
    
    update = input("Bạn có muốn cập nhật lại dữ liệu không? (Y/N): ").strip().upper()
    if update == 'Y':
        os.makedirs('data/HOSE/year', exist_ok=True)
        os.makedirs('data/HOSE/quarter', exist_ok=True)
        os.makedirs('data/HNX/year', exist_ok=True)
        os.makedirs('data/HNX/quarter', exist_ok=True)
        os.makedirs('stock_lists', exist_ok=True)
        
        stock = Vnstock().stock(symbol='ACB', source='VCI')
        hose_symbols = stock.listing.symbols_by_group('HOSE')
        hnx_symbols = stock.listing.symbols_by_group('HNX')
        
        with open('stock_lists/hose_symbols.txt', 'w') as f:
            for symbol in hose_symbols:
                f.write(symbol + '\n')
        with open('stock_lists/hnx_symbols.txt', 'w') as f:
            for symbol in hnx_symbols:
                f.write(symbol + '\n')
        
        for exchange, symbols in [('HOSE', hose_symbols), ('HNX', hnx_symbols)]:
            filtered_symbols = [s for s in symbols if len(s) <= 3]
            batches = [filtered_symbols[i:i+40] for i in range(0, len(filtered_symbols), 40)]
            
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
                time.sleep(65)
        
        with open('last_update.txt', 'w') as f:
            f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    else:
        print("Bỏ qua cập nhật dữ liệu.")
    
    # Giao diện người dùng để vẽ biểu đồ
    while True:
        print("\n=== TRUY XUẤT VÀ VẼ BIỂU ĐỒ ===")
        print("Vui lòng nhập Mã cổ phiếu (hoặc 'exit' để thoát):")
        symbol = input().strip().upper()
        if symbol == 'EXIT':
            break
        
        # Chọn loại báo cáo
        print("\nChọn loại báo cáo:")
        for idx, report in enumerate(REPORT_TYPES, 1):
            print(f"{idx}. {REPORT_TYPES_VN[report]}")
        while True:
            try:
                choice = int(input("Nhập số tương ứng với loại báo cáo: "))
                if 1 <= choice <= len(REPORT_TYPES):
                    report_type = REPORT_TYPES[choice - 1]
                    break
                else:
                    print(f"Vui lòng nhập số từ 1 đến {len(REPORT_TYPES)}.")
            except ValueError:
                print("Vui lòng nhập một số hợp lệ.")
        
        # Chọn chu kỳ
        print("\nChọn chu kỳ:")
        for idx, period in enumerate(PERIODS, 1):
            print(f"{idx}. {PERIODS_VN[period]}")
        while True:
            try:
                choice = int(input("Nhập số tương ứng với chu kỳ: "))
                if 1 <= choice <= len(PERIODS):
                    period = PERIODS[choice - 1]
                    break
                else:
                    print(f"Vui lòng nhập số từ 1 đến {len(PERIODS)}.")
            except ValueError:
                print("Vui lòng nhập một số hợp lệ.")
        
        # Chọn số năm
        print("Nhập số năm muốn xem:")
        while True:
            try:
                num_years = int(input().strip())
                if num_years > 0:
                    break
                else:
                    print("Số năm phải lớn hơn 0!")
            except ValueError:
                print("Vui lòng nhập một số hợp lệ!")
        
        # Xác định đường dẫn cơ sở dữ liệu
        hose_symbols = open('stock_lists/hose_symbols.txt').read().splitlines()
        exchange = 'HOSE' if symbol in hose_symbols else 'HNX'
        db_path = f"data/{exchange}/{period.lower()}/{symbol}.db"
        
        if not os.path.exists(db_path):
            print(f"Không tìm thấy dữ liệu cho mã {symbol} trong chu kỳ {PERIODS_VN[period]}.")
            continue
        
        conn = sqlite3.connect(db_path)
        
        if period.lower() == 'year':
            table_name = f"{report_type}_year" if report_type != 'dividends' else 'dividends'
        else:
            table_name = f"{report_type}_quarter"
        
        # Vòng lặp để chọn và vẽ chỉ tiêu
        while True:
            plot_indicator(symbol, report_type, period, num_years, conn, table_name)
            
            # Hiển thị menu tùy chọn
            print("\nSau khi xem biểu đồ, bạn muốn:")
            print("1. Xem thêm chỉ tiêu khác cho cùng mã cổ phiếu")
            print("2. Quay lại menu chính để nhập mã cổ phiếu mới")
            print("3. Thoát chương trình")
            choice = input("Nhập lựa chọn (1/2/3): ").strip()
            
            if choice == '1':
                continue  # Quay lại chọn chỉ tiêu khác
            elif choice == '2':
                break  # Quay lại menu chính
            elif choice == '3':
                conn.close()
                exit()  # Thoát chương trình
            else:
                print("Lựa chọn không hợp lệ, vui lòng nhập lại.")
        
        conn.close()

if __name__ == "__main__":
    main()
