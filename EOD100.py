import sqlite3
import pandas as pd
from vnstock import Vnstock
from concurrent.futures import ThreadPoolExecutor
import datetime
import time
import os
import subprocess
from tqdm import tqdm  # Thư viện để hiển thị thanh tiến độ

# Định nghĩa đường dẫn file tuyệt đối cho danh sách cổ phiếu (giữ nguyên)
HOSE_DB_PATH = r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_HOSE.db"

GROUP_DB_PATHS = {
    'VN30': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VN30.db",
    'VN100': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VN100.db",
    'VNAllShare': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VNAllShare.db",
    'VNMidCap': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VNMidCap.db",
    'VNSmallCap': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VNSmallCap.db"
}

# Xác định thư mục chứa file mã nguồn
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Định nghĩa đường dẫn cho các file DB dữ liệu giao dịch và file mô tả
HOSE_DATA_DB_PATH = os.path.join(SCRIPT_DIR, "stock_data_HOSE.db")
TXT_PATH = os.path.join(SCRIPT_DIR, "db_description.txt")

# Hàm kiểm tra tuổi của file (so với số ngày)
def is_file_older_than(db_path, days=30):
    """Kiểm tra xem file có cũ hơn số ngày cho trước không."""
    if not os.path.exists(db_path):
        return True  # Nếu file không tồn tại, coi như cũ
    file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(db_path))
    current_time = datetime.datetime.now()
    return (current_time - file_mod_time).days > days

# Hàm kiểm tra và cập nhật danh sách cổ phiếu nếu cần
def check_and_update_stock_lists():
    """Kiểm tra các file DB danh sách cổ phiếu và hỏi người dùng có muốn cập nhật nếu cũ quá 30 ngày."""
    db_paths = [HOSE_DB_PATH] + list(GROUP_DB_PATHS.values())
    for db_path in db_paths:
        if is_file_older_than(db_path, days=30):
            print(f"File {db_path} đã cũ hơn 30 ngày.")
            user_input = input("Bạn có muốn tải lại danh sách cổ phiếu không? (Y/N): ").strip().upper()
            if user_input == 'Y':
                print("Đang khởi động script để cập nhật danh sách...")
                subprocess.run(["python", r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\khoitaodanhsach.py.py"])
                print(f"Đã cập nhật danh sách từ {db_path}")
            else:
                print(f"Bỏ qua cập nhật cho {db_path}")
        else:
            print(f"File {db_path} vẫn còn mới, không cần cập nhật.")

# Hàm đọc danh sách cổ phiếu từ DB
def get_stocks_from_db(db_path):
    """Đọc danh sách mã cổ phiếu từ cơ sở dữ liệu."""
    if not os.path.exists(db_path):
        print(f"File không tồn tại: {db_path}")
        return []
    conn = sqlite3.connect(db_path)
    query = "SELECT symbol FROM stocks"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['symbol'].tolist()

# Hàm tải dữ liệu với cơ chế retry
def fetch_stock_data_with_retry(symbol, max_retries=5, delay=5):
    """Thử tải dữ liệu tối đa 5 lần nếu gặp lỗi, với khoảng nghỉ 5 giây giữa các lần thử."""
    for attempt in range(1, max_retries + 1):
        try:
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            start_date = (datetime.datetime.now() - datetime.timedelta(days=200)).strftime('%Y-%m-%d')
            df = stock.quote.history(start=start_date, interval='1D')
            if df.empty:
                print(f"Không có dữ liệu cho {symbol} trong khoảng thời gian đã cho.")
                return pd.DataFrame(), False  # Không có dữ liệu, không retry
            df = df.tail(100)
            return df, True  # Thành công
        except Exception as e:
            print(f"Lỗi khi tải dữ liệu cho {symbol} (lần thử {attempt}/{max_retries}): {str(e)}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                print(f"Không thể tải dữ liệu cho {symbol} sau {max_retries} lần thử.")
                return pd.DataFrame(), False  # Thất bại sau khi retry

# Hàm lưu dữ liệu vào DB
def save_to_db(df, db_path, table_name):
    """Lưu DataFrame vào cơ sở dữ liệu SQLite."""
    if not df.empty:  # Chỉ lưu nếu DataFrame không rỗng
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()

# Hàm kiểm tra dữ liệu có cũ quá 24h không
def is_data_up_to_date(db_path, table_name):
    """Kiểm tra xem dữ liệu có được cập nhật trong vòng 24 giờ không."""
    conn = sqlite3.connect(db_path)
    query = f"SELECT MAX(time) FROM {table_name}"
    try:
        latest_date = pd.read_sql_query(query, conn).iloc[0, 0]
        if latest_date:
            latest_date = datetime.datetime.strptime(latest_date, '%Y-%m-%d')
            if (datetime.datetime.now() - latest_date).days < 1:
                conn.close()
                return True
    except:
        pass
    conn.close()
    return False

# Hàm tải dữ liệu theo batch với thanh tiến độ
def fetch_batch_data(batch, failed_symbols):
    """Tải dữ liệu cho một batch cổ phiếu và ghi nhận các mã không thành công."""
    data = {}
    for symbol in tqdm(batch, desc="Tải batch", leave=False):
        df, success = fetch_stock_data_with_retry(symbol)
        if success:
            data[symbol] = df
        else:
            failed_symbols.append(symbol)
    return data

# Hàm tải và lưu dữ liệu cho HOSE
def load_hose_data():
    """Tải và lưu dữ liệu cho tất cả cổ phiếu trong HOSE."""
    hose_stocks = get_stocks_from_db(HOSE_DB_PATH)
    if not hose_stocks:
        print("Không có cổ phiếu nào để tải dữ liệu.")
        return
    batch_size = 40  # Kích thước batch
    batches = [hose_stocks[i:i + batch_size] for i in range(0, len(hose_stocks), batch_size)]
    failed_symbols = []  # Danh sách các mã không thành công

    with ThreadPoolExecutor(max_workers=12) as executor:
        for batch in batches:
            need_update = False
            for symbol in batch:
                if not os.path.exists(HOSE_DATA_DB_PATH) or not is_data_up_to_date(HOSE_DATA_DB_PATH, symbol):
                    need_update = True
                    break
            
            if need_update:
                future = executor.submit(fetch_batch_data, batch, failed_symbols)
                batch_data = future.result()
                for symbol, df in batch_data.items():
                    save_to_db(df, HOSE_DATA_DB_PATH, symbol)
                print(f"Đã tải và lưu batch chứa {len(batch)} cổ phiếu.")
                time.sleep(65)  # Nghỉ 65 giây để tránh quá tải API
            else:
                print(f"Dữ liệu batch chứa {len(batch)} cổ phiếu đã mới, không cần tải lại.")

    # In danh sách các mã không thành công
    if failed_symbols:
        print("Các mã cổ phiếu không tải được dữ liệu sau khi retry:")
        for symbol in failed_symbols:
            print(f"- {symbol}")
    else:
        print("Tất cả mã cổ phiếu đã được tải thành công.")

# Hàm tái phân bổ dữ liệu cho các nhóm khác
def extract_data_for_groups():
    """Sao chép dữ liệu từ HOSE sang các nhóm khác (VN30, VN100, ...)."""
    for group_name, group_db_path in GROUP_DB_PATHS.items():
        group_stocks = get_stocks_from_db(group_db_path)
        group_data_db_path = os.path.join(SCRIPT_DIR, f"stock_data_{group_name}.db")
        
        conn = sqlite3.connect(HOSE_DATA_DB_PATH)
        for symbol in group_stocks:
            try:
                query = f"SELECT * FROM {symbol}"
                df = pd.read_sql_query(query, conn)
                save_to_db(df, group_data_db_path, symbol)
            except:
                print(f"Không tìm thấy dữ liệu cho {symbol} trong HOSE DB.")
        conn.close()
        print(f"Đã tái phân bổ dữ liệu cho {group_name} vào {group_data_db_path}")

# Hàm tạo file txt mô tả cấu trúc DB
def describe_db(db_path, txt_path, append=False):
    """Tạo file txt mô tả cấu trúc cơ sở dữ liệu."""
    mode = 'a' if append else 'w'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    with open(txt_path, mode, encoding='utf-8') as f:
        f.write(f"\n**Mô tả**: Chứa dữ liệu giao dịch 100 phiên gần nhất của cổ phiếu.\n")
        f.write(f"- **File**: {os.path.basename(db_path)}\n")
        f.write(f"- **Bảng**: Mỗi bảng tương ứng với một mã cổ phiếu (ví dụ: 'ACB')\n")
        f.write("- **Cấu trúc bảng**:\n")
        if tables:
            table_name = tables[0][0]
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            for col in columns:
                f.write(f"  - Cột {col[0]}: `{col[1]}` ({col[2]}) - {col[1]}\n")
            try:
                cursor.execute(f"SELECT MAX(time) FROM {table_name}")
                latest_date = cursor.fetchone()[0]
                f.write(f"- **Ngày cập nhật mới nhất**: {latest_date}\n")
            except:
                f.write("- **Ngày cập nhật mới nhất**: Không xác định (bảng rỗng hoặc không có cột thời gian)\n")
        else:
            f.write("- Không có bảng nào trong cơ sở dữ liệu.\n")
        f.write("\n")
    conn.close()

# Thực thi chương trình
if __name__ == "__main__":
    # Bước 0: Kiểm tra và cập nhật danh sách cổ phiếu nếu cần
    print("Kiểm tra tuổi của các file danh sách cổ phiếu...")
    check_and_update_stock_lists()

    # Bước 1: Tải dữ liệu cho HOSE
    print("Bắt đầu tải dữ liệu cho HOSE...")
    load_hose_data()

    # Bước 2: Tái phân bổ dữ liệu cho các nhóm khác
    print("Bắt đầu tái phân bổ dữ liệu cho các danh sách khác...")
    extract_data_for_groups()

    # Bước 3: Tạo file txt mô tả
    print("Tạo file mô tả cấu trúc DB...")
    describe_db(HOSE_DATA_DB_PATH, TXT_PATH)
    for group_name, group_db_path in GROUP_DB_PATHS.items():
        group_data_db_path = os.path.join(SCRIPT_DIR, f"stock_data_{group_name}.db")
        describe_db(group_data_db_path, TXT_PATH, append=True)
    print(f"Hoàn tất! File mô tả đã được lưu tại {TXT_PATH}")