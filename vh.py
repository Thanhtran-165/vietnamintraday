import sqlite3
import os
import time
import subprocess

# Định nghĩa các đường dẫn đến DB stock data
db_paths = {
    "HOSE": "E:/Python/realtime Stock Information/Tải danh sách cổ phiếu/EOD/stock_data_HOSE.db",
    "VN30": "E:/Python/realtime Stock Information/Tải danh sách cổ phiếu/EOD/stock_data_VN30.db",
    "VN100": "E:/Python/realtime Stock Information/Tải danh sách cổ phiếu/EOD/stock_data_VN100.db",
    "VNAllShare": "E:/Python/realtime Stock Information/Tải danh sách cổ phiếu/EOD/stock_data_VNAllShare.db",
    "VNMidCap": "E:/Python/realtime Stock Information/Tải danh sách cổ phiếu/EOD/stock_data_VNMidCap.db",
    "VNSmallCap": "E:/Python/realtime Stock Information/Tải danh sách cổ phiếu/EOD/stock_data_VNSmallCap.db"
}

# Đường dẫn đến DB số cổ phiếu lưu hành (sử dụng chuỗi thô)
outstanding_db_path = r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\Vốn điều lệ\outstanding_share.db"
eod_script_path = "E:/Python/realtime Stock Information/Tải danh sách cổ phiếu/EOD/EOD100.py"

# Lấy thư mục hiện tại của mã nguồn để lưu DB mới
current_dir = os.path.dirname(os.path.abspath(__file__))

# Hàm kiểm tra thời gian cập nhật cuối cùng
def check_last_update(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Lấy danh sách tất cả các bảng
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    if not tables:
        print(f"Database {db_path} không có bảng nào.")
        conn.close()
        return None
    
    # Tìm MAX(time) từ tất cả các bảng
    max_times = []
    for table in tables:
        symbol = table[0]
        cursor.execute(f"SELECT MAX(time) FROM {symbol}")
        max_time = cursor.fetchone()[0]
        if max_time:
            max_times.append(max_time)
    
    if not max_times:
        print(f"Không có dữ liệu time trong database {db_path}.")
        conn.close()
        return None
    
    # Tìm giá trị lớn nhất trong các max_times
    last_update = max(max_times)
    conn.close()
    return last_update

# Hàm cập nhật dữ liệu bằng cách chạy EOD100.py
def update_data():
    subprocess.run(["python", eod_script_path])

# Hàm tính vốn hóa thực tế (đã sửa tên bảng)
def calculate_market_cap(db_path, outstanding_db_path):
    # Kiểm tra sự tồn tại của file
    if not os.path.exists(outstanding_db_path):
        print(f"Lỗi: File không tồn tại tại {outstanding_db_path}")
        return {}
    
    try:
        conn_outstanding = sqlite3.connect(outstanding_db_path)
        print(f"Đã kết nối thành công đến {outstanding_db_path}")
    except sqlite3.OperationalError as e:
        print(f"Lỗi khi mở file: {e}")
        return {}
    
    conn_stock = sqlite3.connect(db_path)
    cursor_stock = conn_stock.cursor()
    cursor_outstanding = conn_outstanding.cursor()
    
    # Kiểm tra bảng outstanding_shares có tồn tại không
    cursor_outstanding.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='outstanding_shares'")
    if not cursor_outstanding.fetchone():
        print(f"Lỗi: Bảng 'outstanding_shares' không tồn tại trong {outstanding_db_path}")
        conn_stock.close()
        conn_outstanding.close()
        return {}
    
    # Lấy danh sách các bảng (mỗi bảng là một mã cổ phiếu)
    cursor_stock.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor_stock.fetchall()
    
    market_caps = {}
    
    for table in tables:
        symbol = table[0]
        # Lấy giá đóng cửa mới nhất, sắp xếp theo 'time'
        cursor_stock.execute(f"SELECT close FROM {symbol} ORDER BY time DESC LIMIT 1")
        result = cursor_stock.fetchone()
        if result:
            close_price = result[0]
        else:
            close_price = 0  # Gán 0 nếu không có dữ liệu
        
        # Lấy số cổ phiếu lưu hành từ bảng outstanding_shares
        cursor_outstanding.execute("SELECT outstanding_share FROM outstanding_shares WHERE symbol=?", (symbol,))
        result = cursor_outstanding.fetchone()
        outstanding_share = result[0] if result else 0
        
        # Tính vốn hóa thực tế
        market_cap = close_price * outstanding_share
        market_caps[symbol] = market_cap
    
    conn_stock.close()
    conn_outstanding.close()
    return market_caps

# Hàm lưu trữ kết quả vào DB mới
def save_market_cap_to_db(market_caps, group_name):
    # Đường dẫn DB mới được lưu cùng thư mục với mã nguồn
    db_path = os.path.join(current_dir, f"{group_name}_market_cap.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Tạo bảng market_cap
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_cap (
            symbol TEXT PRIMARY KEY,
            market_cap REAL
        )
    ''')
    
    # Chèn dữ liệu vào bảng
    for symbol, market_cap in market_caps.items():
        cursor.execute("INSERT OR REPLACE INTO market_cap (symbol, market_cap) VALUES (?, ?)", 
                       (symbol, market_cap))
    
    conn.commit()
    conn.close()

# Hàm tạo file README
def create_readme():
    readme_path = os.path.join(current_dir, "README.txt")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(f"Thời gian cập nhật: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("Cấu trúc bảng market_cap:\n")
        f.write("Cột 0: symbol (TEXT) - Mã cổ phiếu\n")
        f.write("Cột 1: market_cap (REAL) - Vốn hóa thực tế\n")

# Hàm chính
if __name__ == "__main__":
    # Kiểm tra và cập nhật dữ liệu nếu cần
    for group, db_path in db_paths.items():
        last_update = check_last_update(db_path)
        if last_update:
            # Giả sử last_update có dạng 'YYYY-MM-DD HH:MM:SS'
            last_update_time = time.mktime(time.strptime(last_update, "%Y-%m-%d %H:%M:%S"))
            current_time = time.time()
            if current_time - last_update_time > 24 * 3600:  # Kiểm tra nếu quá 24 giờ
                choice = input(f"Dữ liệu của {group} đã cũ quá 24 giờ. Bạn có muốn cập nhật không? (Y/N): ")
                if choice.lower() == 'y':
                    update_data()
                break  # Chỉ cần hỏi một lần và cập nhật toàn bộ nếu chọn Y
    
    # Tính vốn hóa và lưu trữ kết quả
    for group, db_path in db_paths.items():
        market_caps = calculate_market_cap(db_path, outstanding_db_path)
        save_market_cap_to_db(market_caps, group)
    
    # Tạo file README
    create_readme()

    print("Hoàn tất tính toán và lưu trữ vốn hóa thực tế!")