import pandas as pd
from vnstock import Vnstock
import sqlite3
import os
from datetime import datetime, timedelta
import time

# Khởi tạo đối tượng Vnstock
stock = Vnstock().stock(symbol='ACB', source='VCI')

# Danh sách các chỉ số cần tải
indices = [
    'HOSE', 'VN30', 'VNMidCap', 'VNSmallCap', 'VNAllShare', 'VN100', 'ETF',
    'HNX', 'HNX30', 'HNXCon', 'HNXFin', 'HNXLCap', 'HNXMSCap', 'HNXMan', 'UPCOM',
    'FU_INDEX', 'CW'
]

# Đường dẫn lưu database outstanding_share
outstanding_db_path = r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\Vốn điều lệ\outstanding_share.db"

# Hàm lưu DataFrame vào file SQLite
def save_to_sqlite(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Đã lưu vào '{db_name}' với bảng '{table_name}'")

# Hàm đọc ngày cập nhật gần nhất từ README
def get_last_update_date():
    if not os.path.exists('README.txt'):
        return None
    with open('README.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in reversed(lines):  # Đọc từ dưới lên để lấy ngày gần nhất
            if line.startswith("Ngày cập nhật gần nhất:"):
                date_str = line.split(":")[1].strip()
                return datetime.strptime(date_str, "%Y-%m-%d")
    return None

# Hàm cập nhật README với ngày mới
def update_readme_with_date(date):
    with open('README.txt', 'a', encoding='utf-8') as f:
        f.write(f"Ngày cập nhật gần nhất: {date.strftime('%Y-%m-%d')}\n")

# Hàm chia danh sách thành các batch
def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Tạo hoặc cập nhật file README.txt để mô tả cấu trúc dữ liệu
if not os.path.exists('README.txt'):
    with open('README.txt', 'w', encoding='utf-8') as f:
        f.write("CẤU TRÚC CÁC FILE CƠ SỞ DỮ LIỆU\n")
        f.write("==============================\n\n")
        f.write("Mỗi danh sách cổ phiếu được lưu trong một file cơ sở dữ liệu SQLite riêng biệt. Dưới đây là mô tả chi tiết cấu trúc của từng file và bảng để bạn có thể tái sử dụng cho các tác vụ khác.\n\n")

        # 1. Danh sách cổ phiếu theo chỉ số
        f.write("#### 1. DANH SÁCH CỔ PHIẾU THEO CHỈ SỐ\n")
        f.write("**Mô tả**: Chứa danh sách cổ phiếu thuộc các chỉ số cụ thể (ví dụ: HOSE, VN30, ...).\n")
        f.write("- **File**: `stock_group_{tên_chỉ_số}.db` (ví dụ: `stock_group_HOSE.db`)\n")
        f.write("- **Bảng**: `stocks`\n")
        f.write("- **Cấu trúc bảng**:\n")
        f.write("  - Cột 0: `symbol` (TEXT) - Mã cổ phiếu, ví dụ: \"VNM\".\n")
        f.write("  - Cột 1: `name` (TEXT) - Tên công ty, ví dụ: \"Công ty Cổ phần Sữa Việt Nam\".\n")
        f.write("  - Cột 2: `exchange` (TEXT) - Sàn giao dịch, ví dụ: \"HOSE\".\n")
        f.write("- **Ví dụ dòng dữ liệu**: \n")
        f.write("  - `symbol: \"VNM\", name: \"Công ty Cổ phần Sữa Việt Nam\", exchange: \"HOSE\"`\n\n")

        # 2. Danh sách phân ngành
        f.write("#### 2. DANH SÁCH PHÂN NGÀNH THEO CHUẨN ICB\n")
        f.write("**Mô tả**: Chứa danh sách cổ phiếu được phân loại theo ngành và ngành phụ theo chuẩn ICB.\n")
        f.write("- **File**: `stock_industries.db`\n")
        f.write("- **Bảng**: `industries`\n")
        f.write("- **Cấu trúc bảng**:\n")
        f.write("  - Cột 0: `symbol` (TEXT) - Mã cổ phiếu, ví dụ: \"HPG\".\n")
        f.write("  - Cột 1: `industry` (TEXT) - Ngành chính, ví dụ: \"Công nghiệp\".\n")
        f.write("  - Cột 2: `sub_industry` (TEXT) - Ngành phụ, ví dụ: \"Khai khoáng\".\n")
        f.write("- **Ví dụ dòng dữ liệu**: \n")
        f.write("  - `symbol: \"HPG\", industry: \"Công nghiệp\", sub_industry: \"Khai khoáng\"`\n\n")

        # 3. Danh sách theo sàn giao dịch
        f.write("#### 3. DANH SÁCH PHÂN LOẠI THEO SÀN GIAO DỊCH\n")
        f.write("**Mô tả**: Chứa danh sách cổ phiếu được phân loại theo sàn giao dịch.\n")
        f.write("- **File**: `stock_exchange.db`\n")
        f.write("- **Bảng**: `exchange`\n")
        f.write("- **Cấu trúc bảng**:\n")
        f.write("  - Cột 0: `symbol` (TEXT) - Mã cổ phiếu, ví dụ: \"SSI\".\n")
        f.write("  - Cột 1: `exchange` (TEXT) - Sàn giao dịch, ví dụ: \"HOSE\".\n")
        f.write("- **Ví dụ dòng dữ liệu**: \n")
        f.write("  - `symbol: \"SSI\", exchange: \"HOSE\"`\n\n")

        # 4. Số cổ phiếu lưu hành
        f.write("#### 4. SỐ CỔ PHIẾU LƯU HÀNH (OUTSTANDING SHARE)\n")
        f.write("**Mô tả**: Chứa thông tin về số cổ phiếu lưu hành của từng công ty.\n")
        f.write("- **File**: `outstanding_share.db`\n")
        f.write("- **Bảng**: `outstanding_shares`\n")
        f.write("- **Cấu trúc bảng**:\n")
        f.write("  - Cột 0: `symbol` (TEXT) - Mã cổ phiếu, ví dụ: \"FPT\".\n")
        f.write("  - Cột 1: `outstanding_share` (INTEGER) - Số cổ phiếu lưu hành, ví dụ: 123456789.\n")
        f.write("- **Ví dụ dòng dữ liệu**: \n")
        f.write("  - `symbol: \"FPT\", outstanding_share: 123456789`\n\n")

        f.write("**Ghi chú**:\n")
        f.write("- Các kiểu dữ liệu (TEXT, INTEGER) được sử dụng trong SQLite để lưu trữ thông tin.\n")
        f.write("- Số thứ tự cột được ghi rõ để hỗ trợ truy xuất dữ liệu bằng chỉ số.\n")
        f.write("- File được cập nhật tự động khi chạy mã nguồn, vì vậy hãy kiểm tra ngày cập nhật cuối cùng trong file để đảm bảo dữ liệu mới nhất.\n\n")

# Kiểm tra xem có cần tải lại dữ liệu không (cập nhật mỗi 30 ngày)
last_update = get_last_update_date()
today = datetime.today()
if last_update is None or (today - last_update).days >= 30:
    print("Đang tải lại dữ liệu...")

    # 1. Tải và lưu danh sách cổ phiếu theo các chỉ số
    for index in indices:
        try:
            df = stock.listing.symbols_by_group(index)
            db_name = f'stock_group_{index}.db'
            save_to_sqlite(df, db_name, 'stocks')
        except Exception as e:
            print(f"Không thể tải dữ liệu cho {index}: {e}")

    # 2. Tải và lưu danh sách phân ngành theo chuẩn ICB
    try:
        df_industries = stock.listing.symbols_by_industries()
        save_to_sqlite(df_industries, 'stock_industries.db', 'industries')
    except Exception as e:
        print(f"Không thể tải dữ liệu phân ngành: {e}")

    # 3. Tải và lưu danh sách phân loại theo sàn giao dịch
    try:
        df_exchange = stock.listing.symbols_by_exchange()
        save_to_sqlite(df_exchange, 'stock_exchange.db', 'exchange')
    except Exception as e:
        print(f"Không thể tải dữ liệu theo sàn giao dịch: {e}")

    # 4. Tải và lưu thông tin outstanding_share với cơ chế chia batch và % hoàn thành
    try:
        # Đọc danh sách mã cổ phiếu từ stock_exchange.db
        conn = sqlite3.connect('stock_exchange.db')
        df_exchange = pd.read_sql('SELECT symbol FROM exchange', conn)
        conn.close()

        # Chuẩn bị danh sách để lưu dữ liệu outstanding_share
        outstanding_data = []

        # Chia danh sách mã cổ phiếu thành các batch, mỗi batch 60 mã
        batch_size = 60
        symbol_list = df_exchange['symbol'].tolist()
        symbol_batches = list(chunk_list(symbol_list, batch_size))
        total_symbols = len(symbol_list)
        processed_symbols = 0

        for batch in symbol_batches:
            for symbol in batch:
                try:
                    company = Vnstock().stock(symbol=symbol, source='TCBS').company
                    profile = company.profile()
                    # Lấy outstanding_share từ cột thứ 7 (chỉ số 6)
                    outstanding_share = profile.iloc[0, 6]
                    outstanding_data.append({'symbol': symbol, 'outstanding_share': outstanding_share})
                    print(f"Đã lấy outstanding_share cho {symbol}")
                except Exception as e:
                    print(f"Không thể lấy dữ liệu cho {symbol}: {e}")
                processed_symbols += 1

            # Tính và hiển thị % hoàn thành
            progress = (processed_symbols / total_symbols) * 100
            print(f"Đã xử lý {processed_symbols}/{total_symbols} mã ({progress:.2f}%)")

            # Dừng 65 giây sau khi xử lý xong một batch
            if processed_symbols < total_symbols:
                print("Tạm dừng 65 giây trước khi xử lý batch tiếp theo...")
                time.sleep(65)

        # Lưu vào database outstanding_share.db
        df_outstanding = pd.DataFrame(outstanding_data)
        save_to_sqlite(df_outstanding, outstanding_db_path, 'outstanding_shares')

    except Exception as e:
        print(f"Không thể tạo database outstanding_share: {e}")

    # Cập nhật README với ngày mới
    update_readme_with_date(today)
else:
    print("Dữ liệu vẫn còn mới, không cần tải lại.")