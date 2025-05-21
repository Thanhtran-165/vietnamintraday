import sqlite3
import pandas as pd
from vnstock import Vnstock
from concurrent.futures import ThreadPoolExecutor
import datetime
import time
import os
import subprocess
from tqdm import tqdm
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

# ### Định nghĩa các hằng số và đường dẫn
HOSE_DB_PATH = r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_HOSE.db"
GROUP_DB_PATHS = {
    'VN30': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VN30.db",
    'VN100': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VN100.db",
    'VNAllShare': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VNAllShare.db",
    'VNMidCap': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VNMidCap.db",
    'VNSmallCap': r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\stock_group_VNSmallCap.db"
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HOSE_DATA_DB_PATH = os.path.join(SCRIPT_DIR, "stock_data_HOSE.db")
TXT_PATH = os.path.join(SCRIPT_DIR, "db_description.txt")
LOG_PATH = os.path.join(SCRIPT_DIR, "data_issues.log")

PERIODS = [5, 10, 20, 50, 100, 200]

# ### Các hàm hỗ trợ
def is_file_older_than(db_path, days=30):
    if not os.path.exists(db_path):
        return True
    file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(db_path))
    current_time = datetime.datetime.now()
    return (current_time - file_mod_time).days > days

def check_and_update_stock_lists():
    db_paths = [HOSE_DB_PATH] + list(GROUP_DB_PATHS.values())
    for db_path in db_paths:
        if is_file_older_than(db_path, days=30):
            print(f"File {db_path} đã cũ hơn 30 ngày.")
            user_input = input("Bạn có muốn tải lại danh sách cổ phiếu không? (Y/N): ").strip().upper()
            if user_input == 'Y':
                print("Đang khởi động script để cập nhật danh sách...")
                subprocess.run(["python", r"E:\Python\realtime Stock Information\Tải danh sách cổ phiếu\khoitaodanhsach.py"])
                print(f"Đã cập nhật danh sách từ {db_path}")
            else:
                print(f"Bỏ qua cập nhật cho {db_path}")
        else:
            print(f"File {db_path} vẫn còn mới, không cần cập nhật.")

def get_stocks_from_db(db_path):
    if not os.path.exists(db_path):
        print(f"File không tồn tại: {db_path}")
        return []
    conn = sqlite3.connect(db_path)
    query = "SELECT symbol FROM stocks"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['symbol'].tolist()

def fetch_stock_data_with_retry(symbol, max_retries=5, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            start_date = (datetime.datetime.now() - datetime.timedelta(days=1200)).strftime('%Y-%m-%d')
            df = stock.quote.history(start=start_date, interval='1D')
            if df.empty:
                print(f"Không có dữ liệu cho {symbol} trong khoảng thời gian đã cho.")
                return pd.DataFrame(), False
            df = df.tail(1000)
            return df, True
        except Exception as e:
            print(f"Lỗi khi tải dữ liệu cho {symbol} (lần thử {attempt}/{max_retries}): {str(e)}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                print(f"Không thể tải dữ liệu cho {symbol} sau {max_retries} lần thử.")
                return pd.DataFrame(), False

def save_to_db(df, db_path, table_name):
    if not df.empty:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()

def is_data_up_to_date(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT MAX(time) FROM {table_name}"
    try:
        latest_date = pd.read_sql_query(query, conn).iloc[0, 0]
        if latest_date:
            latest_date = datetime.datetime.strptime(latest_date, '%Y-%m-%d %H:%M:%S')
            if (datetime.datetime.now() - latest_date).days < 1:
                conn.close()
                return True
    except Exception as e:
        print(f"Lỗi khi kiểm tra ngày cập nhật cho {table_name}: {e}")
    conn.close()
    return False

def fetch_batch_data(batch, failed_symbols):
    data = {}
    for symbol in tqdm(batch, desc="Tải batch", leave=False):
        df, success = fetch_stock_data_with_retry(symbol)
        if success:
            data[symbol] = df
        else:
            failed_symbols.append(symbol)
    return data

def load_hose_data():
    hose_stocks = get_stocks_from_db(HOSE_DB_PATH)
    if not hose_stocks:
        print("Không có cổ phiếu nào để tải dữ liệu.")
        return
    batch_size = 40
    batches = [hose_stocks[i:i + batch_size] for i in range(0, len(hose_stocks), batch_size)]
    failed_symbols = []

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
                time.sleep(65)
            else:
                print(f"Dữ liệu batch chứa {len(batch)} cổ phiếu đã mới, không cần tải lại.")

    if failed_symbols:
        print("Các mã cổ phiếu không tải được dữ liệu sau khi retry:")
        for symbol in failed_symbols:
            print(f"- {symbol}")
    else:
        print("Tất cả mã cổ phiếu đã được tải thành công.")

def extract_data_for_groups():
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

def describe_db(db_path, txt_path, append=False):
    mode = 'a' if append else 'w'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    with open(txt_path, mode, encoding='utf-8') as f:
        f.write(f"\n**Mô tả**: Chứa dữ liệu giao dịch 1000 phiên gần nhất của cổ phiếu.\n")
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
                f.write("- **Ngày cập nhật mới nhất**: Không xác định\n")
        else:
            f.write("- Không có bảng nào trong cơ sở dữ liệu.\n")
        f.write("\n")
    conn.close()

def is_valid_stock_symbol(symbol):
    return len(symbol) == 3 and symbol.isalnum()

def check_data_availability(db_path, stock_list, required_days, min_period=200):
    conn = sqlite3.connect(db_path)
    earliest_date = datetime.datetime.now()
    min_sessions = min_period
    for symbol in stock_list:
        try:
            df = pd.read_sql_query(f"SELECT time FROM {symbol}", conn)
            if len(df) < min_sessions:
                continue
            min_date = pd.to_datetime(df['time'].min())
            if min_date and min_date < earliest_date:
                earliest_date = min_date
        except:
            continue
    conn.close()
    days_available = (datetime.datetime.now() - earliest_date).days
    return days_available >= required_days

def get_stock_list(selected_list):
    if selected_list == 'HOSE':
        return get_stocks_from_db(HOSE_DB_PATH)
    else:
        return get_stocks_from_db(GROUP_DB_PATHS[selected_list])

# ### Các hàm tính toán và vẽ biểu đồ
def calculate_ma_statistics(stock_list, db_path):
    counts = {period: 0 for period in PERIODS}
    total_stocks = 0
    conn = sqlite3.connect(db_path)
    for symbol in stock_list:
        if not is_valid_stock_symbol(symbol):
            print(f"Bỏ qua mã không hợp lệ: {symbol}")
            continue
        try:
            df = pd.read_sql_query(f"SELECT * FROM {symbol}", conn)
            if df.empty:
                continue
            total_stocks += 1
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time')
            latest_close = df['close'].iloc[-1]
            for period in PERIODS:
                if len(df) >= period:
                    ma = df['close'].rolling(window=period).mean().iloc[-1]
                    if latest_close > ma:
                        counts[period] += 1
        except Exception as e:
            print(f"Lỗi khi xử lý dữ liệu cho {symbol}: {e}")
    conn.close()
    return counts, total_stocks

def calculate_ma_ratio_over_time(stock_list, db_path, num_days_display=100, num_days_data=400):
    conn = sqlite3.connect(db_path)
    
    latest_date_query = f"SELECT MAX(time) FROM {stock_list[0]}"
    latest_date_str = pd.read_sql_query(latest_date_query, conn).iloc[0, 0]
    latest_date = datetime.datetime.strptime(latest_date_str, '%Y-%m-%d %H:%M:%S')
    
    display_dates = []
    current_date = latest_date
    while len(display_dates) < num_days_display:
        if current_date.weekday() < 5:
            display_dates.append(current_date.strftime('%Y-%m-%d'))
        current_date -= datetime.timedelta(days=1)
    display_dates = sorted(display_dates)
    
    ratio_data = {period: [] for period in PERIODS}
    
    for date in display_dates:
        above_ma_count = {period: 0 for period in PERIODS}
        total_stocks = 0
        
        start_date = (datetime.datetime.strptime(date, '%Y-%m-%d') - 
                     datetime.timedelta(days=num_days_data - 1)).strftime('%Y-%m-%d')
        
        for symbol in stock_list:
            if not is_valid_stock_symbol(symbol):
                continue
            try:
                query = f"SELECT * FROM {symbol} WHERE time >= '{start_date}' AND time <= '{date}' ORDER BY time ASC"
                df = pd.read_sql_query(query, conn)
                
                if len(df) < 5:
                    continue
                
                total_stocks += 1
                df['time'] = pd.to_datetime(df['time'])
                df = df.sort_values('time')
                latest_close = df['close'].iloc[-1]
                
                for period in PERIODS:
                    if len(df) >= period:
                        ma = df['close'].rolling(window=period).mean().iloc[-1]
                        if latest_close > ma:
                            above_ma_count[period] += 1
            except Exception as e:
                print(f"Lỗi xử lý {symbol} ngày {date}: {e}")
                continue
        
        if total_stocks == 0:
            print(f"Không có mã hợp lệ cho ngày {date}")
            for period in PERIODS:
                ratio_data[period].append(float('nan'))
        else:
            for period in PERIODS:
                ratio = (above_ma_count[period] / total_stocks) * 100
                ratio_data[period].append(ratio)
            print(f"Ngày {date}: Đã xử lý {total_stocks} mã")
    
    conn.close()
    df_ratio = pd.DataFrame(ratio_data, index=display_dates)
    return df_ratio

def plot_ma_combined(counts, total_stocks, df_ratio, selected_list):
    # Tạo subplot mà không có subplot_titles
    fig = make_subplots(
        rows=3,
        cols=3,
        specs=[
            [{'type': 'indicator'}, {'type': 'indicator'}, {'type': 'indicator'}],
            [{'type': 'indicator'}, {'type': 'indicator'}, {'type': 'indicator'}],
            [{'type': 'xy', 'colspan': 3}, None, None]
        ],
        vertical_spacing=0.15
    )
    
    # Thêm các biểu đồ Gauge với nhãn trong Indicator
    for i, period in enumerate(PERIODS):
        percentage = (counts[period] / total_stocks) * 100 if total_stocks > 0 else 0
        row = 1 if i < 3 else 2
        col = (i % 3) + 1
        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=percentage,
                title={'text': f"MA{period}"},  # Nhãn trong Gauge
                gauge={
                    'axis': {'range': [0, 100]},
                    'steps': [
                        {'range': [0, 10], 'color': "lightgreen"},
                        {'range': [10, 30], 'color': "green"},
                        {'range': [30, 70], 'color': "yellow"},
                        {'range': [70, 100], 'color': "red"}
                    ],
                }
            ),
            row=row,
            col=col
        )
    
    # Thêm vùng nền cho biểu đồ đường
    background_colors = [
        {"range": [0, 10], "color": "lightgreen"},
        {"range": [10, 30], "color": "green"},
        {"range": [30, 70], "color": "yellow"},
        {"range": [70, 100], "color": "red"}
    ]
    
    for bg in background_colors:
        fig.add_shape(
            type="rect",
            x0=df_ratio.index[0],
            y0=bg["range"][0],
            x1=df_ratio.index[-1],
            y1=bg["range"][1],
            fillcolor=bg["color"],
            opacity=0.3,
            layer="below",
            line_width=0,
            row=3,
            col=1
        )
    
    # Thêm các đường MA vào biểu đồ
    colors = ['blue', 'orange', 'green', 'red', 'purple', 'brown']
    for i, period in enumerate(PERIODS):
        fig.add_trace(
            go.Scatter(
                x=df_ratio.index,
                y=df_ratio[period],
                mode="lines",
                name=f'MA{period}',  # Nhãn cho legend
                line=dict(color=colors[i], width=2)
            ),
            row=3,
            col=1
        )
    
    # Cập nhật layout
    fig.update_layout(
        height=1000,
        width=1200,
        title_text=f"Phân tích MA cho danh sách {selected_list} (100 ngày gần nhất)",
        showlegend=True,
        legend_title_text='Chu kỳ MA'
    )
    
    fig.update_xaxes(title_text="Ngày", row=3, col=1)
    fig.update_yaxes(title_text="Tỷ lệ (%)", range=[0, 100], row=3, col=1)
    
    fig.show()

def calculate_changes(stock_list, db_path):
    data = []
    conn = sqlite3.connect(db_path)
    for symbol in stock_list:
        if not is_valid_stock_symbol(symbol):
            continue
        try:
            df = pd.read_sql_query(f"SELECT * FROM {symbol} ORDER BY time DESC LIMIT 2", conn)
            if len(df) < 2:
                continue
            today = df.iloc[0]
            yesterday = df.iloc[1]
            price_change = 'up' if today['close'] > yesterday['close'] else 'down' if today['close'] < yesterday['close'] else 'same'
            volume_change = 'up' if today['volume'] > yesterday['volume'] else 'down' if today['volume'] < yesterday['volume'] else 'same'
            data.append({'symbol': symbol, 'volume': today['volume'], 'price_change': price_change, 'volume_change': volume_change})
        except Exception as e:
            print(f"Lỗi xử lý {symbol}: {e}")
    conn.close()
    return pd.DataFrame(data)

def plot_additional_ma_charts(stock_list, db_path, selected_list):
    df_changes = calculate_changes(stock_list, db_path)
    
    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[[{'type': 'bar'}, {'type': 'bar'}],
               [{'type': 'treemap', 'colspan': 2}, None]],
        subplot_titles=["Số mã tăng/giảm giá", "Số mã tăng/giảm khối lượng", "Treemap Khối lượng"]
    )
    
    price_counts = df_changes['price_change'].value_counts()
    fig.add_trace(
        go.Bar(
            x=['Tăng giá', 'Giảm giá'],
            y=[price_counts.get('up', 0), price_counts.get('down', 0)],
            marker_color=['green', 'red']
        ),
        row=1, col=1
    )
    
    volume_counts = df_changes['volume_change'].value_counts()
    fig.add_trace(
        go.Bar(
            x=['Tăng KL', 'Giảm KL'],
            y=[volume_counts.get('up', 0), volume_counts.get('down', 0)],
            marker_color=['blue', 'orange']
        ),
        row=1, col=2
    )
    
    color_map = {'up': 'green', 'down': 'red', 'same': 'yellow'}
    df_changes['color'] = df_changes['price_change'].map(color_map)
    fig.add_trace(
        go.Treemap(
            labels=df_changes['symbol'],
            parents=[""] * len(df_changes),
            values=df_changes['volume'],
            marker_colors=df_changes['color']
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        height=800,
        width=1200,
        title_text=f"Biểu đồ bổ sung MA cho danh sách {selected_list}"
    )
    fig.show()

def calculate_average_volumes(stock_list, db_path, periods=[5, 10, 20, 50, 100]):
    conn = sqlite3.connect(db_path)
    avg_volumes = {period: {} for period in periods}
    for symbol in stock_list:
        if not is_valid_stock_symbol(symbol):
            continue
        try:
            df = pd.read_sql_query(f"SELECT volume FROM {symbol} ORDER BY time DESC LIMIT 100", conn)
            if len(df) < max(periods):
                continue
            for period in periods:
                avg_vol = df['volume'].iloc[:period].mean()
                avg_volumes[period][symbol] = avg_vol
        except Exception as e:
            print(f"Lỗi xử lý {symbol}: {e}")
    conn.close()
    return avg_volumes

def plot_average_volume_treemaps(avg_volumes, selected_list):
    periods = sorted(avg_volumes.keys())
    specs = [[{'type': 'domain'}] for _ in periods]
    fig = make_subplots(
        rows=len(periods),
        cols=1,
        specs=specs,
        subplot_titles=[f"Khối lượng trung bình {period} ngày" for period in periods],
        vertical_spacing=0.05
    )
    for i, period in enumerate(periods):
        symbols = list(avg_volumes[period].keys())
        values = list(avg_volumes[period].values())
        fig.add_trace(
            go.Treemap(
                labels=symbols,
                parents=[""] * len(symbols),
                values=values,
                marker_colors=['lightblue'] * len(symbols)
            ),
            row=i+1,
            col=1
        )
    fig.update_layout(
        height=250 * len(periods),
        width=1200,
        title_text=f"Khối lượng trung bình cho danh sách {selected_list}"
    )
    fig.show()

def calculate_roc_data(stock_list, db_path):
    roc_data = {period: [] for period in PERIODS}
    conn = sqlite3.connect(db_path)
    for symbol in stock_list:
        if not is_valid_stock_symbol(symbol):
            print(f"Bỏ qua mã không hợp lệ: {symbol}")
            continue
        try:
            df = pd.read_sql_query(f"SELECT * FROM {symbol}", conn)
            if df.empty:
                continue
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time')
            latest_close = df['close'].iloc[-1]
            for period in PERIODS:
                if len(df) >= period + 1:
                    past_close = df['close'].iloc[-period-1]
                    roc = (latest_close - past_close) / past_close * 100
                    roc_data[period].append(roc)
        except Exception as e:
            print(f"Lỗi khi xử lý dữ liệu cho {symbol}: {e}")
    conn.close()
    return roc_data

def plot_roc_density(roc_data):
    fig = make_subplots(rows=len(PERIODS), cols=1, subplot_titles=[f'ROC{period}' for period in PERIODS])
    for i, period in enumerate(PERIODS):
        if roc_data[period]:
            fig.add_trace(
                go.Histogram(x=roc_data[period], nbinsx=50, histnorm='probability density', name=f'ROC{period}'),
                row=i+1, col=1
            )
            fig.update_xaxes(title_text='ROC (%)', row=i+1, col=1)
            fig.update_yaxes(title_text='Mật độ', row=i+1, col=1)
        else:
            print(f"Không có dữ liệu cho ROC{period}")
    fig.update_layout(height=200*len(PERIODS), title_text="Biểu đồ mật độ ROC cho các chu kỳ")
    fig.show()

# ### Thực thi chương trình
if __name__ == "__main__":
    update_data = input("Bạn có muốn cập nhật dữ liệu không? (Y/N): ").strip().upper()
    if update_data == 'Y':
        print("Kiểm tra tuổi của các file danh sách cổ phiếu...")
        check_and_update_stock_lists()
        
        print("Bắt đầu tải dữ liệu cho HOSE...")
        load_hose_data()
        
        print("Bắt đầu tái phân bổ dữ liệu cho các danh sách khác...")
        extract_data_for_groups()
    else:
        print("Bỏ qua cập nhật dữ liệu.")

    while True:
        print("\nChọn tính năng để xem:")
        print("1. MA (Moving Average)")
        print("2. ROC (Rate of Change)")
        print("3. Khối lượng trung bình")
        print("4. Thoát")
        choice = input("Nhập lựa chọn của bạn (1, 2, 3 hoặc 4): ").strip()
        
        if choice == '4':
            print("Thoát chương trình.")
            break
        
        if choice in ['1', '2', '3']:
            print("\nChọn danh sách cổ phiếu để phân tích:")
            print("1. HOSE")
            print("2. VN30")
            print("3. VN100")
            print("4. VNAllShare")
            print("5. VNMidCap")
            print("6. VNSmallCap")
            list_choice = input("Nhập lựa chọn của bạn (1-6): ").strip()
            
            list_mapping = {
                '1': 'HOSE',
                '2': 'VN30',
                '3': 'VN100',
                '4': 'VNAllShare',
                '5': 'VNMidCap',
                '6': 'VNSmallCap'
            }
            selected_list = list_mapping.get(list_choice, 'HOSE')
            
            stock_list = get_stock_list(selected_list)
            db_path = HOSE_DATA_DB_PATH if selected_list == 'HOSE' else os.path.join(SCRIPT_DIR, f"stock_data_{selected_list}.db")
            
            if not check_data_availability(db_path, stock_list, 400, min_period=200):
                print("Cảnh báo: Cơ sở dữ liệu không đủ dữ liệu cho 400 ngày hoặc MA200. Vui lòng cập nhật dữ liệu.")
            
            if choice == '1':
                counts, total_stocks = calculate_ma_statistics(stock_list, db_path)
                print(f"\nThống kê cho danh sách {selected_list}:")
                print(f"Số mã đã khảo sát: {total_stocks}")
                for period in PERIODS:
                    percentage = (counts[period] / total_stocks) * 100 if total_stocks > 0 else 0
                    print(f"Số mã đóng cửa trên MA{period}: {counts[period]} ({percentage:.2f}%)")
                
                df_ratio = calculate_ma_ratio_over_time(stock_list, db_path, num_days_display=100, num_days_data=400)
                plot_ma_combined(counts, total_stocks, df_ratio, selected_list)
                plot_additional_ma_charts(stock_list, db_path, selected_list)
            elif choice == '2':
                roc_data = calculate_roc_data(stock_list, db_path)
                plot_roc_density(roc_data)
            elif choice == '3':
                avg_volumes = calculate_average_volumes(stock_list, db_path)
                plot_average_volume_treemaps(avg_volumes, selected_list)
        else:
            print("Lựa chọn không hợp lệ. Vui lòng chọn lại.")
    
    print("\nTạo file mô tả cấu trúc DB...")
    describe_db(HOSE_DATA_DB_PATH, TXT_PATH)
    for group_name in GROUP_DB_PATHS.keys():
        group_data_db_path = os.path.join(SCRIPT_DIR, f"stock_data_{group_name}.db")
        describe_db(group_data_db_path, TXT_PATH, append=True)
    print(f"Hoàn tất! File mô tả đã được lưu tại {TXT_PATH}")