import tkinter as tk
from tkinter import ttk
import pandas as pd
import pygame
import threading
import time
from vnstock import Vnstock

class StockApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Theo dõi giá Intraday")
        self.running = False

        # Khởi tạo pygame mixer để phát âm thanh
        pygame.mixer.init()

        # Khởi tạo các biến
        self.symbol = tk.StringVar(value="ACB")
        self.interval = tk.StringVar(value="1p")

        # Tạo giao diện
        self.create_widgets()

    def create_widgets(self):
        # **Frame cho lựa chọn**
        input_frame = ttk.LabelFrame(self.root, text="Cài đặt", padding=10)
        input_frame.grid(row=0, column=0, padx=10, pady=10, sticky="ew")

        tk.Label(input_frame, text="Mã cổ phiếu:").grid(row=0, column=0, padx=5, pady=5)
        symbol_combo = ttk.Combobox(input_frame, textvariable=self.symbol)
        symbol_combo['values'] = ['ACB', 'VCB', 'BID', 'FPT']
        symbol_combo.grid(row=0, column=1, padx=5, pady=5)

        tk.Label(input_frame, text="Khoảng thời gian:").grid(row=1, column=0, padx=5, pady=5)
        interval_combo = ttk.Combobox(input_frame, textvariable=self.interval)
        interval_combo['values'] = ['1p', '5p', '10p', '15p']
        interval_combo.grid(row=1, column=1, padx=5, pady=5)

        self.start_button = tk.Button(input_frame, text="Bắt đầu", command=self.toggle_monitoring, bg="green", fg="white")
        self.start_button.grid(row=2, column=0, columnspan=2, pady=10)

        # **Frame cho thông tin giá mới nhất**
        latest_frame = ttk.LabelFrame(self.root, text="Thông tin giá mới nhất", padding=10)
        latest_frame.grid(row=1, column=0, padx=10, pady=10, sticky="ew")
        self.latest_price_label = tk.Label(latest_frame, text="Giá mới nhất: N/A")
        self.latest_price_label.grid(row=0, column=0, padx=5, pady=5)
        self.latest_volume_label = tk.Label(latest_frame, text="Khối lượng: N/A")
        self.latest_volume_label.grid(row=0, column=1, padx=5, pady=5)
        self.match_type_label = tk.Label(latest_frame, text="Loại khớp: N/A")
        self.match_type_label.grid(row=0, column=2, padx=5, pady=5)

        # **Frame cho thống kê lệnh**
        stats_frame = ttk.LabelFrame(self.root, text="Thống kê lệnh", padding=10)
        stats_frame.grid(row=2, column=0, padx=10, pady=10, sticky="ew")
        self.num_buy_label = tk.Label(stats_frame, text="Số lệnh Buy: 0")
        self.num_buy_label.grid(row=0, column=0, padx=5, pady=5)
        self.num_sell_label = tk.Label(stats_frame, text="Số lệnh Sell: 0")
        self.num_sell_label.grid(row=0, column=1, padx=5, pady=5)
        self.avg_buy_volume_label = tk.Label(stats_frame, text="TB KL Buy: 0")
        self.avg_buy_volume_label.grid(row=1, column=0, padx=5, pady=5)
        self.avg_sell_volume_label = tk.Label(stats_frame, text="TB KL Sell: 0")
        self.avg_sell_volume_label.grid(row=1, column=1, padx=5, pady=5)
        self.total_buy_label = tk.Label(stats_frame, text="Tổng tiền Buy: 0")
        self.total_buy_label.grid(row=2, column=0, padx=5, pady=5)
        self.total_sell_label = tk.Label(stats_frame, text="Tổng tiền Sell: 0")
        self.total_sell_label.grid(row=2, column=1, padx=5, pady=5)
        self.net_money_label = tk.Label(stats_frame, text="Tiền ròng: 0")
        self.net_money_label.grid(row=3, column=0, columnspan=2, padx=5, pady=5)

        # **Frame cho thống kê theo giá**
        price_frame = ttk.LabelFrame(self.root, text="Thống kê theo giá", padding=10)
        price_frame.grid(row=3, column=0, padx=10, pady=10, sticky="ew")
        self.price_tree = ttk.Treeview(price_frame, columns=('price', 'buy_value', 'sell_value', 'net_value'), show='headings')
        self.price_tree.heading('price', text='Giá')
        self.price_tree.heading('buy_value', text='Giá trị Buy')
        self.price_tree.heading('sell_value', text='Giá trị Sell')
        self.price_tree.heading('net_value', text='Giá trị Ròng')
        self.price_tree.column('price', anchor='w', width=100)
        self.price_tree.column('buy_value', anchor='w', width=150)
        self.price_tree.column('sell_value', anchor='w', width=150)
        self.price_tree.column('net_value', anchor='w', width=150)
        self.price_tree.grid(row=0, column=0, sticky="nsew")

        # **Frame cho so sánh giá**
        price_compare_frame = ttk.LabelFrame(self.root, text="So sánh giá", padding=10)
        price_compare_frame.grid(row=1, column=1, padx=10, pady=10, sticky="nsew")
        self.current_price_label = tk.Label(price_compare_frame, text="Giá hiện tại: N/A")
        self.current_price_label.grid(row=0, column=0, sticky="w", padx=5, pady=2)
        self.sma5_price_label = tk.Label(price_compare_frame, text="Giá TB 5 ngày: N/A")
        self.sma5_price_label.grid(row=1, column=0, sticky="w", padx=5, pady=2)
        self.sma10_price_label = tk.Label(price_compare_frame, text="Giá TB 10 ngày: N/A")
        self.sma10_price_label.grid(row=2, column=0, sticky="w", padx=5, pady=2)
        self.sma20_price_label = tk.Label(price_compare_frame, text="Giá TB 20 ngày: N/A")
        self.sma20_price_label.grid(row=3, column=0, sticky="w", padx=5, pady=2)

        # **Frame cho so sánh khối lượng**
        volume_compare_frame = ttk.LabelFrame(self.root, text="So sánh khối lượng", padding=10)
        volume_compare_frame.grid(row=2, column=1, padx=10, pady=10, sticky="nsew")
        self.today_volume_label = tk.Label(volume_compare_frame, text="KL hôm nay: N/A")
        self.today_volume_label.grid(row=0, column=0, sticky="w", padx=5, pady=2)
        self.sma5_volume_label = tk.Label(volume_compare_frame, text="KL TB 5 ngày: N/A")
        self.sma5_volume_label.grid(row=1, column=0, sticky="w", padx=5, pady=2)
        self.sma10_volume_label = tk.Label(volume_compare_frame, text="KL TB 10 ngày: N/A")
        self.sma10_volume_label.grid(row=2, column=0, sticky="w", padx=5, pady=2)
        self.sma20_volume_label = tk.Label(volume_compare_frame, text="KL TB 20 ngày: N/A")
        self.sma20_volume_label.grid(row=3, column=0, sticky="w", padx=5, pady=2)

        # **Frame cho cảnh báo lệnh đột biến**
        alert_frame = ttk.LabelFrame(self.root, text="Cảnh báo lệnh đột biến", padding=10)
        alert_frame.place(relx=1.0, rely=0.0, anchor='ne')  # Đặt ở góc trên bên phải
        self.alert_label = tk.Label(alert_frame, text="", font=("Arial", 14, "bold"))
        self.alert_label.pack(pady=20)

        # Mở rộng kích thước giao diện
        self.root.geometry("1000x600")

    def toggle_monitoring(self):
        if not self.running:
            self.running = True
            self.start_button.config(text="Dừng", bg="red")
            threading.Thread(target=self.monitor_stock, daemon=True).start()
        else:
            self.running = False
            self.start_button.config(text="Bắt đầu", bg="green")

    def monitor_stock(self):
        while self.running:
            symbol = self.symbol.get()
            interval = self.interval.get()
            if not symbol or not interval:
                continue

            # Khởi tạo đối tượng Vnstock
            stock = Vnstock().stock(symbol=symbol, source='VCI')

            # Lấy dữ liệu intraday
            intraday_data = stock.quote.intraday(symbol=symbol, page_size=10000, show_log=False)

            if not self.running:
                break

            # Lấy dữ liệu lịch sử 50 ngày
            end_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=50)).strftime('%Y-%m-%d')
            history_data = stock.quote.history(start=start_date, end=end_date, interval='1D')

            # Hiển thị thông tin
            self.display_info(intraday_data)

            # Phát hiện bất thường
            self.detect_anomalies(intraday_data)

            # Thống kê theo giá
            self.display_price_stats(intraday_data)

            # Hiển thị so sánh với mức trung bình
            self.display_comparison(intraday_data, history_data)

            # Đợi khoảng thời gian cập nhật
            time.sleep(int(interval[:-1]) * 60)

    def display_info(self, data):
        if data.empty:
            print("Không có dữ liệu intraday.")
            return

        latest = data.iloc[-1]
        self.latest_price_label.config(text=f"Giá mới nhất: {latest['price']}")
        self.latest_volume_label.config(text=f"Khối lượng: {latest['volume']}")
        self.match_type_label.config(text=f"Loại khớp: {latest['match_type']}")

        buy_orders = data[data['match_type'] == 'Buy']
        sell_orders = data[data['match_type'] == 'Sell']
        num_buy = len(buy_orders)
        num_sell = len(sell_orders)
        avg_buy_volume = buy_orders['volume'].mean() if num_buy > 0 else 0
        avg_sell_volume = sell_orders['volume'].mean() if num_sell > 0 else 0
        total_buy = (buy_orders['price'] * buy_orders['volume']).sum()
        total_sell = (sell_orders['price'] * sell_orders['volume']).sum()
        net_money = total_buy - total_sell

        self.num_buy_label.config(text=f"Số lệnh Buy: {num_buy}")
        self.num_sell_label.config(text=f"Số lệnh Sell: {num_sell}")
        self.avg_buy_volume_label.config(text=f"TB KL Buy: {avg_buy_volume:.2f}")
        self.avg_sell_volume_label.config(text=f"TB KL Sell: {avg_sell_volume:.2f}")
        self.total_buy_label.config(text=f"Tổng tiền Buy: {total_buy:,.0f}")
        self.total_sell_label.config(text=f"Tổng tiền Sell: {total_sell:,.0f}")
        self.net_money_label.config(text=f"Tiền ròng: {net_money:,.0f}")

    def detect_anomalies(self, data):
        if data.empty:
            return

        Q1 = data['volume'].quantile(0.25)
        Q3 = data['volume'].quantile(0.75)
        IQR = Q3 - Q1
        upper_bound = Q3 + 1.5 * IQR

        anomalies = data[data['volume'] > upper_bound]
        if not anomalies.empty:
            if 'Buy' in anomalies['match_type'].values:
                self.alert_label.config(text="Cảnh báo: Lệnh Buy đột biến!", fg="green")
                try:
                    pygame.mixer.music.load("E:\\Python\\realtime Stock Information\\Tải danh sách cổ phiếu\\intra\\buy_alert.mp3")
                    pygame.mixer.music.play()
                    while pygame.mixer.music.get_busy():
                        time.sleep(0.1)
                except Exception as e:
                    print(f"Lỗi khi phát âm thanh: {e}")
            elif 'Sell' in anomalies['match_type'].values:
                self.alert_label.config(text="Cảnh báo: Lệnh Sell đột biến!", fg="red")
                try:
                    pygame.mixer.music.load("E:\\Python\\realtime Stock Information\\Tải danh sách cổ phiếu\\intra\\sell_alert.mp3")
                    pygame.mixer.music.play()
                    while pygame.mixer.music.get_busy():
                        time.sleep(0.1)
                except Exception as e:
                    print(f"Lỗi khi phát âm thanh: {e}")
            # Tự tắt sau 30 giây
            self.root.after(30000, lambda: self.alert_label.config(text=""))
        else:
            self.alert_label.config(text="")

    def display_price_stats(self, data):
        if data.empty:
            return

        # Tính giá trị giao dịch
        data['value'] = data['price'] * data['volume']
        grouped = data.groupby(['price', 'match_type'])['value'].sum().unstack(fill_value=0)
        grouped['net_value'] = grouped.get('Buy', 0) - grouped.get('Sell', 0)

        # Hiển thị trong treeview
        for item in self.price_tree.get_children():
            self.price_tree.delete(item)
        for price, row in grouped.iterrows():
            self.price_tree.insert('', 'end', values=(f"{price:,.2f}", f"{row.get('Buy', 0):,.0f}", f"{row.get('Sell', 0):,.0f}", f"{row['net_value']:,.0f}"))

    def display_comparison(self, intraday_data, history_data):
        if history_data.empty:
            print("Không có dữ liệu lịch sử.")
            self.current_price_label.config(text="Không có dữ liệu lịch sử")
            self.sma5_price_label.config(text="Không có dữ liệu lịch sử")
            self.sma10_price_label.config(text="Không có dữ liệu lịch sử")
            self.sma20_price_label.config(text="Không có dữ liệu lịch sử")
            self.today_volume_label.config(text="Không có dữ liệu lịch sử")
            self.sma5_volume_label.config(text="Không có dữ liệu lịch sử")
            self.sma10_volume_label.config(text="Không có dữ liệu lịch sử")
            self.sma20_volume_label.config(text="Không có dữ liệu lịch sử")
            return

        # Chuyển đổi dữ liệu sang kiểu float64
        closes = history_data['close'].values.astype(float)
        volumes = history_data['volume'].values.astype(float)

        # Tính SMA bằng phương thức thủ công
        sma5 = calculate_sma(closes, 5)
        sma10 = calculate_sma(closes, 10)
        sma20 = calculate_sma(closes, 20)
        vol_sma5 = calculate_sma(volumes, 5)
        vol_sma10 = calculate_sma(volumes, 10)
        vol_sma20 = calculate_sma(volumes, 20)

        # Giá hiện tại và khối lượng hôm nay
        current_price = closes[-1] if len(closes) > 0 else None
        today_volume = intraday_data['volume'].sum() if not intraday_data.empty else None

        # Cập nhật so sánh giá
        self.current_price_label.config(text=f"Giá hiện tại: {current_price if current_price is not None else 'N/A'}")
        self.sma5_price_label.config(text=f"Giá TB 5 ngày: {f'{sma5:.2f}' if sma5 is not None else 'N/A'}")
        self.sma10_price_label.config(text=f"Giá TB 10 ngày: {f'{sma10:.2f}' if sma10 is not None else 'N/A'}")
        self.sma20_price_label.config(text=f"Giá TB 20 ngày: {f'{sma20:.2f}' if sma20 is not None else 'N/A'}")

        # Cập nhật so sánh khối lượng
        self.today_volume_label.config(text=f"KL hôm nay: {today_volume if today_volume is not None else 'N/A'}")
        self.sma5_volume_label.config(text=f"KL TB 5 ngày: {f'{vol_sma5:.2f}' if vol_sma5 is not None else 'N/A'}")
        self.sma10_volume_label.config(text=f"KL TB 10 ngày: {f'{vol_sma10:.2f}' if vol_sma10 is not None else 'N/A'}")
        self.sma20_volume_label.config(text=f"KL TB 20 ngày: {f'{vol_sma20:.2f}' if vol_sma20 is not None else 'N/A'}")

# Hàm tính SMA thủ công
def calculate_sma(data, period):
    if len(data) < period:
        return None
    return sum(data[-period:]) / period

if __name__ == "__main__":
    root = tk.Tk()
    app = StockApp(root)
    root.mainloop()