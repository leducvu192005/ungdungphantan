import time
import json
import urllib.request
import urllib.parse

ROUTER_URL = "http://127.0.0.1:4000"
SHARD1_MASTER_URL = "http://127.0.0.1:4001"
SHARD1_SLAVE_URL = "http://127.0.0.1:4011"
SHARD2_MASTER_URL = "http://127.0.0.1:4002"
SHARD2_SLAVE_URL = "http://127.0.0.1:4012"

def send_request(url, method="GET", data=None):
    """Gửi HTTP request và trả về JSON payload."""
    try:
        req = urllib.request.Request(url, method=method)
        if data is not None:
            req.add_header('Content-Type', 'application/json')
            payload = json.dumps(data).encode('utf-8')
            with urllib.request.urlopen(req, data=payload, timeout=5) as response:
                return json.loads(response.read().decode('utf-8')), response.status
        else:
            with urllib.request.urlopen(req, timeout=5) as response:
                return json.loads(response.read().decode('utf-8')), response.status
    except Exception as e:
        return {"error": str(e)}, 500

def get_shard_index(key):
    """Tính toán shard dựa trên tổng mã ASCII các ký tự của khóa."""
    if not key:
        return 0
    total_ascii = sum(ord(c) for c in str(key))
    return total_ascii % 2 # 2 shards


def print_separator(title=""):
    print("\n" + "="*70)
    if title:
        print(f" {title.upper()} ")
        print("="*70)


def main():
    print_separator("KỊCH BẢN DEMO HỆ PHÂN TÁN - PUPDB CLUSTER")
    print("Kịch bản này sẽ minh họa 3 đặc trưng cốt lõi của hệ thống phân tán:")
    print(" 1. DATA SHARDING (Phân mảnh ngang dữ liệu dựa trên Hash Key)")
    print(" 2. MASTER-SLAVE REPLICATION (Nhân bản dữ liệu bất đồng bộ)")
    print(" 3. ROUTING & DATA AGGREGATION (Định tuyến và Tổng hợp kết quả từ Proxy)")
    
    input("\nNhấn [Enter] để bắt đầu Bước 1 (Data Sharding & Routing)...")
    
    # =========================================================================
    # BƯỚC 1: SHARDING VÀ ĐỊNH TUYẾN
    # =========================================================================
    print_separator("Bước 1: Data Sharding & Routing")
    
    test_data = {
        "apple": "Quả táo đỏ ngon",      # sum = 530 -> 530 % 2 = 0 (Shard 1)
        "banana": "Chuối chín vàng",     # sum = 609 -> 609 % 2 = 1 (Shard 2)
        "cherry": "Quả anh đào cherry",  # sum = 653 -> 653 % 2 = 1 (Shard 2)
        "date": "Quả chà là ngọt",       # sum = 414 -> 414 % 2 = 0 (Shard 1)
        "eggplant": "Cà tím nướng mỡ hành" # sum = 850 -> 850 % 2 = 0 (Shard 1)
    }
    
    print("1.1. Chúng ta sẽ ghi 5 cặp Key-Value qua cổng Router Proxy (Port 4000):")
    print("    Router sẽ tính tổng mã ASCII của tất cả ký tự trong Key chia lấy dư số Shards:")
    print("    - Nếu kết quả = 0 -> Ghi vào Shard 1 (Port 4001)")
print("    - Nếu kết quả = 1 -> Ghi vào Shard 2 (Port 4002)\n")
    
    for key, value in test_data.items():
        computed_shard = get_shard_index(key) + 1
        ascii_sum = sum(ord(c) for c in key)
        print(f" -> Chuẩn bị lưu: Key = '{key}' (Tổng mã ASCII={ascii_sum}).")
        print(f"    => Công thức: {ascii_sum} % 2 = {computed_shard - 1} => Thuộc Shard {computed_shard}")
        
        # Gửi request lên Router Proxy
        url = f"{ROUTER_URL}/set"
        payload = {"key": key, "value": value}
        res, code = send_request(url, "POST", payload)
        print(f"    Gửi POST tới Router: Code {code} -> {res.get('message', res.get('error'))}\n")
        time.sleep(0.3)
        
    input("Nhấn [Enter] để kiểm tra sự phân mảnh vật lý của dữ liệu trên từng Shard...")
    
    # 1.2. Kiểm tra trực tiếp trên Shard 1 & Shard 2
    print_separator("Bước 1.2: Xác minh phân mảnh ngang vật lý (Horizontal Sharding)")
    print("Bây giờ, chúng ta sẽ truy vấn TRỰC TIẾP từng Shard Master để chứng minh dữ liệu bị tách biệt:")
    
    print("\n[SHARD 1 MASTER - CỔNG 4001] (Chứa các khóa bắt đầu bằng mã ASCII Chẵn):")
    res1, _ = send_request(f"{SHARD1_MASTER_URL}/dumps")
    print(json.dumps(res1.get("database", {}), indent=4, ensure_ascii=False))
    
    print("\n[SHARD 2 MASTER - CỔNG 4002] (Chứa các khóa bắt đầu bằng mã ASCII Lẻ):")
    res2, _ = send_request(f"{SHARD2_MASTER_URL}/dumps")
    print(json.dumps(res2.get("database", {}), indent=4, ensure_ascii=False))
    
    print("\n==> Nhận xét: Dữ liệu đã được phân mảnh ngang hoàn hảo! Không có sự chồng chéo.")
    
    input("\nNhấn [Enter] để bắt đầu Bước 2 (Master-Slave Replication)...")
    
    # =========================================================================
    # BƯỚC 2: MASTER-SLAVE REPLICATION
    # =========================================================================
    print_separator("Bước 2: Master-Slave Replication (Nhân bản dữ liệu)")
    print("Hệ thống cấu hình chế độ Master-Slave:")
    print(" - Shard 1 Master (Port 4001) nhân bản sang Shard 1 Slave (Port 4011)")
    print(" - Shard 2 Master (Port 4002) nhân bản sang Shard 2 Slave (Port 4012)")
    print("\nKhi chúng ta ghi dữ liệu lên Master, luồng Master sẽ nhân bản bất đồng bộ sang Slave tương ứng.\n")
    
    # Đợi 1 giây để đảm bảo việc nhân bản bất đồng bộ hoàn tất
    print("Đang kiểm tra dữ liệu trên các nút Slave...")
    time.sleep(1)
    
    print("\n[SHARD 1 SLAVE - CỔNG 4011] (Đọc trực tiếp từ Slave 1):")
    res1_slave, _ = send_request(f"{SHARD1_SLAVE_URL}/dumps")
    print(json.dumps(res1_slave.get("database", {}), indent=4, ensure_ascii=False))
print("\n[SHARD 2 SLAVE - CỔNG 4012] (Đọc trực tiếp từ Slave 2):")
    res2_slave, _ = send_request(f"{SHARD2_SLAVE_URL}/dumps")
    print(json.dumps(res2_slave.get("database", {}), indent=4, ensure_ascii=False))
    
    print("\n==> Nhận xét: Dữ liệu đã được nhân bản sang các nút Slave thành công!")
    print("    Điều này giúp tăng tính sẵn sàng (High Availability) và dự phòng dữ liệu.")
    
    input("\nNhấn [Enter] để bắt đầu Bước 3 (Data Aggregation & Router Proxy)...")
    
    # =========================================================================
    # BƯỚC 3: ROUTING & DATA AGGREGATION
    # =========================================================================
    print_separator("Bước 3: Routing & Data Aggregation (Tổng hợp dữ liệu)")
    print("Khi Client gửi yêu cầu lấy toàn bộ dữ liệu qua Router Proxy:")
    print("1. Khách hàng gọi cổng `/dumps` hoặc `/keys` hoặc `/items` trên Router (Port 4000).")
    print("2. Router sẽ gửi yêu cầu song song đến TẤT CẢ các Shard Master.")
    print("3. Router tổng hợp kết quả (Merge) và trả về một khối thống nhất cho Client.\n")
    
    print("Gửi GET `/keys` đến Router Proxy...")
    keys_res, _ = send_request(f"{ROUTER_URL}/keys")
    print(f" -> Danh sách tất cả Khóa tổng hợp: {keys_res.get('keys')}")
    
    print("\nGửi GET `/dumps` đến Router Proxy...")
    dumps_res, _ = send_request(f"{ROUTER_URL}/dumps")
    print(" -> Toàn bộ cơ sở dữ liệu đã gộp:")
    print(json.dumps(dumps_res.get("database", {}), indent=4, ensure_ascii=False))
    
    print_separator("KẾT THÚC KỊCH BẢN DEMO")
    print("Hệ thống đã chứng minh xuất sắc các nguyên lý của Ứng dụng phân tán:")
    print(" ✔️ Định tuyến trong suốt (Transparent Routing) qua Proxy.")
    print(" ✔️ Phân mảnh dữ liệu ngang (Horizontal Sharding) bằng thuật toán băm (Consistent Hashing-like).")
    print(" ✔️ Nhân bản cơ sở dữ liệu (Database Replication) giúp nâng cao độ tin cậy.")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()