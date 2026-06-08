import os
import sys
import time
import subprocess
import shutil

# Thêm thư mục hiện tại vào sys.path để import được gói pupdb
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

processes = []

def clean_database_files():
    """Dọn dẹp các tệp cơ sở dữ liệu cũ để demo sạch sẽ từ đầu."""
    files_to_delete = [
        "shard1_master.json", "shard1_master.json.lock",
        "shard1_slave.json", "shard1_slave.json.lock",
        "shard2_master.json", "shard2_master.json.lock",
        "shard2_slave.json", "shard2_slave.json.lock"
    ]
    print("\n--- DỌN DẸP DỮ LIỆU CŨ ---")
    for filename in files_to_delete:
        path = os.path.join(current_dir, filename)
        if os.path.exists(path):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                print(f"Đã xóa: {filename}")
            except Exception as e:
                print(f"Không thể xóa {filename}: {e}")
    print("--------------------------\n")

def run_node(module, port, env_vars, name):
    env = os.environ.copy()
    env.update(env_vars)
    # Thiết lập PYTHONPATH để Flask import đúng gói pupdb
    env["PYTHONPATH"] = current_dir
    
    # Chạy ứng dụng Flask bằng cách gọi trực tiếp APP.run qua python -c
    code = f"from pupdb.{module} import APP; APP.run(port={port}, host='127.0.0.1', debug=False, use_reloader=False)"
    cmd = [sys.executable, "-c", code]
    
    # Tạo tệp nhật ký riêng cho từng node để dễ dàng kiểm tra/gỡ lỗi
    log_file = open(os.path.join(current_dir, f"log_{name.replace(' ', '_').lower()}.txt"), "w", encoding="utf-8")
    
    # Khởi chạy tiến trình con
    p = subprocess.Popen(
        cmd, 
        env=env, 
        stdout=log_file, 
        stderr=log_file, 
        cwd=current_dir
    )
    processes.append((p, name, port, log_file))
    print(f"-> Đang khởi động {name} tại cổng {port}...")
    return p

def main():
    # clean_database_files()
    print("=== KHỞI ĐỘNG HỆ THỐNG PHÂN TÁN PUPDB CLUSTER ===")
    
    try:
        # 1. Khởi động Shard 1 - Slave (Cổng 4011)
        run_node(
            module="rest", 
            port=4011, 
            env_vars={"PUPDB_FILE_PATH": "shard1_slave.json", "PUPDB_ROLE": "slave"}, 
            name="Shard 1 Slave"
        )
        
        # 2. Khởi động Shard 1 - Master (Cổng 4001) - có nhân bản sang Slave ở cổng 4011
        run_node(
            module="rest", 
            port=4001, 
            env_vars={
                "PUPDB_FILE_PATH": "shard1_master.json",
                "PUPDB_ROLE": "master",
                "PUPDB_SLAVE_URL": "http://127.0.0.1:4011"
            }, 
            name="Shard 1 Master"
        )
        
        # 3. Khởi động Shard 2 - Slave (Cổng 4012)
run_node(
            module="rest", 
            port=4012, 
            env_vars={"PUPDB_FILE_PATH": "shard2_slave.json", "PUPDB_ROLE": "slave"}, 
            name="Shard 2 Slave"
        )
        
        # 4. Khởi động Shard 2 - Master (Cổng 4002) - có nhân bản sang Slave ở cổng 4012
        run_node(
            module="rest", 
            port=4002, 
            env_vars={
                "PUPDB_FILE_PATH": "shard2_master.json",
                "PUPDB_ROLE": "master",
                "PUPDB_SLAVE_URL": "http://127.0.0.1:4012"
            }, 
            name="Shard 2 Master"
        )
        
        # Đợi 2 giây cho các Shard Master/Slave khởi động hoàn toàn trước khi khởi động Router
        time.sleep(2)
        
        # 5. Khởi động Sharding Router Proxy (Cổng 4000) - chuyển tiếp yêu cầu đến các Shard
        run_node(
            module="router", 
            port=4000, 
            env_vars={
                "PUPDB_SHARDS": "http://127.0.0.1:4001,http://127.0.0.1:4002",
                "PUPDB_SLAVES": "http://127.0.0.1:4011,http://127.0.0.1:4012"
            }, 
            name="Router Proxy"
        )
        
        print("\n" + "="*50)
        print(" TẤT CẢ CÁC THÀNH PHẦN ĐÃ ĐƯỢC KHỞI ĐỘNG THÀNH CÔNG!")
        print("="*50)
        print(" - Router Proxy (Cổng nhận yêu cầu chính): http://127.0.0.1:4000")
        print(" - Shard 1 (Master): http://127.0.0.1:4001  |  Shard 1 (Slave): http://127.0.0.1:4011")
        print(" - Shard 2 (Master): http://127.0.0.1:4002  |  Shard 2 (Slave): http://127.0.0.1:4012")
        print("-"*50)
        print(" * Lưu ý: Nhật ký (logs) chi tiết được ghi vào các tệp log_*.txt")
        print(" * Nhấn Ctrl+C để dừng toàn bộ hệ thống cluster.")
        print("="*50 + "\n")
        
        reported_stopped = set()
        while True:
            time.sleep(1)
            # Kiểm tra trạng thái các tiến trình con
            for p, name, port, _ in processes:
                if p.poll() is not None and name not in reported_stopped:
                    print(f"[CẢNH BÁO] {name} (cổng {port}) đã dừng đột ngột!")
                    reported_stopped.add(name)
                    
    except KeyboardInterrupt:
        print("\nĐang dừng hệ thống cluster...")
        for p, name, port, log_file in processes:
            print(f"-> Đang tắt {name}...")
            p.terminate()
            p.wait()
            log_file.close()
        print("Hệ thống cluster đã dừng hoạt động.")

if __name__ == "__main__":
    main()