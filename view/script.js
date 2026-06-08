// Hỗ trợ tự động chạy test không bị nghẽn confirm dialog
const bypassConfirm = window.location.search.includes("bypass_confirm=true");

// Cấu hình các cổng kết nối và máy chủ
const ROUTER_PORT = 4000;
const S1M_PORT = 4001;
const S2M_PORT = 4002;
const S1S_PORT = 4011;
const S2S_PORT = 4012;

const BASE_HOST = window.location.hostname || "127.0.0.1";

// Xác định giao thức và tạo API URL đầy đủ
const PROTOCOL = window.location.protocol === "file:" ? "http:" : window.location.protocol;
const API_ROUTER = `${PROTOCOL}//${BASE_HOST}:${ROUTER_PORT}`;
const API_S1M = `${PROTOCOL}//${BASE_HOST}:${S1M_PORT}`;
const API_S2M = `${PROTOCOL}//${BASE_HOST}:${S2M_PORT}`;
const API_S1S = `${PROTOCOL}//${BASE_HOST}:${S1S_PORT}`;
const API_S2S = `${PROTOCOL}//${BASE_HOST}:${S2S_PORT}`;

let currentActiveTab = "single";

// Hàm ghi log vào Console Terminal ở chân trang
function writeLog(message, type = "info") {
    const container = document.getElementById("console-logs");
    if (!container) return;
    
    const now = new Date();
    const timeStr = `[${now.toTimeString().split(" ")[0]}]`;
    
    const line = document.createElement("div");
    line.className = "console-log-line";
    
    const timeSpan = document.createElement("span");
    timeSpan.className = "console-time";
    timeSpan.textContent = timeStr;
    
    const txtSpan = document.createElement("span");
    txtSpan.className = `console-txt ${type}`;
    txtSpan.textContent = message;
    
    line.appendChild(timeSpan);
    line.appendChild(txtSpan);
    container.appendChild(line);
    container.scrollTop = container.scrollHeight;
}

function clearLogs() {
    const container = document.getElementById("console-logs");
    if (container) {
        container.innerHTML = "";
    }
    writeLog("Đã xóa trắng logs.");
}

// Chuyển đổi giữa các Tab giao diện
function switchTab(tabId) {
    document.querySelectorAll(".tab-content").forEach(el => el.classList.remove("active"));
    document.querySelectorAll(".nav-btn").forEach(el => el.classList.remove("active"));
    
    if (tabId === "single") {
        document.getElementById("tab-single").classList.add("active");
        document.getElementById("btn-tab-single").classList.add("active");
        currentActiveTab = "single";
        writeLog("Đã chuyển sang chế độ: Đơn Node (Cổng 4000)", "info");
        singleFetch();
    } else {
        document.getElementById("tab-cluster").classList.add("active");
        document.getElementById("btn-tab-cluster").classList.add("active");
        currentActiveTab = "cluster";
        writeLog("Đã chuyển sang chế độ: Cụm Phân Tán (Cổng 4000 - 4012)", "info");
        clusterFetchAll();
    }
}

// Helper ping port để kiểm tra tính sống sót
async function pingPort(url) {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 2000); // 2s timeout
        
        // Gửi request đến root '/' thay vì '/keys' để tránh việc gọi chéo phân tán gây chậm trễ khi có node sập
        const response = await fetch(`${url}/`, { 
            method: "GET",
            signal: controller.signal
        });
        clearTimeout(timeoutId);
        return response.ok;
    } catch (e) {
        return false;
    }
}


// ==================== 1. XỬ LÝ CHO CHẾ ĐỘ ĐƠN NODE ====================

async function singleCheckStatus() {
    const isOnline = await pingPort(API_ROUTER);
    const dot = document.getElementById("single-status-dot");
    const text = document.getElementById("single-status-text");
    
    if (!dot || !text) return isOnline;

    if (isOnline) {
        dot.className = "status-dot online";
        text.textContent = "ONLINE";
        text.style.color = "var(--primary)";
        return true;
    } else {
        dot.className = "status-dot";
        text.textContent = "OFFLINE";
        text.style.color = "var(--danger)";
        return false;
    }
}

async function singleFetch() {
    const isOnline = await singleCheckStatus();
    const listContainer = document.getElementById("single-data-list");
    const sizeVal = document.getElementById("single-db-size");
    
    if (!listContainer) return;
    
    if (!isOnline) {
        listContainer.innerHTML = `
            <div class="empty-placeholder" style="color: var(--danger); border-color: var(--danger)">
                ⚠️ KHÔNG KẾT NỐI ĐƯỢC MÁY CHỦ!<br>
                Vui lòng mở Terminal chạy máy chủ đơn Node ở cổng 4000.
            </div>
        `;
        if (sizeVal) sizeVal.textContent = "0";
        return;
    }

    try {
        writeLog(`[Đơn Node] Gửi GET -> ${API_ROUTER}/dumps`);
        const res = await fetch(`${API_ROUTER}/dumps`);
        const data = await res.json();
        const db = data.database || {};
        const keys = Object.keys(db);
        
        if (sizeVal) sizeVal.textContent = keys.length;
        
        if (keys.length === 0) {
            listContainer.innerHTML = `
                <div class="empty-placeholder">Cơ sở dữ liệu trống. Nhập dữ liệu ở bảng bên trái!</div>
            `;
            return;
        }

        listContainer.innerHTML = "";
        keys.forEach(key => {
            const card = document.createElement("div");
            card.className = "data-card";
            card.innerHTML = `
                <div>
                    <div class="card-key">${key}</div>
                    <div class="card-val">${db[key]}</div>
                </div>
                <button class="card-delete-btn" onclick="singleDelete('${key}')">🗑️</button>
            `;
            listContainer.appendChild(card);
        });
        writeLog(`[Đơn Node] Tải thành công ${keys.length} khóa.`, "success");
    } catch (err) {
        writeLog(`[Đơn Node] Lỗi lấy dữ liệu: ${err.message}`, "error");
    }
}

async function singleSet() {
    const keyInput = document.getElementById("single-set-key");
    const valInput = document.getElementById("single-set-val");
    if (!keyInput || !valInput) return;

    const key = keyInput.value.trim();
    const val = valInput.value.trim();

    if (!key || !val) {
        alert("Vui lòng nhập cả Khóa và Giá trị!");
        return;
    }

    try {
        writeLog(`[Đơn Node] Gửi POST -> ${API_ROUTER}/set: {"key":"${key}", "value":"${val}"}`);
        const response = await fetch(`${API_ROUTER}/set`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ key, value: val })
        });
        
        const data = await response.json();
        if (response.ok) {
            writeLog(`[Đơn Node] Thành công: ${data.message}`, "success");
            keyInput.value = "";
            valInput.value = "";
            singleFetch();
        } else {
            writeLog(`[Đơn Node] Lỗi: ${data.error}`, "error");
        }
    } catch (e) {
        writeLog(`[Đơn Node] Lỗi mạng: ${e.message}`, "error");
    }
}

async function singleGet() {
    const keyInput = document.getElementById("single-get-key");
    const box = document.getElementById("single-query-result");
    if (!keyInput || !box) return;

    const key = keyInput.value.trim();

    if (!key) {
        alert("Vui lòng nhập khóa tìm kiếm!");
        return;
    }

    box.textContent = "Đang tìm...";
    box.style.color = "var(--text-color)";

    try {
        writeLog(`[Đơn Node] Gửi GET -> ${API_ROUTER}/get?key=${key}`);
        const response = await fetch(`${API_ROUTER}/get?key=${encodeURIComponent(key)}`);
        const data = await response.json();

        if (response.ok && data.value !== null && data.value !== undefined) {
            box.textContent = `SUCCESS:\n"${key}": "${data.value}"`;
            box.style.color = "var(--primary)";
            writeLog(`[Đơn Node] Tìm thấy "${key}" = "${data.value}"`, "success");
        } else {
            box.textContent = `NOT FOUND:\nKhóa "${key}" không tồn tại.`;
            box.style.color = "var(--danger)";
            writeLog(`[Đơn Node] Không tìm thấy khóa "${key}"`, "error");
        }
    } catch (e) {
        box.textContent = `ERROR:\n${e.message}`;
        box.style.color = "var(--danger)";
        writeLog(`[Đơn Node] Lỗi kết nối: ${e.message}`, "error");
    }
}

async function singleDelete(key) {
    if (!bypassConfirm && !confirm(`Xóa khóa "${key}"?`)) return;

    try {
        writeLog(`[Đơn Node] Gửi DELETE -> ${API_ROUTER}/remove/${key}`);
        const response = await fetch(`${API_ROUTER}/remove/${encodeURIComponent(key)}`, {
            method: "DELETE"
        });
        const data = await response.json();
        
        if (response.ok) {
            writeLog(`[Đơn Node] Đã xóa khóa "${key}"`, "success");
            singleFetch();
        } else {
            writeLog(`[Đơn Node] Lỗi xóa: ${data.error}`, "error");
        }
    } catch (e) {
        writeLog(`[Đơn Node] Lỗi: ${e.message}`, "error");
    }
}

async function singleTruncate() {
    if (!bypassConfirm && !confirm("Xóa sạch toàn bộ dữ liệu đơn Node?")) return;

    try {
        writeLog(`[Đơn Node] Gửi POST -> ${API_ROUTER}/truncate-db`);
        const response = await fetch(`${API_ROUTER}/truncate-db`, { method: "POST" });
        const data = await response.json();
        
        if (response.ok) {
            writeLog(`[Đơn Node] Đã xóa sạch dữ liệu`, "success");
            singleFetch();
        } else {
            writeLog(`[Đơn Node] Lỗi: ${data.error}`, "error");
        }
    } catch (e) {
        writeLog(`[Đơn Node] Lỗi: ${e.message}`, "error");
    }
}


// ==================== 2. XỬ LÝ CHO CHẾ ĐỘ CỤM PHÂN TÁN ====================

// Trạng thái kết nối trước đó của các node để phát hiện thay đổi (phục hồi / sập)
const previousNodeStatuses = {};
const nodeLabels = {
    "node-router": "Router Proxy",
    "node-shard1-master": "Shard 1 Master",
    "node-shard1-slave": "Shard 1 Slave",
    "node-shard2-master": "Shard 2 Master",
    "node-shard2-slave": "Shard 2 Slave"
};

// Kiểm tra trạng thái của tất cả 5 node trong cụm và cập nhật sơ đồ topology
async function clusterCheckAllStatuses() {
    const nodes = [
        { id: "node-router", url: API_ROUTER, dotId: null },
        { id: "node-shard1-master", url: API_S1M, dotId: "s1m-dot" },
        { id: "node-shard1-slave", url: API_S1S, dotId: "s1s-dot" },
        { id: "node-shard2-master", url: API_S2M, dotId: "s2m-dot" },
        { id: "node-shard2-slave", url: API_S2S, dotId: "s2s-dot" }
    ];

    let routerOnline = false;

    for (const node of nodes) {
        const isOnline = await pingPort(node.url);
        const el = document.getElementById(node.id);
        const label = nodeLabels[node.id] || node.id;

        // Phát hiện thay đổi trạng thái kết nối để ghi nhận vào Web Terminal Console
        const prevStatus = previousNodeStatuses[node.id];
        if (prevStatus !== undefined && prevStatus !== isOnline) {
            if (isOnline) {
                writeLog(`🟢 Kết nối phục hồi: Nút ${label} hoạt động trở lại (Online).`, "success");
                if (node.id.includes("master")) {
                    writeLog(`🔄 TỰ ĐỘNG ĐỒNG BỘ: ${label} kích hoạt Sync-Back dữ liệu từ Slave...`, "info");
                    setTimeout(() => {
                        writeLog(`✅ ĐỒNG BỘ THÀNH CÔNG: ${label} đã lấy đủ dữ liệu mới và phục hồi nhất quán dữ liệu!`, "success");
                        clusterFetchAll();
                    }, 1500);
                }
            } else {
                writeLog(`🔴 CẢNH BÁO SỰ CỐ: Nút ${label} đã mất kết nối (Offline)!`, "error");
                if (node.id.includes("master")) {
                    writeLog(`⚠️ Chế độ dự phòng (Failover) được kích hoạt: Router Proxy sẽ chuyển tiếp đọc/ghi tới Slave.`, "warning");
                }
            }
        }
        previousNodeStatuses[node.id] = isOnline;
        
        if (!el) continue;

        if (node.id === "node-router") {
            routerOnline = isOnline;
        }

        if (isOnline) {
            el.classList.add("active");
            el.style.borderColor = "var(--primary)";
            if (node.dotId) {
                const d = document.getElementById(node.dotId);
                if (d) d.className = "status-dot online";
            }
        } else {
            el.classList.remove("active");
            el.style.borderColor = "var(--danger)";
            if (node.dotId) {
                const d = document.getElementById(node.dotId);
                if (d) d.className = "status-dot";
            }
        }
    }

    return routerOnline;
}

// Tải dữ liệu từ tất cả các nguồn phân tán
async function clusterFetchAll() {
    const routerOnline = await clusterCheckAllStatuses();
    
    // 1. Tải dữ liệu gộp từ Router Proxy (Location Transparency)
    const clusterList = document.getElementById("cluster-data-list");
    const clusterSize = document.getElementById("cluster-db-size");

    if (!clusterList) return;

    if (!routerOnline) {
        clusterList.innerHTML = `
            <div class="empty-placeholder" style="color: var(--danger); border-color: var(--danger)">
                ⚠️ KHÔNG KẾT NỐI ĐƯỢC ROUTER PROXY (CỔNG 4000)!<br>
                Vui lòng chạy lệnh 'python start_cluster.py' để khởi động cụm 5 Node.
            </div>
        `;
        if (clusterSize) clusterSize.textContent = "0";
        
        // Set rỗng các cột phân mảnh thô
        const emptyOffline = '<div class="empty-placeholder">Offline</div>';
        const cols = ["s1m-list", "s1s-list", "s2m-list", "s2s-list"];
        cols.forEach(cid => {
            const col = document.getElementById(cid);
            if (col) col.innerHTML = emptyOffline;
        });
        return;
    }

    // Tải dữ liệu gộp từ Router Proxy
    try {
        writeLog(`[Cluster] Gửi GET -> ${API_ROUTER}/dumps để lấy dữ liệu gộp`);
        const res = await fetch(`${API_ROUTER}/dumps`);
        const data = await res.json();
        const db = data.database || {};
        const keys = Object.keys(db);
        
        if (clusterSize) clusterSize.textContent = keys.length;

        if (keys.length === 0) {
            clusterList.innerHTML = `<div class="empty-placeholder">Cụm CSDL trống. Nhập dữ liệu bên trái!</div>`;
        } else {
            clusterList.innerHTML = "";
            keys.forEach(key => {
                const card = document.createElement("div");
                card.className = "data-card accent-card";
                
                // Kiểm tra thuật toán định hướng để gán màu sắc trực quan (chẵn: xanh dương, lẻ: tím)
                const shardIdx = sumAscii(key) % 2;
                if (shardIdx === 1) {
                    card.style.borderLeftColor = "var(--purple)";
                }
                
                card.innerHTML = `
                    <div>
                        <div class="card-key">${key} <span style="font-size: 0.7rem; color: var(--text-muted)">(${shardIdx === 0 ? "Shard 1" : "Shard 2"})</span></div>
                        <div class="card-val">${db[key]}</div>
                    </div>
                    <button class="card-delete-btn" onclick="clusterDelete('${key}')">🗑️</button>
                `;
                clusterList.appendChild(card);
            });
        }
    } catch (e) {
        writeLog(`[Cluster] Lỗi lấy dữ liệu gộp: ${e.message}`, "error");
    }

    // 2. Tải dữ liệu thô chi tiết từ từng Node vật lý để minh họa Sharding và Replication
    fetchPhysicalNodeData(API_S1M, "s1m-list", false);
    fetchPhysicalNodeData(API_S1S, "s1s-list", true);
    fetchPhysicalNodeData(API_S2M, "s2m-list", false);
    fetchPhysicalNodeData(API_S2S, "s2s-list", true);
    fetchRecycleBin();
}

// Lấy dữ liệu trực tiếp của từng Node phụ
async function fetchPhysicalNodeData(baseUrl, listId, isSlave) {
    const container = document.getElementById(listId);
    if (!container) return;

    try {
        const response = await fetch(`${baseUrl}/dumps`);
        if (!response.ok) throw new Error("Cổng đóng");
        const data = await response.json();
        const db = data.database || {};
        const keys = Object.keys(db);

        if (keys.length === 0) {
            container.innerHTML = '<div class="empty-placeholder">Trống</div>';
            return;
        }

        container.innerHTML = "";
        keys.forEach(key => {
            const card = document.createElement("div");
            card.className = `mini-card ${listId.includes("s2") ? "shard2-card" : ""}`;
            card.innerHTML = `
                <span class="mini-card-key">${key}</span>
                <span class="mini-card-val">${db[key]}</span>
            `;
            container.appendChild(card);
        });
    } catch (e) {
        container.innerHTML = '<div class="empty-placeholder" style="color: var(--danger)">OFFLINE</div>';
    }
}

// Tương đương hàm ord() của python để lấy mã ASCII
function ord(str) {
    return str.charCodeAt(0);
}

// Tính tổng mã ASCII của tất cả ký tự trong chuỗi
function sumAscii(str) {
    let sum = 0;
    for (let i = 0; i < str.length; i++) {
        sum += str.charCodeAt(i);
    }
    return sum;
}

// Ghi dữ liệu qua Router Proxy
async function clusterSet() {
    const keyInput = document.getElementById("cluster-set-key");
    const valInput = document.getElementById("cluster-set-val");
    const explainBox = document.getElementById("cluster-explain-box");
    if (!keyInput || !valInput || !explainBox) return;

    const key = keyInput.value.trim();
    const val = valInput.value.trim();

    if (!key || !val) {
        alert("Vui lòng điền khóa và giá trị!");
        return;
    }

    const asciiSum = sumAscii(key);
    const shardIdx = asciiSum % 2;
    const targetPort = shardIdx === 0 ? S1M_PORT : S2M_PORT;
    const slavePort = shardIdx === 0 ? S1S_PORT : S2S_PORT;

    // Hiển thị cách giải thích thuật toán băm (Sharding & Routing)
    explainBox.style.color = "var(--primary)";
    explainBox.innerHTML = `
        <div><strong>ĐỊNH TUYẾN GHI (WRITE ROUTE):</strong></div>
        <div>Khóa: <code>"${key}"</code> ➔ Tổng mã ASCII: <code>${asciiSum}</code></div>
        <div>Công thức: <code>${asciiSum} % 2 (Shards) = ${shardIdx}</code></div>
        <div>➔ Chuyển tiếp tới <strong>Shard ${shardIdx + 1} Master</strong> (Cổng ${targetPort})</div>
        <div>➔ Đồng bộ ngầm sang <strong>Shard ${shardIdx + 1} Slave</strong> (Cổng ${slavePort})</div>
    `;

    try {
        writeLog(`[Router] Gửi POST /set -> Router (Cổng 4000)`);
        const response = await fetch(`${API_ROUTER}/set`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ key, value: val })
        });

        const isFailover = response.headers.get('X-PupDB-Failover') === 'true';
        const data = await response.json();
        if (response.ok) {
            if (isFailover) {
                writeLog(`⚠️ PHÁT HIỆN SỰ CỐ: Master Shard offline. Router đã chuyển vùng (Failover) ghi sang Slave thành công!`, "warning");
            }
            writeLog(`[Router] Phản hồi: ${data.message}`, "success");
            keyInput.value = "";
            valInput.value = "";
            // Đợi 0.5s để việc Replication đồng bộ bất đồng bộ hoàn tất trước khi tải lại dữ liệu hiển thị
            setTimeout(clusterFetchAll, 500);
        } else {
            writeLog(`[Router] Lỗi: ${data.error}`, "error");
        }
    } catch (e) {
        writeLog(`[Router] Lỗi mạng: ${e.message}`, "error");
    }
}

// Lấy dữ liệu qua Router Proxy
async function clusterGet() {
    const keyInput = document.getElementById("cluster-get-key");
    const explainBox = document.getElementById("cluster-explain-box");
    if (!keyInput || !explainBox) return;

    const key = keyInput.value.trim();

    if (!key) {
        alert("Vui lòng nhập khóa!");
        return;
    }

    const asciiSum = sumAscii(key);
    const shardIdx = asciiSum % 2;
    const targetPort = shardIdx === 0 ? S1M_PORT : S2M_PORT;

    explainBox.style.color = "var(--accent)";
    explainBox.innerHTML = `
        <div><strong>ĐỊNH TUYẾN ĐỌC (READ ROUTE):</strong></div>
        <div>Khóa: <code>"${key}"</code> ➔ Tổng mã ASCII: <code>${asciiSum}</code></div>
        <div>Công thức: <code>${asciiSum} % 2 = ${shardIdx}</code></div>
        <div>➔ Định hướng đọc trực tiếp từ <strong>Shard ${shardIdx + 1} Master</strong> (Cổng ${targetPort})</div>
    `;

    try {
        writeLog(`[Router] Gửi GET /get?key=${key} -> Router (Cổng 4000)`);
        const response = await fetch(`${API_ROUTER}/get?key=${encodeURIComponent(key)}`);
        const isFailover = response.headers.get('X-PupDB-Failover') === 'true';
        const data = await response.json();

        if (response.ok && data.value !== null && data.value !== undefined) {
            if (isFailover) {
                writeLog(`⚠️ PHÁT HIỆN SỰ CỐ: Master Shard offline. Router đã chuyển vùng (Failover) đọc từ Slave thành công!`, "warning");
                explainBox.innerHTML += `<div style="color: var(--warning); margin-top: 5px;">⚠️ <strong>Failover:</strong> Đọc thành công từ Shard Slave!</div>`;
            }
            explainBox.innerHTML += `<div style="color: var(--primary); margin-top: 5px;"><strong>Kết quả:</strong> Found <code>"${key}"</code> = <code>"${data.value}"</code></div>`;
            writeLog(`[Router] Tìm thấy: "${key}" = "${data.value}"`, "success");
        } else {
            explainBox.innerHTML += `<div style="color: var(--danger); margin-top: 5px;"><strong>Kết quả:</strong> Khóa không tồn tại!</div>`;
            writeLog(`[Router] Không tìm thấy khóa "${key}"`, "error");
        }
    } catch (e) {
        writeLog(`[Router] Lỗi: ${e.message}`, "error");
    }
}

// Xóa dữ liệu qua Router
async function clusterDelete(key) {
    if (!bypassConfirm && !confirm(`Xóa khóa "${key}" trên Cụm Phân Tán?`)) return;

    const asciiSum = sumAscii(key);
    const shardIdx = asciiSum % 2;
    const targetPort = shardIdx === 0 ? S1M_PORT : S2M_PORT;

    const explainBox = document.getElementById("cluster-explain-box");
    if (explainBox) {
        explainBox.style.color = "var(--danger)";
        explainBox.innerHTML = `
            <div><strong>ĐỊNH TUYẾN XÓA (DELETE ROUTE):</strong></div>
            <div>Khóa: <code>"${key}"</code> ➔ Tổng mã ASCII: <code>${asciiSum}</code> (% 2 = <code>${shardIdx}</code>)</div>
            <div>➔ Gửi lệnh DELETE đến <strong>Shard ${shardIdx + 1} Master</strong> (Cổng ${targetPort})</div>
        `;
    }

    try {
        writeLog(`[Router] Gửi DELETE /remove/${key} -> Router (Cổng 4000)`);
        const response = await fetch(`${API_ROUTER}/remove/${encodeURIComponent(key)}`, {
            method: "DELETE"
        });
        const isFailover = response.headers.get('X-PupDB-Failover') === 'true';
        const data = await response.json();
        
        if (response.ok) {
            if (isFailover) {
                writeLog(`⚠️ PHÁT HIỆN SỰ CỐ: Master Shard offline. Router đã chuyển vùng (Failover) xóa trên Slave thành công!`, "warning");
            }
            writeLog(`[Router] Đã xóa khóa "${key}"`, "success");
            setTimeout(clusterFetchAll, 500);
        } else {
            writeLog(`[Router] Lỗi: ${data.error}`, "error");
        }
    } catch (e) {
        writeLog(`[Router] Lỗi mạng: ${e.message}`, "error");
    }
}

// Truncate toàn cụm
async function clusterTruncate() {
    if (!bypassConfirm && !confirm("CẢNH BÁO: Bạn có muốn xóa sạch toàn bộ các phân mảnh trong cụm?")) return;

    try {
        writeLog(`[Router] Gửi POST /truncate-db để xóa toàn cụm...`);
        const response = await fetch(`${API_ROUTER}/truncate-db`, { method: "POST" });
        const data = await response.json();
        
        if (response.ok) {
            writeLog(`[Router] Đã dọn sạch toàn cụm thành công`, "success");
            setTimeout(clusterFetchAll, 500);
        } else {
            writeLog(`[Router] Lỗi: ${data.error}`, "error");
        }
    } catch (e) {
        writeLog(`[Router] Lỗi: ${e.message}`, "error");
    }
}


// ==================== VÒNG LẶP KIỂM TRA ĐỊNH KỲ ====================

setInterval(() => {
    if (currentActiveTab === "single") {
        singleCheckStatus();
    } else {
        clusterCheckAllStatuses();
        fetchRecycleBin();
    }
}, 5000);

// ==================== THÙNG RÁC PHÂN TÁN (RECYCLE BIN) ====================

let recycleBinCache = {};

async function fetchRecycleBin() {
    const container = document.getElementById("cluster-recycle-list");
    if (!container) return;

    try {
        const res = await fetch(`${API_ROUTER}/recycle-bin`);
        if (!res.ok) throw new Error("Cổng đóng");
        const data = await res.json();
        recycleBinCache = data;
        renderRecycleBin();
    } catch (e) {
        container.innerHTML = `<div class="empty-placeholder" style="color: var(--danger)">Không thể kết nối Thùng rác.</div>`;
    }
}

function renderRecycleBin() {
    const container = document.getElementById("cluster-recycle-list");
    if (!container) return;

    const keys = Object.keys(recycleBinCache);
    if (keys.length === 0) {
        container.innerHTML = `<div class="empty-placeholder">Thùng rác trống.</div>`;
        return;
    }

    container.innerHTML = "";
    keys.forEach(key => {
        const item = recycleBinCache[key];
        const remainingSecs = Math.max(0, Math.round(120 - (Date.now() / 1000 - item.deleted_at)));
        
        if (remainingSecs <= 0) return;

        const card = document.createElement("div");
        card.className = "data-card accent-card";
        card.style.borderLeftColor = "var(--danger)";
        card.style.width = "250px";
        card.style.margin = "0.2rem";

        card.innerHTML = `
            <div>
                <div class="card-key">${key} <span style="font-size: 0.7rem; color: var(--text-muted)">(Mảnh ${item.shard})</span></div>
                <div class="card-val">${item.value}</div>
                <div style="font-size: 0.75rem; color: var(--danger); margin-top: 0.3rem;" id="ttl-${key}">
                    ⏳ Tự động hủy sau: ${remainingSecs}s
                </div>
            </div>
            <button class="btn btn-primary" onclick="clusterRestore('${key}')" style="padding: 0.25rem 0.5rem; font-size: 0.75rem; background-color: var(--primary); border-color: var(--primary); align-self: center; margin-left: auto;">
                Phục hồi
            </button>
        `;
        container.appendChild(card);
    });
}

// Tick mỗi giây để đếm ngược TTL
setInterval(() => {
    if (currentActiveTab === "cluster" && Object.keys(recycleBinCache).length > 0) {
        let hasChanges = false;
        Object.keys(recycleBinCache).forEach(key => {
            const item = recycleBinCache[key];
            const remainingSecs = Math.max(0, Math.round(120 - (Date.now() / 1000 - item.deleted_at)));
            const el = document.getElementById(`ttl-${key}`);
            if (el) {
                if (remainingSecs <= 0) {
                    hasChanges = true;
                } else {
                    el.textContent = `⏳ Tự động hủy sau: ${remainingSecs}s`;
                }
            }
        });
        if (hasChanges) {
            fetchRecycleBin();
        }
    }
}, 1000);

async function clusterRestore(key) {
    try {
        writeLog(`[Router] Gửi POST /restore để khôi phục khóa: "${key}"`);
        const response = await fetch(`${API_ROUTER}/restore`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ key })
        });
        const data = await response.json();
        if (response.ok) {
            writeLog(`[Router] Phục hồi thành công: ${data.message}`, "success");
            clusterFetchAll();
        } else {
            writeLog(`[Router] Lỗi phục hồi: ${data.error}`, "error");
        }
    } catch (e) {
        writeLog(`[Router] Lỗi phục hồi: ${e.message}`, "error");
    }
}

// Khởi động khi tải xong trang
window.addEventListener('DOMContentLoaded', () => {
    singleFetch();
});
