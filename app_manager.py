import os
import re
import json
import socket
import threading
from urllib.parse import urlparse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from dotenv import load_dotenv
import boto3
from botocore.config import Config

# Cấu hình đường dẫn thư mục
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
RESULT_DIR = os.path.join(PROJECT_ROOT, "result")

# Tải cấu hình từ file .env trong cùng thư mục audio17k/
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path=env_path)

PORT = int(os.getenv("PORT", 8080))
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "audio-17k")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "").rstrip("/")

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4'),
        region_name="auto"
    )

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

def extract_chapter_number(filename):
    """Trích xuất số chương an toàn từ tên file để sắp xếp, tránh lỗi regex group."""
    match = re.search(r'\d+', filename)
    return int(match.group(0)) if match else 999999

def scan_local_mp3():
    """Chỉ đọc các file .mp3 từ thư mục result ngoài, bỏ qua và giữ nguyên toàn bộ file .wav."""
    chapters = []
    if os.path.exists(RESULT_DIR):
        for f in os.listdir(RESULT_DIR):
            if f.lower().endswith('.mp3'):
                num = extract_chapter_number(f)
                chapters.append({
                    "id": num,
                    "title": f"Chương {num}",
                    "filename": f,
                    "url": f"/stream_audio/{f}"
                })
    chapters.sort(key=lambda x: x["id"])
    return chapters

sync_state = {"running": False, "message": "Sẵn sàng", "current": 0, "total": 0}

def sync_worker():
    global sync_state
    sync_state["running"] = True
    sync_state["message"] = "Đang kiểm tra danh sách file trên Cloudflare R2..."
    
    try:
        if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME]):
            raise ValueError("Thiếu thông tin cấu hình Cloudflare R2 trong file .env!")

        s3 = get_s3_client()
        local_files = [f for f in os.listdir(RESULT_DIR) if f.lower().endswith('.mp3')]
        sync_state["total"] = len(local_files)
        sync_state["current"] = 0

        # Lấy danh sách file đã tồn tại trên R2 để tránh upload trùng lặp
        remote_files = set()
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=R2_BUCKET_NAME):
            if 'Contents' in page:
                for obj in page['Contents']:
                    remote_files.add(obj['Key'])

        chapters_json = []
        sorted_files = sorted(local_files, key=extract_chapter_number)

        for idx, filename in enumerate(sorted_files):
            file_path = os.path.join(RESULT_DIR, filename)
            chap_num = extract_chapter_number(filename)

            if filename not in remote_files:
                sync_state["message"] = f"Đang tải lên: {filename} ({idx+1}/{len(local_files)})"
                s3.upload_file(
                    file_path,
                    R2_BUCKET_NAME,
                    filename,
                    ExtraArgs={'ContentType': 'audio/mpeg'}
                )
            
            chapters_json.append({
                "id": chap_num,
                "title": f"Chương {chap_num}",
                "url": f"{R2_PUBLIC_URL}/{filename}"
            })
            sync_state["current"] = idx + 1

        # Ghi file chapters.json ra thư mục audio17k để dùng cho GitHub Pages
        json_path = os.path.join(BASE_DIR, "chapters.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(chapters_json, f, ensure_ascii=False, indent=2)

        sync_state["message"] = f"Hoàn tất! Đã đồng bộ {len(local_files)} chương MP3 lên R2."
    except Exception as e:
        sync_state["message"] = f"Lỗi đồng bộ: {str(e)}"
    finally:
        sync_state["running"] = False

class AudioHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        clean_path = parsed.path.strip('/')

        if clean_path == 'api/chapters':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(scan_local_mp3()).encode('utf-8'))
        elif clean_path == 'api/sync_status':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(sync_state).encode('utf-8'))
        elif clean_path.startswith('stream_audio/'):
            filename = clean_path.replace('stream_audio/', '')
            file_path = os.path.join(RESULT_DIR, filename)
            if os.path.exists(file_path):
                self.send_response(200)
                self.send_header('Content-Type', 'audio/mpeg')
                self.send_header('Content-Length', str(os.path.getsize(file_path)))
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                with open(file_path, 'rb') as f:
                    self.wfile.write(f.read())
            else:
                self.send_error(404, "File not found")
        else:
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        clean_path = parsed.path.strip('/')

        if clean_path == 'api/trigger_sync':
            if not sync_state["running"]:
                threading.Thread(target=sync_worker, daemon=True).start()
                res = {"status": "started"}
            else:
                res = {"status": "in_progress"}
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(res).encode('utf-8'))
        else:
            self.send_error(404, "Endpoint not found")

if __name__ == "__main__":
    ip = get_local_ip()
    print("=" * 60)
    print("📻 TRÌNH QUẢN LÝ VÀ PHÁT AUDIO NOVEL (AUDIO17K)")
    print(f"📁 Thư mục App:    {BASE_DIR}")
    print(f"📁 Thư mục Audio:  {RESULT_DIR}")
    print(f"👉 Local máy tính: http://localhost:{PORT}")
    print(f"👉 Điện thoại:     http://{ip}:{PORT}")
    print("=" * 60)
    server = ThreadingHTTPServer(('0.0.0.0', PORT), AudioHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nĐã dừng server.")