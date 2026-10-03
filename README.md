# AI Guru Season 3 - Medical Corpus Crawler & Dataset Pipeline

Pipeline thu thập dữ liệu và xử lý corpus y sinh đa ngôn ngữ (Tiếng Việt, Tiếng Anh, Tiếng Trung) cho cuộc thi **MedKB - AI Guru Season 3** (ViBioMIR Dataset).

## 1. Tổng quan dữ liệu đã thu thập

Tính đến hiện tại, hệ thống đã thu thập và lưu trữ thành công:
* **Tổng số trang đã lưu trữ:** **3,614,216 / 4,394,718 trang (82.2%)**
* **Dung lượng cơ sở dữ liệu thô:** **~48.5 GB** (lưu dưới dạng SQLite nén `zstandard` level 6)
* **Các domain chính đã hoàn thành:**
  * `www.120ask.com`: 918,084 trang (100%)
  * `www.familydoctor.com.cn`: 445,098 trang (100%)
  * `www.cnkang.com`: 926,989 trang (>96%)
  * `laodong.vn`: 21,745 trang (100%)
  * `vov.vn`: 2,131 trang (100%)
  * `baolangson.vn`: 1,981 trang (100%)
  * `tamanhhospital.vn`: 1,381 trang (100%)
  * `nhathuoclongchau.com.vn`: 53,618 trang (direct & archives)
  * `zysjonline.com`: 27,494 trang (Wayback & Common Crawl WARC)

## 2. Cấu trúc thư mục

```text
├── crawler/                  # Các kịch bản cào dữ liệu
│   ├── crawl.py              # Crawler async chính theo nhóm domain và rate limit
│   ├── crawl_missing.py      # Crawler chuyên dụng xử lý các trang bị chặn (Cloudflare / Cookie challenge)
│   ├── wayback.py            # Khôi phục trang lịch sử từ Wayback Machine
│   ├── cc.py                 # Tải và trích xuất WARC record trực tiếp từ AWS Common Crawl
│   ├── alt.py                # Thu thập từ các domain thay thế (Pharmacity blog)
│   ├── zysj_books.py         # Mapping và căn chỉnh sách từ domain zysjonline sang zysj.com.cn
│   ├── coverage.py           # Phân tích độ phủ dữ liệu và xuất báo cáo missing
│   └── progress.py           # Theo dõi tiến độ thời gian thực
├── data/                     # Dữ liệu gốc và truy vấn cuộc thi
│   ├── links_corpus.parquet  # Danh sách toàn bộ 4.39M URL mục tiêu
│   └── query.parquet         # Danh sách 1,200 câu truy vấn đánh giá
├── missing_pages.csv         # Danh sách các trang thiếu kèm phân loại lý do
├── missing_pages.md          # Báo cáo chi tiết các trang chưa tải được
└── crawl/                    # Dữ liệu cào (được lưu cục bộ hoặc trên Hugging Face)
    ├── db/                   # SQLite databases cào trực tiếp
    ├── db_wayback/           # SQLite databases từ Wayback Machine
    ├── db_cc/                # SQLite databases từ Common Crawl
    └── coverage.csv          # Bảng thống kê độ phủ mới nhất
```

## 3. Hướng dẫn sử dụng

### Cài đặt môi trường
```bash
pip install -r requirements.txt # hoặc: pip install curl_cffi aiohttp zstandard pandas pyarrow
```

### Chạy kiểm tra tiến độ
```bash
python crawler/progress.py
python crawler/coverage.py
```

### Chạy các crawler
```bash
# Cào các domain còn thiếu
python crawler/crawl_missing.py

# Theo dõi độ phủ
python crawler/coverage.py --export
```
