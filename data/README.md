---
pretty_name: Medical Query & Link Corpus
language:
  - vi
  - en
  - zh
multilinguality:
  - multilingual
task_categories:
  - text-retrieval
size_categories:
  - 1M<n<10M
tags:
  - medical
  - information-retrieval
  - vietnamese
  - english
  - chinese
configs:
  - config_name: query
    data_files:
      - split: train
        path: query.parquet
  - config_name: corpus
    data_files:
      - split: train
        path: links_corpus.parquet
---

<div align="center">
  <table align="center" border="0" cellpadding="0" cellspacing="0">
    <tr>
      <td align="center"><a href="https://medkb.tinix.ai/"><img src="figures/MedKB.png" alt="MedKB" width="180"></a></td>
      <td align="center"><a href="https://leaderboard.aiguru.com.vn/"><img src="figures/AI%20Guru.jpg" alt="AI Guru" width="180"></a></td>
    </tr>
  </table>
  <h1> ViBioMIR: A Vietnamese-Centric Multilingual Biomedical Information Retrieval Dataset </h1>
  <h1>MedKB - AI Guru</h1>
  <p>
    <a href="https://www.facebook.com/tinix.vn/"><img src="https://img.shields.io/badge/Facebook-TiniX-1877F2?logo=facebook&logoColor=white" alt="TiniX Facebook"></a>
    <a href="https://medkb.tinix.ai/"><img src="https://img.shields.io/badge/Website-MedKB-00A98F?logo=googlechrome&logoColor=white" alt="MedKB Website"></a>
    <a href="https://www.facebook.com/aiguru.vn/"><img src="https://img.shields.io/badge/Facebook-AI%20Guru-1877F2?logo=facebook&logoColor=white" alt="AI Guru Facebook"></a>
    <a href="https://leaderboard.aiguru.com.vn/"><img src="https://img.shields.io/badge/AI%20Guru-Leaderboard-F5A623?logo=googleanalytics&logoColor=white" alt="AI Guru Leaderboard"></a>
    <a href="https://huggingface.co/tinixai"><img src="https://img.shields.io/badge/Hugging%20Face-TiniX%20AI-FFD21E?logo=huggingface&logoColor=black" alt="TiniX AI on Hugging Face"></a>
  </p>
</div>

## 1. Giới thiệu

Tri thức y sinh hiện nay được phân bố không đồng đều giữa các ngôn ngữ. Mặc dù nguồn dữ liệu tiếng Việt ngày càng được mở rộng, nguồn này vẫn còn hạn chế về chiều sâu và tính chuyên môn. Trong khi đó, tiếng Anh và tiếng Trung sở hữu kho tài nguyên y sinh đồ sộ. Sự chênh lệch này tạo ra một **khoảng cách thông tin y tế** đáng kể, khiến người dùng tiếng Việt gặp nhiều trở ngại khi tiếp cận các bằng chứng y học được công bố bằng ngôn ngữ khác.

Xuất phát từ thực trạng đó, bài toán hướng tới xây dựng các hệ thống AI có khả năng truy hồi thông tin y sinh đa ngôn ngữ từ nguồn tiếng Việt, tiếng Anh và tiếng Trung. Với mỗi truy vấn bằng tiếng Việt, hệ thống cần xác định các tài liệu và nội dung liên quan mà không phụ thuộc vào ngôn ngữ của nguồn. Ban Tổ chức cung cấp hoặc chỉ định các nguồn dữ liệu để các đội chủ động thu thập, xử lý và xây dựng cơ sở tri thức phục vụ truy hồi.

## 2. Tổng quan và thống kê

### Tổng quan query và corpus

| Chỉ số | Giá trị |
|:--|--:|
| Số query | 1.200 |
| Số từ trung vị | 18 |
| Số từ P95 | 84 |
| Số từ min–max | 5–283 |
| Tổng số liên kết | 4.420.561 |
| Khoảng ID | 1–4.420.561 |


### Phân bố số lượng token theo thể loại

![Phân bố số lượng token theo thể loại](figures/tokens_by_category_without_books.png)

### Phân bố số lượng bản ghi theo năm

![Phân bố số lượng bản ghi theo năm](figures/records_by_year_without_books.png)

### Phân bố độ dài truy vấn

![Phân bố độ dài truy vấn](figures/01_query_length_distribution.png)

## 3. Data Schema

### `query.parquet`

| Trường | Kiểu dữ liệu | Mô tả |
|:--|:--|:--|
| `id` | `integer` | Mã định danh của truy vấn |
| `query` | `string` | Nội dung truy vấn y tế bằng tiếng Việt |

Ví dụ:

```json
{
  "id": 1,
  "query": "<string>"
}
```

### `links_corpus.parquet`

| Trường | Kiểu dữ liệu | Mô tả |
|:--|:--|:--|
| `id` | `integer` | Mã định danh của tài liệu trong corpus |
| `url` | `string` | URL của tài liệu |

Ví dụ:

```json
{
  "id": 1,
  "url": "https://example.com/article"
}
```

## 4. Potential Use Cases

- Đánh giá hệ thống truy hồi thông tin y tế với truy vấn tiếng Việt.
- Xây dựng corpus tài liệu y sinh đa ngôn ngữ Việt–Anh–Trung.
- Phân loại intent của câu hỏi y tế.
- Phân tích độ dài, cấu trúc và mức độ phức tạp của truy vấn.
- Xây dựng các tập đánh giá theo intent, độ dài hoặc đặc trưng câu hỏi.
- Huấn luyện hoặc đánh giá mô hình embedding và semantic search.
- Nghiên cứu truy hồi xuyên ngôn ngữ giữa tiếng Việt, tiếng Anh và tiếng Trung.

## 5. Quickstart

```python
import pyarrow.parquet as pq

queries = pq.read_table("query.parquet")
corpus = pq.read_table("links_corpus.parquet")

print(queries.schema)
print(queries.slice(0, 5).to_pylist())

print(corpus.schema)
print(corpus.slice(0, 5).to_pylist())
```

## 6. License & Citation

Bộ dữ liệu được phát hành theo giấy phép [Creative Commons Attribution Non-Commercial 4.0 International (CC BY-NC 4.0)](https://creativecommons.org/licenses/by-nc/4.0/).

Theo giấy phép này, người dùng có thể chia sẻ và điều chỉnh bộ dữ liệu cho mục đích phi thương mại, với điều kiện ghi công phù hợp. Vui lòng kiểm tra điều khoản giấy phép và điều khoản của các nguồn dữ liệu liên quan trước khi sử dụng trong sản phẩm, thương mại hóa hoặc môi trường production.

Nếu sử dụng bộ dữ liệu này, vui lòng trích dẫn:

```bibtex
@online{medkb,
  title   = {MedKB -- Medical Knowledge Base},
  url     = {https://medkb.tinix.ai/},
  urldate = {2026-09-29}
}
```
