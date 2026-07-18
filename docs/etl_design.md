# ETL Pipeline Design: E-Commerce Orders

## 1. Overview
Pipeline ini memproses data transaksi e-commerce harian (`raw_orders.csv` dan `raw_products.csv`) yang masih kotor — mengandung duplikasi order, format tanggal yang tidak konsisten, penulisan kota/channel yang tidak seragam, serta nilai `total_harga` yang hilang atau negatif. Pipeline membersihkan data tersebut lalu menghasilkan tabel ringkasan penjualan yang siap dianalisis, disimpan di DuckDB.

## 2. Extract
- **Sumber**: Dua file CSV lokal — `raw_orders.csv` (data transaksi) dan `raw_products.csv` (katalog produk & harga satuan).
- **Format**: CSV, dibaca langsung dengan `pandas.read_csv()`.
- **Volume**: ~130 baris order dan 10 baris produk (skala kecil/contoh) — cukup untuk didorong penuh ke memory tanpa perlu chunking atau batching.

## 3. Transform
- **Langkah 1 — Hapus duplikat (`drop_duplicates` pada `order_id`)**: beberapa `order_id` muncul lebih dari sekali dengan data identik. Ini kemungkinan besar akibat re-export atau double submission dari sumber, dan akan menggelembungkan angka revenue/quantity kalau tidak dibersihkan.
- **Langkah 2 — Normalisasi tanggal (`tanggal_order`)**: kolom ini punya 4 format berbeda dalam file yang sama (`DD/MM/YYYY`, `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, `"Mon DD, YYYY"`). Fungsi parsing mencoba tiap format secara berurutan, lalu fallback ke inferensi pandas kalau tidak cocok. Tanpa ini, agregasi berbasis waktu (misalnya per bulan) akan salah atau gagal total.
- **Langkah 3 — Normalisasi teks (`kota`, `channel`, `status`, `kategori`)**: penulisan seperti `SURABAYA` / `surabaya` / `Surabaya` atau `MARKETPLACE` / `Marketplace` dianggap entitas berbeda oleh `GROUP BY` kalau tidak diseragamkan. Kota di-title-case, channel/status di-lowercase.
- **Langkah 4 — Perbaikan `total_harga` yang hilang/negatif**: nilai kosong atau negatif dianggap error input, lalu dihitung ulang dari `quantity × harga_satuan` (join ke `cleaned_products`). Ini asumsi desain yang perlu divalidasi ke pemilik data — bisa saja nilai negatif sebenarnya dimaksudkan sebagai refund, bukan error.

## 4. Load
- **Tujuan**: DuckDB lokal (file `sales.duckdb`), sebagai OLAP store ringan untuk query analitik cepat tanpa perlu server database terpisah.
- **Format output**: Tiga tabel — `cleaned_orders` (data order yang sudah bersih), `sales_by_category_city` (revenue & volume per kategori produk × kota, hanya order berstatus `completed`), dan `sales_by_channel_status` (jumlah order & revenue per channel × status).

## 5. Orchestration
- **Tool**: Dagster (bukan Airflow) — dipilih karena model asset-based-nya cocok untuk pipeline berbasis DataFrame/tabel seperti ini, dengan lineage graph otomatis dan dukungan native ke DuckDB (`dagster-duckdb`).
- **Bahasa**: Python untuk transformasi (`pandas`), SQL untuk agregasi di layer marts (dieksekusi via DuckDB connection).
- **Schedule**: belum dijadwalkan — saat ini dijalankan manual via "Materialize all" di Dagster UI. Untuk produksi, bisa ditambahkan Dagster schedule harian (misalnya jam 02:00 setelah data harian tersedia) menggunakan `@schedule` atau sensor berbasis kemunculan file baru.
- **DAG flow**:
  ```
  raw_orders ─┐
              ├─→ cleaned_orders ─→ orders_in_duckdb ─┬─→ sales_by_category_city
  raw_products┘        ↑                              └─→ sales_by_channel_status
              cleaned_products
  ```
  (raw → cleaned → marts, sesuai 3 grup asset di project: `raw`, `cleaned`, `marts`)

## 6. Error Handling
- **Skenario 1 — Tanggal tidak bisa di-parse**: fungsi `parse_date` mencatat log `WARNING` berisi jumlah baris yang gagal, mengembalikan `NaT` (bukan meng-crash seluruh run) sehingga baris lain tetap bisa diproses. Baris `NaT` perlu ditinjau manual sebelum dipakai di analisis berbasis waktu.
- **Skenario 2 — `product_id` di orders tidak ditemukan di katalog produk**: saat `total_harga` dihitung ulang lewat lookup harga, `product_id` yang tidak match akan menghasilkan `NaN` alih-alih error keras. Ini perlu ditambah validasi eksplisit (assign check / Dagster Asset Check) agar tidak lolos diam-diam ke tabel marts.
- **Skenario 3 (rekomendasi tambahan)** — kegagalan resource DuckDB (file terkunci/corrupt): karena `orders_in_duckdb` memakai `CREATE OR REPLACE TABLE`, retry otomatis aman dilakukan (idempoten) tanpa duplikasi data.

## 7. Monitoring
- **Bagaimana cara tahu pipeline sukses?** Dagster UI menampilkan status run (`Success` / `Failed`) per asset dan keseluruhan job, lengkap dengan durasi eksekusi dan log event (`ASSET_MATERIALIZATION`, `STEP_SUCCESS`, dst). Untuk otomatisasi, bisa ditambah sensor yang mengirim notifikasi (Slack/email) saat run gagal.
- **Bagaimana cara tahu data berkualitas?** Saat ini kualitas dicek lewat log info di `cleaned_orders` (jumlah duplikat yang dihapus, jumlah `total_harga` yang diperbaiki, jumlah tanggal gagal parse) — ini baru observability dasar, belum validasi formal. Rekomendasi ke depan: tambahkan **Dagster Asset Checks** untuk aturan eksplisit, misalnya "tidak boleh ada `total_harga` negatif di `cleaned_orders`" atau "`order_count` di marts harus konsisten dengan jumlah baris di `cleaned_orders`".
