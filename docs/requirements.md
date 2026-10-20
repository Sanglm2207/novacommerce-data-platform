# Yêu cầu hệ thống NovaCommerce Data Platform

## 1. Mục tiêu và phạm vi

NovaCommerce là dự án thực hành xây dựng nền tảng dữ liệu thương mại điện tử trên
PostgreSQL: tạo dữ liệu giao dịch, phân tích bằng SQL, đồng bộ staging, kiểm tra
chất lượng và nạp warehouse phục vụ báo cáo. Tài liệu mô tả implementation hiện
có trong repository, đến migration V006.

Hệ thống dùng một database training `novacommerce_oltp`, chia thành các schema:

| Schema | Vai trò |
| --- | --- |
| `public` | Khách hàng, sản phẩm, đơn hàng, dòng hàng và thanh toán nguồn. |
| `staging` | Trạng thái tổng hợp mới nhất của từng đơn hàng. |
| `etl` | Checkpoint, journal thay đổi và kết quả kiểm tra chất lượng. |
| `warehouse` | Dimensions, fact theo dòng hàng, lịch sử sản phẩm và hàng đợi refresh. |

Mô hình bảng và quan hệ được mô tả trong [erd.md](erd.md).

## 2. Nền tảng và cấu hình

- PostgreSQL 17; Python 3.12 là phiên bản dùng trong Docker ETL.
- Docker Compose chạy database, ETL và bộ kiểm tra tích hợp.
- Thư viện Python khai báo tại [requirements.txt](../requirements.txt): psycopg,
  Faker, python-dotenv, pytest và ruff. Ruff đã được khai báo nhưng workflow CI
  hiện tại chỉ chạy pytest.
- Các script đọc `.env` tại thư mục gốc. Biến môi trường có sẵn được ưu tiên hơn
  giá trị trong file theo cách gọi `load_dotenv()` hiện tại.

| Biến | Ý nghĩa |
| --- | --- |
| `POSTGRES_DB` | Phải là `novacommerce_oltp` để qua kiểm tra database training. |
| `POSTGRES_USER` | Tài khoản kết nối. |
| `POSTGRES_PASSWORD` | Mật khẩu kết nối; cấu hình riêng trong `.env`. |
| `POSTGRES_PORT` | Port kết nối; mặc định `5432`. |
| `POSTGRES_HOST` | Pipeline mặc định `127.0.0.1`; trong Compose dùng tên service. Seed luôn dùng `127.0.0.1`. |

Database local chỉ publish port trên loopback. `.env` được Git bỏ qua; dùng
[.env.example](../.env.example) làm mẫu cấu hình và thay thông tin theo môi trường.

## 3. Quy tắc dữ liệu và chỉ số

### Giao dịch nguồn

- Một khách hàng có thể chưa mua hàng hoặc có nhiều đơn hàng.
- Một đơn thuộc một khách hàng; một dòng hàng thuộc một đơn và một sản phẩm.
- Trong cùng đơn, mỗi sản phẩm chỉ có một dòng: unique `(order_id, product_id)`.
- Một đơn có thể có nhiều lần thanh toán, kể cả nhiều payment `SUCCESS`.
- Trạng thái order: `PENDING`, `CONFIRMED`, `COMPLETED`, `CANCELLED`.
- Trạng thái payment: `PENDING`, `SUCCESS`, `FAILED`, `REFUNDED`.
- `quantity > 0`, giá sản phẩm và giá dòng hàng không âm; `payments.amount > 0`.
- SQL không bắt buộc mỗi đơn có item/payment, không ràng buộc chuyển trạng thái
  và không đối chiếu số tiền thanh toán với giá trị đơn.

### Quy ước báo cáo

| Chỉ số | Định nghĩa |
| --- | --- |
| Đơn đủ điều kiện doanh thu | Order `COMPLETED` và tồn tại ít nhất một payment `SUCCESS`. |
| Giá trị dòng hàng | `quantity * unit_price`, dùng giá tại dòng hàng. |
| Giá trị đơn | Tổng giá trị các dòng hàng trong đơn. |
| Gross revenue | Tổng giá trị các đơn đủ điều kiện; chưa trừ hoàn tiền, chi phí hoặc thuế riêng. |
| Units sold | Tổng `quantity`, không phải số dòng hàng. |
| Average order value | Trung bình tổng tiền mỗi đơn đủ điều kiện. |
| Customer spending | Chi tiêu lịch sử, không phải CLV dự báo. Khách chưa có đơn đủ điều kiện có tổng và số đơn bằng 0, AOV là `NULL`. |
| Customer rank | `DENSE_RANK()` theo tổng chi tiêu giảm dần; bằng tiền có cùng hạng. |
| Revenue date | Ngày tạo order theo UTC; không phải ngày hoàn tất hay ngày nhận tiền. |
| DoD growth (%) | `100 * (daily_revenue - previous_day_revenue) / NULLIF(previous_day_revenue, 0)`. |
| Trung bình trượt 7 ngày | Ngày hiện tại và 6 ngày lịch trước đó; đầu chuỗi dùng số ngày hiện có. |

Dùng `EXISTS` để kiểm tra payment, tránh nhân đôi doanh thu khi có nhiều lần
thanh toán thành công. Gom item về một order trước khi tính số đơn và AOV.
Với fact ở grain dòng hàng, đếm đơn bằng `COUNT(DISTINCT order_id)`.

Báo cáo tăng trưởng bổ sung ngày không có doanh thu bằng 0 từ ngày order đầu
tiên đến cuối cùng. DoD là `NULL` khi không có ngày trước hoặc doanh thu trước
bằng 0. `dim_date` chỉ chứa ngày xuất hiện trong staging, không phải lịch đầy đủ.

## 4. Yêu cầu chức năng

| ID | Chức năng và hành vi hiện có | Mã nguồn |
| --- | --- | --- |
| FR-01 | Seed xác định bằng seed 42: 12.000 khách, 3.000 sản phẩm, 40.000 đơn; số item/payment phụ thuộc quá trình sinh dữ liệu. COPY và commit trong một transaction. Mặc định yêu cầu bảng rỗng; `--reset` xóa 5 bảng nguồn và reset identity. | `database/seeds/seed.py` |
| FR-02 | Báo cáo doanh thu ngày, top 10 sản phẩm theo doanh thu, chi tiêu khách hàng, tăng trưởng và execution plan. | `database/queries/01_*.sql` đến `05_*.sql` |
| FR-03 | Nạp order mới theo watermark ghép `(created_at, order_id)`, mặc định batch 5.000; upsert staging và checkpoint trong cùng transaction cho toàn lượt chạy. Có advisory lock và retry lỗi tạm thời. | `src/novacommerce/etl/orders_pipeline.py` |
| FR-04 | `reconcile --order-id` đọc lại một order và upsert staging mà không tiến checkpoint. | `src/novacommerce/etl/orders_pipeline.py` |
| FR-05 | Trigger trên orders/items/payments ghi journal. CDC lấy event chưa xử lý bằng `FOR UPDATE SKIP LOCKED`, gom order trùng trong batch và tính lại trạng thái hiện tại. Order đã bị xóa thì xóa staging. Ghi staging và xác nhận event commit cùng batch. | V003; `orders_cdc_pipeline.py` |
| FR-06 | Kiểm tra chất lượng nguồn/staging trên snapshot nhất quán, ghi audit run và từng check; ngưỡng tuổi event mặc định 300 giây. | V004; `validate_pipeline.py` |
| FR-07 | Quan sát audit gần nhất, chi tiết check và backlog/tuổi event CDC. | `database/queries/observe_pipeline.sql` |
| FR-08 | Full-scan upsert customer/product SCD1, date UTC và fact từng order item. Dùng REPEATABLE READ, advisory lock và một transaction; kiểm tra số đơn nguồn bằng staging trước khi nạp. | V005; `build_star_schema.py` |
| FR-09 | Bắt snapshot thay đổi sản phẩm; lưu lịch sử SCD2 riêng. Hàng đợi dirty key độc lập với journal CDC, hỗ trợ PRODUCT/ORDER/CUSTOMER. | V006 |
| FR-10 | Incremental warehouse xử lý product events và refresh jobs theo batch, mặc định 500. Từ chối chạy khi journal CDC còn pending; refresh fact bằng xóa/nạp lại các order bị ảnh hưởng. Cập nhật dữ liệu và marker trong transaction batch. | `incremental_warehouse.py` |
| FR-11 | Báo cáo warehouse theo ngày UTC, lọc completed + payment thành công, đếm distinct order. | `database/queries/06_warehouse_daily_revenue.sql` |

Tên migration đầy đủ nằm trong [database/migrations](../database/migrations).

## 5. Chất lượng dữ liệu và tiêu chí nghiệm thu

Validator tự động hiện có tám check; tất cả dùng ngưỡng số vi phạm bằng 0:

| Check | Severity | Ý nghĩa |
| --- | --- | --- |
| `missing_staging_orders` | ERROR | Order nguồn thiếu trong staging. |
| `orphan_staging_orders` | ERROR | Staging không còn order nguồn. |
| `invalid_staging_values` | ERROR | Tổng số lượng hoặc tổng tiền âm. |
| `mismatched_order_status` | ERROR | Trạng thái staging lệch nguồn. |
| `mismatched_item_aggregates` | ERROR | Tổng số lượng/tiền lệch nguồn. |
| `mismatched_payment_flags` | ERROR | Cờ payment thành công lệch nguồn. |
| `unresolved_aged_events` | ERROR | Event chưa xử lý vượt tuổi cấu hình. |
| `multiple_successful_payments` | WARNING | Order có nhiều payment thành công. |

Validator trả exit code `0` khi không có ERROR thất bại, `1` khi check ERROR thất
bại, `2` khi có exception trong bước collect/finish. Exception lúc tạo audit run
ban đầu vẫn có thể thoát trực tiếp. WARNING không làm run thất bại;
`checks_failed` chỉ đếm ERROR. Audit ghi vào `etl`, không sửa dữ liệu nghiệp vụ.

Sau khi ngừng phát sinh giao dịch và xử lý hết queue, yêu cầu nghiệm thu là:

1. Không thiếu hoặc lệch dữ liệu staging theo `tests/validate_etl.sql` và
   `tests/validate_cdc.sql`; CDC không còn event pending.
2. Fact không thiếu, không orphan, không lệch số tiền/trạng thái theo
   `tests/validate_day09.sql` sau full load.
3. Lịch sử không chồng lấn, mỗi product nguồn có một phiên bản current, queue
   product/refresh rỗng và fact không stale theo `tests/validate_day10.sql`.
4. Chạy lại khi không có thay đổi không tạo thêm dòng nghiệp vụ trùng; không
   yêu cầu identity sequence hoặc timestamp audit giữ nguyên.
5. Lỗi được chủ động chèn phải rollback cả dữ liệu và checkpoint/marker trong
   phạm vi transaction tương ứng. Các batch đã commit trước đó được giữ lại.

## 6. Trình tự vận hành

Các lệnh bên dưới chạy từ thư mục gốc, trong Python environment của dự án:

```sh
python -m pip install -r requirements.txt
cp .env.example .env
# Chỉnh .env trước khi khởi động database.
docker compose --env-file .env -f docker/compose.yaml up -d
```

Với database mới, áp dụng V001 đến V006 theo thứ tự, rồi seed. Compose local
không tự mount migrations; có thể dùng container PostgreSQL để chạy từng file:

```sh
for migration in database/migrations/V*.sql; do
  docker exec -i novacommerce-postgres sh -c \
    'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
    < "$migration" || exit 1
done
python database/seeds/seed.py
```

V001 tạo bảng không có `IF NOT EXISTS`: chỉ dùng vòng lặp trên cho database mới,
không chạy lại toàn bộ migrations trên database đang có dữ liệu.

Nếu dữ liệu nguồn đã tồn tại trước V003, phải bootstrap journal một lần để CDC
nạp được toàn bộ order; migration chỉ chứa đoạn backfill dưới dạng comment:

```sql
INSERT INTO etl.order_changes (order_id, source_table, operation)
SELECT order_id, 'bootstrap', 'BACKFILL'
FROM public.orders;
```

Sau đó chạy tuần tự, tránh ghi nguồn đồng thời trong lượt kiểm tra nghiệm thu:

```sh
python src/novacommerce/etl/orders_cdc_pipeline.py
python src/novacommerce/etl/validate_pipeline.py --max-pending-age-seconds 300
python src/novacommerce/warehouse/build_star_schema.py
python src/novacommerce/warehouse/incremental_warehouse.py
```

Những lần đồng bộ tiếp theo chạy CDC → quality → incremental warehouse. Lệnh
full build dành cho khởi tạo/refresh toàn bộ, không cần chạy trước mỗi batch.
Có thể chạy từng file `tests/validate*.sql` bằng `psql -v ON_ERROR_STOP=1 -f ...`;
những file này trả số vi phạm để kiểm tra, không tự raise khi số đó lớn hơn 0.

## 7. Kiểm thử và CI

```sh
PYTHONPATH=src python -m pytest -q tests/test_day08_unit.py

docker compose -p novacommerce-ci -f docker/compose.ci.yaml up \
  --build --abort-on-container-exit --exit-code-from tests tests
docker compose -p novacommerce-ci -f docker/compose.ci.yaml down -v --remove-orphans
```

Compose CI dùng PostgreSQL 17 riêng với tmpfs và migrations tự khởi tạo. Unit
tests kiểm tra threshold, severity, lock SQL và upsert; integration tests kiểm
tra CDC, đổi trạng thái/payment, chạy lại, rollback và quality audit. Chạy
integration bằng Compose để tránh ghi fixture vào database làm việc.

Workflow `.github/workflows/etl-ci.yml` chạy hai file test Day 08 trên push,
pull request và workflow dispatch. Các SQL validation Day 09/10 chưa được gọi
tự động bởi workflow; không suy ra warehouse đã được kiểm thử chỉ từ CI xanh.

## 8. Giới hạn hiện tại

- CDC ở đây là trigger journal trong cùng PostgreSQL, không phải logical
  replication/WAL connector. Event giữ dirty key; staging lưu trạng thái mới
  nhất, không phải lịch sử đầy đủ của từng thay đổi order.
- Watermark tạo đơn không bắt được update/delete hoặc insert backdated sau
  checkpoint. Dùng journal CDC cho các trường hợp đó. V003 hiện chỉ enqueue
  `NEW.order_id` khi item/payment chuyển sang đơn khác; cần reconcile cả đơn cũ
  nếu nghiệp vụ cho phép thao tác này. Queue warehouse V006 có bắt cả hai ID.
- Code hiện đọc `last_created_at` trực tiếp dù V002 khởi tạo `-infinity`;
  psycopg có thể báo `timestamp too small` ở lần chạy watermark đầu tiên.
- Product history khởi tạo `valid_from = '-infinity'`, nhưng loader đọc lại nó
  bằng datetime khi xử lý sự kiện sau; đây cũng là điểm cần xử lý để tránh lỗi
  tương tự. Tài liệu không coi các đường chạy này đã được sửa.
- SCD2 phản ánh lịch sử quan sát từ khi capture; baseline đầu tiên không tái
  dựng được lịch sử trước đó. Fact vẫn tham chiếu SCD1 `dim_product`, không có
  `product_version_key` hay phép nối as-of tự động.
- Trigger customer/product warehouse không bắt DELETE; xóa nguồn còn bị các
  FK OLTP hạn chế. Full build không tự loại fact đã bị xóa khỏi nguồn; incremental
  chỉ làm sạch theo order đã được enqueue.
- Khóa advisory của full build, incremental warehouse và watermark là khác nhau;
  không có orchestration chung bảo đảm tất cả pipeline chạy loại trừ lẫn nhau.
  Điều kiện queue CDC rỗng là kiểm tra tại thời điểm chạy, không đóng băng nguồn.
- `seed.py --reset` chỉ reset 5 bảng nguồn, không reset staging, journal hay
  warehouse. TRUNCATE không phát row-level DELETE trigger; không dùng thao tác
  này như một cách đồng bộ lại toàn bộ platform.
- Chưa có scheduler, dashboard, chính sách purge queue/audit, SLA thời gian chạy
  hoặc mục tiêu throughput được kiểm chứng. Không có bảo đảm exactly-once toàn
  hệ thống; cơ chế transaction/upsert áp dụng trong phạm vi từng loader.
