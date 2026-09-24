# Business Context Analysis - Home Credit Dataset

## 1. Mục tiêu kinh doanh
Dataset này thuộc lĩnh vực tín dụng tiêu dùng của Home Credit. Mục tiêu chính là đánh giá rủi ro tín dụng và dự đoán khả năng khách hàng gặp khó khăn trong việc thanh toán khoản vay.

- `TARGET = 1`: khách hàng có vấn đề thanh toán (payment difficulties)
- `TARGET = 0`: khách hàng còn lại

=> Đây là bài toán credit risk / default prediction.

## 2. Bối cảnh nghiệp vụ của dữ liệu
Home Credit cung cấp khoản vay cho khách hàng có hồ sơ tín dụng chưa hoàn chỉnh hoặc chưa đủ điều kiện với các tổ chức tín dụng truyền thống. Vì vậy, dữ liệu chứa nhiều tín hiệu để đánh giá khả năng trả nợ của khách hàng.

### Ý nghĩa kinh doanh của dữ liệu
- Dữ liệu không chỉ mô tả khách hàng hiện tại, mà còn phản ánh lịch sử vay, lịch sử tín dụng và hành vi thanh toán trước đó.
- Mục tiêu là xây dựng một hệ thống quyết định tín dụng tốt hơn, giảm rủi ro tổn thất và hỗ trợ phê duyệt vay hợp lý.

## 3. Business glossary theo bảng

### 3.1 Bảng application_{train|test}.csv
Đây là bảng trung tâm của dataset.

**Grain:** 1 dòng = 1 đơn xin vay hiện tại của 1 khách hàng.

**Business meaning:**
- Chứa thông tin hồ sơ khách hàng và thông tin khoản vay hiện tại.
- Là bảng chính dùng để phân tích và xây dựng mô hình dự đoán `TARGET`.

**Các cột chính:**
- `SK_ID_CURR`: ID duy nhất của khách hàng / khoản vay trong sample.
- `TARGET`: biến mục tiêu, thể hiện khách hàng có gặp khó khăn thanh toán hay không.
- `NAME_CONTRACT_TYPE`: loại hợp đồng vay (cash loan, revolving, v.v.).
- `AMT_INCOME_TOTAL`: tổng thu nhập của khách hàng.
- `AMT_CREDIT`: số tiền vay.
- `AMT_ANNUITY`: khoản thanh toán định kỳ.
- `AMT_GOODS_PRICE`: giá trị hàng hóa / tài sản liên quan đến khoản vay.
- `DAYS_BIRTH`: tuổi của khách hàng tính theo số ngày so với thời điểm ứng tuyển.
- `DAYS_EMPLOYED`: số ngày kể từ khi bắt đầu công việc hiện tại.
- `CODE_GENDER`, `NAME_FAMILY_STATUS`, `NAME_EDUCATION_TYPE`: thông tin nhân khẩu học và xã hội.
- `REGION_*`: thông tin vùng địa lý và mức độ phát triển khu vực.
- `EXT_SOURCE_1`, `EXT_SOURCE_2`, `EXT_SOURCE_3`: điểm tín dụng từ các nguồn dữ liệu bên ngoài.

### 3.2 Bảng bureau.csv
**Grain:** 1 dòng = 1 khoản tín dụng liên quan của khách hàng trong Credit Bureau.

**Business meaning:**
- Đại diện cho lịch sử tín dụng của khách hàng tại các tổ chức tín dụng khác.
- Giúp đánh giá mức độ tín nhiệm của khách hàng trước khi phê duyệt khoản vay mới.

**Các cột chính:**
- `SK_ID_CURR`: khách hàng trong sample.
- `CREDIT_ACTIVE`: tình trạng khoản tín dụng đang hoạt động hay đóng.
- `DAYS_CREDIT`: thời điểm khách hàng xin khoản tín dụng này so với thời điểm hiện tại.
- `AMT_CREDIT_SUM`: tổng số tiền tín dụng.
- `AMT_CREDIT_MAX_OVERDUE`: số nợ quá hạn tối đa.
- `CREDIT_TYPE`: loại hình tín dụng.

### 3.3 Bảng bureau_balance.csv
**Grain:** 1 dòng = 1 tháng trạng thái của một khoản tín dụng trong bureau.

**Business meaning:**
- Theo dõi tình trạng nợ theo từng tháng.
- Giúp nhận diện xu hướng thanh toán và chất lượng tín dụng theo thời gian.

**Các cột chính:**
- `SK_BUREAU_ID`: ID khoản tín dụng trong bureau.
- `MONTHS_BALANCE`: tháng tương đối so với thời điểm ứng tuyển.
- `STATUS`: trạng thái khoản vay trong tháng đó.

### 3.4 Bảng previous_application.csv
**Grain:** 1 dòng = 1 đơn xin vay trước đó của cùng khách hàng.

**Business meaning:**
- Cho thấy lịch sử yêu cầu vay trong hệ thống Home Credit.
- Dùng để đánh giá hành vi xin vay trước đó và mức độ tín nhiệm của khách hàng.

**Các cột chính:**
- `SK_ID_PREV`: ID khoản vay trước đó.
- `SK_ID_CURR`: khách hàng hiện tại.
- `AMT_APPLICATION`, `AMT_CREDIT`: số tiền ứng trước và số tiền cuối cùng được duyệt.
- `NAME_CONTRACT_STATUS`: trạng thái phê duyệt / hủy / từ chối.
- `DAYS_DECISION`: thời điểm quyết định xin vay so với thời điểm ứng tuyển hiện tại.
- `CODE_REJECT_REASON`: lý do từ chối.

### 3.5 Bảng installments_payments.csv
**Grain:** 1 dòng = 1 kỳ thanh toán của một khoản vay trước đó.

**Business meaning:**
- Là nguồn dữ liệu quan trọng để đánh giá hành vi trả nợ theo từng kỳ.
- Cho thấy khách hàng có thanh toán đúng hạn hay không.

**Các cột chính:**
- `SK_ID_PREV`: khoản vay trước đó.
- `NUM_INSTALMENT_NUMBER`: số kỳ thanh toán.
- `DAYS_INSTALMENT`: ngày dự kiến thanh toán.
- `DAYS_ENTRY_PAYMENT`: ngày thực tế thanh toán.
- `AMT_INSTALMENT`: số tiền phải thanh toán mỗi kỳ.
- `AMT_PAYMENT`: số tiền khách hàng thực tế thanh toán.

### 3.6 Bảng POS_CASH_balance.csv
**Grain:** 1 dòng = 1 tháng trạng thái của khoản vay POS/CASH trước đó.

**Business meaning:**
- Cho biết cách khách hàng sử dụng và trả nợ khoản vay POS/CASH trong các tháng trước.
- Các cột `SK_DPD` và `SK_DPD_DEF` rất quan trọng để nhận diện quá hạn.

**Các cột chính:**
- `SK_ID_PREV`: khoản vay trước đó.
- `MONTHS_BALANCE`: tháng tương đối so với thời điểm ứng tuyển.
- `CNT_INSTALMENT`, `CNT_INSTALMENT_FUTURE`: số kỳ đã thanh toán và số kỳ còn lại.
- `SK_DPD`, `SK_DPD_DEF`: số ngày quá hạn.

### 3.7 Bảng credit_card_balance.csv
**Grain:** 1 dòng = 1 tháng trạng thái của thẻ tín dụng trước đó.

**Business meaning:**
- Miêu tả hành vi sử dụng thẻ tín dụng của khách hàng.
- Dùng để đánh giá mức độ phụ thuộc vào tín dụng và khả năng quản lý nợ.

**Các cột chính:**
- `SK_ID_PREV`: khoản vay/thẻ tín dụng trước đó.
- `MONTHS_BALANCE`: tháng tương đối.
- `AMT_BALANCE`: số dư thẻ tín dụng.
- `AMT_CREDIT_LIMIT_ACTUAL`: hạn mức thẻ.
- `AMT_PAYMENT_TOTAL_CURRENT`: tổng số thanh toán trong tháng.
- `NAME_CONTRACT_STATUS`: trạng thái thẻ tín dụng.

## 4. Nguyên tắc hiểu dữ liệu thời gian
Một số cột có tiền tố `DAYS_*` không phải là ngày thực tế mà là số ngày tương đối so với thời điểm ứng tuyển.

- `DAYS_BIRTH`: tuổi theo số ngày từ hiện tại về quá khứ.
- `DAYS_EMPLOYED`: số ngày trước khi bắt đầu công việc hiện tại.
- `DAYS_DECISION`: thời điểm quyết định vay trước đó so với thời điểm hiện tại.

=> Khi EDA, cần hiểu rằng đây là dữ liệu thời gian tương đối, không phải kiểu datetime chuẩn.

## 5. Cách nhìn dữ liệu theo business logic
Có thể hình dung dữ liệu theo kiến trúc sau:

1. `application` = bảng chính, chứa hồ sơ vay hiện tại.
2. `bureau` = lịch sử tín dụng bên ngoài.
3. `previous_application` = lịch sử xin vay trước đó trong Home Credit.
4. `installments_payments`, `POS_CASH_balance`, `credit_card_balance` = hành vi trả nợ và sử dụng tín dụng theo thời gian.

=> Đây là một hệ thống dữ liệu hướng về tín dụng và rủi ro, nơi các bảng “lịch sử” hỗ trợ giải thích ý nghĩa của `TARGET`.

## 6. Những câu hỏi kinh doanh nên đặt khi EDA
- Khách hàng nào có nguy cơ default cao hơn?
- Những người có thu nhập thấp, vay cao, và lịch sử quá hạn nhiều có rủi ro hơn không?
- Hành vi thanh toán đúng hạn có liên quan mạnh đến `TARGET` không?
- Người có điểm tín dụng ngoài thấp (`EXT_SOURCE_*`) có xu hướng default nhiều hơn không?
- Lịch sử vay trước đó có giúp dự báo rủi ro tốt hơn không?

## 7. Kết luận ngắn
Bài toán này không chỉ là phân tích dữ liệu thống kê, mà còn là phân tích tín dụng và rủi ro. Các cột có giá trị kinh doanh nhất thường nằm ở ba nhóm:
- thông tin khách hàng,
- thông tin khoản vay,
- lịch sử tín dụng và hành vi trả nợ.
