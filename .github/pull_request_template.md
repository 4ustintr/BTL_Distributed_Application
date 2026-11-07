## Mục spec đã làm
<!-- ví dụ: F6 POST /outbound, F10.a -->

## Test
<!-- ID test trong docs/tests.md và kết quả: TC01 xanh, TC02 xanh -->

## File đã sửa
<!-- chỉ trong phạm vi của mình; nếu đụng file người khác, ghi lý do -->

## Checklist (docs/plan.md mục 3.5)
- [ ] Đúng tên bảng, cột, endpoint, mã lỗi, sự kiện như docs/; dùng hằng số trong common/
- [ ] Thứ tự bước đúng F10; kiểm tra trùng request_id trước khi trừ kho
- [ ] outbox_event cùng transaction; không gọi HTTP trong transaction
- [ ] UPDATE inventory có điều kiện version và khả dụng; không tự tăng version
- [ ] Cập nhật trạng thái có điều kiện trạng thái liền trước
- [ ] Consumer: processed_event, last_version, commit offset sau khi commit CSDL
- [ ] Gọi service khác bằng client trong common/
- [ ] Có test tương ứng và test xanh

## Giả định còn mở
