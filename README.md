# CASE STUDY 2
Dưới đây là một chương trình có nhiệm vụ chuyển file ảnh tiếng Anh sang một file `pdf` tiếng Việt. Các bước xử lý lần lượt bao gồm: chuyển đổi ảnh sang text, dịch tiếng Anh sang tiếng Việt, chuyển đổi nội dung text thành file `pdf`. Chương trình chính chỉ demo các tính năng này tuần tự.

## Hướng dẫn cài đặt
Yêu cầu cài đặt trước [tesseract](https://tesseract-ocr.github.io/tessdoc/Installation.html) trên hệ điều hành của bạn.
Node version 20

## Chạy ứng dụng BE
Trong folder CS2
```sh
# Cài đặt các gói liên quan
$ npm install
# Tạo folder cho output
$ mkdir output
# Khởi chạy ứng dụng BE demo
$ npm start
```

## Chạy ứng dụng FE
Trong folder CS2/frontend-app
```sh
# Cài đặt các gói liên quan
$ npm install
# Khởi chạy ứng dụng demo
$ npm start
```

## Testing
Chạy file upload-test.bat
Khi chưa áp dụng lưu lại, trung bình 1rq cần 1.001s để xử lý (upload-test-result-1.csv)
Khi có áp dụng lưu lại, trung bình 1rq chỉ cần 0.003s để xử lý (upload-test-result-2.csv)
Đánh giá: đây chỉ là lưu lại cơ bản nhưng đã có thể tối ưu api, trong thực tế có thể cần **sử dụng nhiều kỹ thuật hơn để tối ưu API**
