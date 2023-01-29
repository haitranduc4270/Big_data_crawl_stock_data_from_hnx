Hướng dẫn sử dụng

Project: Công cụ hỗ trợ phân tích thị trường chứng khoán

Dữ liệu được lấy từ api của ssi về 2 sàn chứng khoán hose và hnx

Cách thực hiện

Sử dụng:

- Chạy docker compose up -d và hệ thống sẽ tự động crawl dữ liệu và lưu vào elasticsearch + hadoop
- Bật http://localhost:9870/explorer.html#/project20221 để kiểm tra hệ thống file hadoop
- Vào http://localhost:5601/app/dev_tools#/console và chạy GET /stock_data_realtime/\_search để xem các bản ghi được lưu về
