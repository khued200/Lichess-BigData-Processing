from kafka import KafkaConsumer
import json

# Tạo Kafka Consumer
consumer = KafkaConsumer(
    "chess",  # Tên topic
    bootstrap_servers= "localhost:9092",
    auto_offset_reset='earliest', # Đọc từ đầu nếu không có offset
    enable_auto_commit=True,      # Tự động lưu offset
    group_id="my_group",          # Nhóm tiêu thụ
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Chuyển đổi giá trị JSON
)

count = 0

# Vòng lặp để nhận tin nhắn mới
try:
    for message in consumer:
        count += 1
        print(f"Received message {count}: {message.value}")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()  # Đóng consumer