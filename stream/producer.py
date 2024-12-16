from kafka import KafkaProducer
import json
import time

def send_data_in_batches(df, topic = "fall", kafka_server = "localhost:29092"):
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=100000*10000, batch_size=16384*100 
    )

    try:
        for i, row in enumerate(df):
            # message = json.dumps(row, indent=4)
            message = row
            producer.send(topic, value=message)
            print(f"Sent {i} : {message}")
            time.sleep(5)
    except Exception as e:
        print(f"Error sending message: {e}")
    finally:
        print("Dữ liệu đã được gửi đến kafka")
        producer.flush()
