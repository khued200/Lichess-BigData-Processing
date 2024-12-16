from kafka import KafkaConsumer
import json

def consume_messages(topic = "fall", kafka_server='localhost:29092', group_id='default_group'):
   
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        auto_offset_reset='earliest',  
        enable_auto_commit=True,      
        group_id=group_id,             
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
    )

    count = 0

    try:
        print(f"Consuming messages from topic: {topic}")
        for message in consumer:
            count += 1
            print(f"Received message {count}: {message.value}")

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()
        print("Consumer closed.")

consume_messages()