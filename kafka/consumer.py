from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'chess_games', 
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',  
    enable_auto_commit=True,      
    group_id='chess_game',         
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
)

count = 0

try:
    for message in consumer:
        count += 1
        print(f"Received message {count}: {message.value}")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close() 
