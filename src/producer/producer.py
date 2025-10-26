import pandas as pd
from kafka import KafkaProducer
import json
import time

def produce_transactions(file_path='data/raw/transactions.csv', topic_name='transactions'):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    df = pd.read_csv(file_path)
    
    for index, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic_name, value=message)
        print(f"Sent: {message['transaction_id']}")
        time.sleep(0.5) # Simulate real-time stream
        
    producer.flush()
    producer.close()
    print("Finished sending all transactions.")

if __name__ == "__main__":
    produce_transactions()