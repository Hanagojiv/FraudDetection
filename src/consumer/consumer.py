import json
from kafka import KafkaConsumer
import psycopg2

def consume_and_store():
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    try:
        conn = psycopg2.connect("host=localhost dbname=fraud_db user=user password=password")
        cur = conn.cursor()
        print("Database connection successful.")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return

    for message in consumer:
        data = message.value
        transaction_id = data.get('transaction_id')
        user_id = data.get('user_id')
        transaction_time = data.get('transaction_time')
        amount = data.get('amount')
        is_fraud_actual = data.get('is_fraud')
        
        # Simple fraud detection rule
        is_fraud_predicted = 1 if amount > 1000 else 0
        
        try:
            cur.execute(
                """
                INSERT INTO transactions (transaction_id, user_id, transaction_time, amount, is_fraud_actual, is_fraud_predicted)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (transaction_id, user_id, transaction_time, amount, is_fraud_actual, is_fraud_predicted)
            )
            conn.commit()
            print(f"Processed and stored transaction: {transaction_id}")
        except Exception as e:
            print(f"Error inserting transaction {transaction_id}: {e}")
            conn.rollback()
            
    cur.close()
    conn.close()

if __name__ == "__main__":
    consume_and_store()