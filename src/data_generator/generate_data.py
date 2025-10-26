import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

def create_transaction_data(num_transactions=1000):
    data = []
    for _ in range(num_transactions):
        # 3% of transactions are fraudulent
        is_fraud = random.choices([0, 1], weights=[97, 3], k=1)[0]
        
        if is_fraud:
            amount = round(random.uniform(500.0, 5000.0), 2) # Higher amounts for fraud
        else:
            amount = round(random.uniform(1.0, 500.0), 2)

        transaction_time = fake.date_time_between(start_date='-30d', end_date='now')
        
        data.append({
            'transaction_id': fake.uuid4(),
            'user_id': fake.uuid4(),
            'transaction_time': transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
            'amount': amount,
            'ip_address': fake.ipv4(),
            'is_fraud': is_fraud
        })
        
    df = pd.DataFrame(data)
    df.to_csv('data/raw/transactions.csv', index=False)
    print(f"Generated {num_transactions} transactions and saved to data/raw/transactions.csv")

if __name__ == "__main__":
    create_transaction_data()
