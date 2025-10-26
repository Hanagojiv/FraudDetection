CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    is_fraud_actual INTEGER NOT NULL,
    is_fraud_predicted INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
