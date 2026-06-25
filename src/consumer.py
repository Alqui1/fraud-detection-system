import json
import psycopg2
from confluent_kafka import Consumer

# 1. Conect to PostgreSQL database

print("Connecting...")

try:
    # data from docker-compose.yml
    conn = psycopg2.connect( 
        host = "localhost",
        database = "fraud_detection",
        user = "admin", # postgres admin
        password = "fraudpassword123",
        port = "5432"
    )
    conn.autocommit = True # persistens change in database
    
    # create database
    cursor = conn.cursor()

    cursor.execute("""
                         CREATE TABLE IF NOT EXISTS transactions (
                         transaction_id VARCHAR(255) PRIMARY KEY,
                         user_id VARCHAR(50),
                         amount NUMERIC(10, 2),
                         merchant VARCHAR(255),
                         location VARCHAR(255),
                         timestamp BIGINT,
                         is_suspicious BOOLEAN
                         );
                         """)

    print("ready!")

except Exception as e:
    print(f"critical error: {e}")


# 2. Set-up Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud-detector-group',
    'auto.offset.reset': 'earliest' # read message instantly
}

consumer = Consumer(conf)
consumer.subscribe(['financial_transactions']) # suscribe to the topic! product by producer in producer.py

print("real time transactions....")


try:

    while True:

        # wait 1 second to read message
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        
        if msg.error():
            print(f"kafka error: {msg.error()}")
            continue

        # extract from a string and decode the message
        data = json.loads(msg.value().decode('utf-8'))

        # example: if amount > 500, a possible fraud
        is_suspicious = data['amount'] > 500.00

        # save in postgresql
        cursor.execute("""
                       INSERT INTO transactions (transaction_id, user_id, amount, merchant, location, timestamp, is_suspicious) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING;
                       """, ( 
                           data['transaction_id'], data['user_id'], data['amount'],
                           data['merchant'], data['location'], data['timestamp'], is_suspicious
                        ))
    
        alert = "fraud detection" if is_suspicious else "ok"
        print(f"{data['user_id']} | amount: ${data['amount']:,.2f} | status: {alert}")

except KeyboardInterrupt:
    print("finishing")
finally:
    # close database objects
    consumer.close()
    cursor.close()
    conn.close()