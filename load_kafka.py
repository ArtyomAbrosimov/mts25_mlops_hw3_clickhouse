from kafka import KafkaProducer
import json
import pandas as pd
import os

KAFKA_BROKER = 'kafka:29092'
TOPIC_NAME = 'transactions'

df = pd.read_csv("train.csv")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=16384,
    linger_ms=10
)

for index, row in df.iterrows():
    transaction_data = {
        'transaction_time': row['transaction_time'],
        'merch': row['merch'],
        'cat_id': row['cat_id'],
        'amount': float(row['amount']),
        'name_1': row['name_1'],
        'name_2': row['name_2'],
        'gender': row['gender'],
        'street': row['street'],
        'one_city': row['one_city'],
        'us_state': row['us_state'],
        'post_code': row['post_code'],
        'lat': float(row['lat']),
        'lon': float(row['lon']),
        'population_city': int(row['population_city']),
        'jobs': row['jobs'],
        'merchant_lat': float(row['merchant_lat']),
        'merchant_lon': float(row['merchant_lon']),
        'target': int(row['target'])
    }

    producer.send(TOPIC_NAME, value=transaction_data)

    if index % 100000 == 0:
        print(f"Sent {index} messages")
        producer.flush()

producer.flush()
producer.close()
print(f"Sent {len(df)} messages")
