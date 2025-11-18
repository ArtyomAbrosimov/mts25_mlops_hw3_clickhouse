from clickhouse_driver import Client
import time
import csv
import os

client = Client(host='clickhouse', user='click', password='click')

with open('query.sql', 'r') as f:
    query = f.read()

result = client.execute(query)

with open('submission.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['state', 'category', 'highest_transaction'])
    for row in result:
        writer.writerow(row)

print(f"Results saved to submission.csv")
