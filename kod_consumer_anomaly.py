%%file consumer_anomaly.py
from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(list)

print("Monitorowanie anomalii prędkości (3+ transakcje w 60s)...")

for message in consumer:
    tx = message.value
    user_id = tx.get('user_id')
    current_time = time.time()  
    
    if not user_id:
        continue

    user_history[user_id].append(current_time)

    user_history[user_id] = [t for t in user_history[user_id] if current_time - t <= 60]

    if len(user_history[user_id]) > 3:
        print(f"ALERT: Wykryto anomalię prędkości!")
        print(f"Użytkownik: {user_id} | Liczba transakcji w 60s: {len(user_history[user_id])}")
        print(f"Ostatnia kwota: {tx.get('amount')} PLN | Sklep: {tx.get('store')}")
        print("-" * 40)