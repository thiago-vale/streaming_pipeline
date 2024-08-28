import pandas as pd
from kafka import KafkaProducer
import json
from time import sleep

producer = KafkaProducer(bootstrap_servers=['host'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Ler o CSV
df = pd.read_csv('Sales Transaction v.4a.csv')

# Enviar cada linha do CSV para o tópico Kafka
for _, row in df.iterrows():
    # Converter a linha para um dicionário
    message = row.to_dict()
    sleep(2)
    producer.send('topic', message)
    print(f"Mensagem enviada: {message}")

print("Envio concluído.")