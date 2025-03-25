from kafka import KafkaProducer
import json
import random
import time
import jwt
from dotenv import load_dotenv
import os

load_dotenv()

def generate_token(user_id):
    return jwt.encode(
        {"user_id": user_id},
        os.getenv("JWT_SECRET"),
        algorithm=os.getenv("JWT_ALGORITHM")
    )

# Carrega o catálogo de produtos
with open('data/products.json', 'r') as file:
    catalogo = json.load(file)  

# Lista plana de todos os produtos para sorteio
todos_produtos = []
for categoria in catalogo.values():
    todos_produtos.extend(categoria)

# Configura o produtor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simula cliques aleatórios
for user_id in range(1, 1000):
    produto = random.choice(todos_produtos)
    message = {
        "user_id": user_id,
        "product_id": produto["id"],
        "token": generate_token(user_id),
        "product_name": produto["nome"],
        "categoria": produto["categoria"],
        "timestamp": int(time.time())
    }
    producer.send('clicks', message)
    time.sleep(random.uniform(0.1, 0.5))  # Intervalo aleatório entre 0.1s e 0.5s

producer.flush()