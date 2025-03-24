from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta
import redis
import random

# --- Novas Adições ---
# Carrega o catálogo de produtos e recomendações
with open('data/products.json', 'r') as f:
    catalogo = json.load(f)

with open('data/recommendations.json', 'r') as f:
    recomendacoes_por_categoria = json.load(f)

# Mapeamento rápido de categorias
id_para_categoria = {produto["id"]: produto["categoria"] 
                    for categoria in catalogo.values() 
                    for produto in categoria}

# --- Configurações Existentes (com ajustes) ---
consumer = KafkaConsumer(
    'clicks',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

r = redis.Redis(host='localhost', port=6379, db=0)
clicks = defaultdict(lambda: defaultdict(int))
window_size = timedelta(minutes=5)

# --- Função de Recomendação Modificada ---
def get_recommendations(product_id, clicks):
    # Obtém a categoria do produto clicado
    categoria = id_para_categoria[product_id]
    
    # Filtra produtos da mesma categoria
    produtos_categoria = recomendacoes_por_categoria[categoria]
    
    # Ordena por cliques dentro da categoria
    cliques_filtrados = {
        k: v for k, v in clicks.items() 
        if int(k) in produtos_categoria
    }
    
    sorted_products = sorted(
        cliques_filtrados.items(),
        key=lambda x: sum(x[1].values()),
        reverse=True
    )
    
    # Usa recomendação padrão se não houver cliques suficientes
    if len(sorted_products) < 3:
        return random.sample(produtos_categoria, 3)
    
    return [product_id for product_id, _ in sorted_products[:3]]

# --- Loop Principal Atualizado ---
print("Iniciando consumidor de cliques...")
for msg in consumer:
    try:
        data = msg.value
        product_id = int(data['product_id'])
        timestamp = datetime.fromtimestamp(data['timestamp'])
        
        # Atualiza cliques
        clicks[product_id][timestamp] += 1
        
        # Limpeza da janela temporal
        cutoff = timestamp - window_size
        for ts in list(clicks[product_id].keys()):
            if ts < cutoff:
                del clicks[product_id][ts]
        
        # Calcula total de cliques ATUALIZADO
        total_clicks = sum(clicks[product_id].values()) 
        
        # Salva cliques no Redis PRIMEIRO
        r.set(f"clicks:{product_id}", total_clicks)
        
        # Gera e armazena recomendações
        recommendations = get_recommendations(product_id, clicks)
        r.set(f"recommendations:{product_id}", json.dumps({
            "produto": data['product_name'],
            "categoria": id_para_categoria[product_id],
            "recomendacoes": recommendations
        }))
        
        print(f"[Cliques: {total_clicks}] Recomendações para {data['product_name']}: {recommendations}")
        
    except Exception as e:
        print(f"Erro ao processar mensagem: {str(e)}")