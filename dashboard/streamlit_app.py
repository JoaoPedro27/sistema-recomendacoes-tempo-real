import streamlit as st
import redis
import json
import time

with open('data/products.json', 'r') as f:
    catalogo = json.load(f)

id_to_name = {}
category_map = {}
for category_name, produtos in catalogo.items():
    category_map[category_name] = [prod["id"] for prod in produtos]
    for produto in produtos:
        id_to_name[produto["id"]] = produto["nome"]

r = redis.Redis(host='localhost', port=6379, db=0)

st.title("📊 Dashboard de Recomendações em Tempo Real")
st.markdown("---")

def get_all_products_data():
    products_data = []
    
    # Para cada categoria
    for category_name, produtos in catalogo.items():
        # Para cada produto na categoria
        for produto in produtos:
            product_id = produto["id"]
            
            # Busca cliques (com tratamento para None)
            clicks = r.get(f"clicks:{product_id}")
            click_count = int(clicks) if clicks else 0
            
            # Busca recomendações (com tratamento para None)
            recommendations = []
            rec_data = r.get(f"recommendations:{product_id}")
            if rec_data:
                try:
                    rec_data = json.loads(rec_data)
                    recommendations = [
                        id_to_name.get(int(pid), f"ID: {pid}") 
                        for pid in rec_data.get("recomendacoes", [])
                    ]
                except json.JSONDecodeError:
                    recommendations = ["Erro ao carregar recomendações"]
            
            products_data.append({
                "Produto": produto["nome"],
                "Categoria": category_name,
                "Cliques": click_count,
                "Recomendações": recommendations
            })
    
    return products_data

# Container principal que será atualizado
placeholder = st.empty()

while True:
    with placeholder.container():
        st.header("📈 Estatísticas Detalhadas de Todos os Produtos")
        
        # Obtém e ordena os dados
        products_data = sorted(
            get_all_products_data(),
            key=lambda x: x["Cliques"],
            reverse=True
        )
        
        # Cria abas por categoria
        categorias = sorted(set(p["Categoria"] for p in products_data))
        
        for categoria in categorias:
            st.subheader(f"🗂️ {categoria}")
            
            # Filtra produtos da categoria
            cat_produtos = [p for p in products_data if p["Categoria"] == categoria]
            
            # Cria colunas para cada produto
            cols = st.columns(3)
            col_idx = 0
            
            for produto in cat_produtos:
                with cols[col_idx]:
                    st.markdown(f"### {produto['Produto']}")
                    st.metric("Cliques Totais", produto["Cliques"])
                    
                    st.markdown("**Recomendações:**")
                    for i, rec in enumerate(produto["Recomendações"], 1):
                        st.markdown(f"{i}. {rec}")
                    
                    st.markdown("---")
                
                # Atualiza índice da coluna
                col_idx = (col_idx + 1) % 3

        time.sleep(2)