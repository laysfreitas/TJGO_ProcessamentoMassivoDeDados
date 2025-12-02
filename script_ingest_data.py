from google.cloud import pubsub_v1
import pandas as pd
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID")
DATA_FILE = os.getenv("DATA_FILE")

# Configurações do Google Cloud
publisher = pubsub_v1.PublisherClient() # Cliente do Pub/Sub
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID) # Caminho completo do tópico

print(f"Iniciando Produtor para o Tópico: {topic_path}")
print(f"Projeto ID: {PROJECT_ID}")

def publish_messages(dataframe):
    print("Iniciando a publicação de mensagens...")
    print(dataframe.head())

    # AJUSTE: Renomear colunas para remover espaços e caracteres especiais
    # Isso facilita muito no Dataflow e no Bigtable
    dataframe.rename(columns={
        'User_ID': 'user_id',
        'Product_ID': 'product_id',
        'Category': 'category',
        'Price (Rs.)': 'price',
        'Discount (%)': 'discount_percent',
        'Final_Price(Rs.)': 'final_price',
        'Payment_Method': 'payment_method',
        'Purchase_Date': 'purchase_date_original'
    }, inplace=True)

    records = dataframe.to_dict(orient='records')

    for row in records:
        # AJUSTE: Injetar Timestamp de "Agora"
        # Para o Bigtable, precisamos de um timestamp numérico (milissegundos)
        # para criar a Row Key invertida corretamente.
        current_timestamp_ms = int(time.time() * 1000)
        row['timestamp_ingestao'] = current_timestamp_ms

        # Converte a linha do DataFrame para JSON
        message_json = json.dumps(row)

        # Codificar a mensagem em bytes (Formato do Pub/Sub)
        message_bytes = message_json.encode('utf-8')
        
        try:
            # Publica a mensagem no tópico do Pub/Sub
            future = publisher.publish(topic_path, data=message_bytes)
            # O result() é bloqueante, ideal para testes, mas em prod usa-se callback
            msg_id = future.result() 
            print(f"Publicado: {row['user_id']} | Time: {current_timestamp_ms} | ID: {msg_id}")

            #print(f"Mensagem publicada: {message_json}")

        except Exception as e:
            print(f"Erro ao publicar mensagem: {e}")

        #print(f"Mensagem publicada: {message_json}")

        time.sleep(2)  # Pequena pausa para simular um envio em tempo real


if __name__ == "__main__":

    #carregar o dataset
    df = pd.read_csv(DATA_FILE)
    if df.empty:
        print("O dataset está vazio. Nenhuma mensagem será publicada.")
    else:
        print(f"Dataset carregado com {len(df)} registros.")
        publish_messages(df)    