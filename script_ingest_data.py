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
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

print(f"Iniciando Produtor para o Tópico: {topic_path}")
print(f"Projeto ID: {PROJECT_ID}")

def publish_messages(dataframe):
    print("Iniciando a publicação de mensagens...")
    print(dataframe.head())

    records = dataframe.to_dict(orient='records')

    for row in records:
        # Converte a linha do DataFrame para JSON
        message_json = json.dumps(row)

        # Codificar a mensagem em bytes (Formato do Pub/Sub)
        message_bytes = message_json.encode('utf-8')
        
        # Publica a mensagem no tópico do Pub/Sub
        #future = publisher.publish(topic_path, data=message_bytes)
        #print(f"Publicado mensagem ID: {future.result()}")

        print(f"Mensagem publicada: {message_json}")

        time.sleep(2)  # Pequena pausa para simular um envio em tempo real


if __name__ == "__main__":

    #carregar o dataset
    df = pd.read_csv(DATA_FILE)
    if df.empty:
        print("O dataset está vazio. Nenhuma mensagem será publicada.")
    else:
        print(f"Dataset carregado com {len(df)} registros.")
        publish_messages(df)    