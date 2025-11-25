from google.cloud import pubsub_v1
import pandas as pd
import os
from dotenv import load_dotenv
import json


load_dotenv()

# Configurações do Google Cloud
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

print(f"Iniciando Produtor para o Tópico: {topic_path}")