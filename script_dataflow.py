# Importações necessárias para o pipeline de processamento de dados
import argparse
import json
import logging
import apache_beam as beam  # Framework Apache Beam para processamento em streaming/batch
from apache_beam.options.pipeline_options import PipelineOptions  # Configurações do pipeline
from apache_beam.options.pipeline_options import SetupOptions  # Opções de setup dos workers
from google.cloud import bigtable  # Cliente do Google Cloud Bigtable
import os  # Para acessar variáveis de ambiente
from dotenv import load_dotenv  # Para carregar variáveis do arquivo .env

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# --- CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE ---
# Recupera as credenciais do GCP do arquivo .env
PROJECT_ID = "e-commerce-data-479220"  # ID do projeto no Google Cloud Platform
INSTANCE_ID = "e-commerce-data"  # ID da instância Bigtable (deve estar no .env)
TABLE_ID = "transacoes"  # Nome da tabela Bigtable onde os dados serão armazenados

# Recupera o ID da subscription do Pub/Sub (tópico de origem dos dados)
TOPIC_ID = "MyTopic"
SUBSCRIPTION_ID = "projects/e-commerce-data-479220/subscriptions/MySub"

# Valida se a subscription foi configurada, caso contrário, interrompe o programa
if not SUBSCRIPTION_ID:
    raise ValueError("A variável PUBSUB_SUBSCRIPTION_ID não foi encontrada no .env!")

# Classe personalizada que escreve dados no Google Cloud Bigtable
class WriteToBigtable(beam.DoFn):
    """
    Transformação que recebe mensagens do Pub/Sub e as escreve no Bigtable.
    Cada elemento é uma mensagem JSON convertida para um registro no Bigtable.
    """
    
    def __init__(self, project_id, instance_id, table_id):
        """Inicializa com as credenciais do Bigtable"""
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id

    def start_bundle(self):
        """
        Chamado uma vez por bundle de dados.
        Conecta ao Bigtable e obtém referência à tabela.
        """
        client = bigtable.Client(project=self.project_id, admin=False)
        instance = client.instance(self.instance_id)
        self.table = instance.table(self.table_id)

    def process(self, element):
        """
        Processa cada elemento (mensagem) recebido do Pub/Sub.
        
        Args:
            element: bytes - mensagem do Pub/Sub em formato JSON
            
        Yields:
            row_key: a chave da linha inserida no Bigtable
        """
        try:
            # Decodifica a mensagem JSON recebida do Pub/Sub
            data = json.loads(element.decode('utf-8'))
            
            # Extrai o ID do usuário e o timestamp de ingestão dos dados
            user_id = data.get('user_id', 'unknown')
            ts = data.get('timestamp_ingestao', 0)
            
            # Inverte o timestamp para permitir ordenação reversa no Bigtable
            # (registros mais recentes ficam primeiro na scanagem de linhas)
            ts_invertido = 9999999999999 - ts 
            row_key = f"{user_id}#{ts_invertido}".encode()
            
            # Cria uma nova linha no Bigtable com a chave gerada
            row = self.table.row(row_key)
            
            # Define a família de colunas onde os dados serão armazenados
            # (A família 'dados' deve ser criada previamente via 'cbt' ou console GCP)
            family = 'dados'
            
            # Armazena os campos de dados na Bigtable
            # Nota: Bigtable armazena tudo como bytes, então convertemos valores para string
            row.set_cell(family, 'price', str(data.get('price')).encode())
            row.set_cell(family, 'final_price', str(data.get('final_price')).encode())
            row.set_cell(family, 'category', str(data.get('category')).encode())
            row.set_cell(family, 'payment', str(data.get('payment_method')).encode())
            
            # Confirma a escrita da linha no Bigtable
            row.commit()
            
            # Retorna a chave para rastreamento
            yield row_key
            
        except Exception as e:
            # Registra qualquer erro durante o processamento
            logging.error(f"Erro no processamento: {e}")

def run():
    """
    Função principal que configura e executa o pipeline Dataflow.
    
    O pipeline:
    1. Lê mensagens do Pub/Sub em tempo real (streaming)
    2. Processa cada mensagem
    3. Escreve os dados processados no Bigtable
    """
    
    # Define o local temporário para armazenar arquivos intermediários do Dataflow
    # O bucket GCS deve ser criado manualmente no console do GCP
    bucket_temp = f'gs://bucket-{PROJECT_ID}/temp'
    bucket_staging = f'gs://bucket-{PROJECT_ID}/staging'
    
    # Configura as opções do pipeline Dataflow
    pipeline_options = PipelineOptions(
        streaming=True,  # Habilita processamento em streaming (dados em tempo real)
        project=PROJECT_ID,  # Projeto GCP onde o Dataflow será executado
        runner='DataflowRunner',  # Executa na infraestrutura gerenciada do Google Cloud
        temp_location=bucket_temp,  # Local para armazenar arquivos temporários
        staging_location=bucket_staging, # Local para arquivos de staging
        region='europe-west10',  # Região onde o job será executado
        # Arquivo com as dependências Python a serem instaladas nos workers
        requirements_file='./requirements.txt' 
    )
    
    # Cria e executa o pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # Lê as mensagens da subscription do Pub/Sub
            | "Ler do Pub/Sub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_ID)
            # Escreve cada mensagem processada no Bigtable
            | "Gravar no Bigtable" >> beam.ParDo(WriteToBigtable(PROJECT_ID, INSTANCE_ID, TABLE_ID))
        )

# Ponto de entrada do script
if __name__ == '__main__':
    # Define o nível de logging como INFO (mostra mensagens informativas e acima)
    logging.getLogger().setLevel(logging.INFO)
    # Inicia a execução do pipeline
    run()