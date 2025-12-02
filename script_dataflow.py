import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigtable
import os
from dotenv import load_dotenv

# Carrega variáveis
load_dotenv()

# --- CONFIGURAÇÕES ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
INSTANCE_ID = os.getenv("INSTANCIA_BIGTABLE_ID") # Certifique-se que esse nome está no .env
TABLE_ID = os.getenv("TABLE_ID")  # Nome da tabela Bigtable

# AJUSTE 1: Pegando do .env para não dar erro de "not found"
SUBSCRIPTION_ID = os.getenv("PUBSUB_SUBSCRIPTION_ID") 

if not SUBSCRIPTION_ID:
    raise ValueError("A variável PUBSUB_SUBSCRIPTION_ID não foi encontrada no .env!")

class WriteToBigtable(beam.DoFn):
    def __init__(self, project_id, instance_id, table_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id

    def start_bundle(self):
        client = bigtable.Client(project=self.project_id, admin=False)
        instance = client.instance(self.instance_id)
        self.table = instance.table(self.table_id)

    def process(self, element):
        try:
            data = json.loads(element.decode('utf-8'))
            
            # Row Key = user_id + (timestamp invertido)
            user_id = data.get('user_id', 'unknown')
            ts = data.get('timestamp_ingestao', 0)
            
            # Timestamp reverso
            ts_invertido = 9999999999999 - ts 
            row_key = f"{user_id}#{ts_invertido}".encode()
            
            row = self.table.row(row_key)
            
            # Família 'dados' (Deve ter sido criada via cbt antes!)
            family = 'dados'
            
            # Convertendo valores. Nota: Bigtable armazena bytes.
            row.set_cell(family, 'price', str(data.get('price')).encode())
            row.set_cell(family, 'final_price', str(data.get('final_price')).encode())
            row.set_cell(family, 'category', str(data.get('category')).encode())
            row.set_cell(family, 'payment', str(data.get('payment_method')).encode())
            
            row.commit()
            yield row_key
            
        except Exception as e:
            logging.error(f"Erro no processamento: {e}")

def run():
    # AJUSTE 2: Garantir que o nome do bucket é válido
    # Você deve criar esse bucket manualmente no console antes: gs://SEU_PROJETO_ID-dataflow
    bucket_temp = f'gs://{PROJECT_ID}-dataflow/temp'
    
    pipeline_options = PipelineOptions(
        streaming=True, 
        project=PROJECT_ID, 
        runner='DataflowRunner',
        temp_location=bucket_temp,
        region='europe-west3',
        # Setup options para garantir que as libs sejam instaladas nos workers
        requirements_file='requirements.txt' 
    )
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Ler do Pub/Sub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_ID)
            | "Gravar no Bigtable" >> beam.ParDo(WriteToBigtable(PROJECT_ID, INSTANCE_ID, TABLE_ID))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()