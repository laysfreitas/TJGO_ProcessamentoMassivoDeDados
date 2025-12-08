# Documentação de Arquitetura: Pipeline de Processamento Massivo de Dados (Streaming)

## 1. Visão Geral do Projeto
Este projeto implementa uma arquitetura de referência para processamento massivo de dados em tempo real (streaming). O objetivo é demonstrar a ingestão, desacoplamento, transformação e armazenamento de transações de e-commerce utilizando o ecossistema **Google Cloud Platform (GCP)** e **Apache Beam**.

A solução foi desenhada para garantir baixa latência e alta escalabilidade, simulando um cenário real de produção onde transações de vendas precisam ser disponibilizadas imediatamente para análise ou aplicações *downstream*.

---

## 2. Arquitetura da Solução
O fluxo de dados segue um padrão clássico de streaming ETL (Extract, Transform, Load), composto pelos seguintes estágios principais:

* **Ingestão (Producer):** Script Python para emulação de streaming a partir de dataset CSV.
* **Mensageria (Broker):** Google Cloud Pub/Sub para bufferização e desacoplamento.
* **Processamento (Consumer):** Google Cloud Dataflow (Apache Beam) para ingestão distribuída.
* **Armazenamento (Sink):** Google Cloud Bigtable para persistência NoSQL de alta performance.

### Diagrama de Fluxo de Dados (Data Flow)
O grafo de execução do Dataflow abaixo ilustra a topologia do pipeline, evidenciando a leitura da fonte de streaming e a escrita direta no banco de dados.

![Grafo do Dataflow](images\IMG-20251208-WA0033.jpg)
*Figura 1: DAG (Directed Acyclic Graph) do pipeline no Dataflow: "Ler do Pub" -> "Gravar no Bigtable".*

---

## 3. Tecnologias e Justificativa
A escolha das tecnologias baseou-se nos requisitos não funcionais de performance e escalabilidade:

* **Python & SDK Apache Beam:** Utilizados pela flexibilidade na manipulação de dados e pela capacidade do modelo Beam de unificar processamento em *batch* e *streaming*.
* **Google Cloud Pub/Sub:** Escolhido como *message broker* para garantir o desacoplamento entre o produtor (script de ingestão) e o consumidor (Dataflow). Ele absorve picos de carga e garante a entrega das mensagens.
* **Google Cloud Dataflow:** Serviço gerenciado para executar o pipeline Apache Beam. Permite o *autoscaling* dos *workers* (nós de processamento) conforme o volume de dados de entrada, eliminando a necessidade de gerenciamento de infraestrutura de clusters.
* **Google Cloud Bigtable:** Banco de dados NoSQL *wide-column*, ideal para gravações de alta vazão (*high throughput*) e baixa latência, características essenciais para armazenar dados de transações financeiras ou de e-commerce em tempo real.
* **Google Cloud Storage (GCS):** Utilizado como área de *staging* para os binários do pipeline e arquivos temporários necessários durante a execução do *job* no Dataflow.

---

## 4. Detalhamento do Pipeline de Dados
Abaixo descrevemos passo a passo o ciclo de vida do dado dentro da arquitetura proposta.

### 4.1. Ingestão e Publicação de Mensagens
A fonte de dados é um arquivo estático nomeado `ecommerce_dataset_updated.csv`. Para simular um comportamento de streaming em tempo real, foi desenvolvido o script `script_ingest_data.py`.

* **Funcionamento:** O script lê o CSV e publica cada linha como uma mensagem individual no tópico do Pub/Sub.
* **Emulação de Tempo Real:** Observa-se no código e nos logs que há uma pausa (`time.sleep`) para simular o envio contínuo, transformando um dado estático em um fluxo de eventos.
* **Evidência de Execução:** Os logs do terminal mostram os registros sendo publicados com IDs únicos e *timestamps*, confirmando a serialização dos dados do dataset (ex: `Product_ID`, `Category`, `Price`).

### 4.2. Bufferização (Pub/Sub)
Os dados ingeridos são recebidos pelo tópico `projects/e-commerce-data-479220/topics/MyTopic`.

* **Assinatura:** Uma assinatura (*subscription*) `MySub` foi configurada para que o Dataflow possa consumir as mensagens. As métricas de "Health" e "Pull" no console do GCP monitoram a latência de confirmação (*ack latency*) e a contagem de mensagens, garantindo que não haja perda de dados durante o trânsito.

### 4.3. Processamento Distribuído (Dataflow)
O coração do processamento é o *job* do Dataflow, executado a partir do script `script_dataflow.py`.

* **Implantação:** O *job* foi submetido com sucesso para a região `europe-west10`, utilizando a versão do SDK 2.69.0.
* **Autoscaling:** O serviço foi configurado para escalar horizontalmente entre 1 e 100 *workers* dependendo da carga, embora nos testes iniciais tenha operado com 1 *worker*.
* **Lógica do Pipeline:**
    1.  **Read:** Conecta na assinatura do Pub/Sub.
    2.  **Transform:** (Implícito) Deserialização e formatação dos dados para o formato de linha do Bigtable.
    3.  **Write:** Grava os dados na tabela de destino.
* **Monitoramento:** O gráfico de execução mostra os estágios "Ler do Pub" e "Gravar no Bigtable" com status de execução bem-sucedida (círculo verde).

### 4.4. Persistência (Bigtable)
Os dados transformados são persistidos em uma instância do Bigtable chamada `e-commerce-data`.

* **Estrutura da Tabela:**
    * Tabela: `transacoes`
    * Column Family: `dados`
* **Cluster:** O cluster `e-commerce-data-c1-berlim` hospeda os nós de processamento do banco.
* **Monitoramento de Recursos:** As métricas de uso de CPU e armazenamento evidenciam a atividade de escrita durante a execução do pipeline, com um pico claro de utilização de armazenamento coincidindo com o momento da ingestão.

---

## 5. Validação e Resultados
A validação da integridade dos dados foi realizada via ferramenta de linha de comando `cbt` (Cloud Bigtable CLI), conectando-se diretamente à instância.

Como evidenciado na saída do terminal abaixo, os registros foram gravados corretamente com a seguinte estrutura:

```text
Row Key: 005258a0#8235030767923  (Provável composição de UserID + TransactionID)
----------------------------------------
Family: dados
    Qualifier: category      Value: "Electronics"
    Qualifier: final_price   Value: "303.14"
    Qualifier: payment       Value: "Debit Card"
    Qualifier: price         Value: "303.14"
    Timestamp: 2025/12/05-18:14:33.600000