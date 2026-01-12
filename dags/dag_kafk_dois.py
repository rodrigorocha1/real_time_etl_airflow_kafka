import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import \
    ProduceToTopicOperator

# -----------------------
# Função para gerar dados
# -----------------------


def gerar_mensagem(**kwargs):
    mensagem = {"id": 1, "conteudo": "Hello Kafka"}
    # Armazena no XCom para o ProduceToTopicOperator
    return mensagem

# -----------------------
# Função que o ProduceToTopicOperator vai usar
# -----------------------


# def produzir_mensagem(teste, **kwargs):
#     print(f"Produzindo mensagem para Kafka: {teste}")
#     # Retorna lista de tuplas (key, value)
#     return [(None, json.dumps(teste))]


def produzir_mensagem(teste, **kwargs):
    print(f"Produzindo mensagem para Kafka: {teste}")
    # Retorna lista de tuplas (key, value)
    if teste:
        return [(None, teste)]
    print("Nenhum dado para enviar ao Kafka.")


# -----------------------
# DAG
# -----------------------
with DAG(
    dag_id="exemplo_kafka_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # Tarefa que gera dados
    gerar = PythonOperator(
        task_id="gerar_mensagem",
        python_callable=gerar_mensagem,
    )

    # Tarefa que envia para Kafka
    enviar_kafka = ProduceToTopicOperator(
        task_id="enviar_para_kafka",
        kafka_config_id="conexao_kafka",  # ID da conexão no Airflow
        topic="topico_teste",
        producer_function=produzir_mensagem,
        producer_function_kwargs={
            # Pegando a mensagem do XCom
            "teste": "{{ ti.xcom_pull(task_ids='gerar_mensagem') }}"
        },
    )

    gerar >> enviar_kafka
