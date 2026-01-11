import json
from datetime import datetime
from typing import Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import \
    ProduceToTopicOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator
from operators.sptrans_operator import SPTransPosicaoOperator

# Funções das tarefas


def process_login_cookie(response, **context):
    """
    Salva o cookie de sessão da SPTrans no XCom
    """
    raw_cookie = response.headers.get("Set-Cookie")
    if not raw_cookie:
        raise ValueError("Não foi possível obter o cookie do login")

    # Extrair apenas apiCredentials=TOKEN
    token = raw_cookie.split(";")[0]  # pega só a primeira parte

    context['ti'].xcom_push(key="sptrans_cookie", value=token)

    return "authenticated"


def send_to_kafka():
    return [{"value": "Olá Kafka do Airflow!"}]


def process_positions(response, **context: Dict):
    data = response.json()
    print(response, context, data)
    print(f"Total de ônibus: {len(data)}")
    return data


def buscar_posicoes(**context: Dict):
    cookie = context['ti'].xcom_pull(
        task_ids='fazer_login', key='sptrans_cookie')
    http = HttpHook(method='GET', http_conn_id='url_api_sptrans')
    response = http.run(endpoint='/Posicao', headers={"Cookie": cookie})

    data = response.json()
    print(f"data: {data}")
    print(f"Total de ônibus: {len(data)}")
    context['ti'].xcom_push(key="sptrans_req", value=data)


def gerar_mensagem(**context):
    # Pega dados do XCom
    data = context['ti'].xcom_pull(
        task_ids='buscar_posicoes_onibus', key='sptrans_req')
    # Transformar em lista de mensagens JSON
    mensagens = [json.dumps(item) for item in data]
    return mensagens


def producer_function():
    return [{"value": "Olá Kafka do Airflow!"}]


# Definição da DAG
with DAG(
    dag_id="dag_um_kafka_bronze",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["exemplo", "basico"],
) as dag:

    obter_posicoes = SPTransPosicaoOperator(
        task_id="obter_posicoes_sptrans",
        token=""
    )
    obter_posicoes

    # produtor = ProduceToTopicOperator(
    #     task_id="send_message_to_kafka",
    #     kafka_config_id="conexao_kafka",
    #     topic="meu_topico",
    #     producer_function=producer_function,
    # )

    # Ordem de execução
