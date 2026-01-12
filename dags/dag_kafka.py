import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import \
    ProduceToTopicOperator
from operators.sptrans_operator import SPTransPosicaoOperator


def produzir_mensagem(teste, **kwargs):
    print(f"Produzindo mensagem para Kafka: {teste}")
    agora = datetime.now()
    key = agora.strftime("%Y_%m_%d_%H_%M_%S")
    if teste:
        return [(key, json.dumps(teste).encode('utf-8'))]
    print("Nenhum dado para enviar ao Kafka.")
    value_dict = {
        "onibus": "1234",
        "linha": "4100",
        "latitude": -23.55052,
        "longitude": -46.633308
    }
    value_dict = json.dumps(value_dict).encode('utf-8')
    return [(key, value_dict)]


# -----------------------
with DAG(
    dag_id="dag_um_kafka_bronze",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["exemplo", "basico"],
    render_template_as_native_obj=True,
) as dag:

    obter_posicoes = SPTransPosicaoOperator(
        task_id="task_obter_posicoes_sptrans",
        http_conn_id="url_api_sptrans",
        do_xcom_push=True
    )

    produtor = ProduceToTopicOperator(
        task_id="task_enviar_para_kafka",
        kafka_config_id="conexao_kafka",
        topic="sptrans_posicao_onibus",
        producer_function_kwargs={
            "teste": "{{ ti.xcom_pull(task_ids='task_obter_posicoes_sptrans')   }}"
        },
        producer_function=produzir_mensagem,
    )

    obter_posicoes >> produtor
