from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

SimpleHttpOperator(
    
)

# Funções das tarefas


def tarefa_1():
    print("Executando a tarefa 1")


def tarefa_2():
    print("Executando a tarefa 2")


# Definição da DAG
with DAG(
    dag_id="dag_simples_exemplo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["exemplo", "basico"],
) as dag:

    task_1 = PythonOperator(
        task_id="tarefa_1",
        python_callable=tarefa_1,
    )

    task_2 = PythonOperator(
        task_id="tarefa_2",
        python_callable=tarefa_2,
    )

    # Ordem de execução
    task_1 >> task_2
