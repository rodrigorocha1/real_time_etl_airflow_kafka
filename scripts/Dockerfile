FROM apache/airflow:3.0.6-python3.11

# Troca para o usuário airflow
USER airflow

# Instala providers do Airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-http \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-kafka
