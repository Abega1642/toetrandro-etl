FROM apache/airflow:3.0.1-python3.11

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY workflows/ /opt/airflow/dags/
COPY src/ /opt/airflow/src/
COPY tests/ /opt/airflow/tests/

ENV PYTHONPATH="/opt/airflow/src:/opt/airflow/workflows"

WORKDIR /opt/airflow
