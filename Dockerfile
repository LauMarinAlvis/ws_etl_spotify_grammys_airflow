FROM apache/airflow:3.3.2
COPY requirements-airflow.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
