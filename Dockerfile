FROM apache/airflow:3.1.5-python3.12

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.1.5" \
    -r /requirements.txt