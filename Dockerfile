FROM apache/airflow:3.0.2

RUN pip install duckdb==1.3.0 \
    pyspark==4.0.0