FROM apache/airflow:3.0.2

# Create user airflow with id $AIRFLOW_UID, if it doesn't exist 
USER root
RUN id -u airflow &>/dev/null || useradd -u $AIRFLOW_UID airflow

# Reset /opt/airflow/ directory
RUN rm -rf /opt/airflow/*
RUN mkdir -v -p /opt/airflow/{logs,dags,plugins,config,data}

# Transfer files from host to docker volumes
COPY ./airflow/dags /opt/airflow/dags
COPY ./scripts /opt/scripts

# Change ownership of files within /opt/airflow to user airflow
RUN chown -R airflow: /opt/airflow

# Change ownership of files within ./airflow/data to user airflow
RUN chown -R airflow: /opt/airflow

# Install necessary Python libs
USER airflow
RUN pip install duckdb==1.3.0 \
    pyspark==4.0.0