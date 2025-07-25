FROM apache/airflow:3.0.2

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Create user airflow with id $AIRFLOW_UID, if it doesn't exist
RUN id -u airflow &>/dev/null || useradd -u $AIRFLOW_UID airflow

# Reset /opt/airflow/ directory
RUN rm -rf /opt/airflow/*
RUN mkdir -v -p /opt/airflow/{logs,dags,plugins,config,data}
RUN mkdir -v -p /opt/db

# Transfer files from host to docker volumes
COPY ./airflow/dags /opt/airflow/dags
COPY ./scripts /opt/scripts

# Change ownership of project files to user airflow
RUN chown -R airflow: /opt/airflow
RUN chown -R airflow: /opt/db

# Install necessary Python libs
USER airflow
RUN pip install duckdb==1.3.0 \
    pyspark==4.0.0 \
    pyarrow==20.0.0