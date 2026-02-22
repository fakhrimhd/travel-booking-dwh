# Airflow with project dependencies (dbt runs in separate container)
FROM apache/airflow:2.9.3-python3.11

USER root

# Install Docker CLI so we can use docker exec
RUN apt-get update && apt-get install -y \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install project requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set working directory
WORKDIR /opt/airflow
