# Standalone dbt container for running transformations
FROM python:3.11-slim

# Install dbt with pinned protobuf to avoid MessageToJson bug
RUN pip install --no-cache-dir \
    dbt-core==1.7.4 \
    dbt-postgres==1.7.4 \
    "protobuf>=4.0.0,<5.0.0"

WORKDIR /dbt

# Default command
CMD ["dbt", "--version"]
