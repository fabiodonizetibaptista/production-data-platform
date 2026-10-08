FROM python:3.13-slim

ARG DBT_CORE_VERSION=1.12.5
ARG DBT_DUCKDB_VERSION=1.11.0
ARG DBT_DATABRICKS_VERSION=1.12.6

# Dependência operacional usada pelo dbt para diagnóstico
# e para instalação de packages baseados em repositórios Git.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dbt-core==${DBT_CORE_VERSION}" \
    "dbt-duckdb==${DBT_DUCKDB_VERSION}" \
    "dbt-databricks==${DBT_DATABRICKS_VERSION}"

WORKDIR /workspace/dbt

ENTRYPOINT ["dbt"]
