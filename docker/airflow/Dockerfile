FROM apache/airflow:3.3.2-python3.13

# Instalamos as dependências específicas do nosso projeto.
#
# Reinstalar explicitamente a mesma versão do Airflow evita
# que o pip altere a versão do Airflow ao resolver dependências.
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt
