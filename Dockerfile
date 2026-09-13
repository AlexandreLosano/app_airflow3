# syntax=docker/dockerfile:1

# Imagem oficial da Apache Software Foundation, já roda como usuário não-root
# (airflow, uid 50000) e traz o entrypoint padrão que trata migração de banco,
# criação de usuário admin, etc.
ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}

LABEL org.opencontainers.image.title="airflow3-institucional" \
      org.opencontainers.image.description="Airflow 3.x customizado (CeleryExecutor + PostgreSQL) para estudo institucional" \
      org.opencontainers.image.licenses="Apache-2.0"

# Dependências de sistema extras (drivers de banco, libs de conectores, etc.)
# entram aqui, como root, e a camada de apt é limpa na mesma instrução para
# não inflar a imagem final.
# USER root
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends <pacotes> \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*
# USER airflow

# Providers/pacotes Python adicionais além do que já vem na imagem base.
# Fixar versões aqui evita que builds diferentes tragam dependências distintas.
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

EXPOSE 8080
