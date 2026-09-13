import io
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

# Preenchido manualmente pelo usuário (nunca versionado, ver .gitignore).
# Template em config/aws_credentials.example.json.
AWS_CREDENTIALS_PATH = "/opt/airflow/config/aws_credentials.json"

# Conexão com o Postgres do app_tcg-pokemon. Valores default batem com
# repos/app_tcg-pokemon/docker-compose.yml; ajustáveis via .env deste projeto.
TCG_POSTGRES_HOST = os.environ.get("TCG_POSTGRES_HOST", "host.docker.internal")
TCG_POSTGRES_PORT = os.environ.get("TCG_POSTGRES_PORT", "25432")
TCG_POSTGRES_DB = os.environ.get("TCG_POSTGRES_DB", "tcg_db")
TCG_POSTGRES_USER = os.environ.get("TCG_POSTGRES_USER", "tcg_user")
TCG_POSTGRES_PASSWORD = os.environ.get("TCG_POSTGRES_PASSWORD", "tcg_pass")

TABLES = [
    "generations",
    "regions",
    "species",
    "forms",
    "tcg_cards",
    "collection_entries",
    "binders",
]


def _load_aws_credentials() -> dict:
    with open(AWS_CREDENTIALS_PATH, encoding="utf-8") as f:
        creds = json.load(f)
    missing = [k for k in ("aws_access_key_id", "aws_secret_access_key", "bucket_name") if not creds.get(k)]
    if missing:
        raise ValueError(
            f"Preencha {AWS_CREDENTIALS_PATH} com os campos: {', '.join(missing)} "
            "(veja config/aws_credentials.example.json)"
        )
    return creds


def _pg_connection():
    return psycopg2.connect(
        host=TCG_POSTGRES_HOST,
        port=TCG_POSTGRES_PORT,
        dbname=TCG_POSTGRES_DB,
        user=TCG_POSTGRES_USER,
        password=TCG_POSTGRES_PASSWORD,
    )


@dag(
    dag_id="export_tcg_pokemon_to_s3",
    description="Exporta as tabelas do Postgres do app_tcg-pokemon para Parquet em um bucket S3",
    # Disparo manual por padrão: evita gravações automáticas no bucket antes
    # de você conferir o resultado. Troque para um cron (ex.: "0 3 * * *")
    # quando quiser agendar.
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tcg-pokemon", "s3", "export"],
)
def export_tcg_pokemon_to_s3():
    @task
    def exportar_tabela(table_name: str) -> str:
        creds = _load_aws_credentials()

        with _pg_connection() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        prefix = creds.get("prefix", "tcg-pokemon").strip("/")
        s3_key = f"{prefix}/{table_name}/dt={run_date}/{table_name}.parquet"

        s3 = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            region_name=creds.get("region_name", "us-east-1"),
        )
        s3.upload_fileobj(buffer, creds["bucket_name"], s3_key)

        logger.info(
            "Tabela %s exportada (%d linhas) para s3://%s/%s",
            table_name,
            len(df),
            creds["bucket_name"],
            s3_key,
        )
        return s3_key

    for table_name in TABLES:
        exportar_tabela.override(task_id=f"exportar_{table_name}")(table_name)


export_tcg_pokemon_to_s3()
