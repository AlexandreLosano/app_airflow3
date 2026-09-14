import logging
import os
from datetime import datetime

import psycopg2
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

LOCAL_POSTGRES_HOST = os.environ.get("LOCAL_POSTGRES_HOST", "host.docker.internal")
LOCAL_POSTGRES_PORT = os.environ.get("LOCAL_POSTGRES_PORT", "5432")
LOCAL_POSTGRES_DB = os.environ.get("LOCAL_POSTGRES_DB", "postgres")
LOCAL_POSTGRES_USER = os.environ.get("LOCAL_POSTGRES_USER", "postgres")
LOCAL_POSTGRES_PASSWORD = os.environ.get("LOCAL_POSTGRES_PASSWORD", "")

INSERT_ANO_SQL = """
    INSERT INTO dim.calendario (data, dia, mes, ano, bimestre, trimestre, quadrimestre, dia_semana)
    SELECT
        d::date,
        EXTRACT(DAY FROM d)::smallint,
        EXTRACT(MONTH FROM d)::smallint,
        EXTRACT(YEAR FROM d)::smallint,
        CEIL(EXTRACT(MONTH FROM d) / 2.0)::smallint,
        CEIL(EXTRACT(MONTH FROM d) / 3.0)::smallint,
        CEIL(EXTRACT(MONTH FROM d) / 4.0)::smallint,
        CASE EXTRACT(DOW FROM d)
            WHEN 0 THEN 'Domingo'
            WHEN 1 THEN 'Segunda-feira'
            WHEN 2 THEN 'Terça-feira'
            WHEN 3 THEN 'Quarta-feira'
            WHEN 4 THEN 'Quinta-feira'
            WHEN 5 THEN 'Sexta-feira'
            WHEN 6 THEN 'Sábado'
        END
    FROM generate_series(%(inicio)s::date, %(fim)s::date, '1 day'::interval) AS d
    ON CONFLICT (data) DO NOTHING;
"""


@dag(
    dag_id="DAG_calendario_proximo_ano",
    description="Adiciona o próximo ano em dim.calendario (Postgres local), rodando todo dezembro",
    schedule="0 3 1 12 *",  # 1º de dezembro às 3h
    start_date=datetime(2024, 10, 7),
    catchup=False,
    tags=["calendario", "manutencao", "anual"],
)
def dag_calendario_proximo_ano():
    @task
    def inserir_proximo_ano():
        proximo_ano = datetime.now().year + 1
        inicio = f"{proximo_ano}-01-01"
        fim = f"{proximo_ano}-12-31"

        connection = psycopg2.connect(
            host=LOCAL_POSTGRES_HOST,
            port=LOCAL_POSTGRES_PORT,
            dbname=LOCAL_POSTGRES_DB,
            user=LOCAL_POSTGRES_USER,
            password=LOCAL_POSTGRES_PASSWORD,
        )
        try:
            cursor = connection.cursor()
            cursor.execute(INSERT_ANO_SQL, {"inicio": inicio, "fim": fim})
            linhas = cursor.rowcount
            connection.commit()
            logger.info("Ano %s: %d datas inseridas em dim.calendario", proximo_ano, linhas)
        except Exception:
            connection.rollback()
            logger.exception("Erro ao inserir calendário do ano %s", proximo_ano)
            raise
        finally:
            connection.close()

    inserir_proximo_ano()


dag_calendario_proximo_ano()
