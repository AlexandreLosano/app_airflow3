from datetime import date, datetime

from airflow.sdk import dag, task

from bcb_common import fetch_exchange_rates_for_date, upsert_cotacao


@dag(
    dag_id="DAG_bc_diario",
    description="Ingest diário das cotações BCB PTAX (USD/EUR/GBP) no Postgres local (api.cotacoes)",
    schedule="0 23 * * *",  # Todos os dias às 23h
    start_date=datetime(2024, 10, 7),
    catchup=False,
    tags=["bcb", "cambio", "diario"],
)
def dag_bc_diario():
    @task
    def fetch_and_insert_today():
        hoje = date.today().strftime("%Y-%m-%d")
        documento = fetch_exchange_rates_for_date(hoje)
        upsert_cotacao(documento)

    fetch_and_insert_today()


dag_bc_diario()
