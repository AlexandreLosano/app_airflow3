from datetime import date, datetime

from airflow.sdk import dag, task

from bcb_common import fetch_exchange_rates_by_range, upsert_cotacao

ANO = 2026


@dag(
    dag_id="DAG_historica_bc_2026",
    description="Ingest cotações BCB PTAX (USD/EUR/GBP) de 2026, do dia 1 até hoje, no Postgres local (api.cotacoes)",
    schedule=None,
    start_date=datetime(2025, 12, 31),
    catchup=False,
    tags=["bcb", "cambio", "historico"],
)
def dag_historica_bc_2026():
    @task
    def fetch_data_2026():
        # Limita ao dia de hoje: o BCB não tem cotação para datas futuras, e a
        # DAG_bc_diario / DAG_dias_perdidos_bc cuidam do restante do ano.
        fim = min(date.today(), date(ANO, 12, 31))
        dados = fetch_exchange_rates_by_range(f"{ANO}-01-01", fim.strftime("%Y-%m-%d"))
        for documento in dados.values():
            upsert_cotacao(documento)

    fetch_data_2026()


dag_historica_bc_2026()
