from datetime import datetime

from airflow.sdk import dag, task

from bcb_common import fetch_exchange_rates_by_range, upsert_cotacao

ANO_INICIAL = 1994
ANO_FINAL = 2025


@dag(
    dag_id="DAG_historica_bc_94ate25",
    description="Ingest historico de cambio (BCB PTAX, USD/EUR/GBP) de 1994 a 2025 no Postgres local (api.cotacoes)",
    schedule=None,
    start_date=datetime(2024, 9, 30),
    catchup=False,
    tags=["bcb", "cambio", "historico"],
)
def dag_historica_bc_94ate25():
    @task
    def fetch_data_ano(ano: int):
        dados = fetch_exchange_rates_by_range(f"{ano}-01-01", f"{ano}-12-31")
        for documento in dados.values():
            upsert_cotacao(documento)

    # Uma task por ano, encadeadas em sequência (1994 -> 2025) para não
    # sobrecarregar a API do BCB com dezenas de anos em paralelo.
    tasks = [fetch_data_ano.override(task_id=f"fetch_data_{ano}")(ano) for ano in range(ANO_INICIAL, ANO_FINAL + 1)]
    for anterior, proximo in zip(tasks, tasks[1:]):
        anterior >> proximo


dag_historica_bc_94ate25()
