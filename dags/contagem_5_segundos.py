import time
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="contagem_5_segundos",
    description="DAG de teste: espera 5 segundos e finaliza com sucesso",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["estudo", "teste"],
)
def contagem_5_segundos():
    @task
    def contar():
        for i in range(5, 0, -1):
            print(f"faltam {i} segundo(s)...")
            time.sleep(1)
        print("contagem concluída com sucesso")

    contar()


contagem_5_segundos()
