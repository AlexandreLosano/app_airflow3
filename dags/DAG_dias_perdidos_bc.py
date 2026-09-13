import logging
from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

from bcb_common import datas_existentes, fetch_exchange_rates_for_date, tem_dados_validos, upsert_cotacao

logger = logging.getLogger(__name__)

DIAS_JANELA = 180  # ~6 meses


@dag(
    dag_id="DAG_dias_perdidos_bc",
    description="Verifica e preenche datas faltantes dos últimos 6 meses de cotações do Banco Central",
    schedule="0 16 * * 0",  # Domingos às 16h
    start_date=datetime(2024, 10, 7),
    catchup=False,
    tags=["bcb", "cambio", "manutencao"],
)
def dag_dias_perdidos_bc():
    @task
    def check_and_fill_missing_dates():
        hoje = date.today()
        inicio = hoje - timedelta(days=DIAS_JANELA)

        logger.info("Verificando período de %s até %s", inicio, hoje)

        existentes = datas_existentes(inicio, hoje)
        logger.info("Encontradas %d datas no banco", len(existentes))

        completas = {inicio + timedelta(days=n) for n in range((hoje - inicio).days + 1)}
        faltantes = sorted(completas - existentes)

        logger.info("Identificadas %d datas faltantes", len(faltantes))
        if not faltantes:
            logger.info("Nenhuma data faltante encontrada!")
            return

        sucesso = 0
        sem_dados = 0
        erros = 0
        dias_fds = 0
        dias_uteis_sem_dados = []

        for dia in faltantes:
            data_str = dia.strftime("%Y-%m-%d")
            logger.info("Processando data faltante: %s", data_str)
            documento = fetch_exchange_rates_for_date(data_str)

            if tem_dados_validos(documento):
                if upsert_cotacao(documento):
                    sucesso += 1
                else:
                    erros += 1
            else:
                sem_dados += 1
                if dia.weekday() >= 5:
                    dias_fds += 1
                else:
                    dias_uteis_sem_dados.append(data_str)
                logger.warning("Sem dados disponíveis no BC para %s", data_str)

        logger.info("=== RESUMO ===")
        logger.info("Total de datas faltantes: %d", len(faltantes))
        logger.info("Inseridas com sucesso: %d", sucesso)
        logger.info("Sem dados no BC: %d", sem_dados)
        logger.info("  - %d dias em FDS", dias_fds)
        logger.info("  - %d dias úteis sem dados", len(dias_uteis_sem_dados))
        logger.info("Erros: %d", erros)
        if dias_uteis_sem_dados:
            logger.info("Dias úteis sem dados no BC:")
            for data_str in dias_uteis_sem_dados:
                logger.info("  - %s", data_str)

    check_and_fill_missing_dates()


dag_dias_perdidos_bc()
