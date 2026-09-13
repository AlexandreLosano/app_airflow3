import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
import requests

logger = logging.getLogger(__name__)

# Conexão com o Postgres local generico (toolData_postgre). Valores default
# batem com repos/toolData_postgre/docker-compose.yml; ajustaveis via .env.
LOCAL_POSTGRES_HOST = os.environ.get("LOCAL_POSTGRES_HOST", "host.docker.internal")
LOCAL_POSTGRES_PORT = os.environ.get("LOCAL_POSTGRES_PORT", "5432")
LOCAL_POSTGRES_DB = os.environ.get("LOCAL_POSTGRES_DB", "postgres")
LOCAL_POSTGRES_USER = os.environ.get("LOCAL_POSTGRES_USER", "postgres")
LOCAL_POSTGRES_PASSWORD = os.environ.get("LOCAL_POSTGRES_PASSWORD", "")

MOEDAS = ["USD", "EUR", "GBP"]


def pg_connection():
    return psycopg2.connect(
        host=LOCAL_POSTGRES_HOST,
        port=LOCAL_POSTGRES_PORT,
        dbname=LOCAL_POSTGRES_DB,
        user=LOCAL_POSTGRES_USER,
        password=LOCAL_POSTGRES_PASSWORD,
    )


def fetch_exchange_rates_for_date(data_cotacao: str) -> dict:
    """Busca as cotações de USD/EUR/GBP do BCB (PTAX) para uma data 'YYYY-MM-DD'."""
    data_api = datetime.strptime(data_cotacao, "%Y-%m-%d").strftime("%m-%d-%Y")
    documento = {"Data": data_cotacao}

    for moeda in MOEDAS:
        url_base = (
            "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            f"CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)?@moeda='{moeda}'"
            f"&@dataCotacao='{data_api}'&$filter=tipoBoletim%20eq%20'Fechamento PTAX'&$format=json"
        )
        try:
            response = requests.get(url_base, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data and len(data["value"]) > 0:
                    documento[moeda] = data["value"][0]["cotacaoCompra"]
                    logger.info("Valor da %s em %s: %s", moeda, data_cotacao, documento[moeda])
                else:
                    documento[moeda] = "Dados não disponíveis"
            else:
                documento[moeda] = f"Erro {response.status_code}"
        except requests.exceptions.RequestException as exc:
            logger.error("Erro na requisição para %s em %s: %s", moeda, data_cotacao, exc)
            documento[moeda] = "Erro na requisição"
        except (json.JSONDecodeError, KeyError) as exc:
            logger.error("Erro ao processar resposta para %s em %s: %s", moeda, data_cotacao, exc)
            documento[moeda] = "Erro ao processar a resposta"

    return documento


def fetch_exchange_rates_by_range(data_inicial: str, data_final: str) -> dict:
    """Busca cotações dia a dia num intervalo ['YYYY-MM-DD', 'YYYY-MM-DD']."""
    inicio = datetime.strptime(data_inicial, "%Y-%m-%d")
    fim = datetime.strptime(data_final, "%Y-%m-%d")
    exchange_rates = {}
    atual = inicio
    while atual <= fim:
        data_str = atual.strftime("%Y-%m-%d")
        exchange_rates[data_str] = fetch_exchange_rates_for_date(data_str)
        atual += timedelta(days=1)
    return exchange_rates


def _valor_valido(documento: dict, moeda: str):
    valor = documento.get(moeda)
    if valor in ("Dados não disponíveis", None) or str(valor).startswith("Erro"):
        return None
    return valor


def tem_dados_validos(documento: dict) -> bool:
    return any(_valor_valido(documento, moeda) is not None for moeda in MOEDAS)


def upsert_cotacao(documento: dict) -> bool:
    """Insere/atualiza uma linha em api.cotacoes a partir de {'Data', 'USD', 'EUR', 'GBP'}."""
    connection = None
    cursor = None
    try:
        connection = pg_connection()
        cursor = connection.cursor()
        sql = """
            INSERT INTO api.cotacoes (dt_cotacao, vlr_usd, vlr_eur, vlr_gbp, dt_atualizacao)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (dt_cotacao)
            DO UPDATE SET
                vlr_usd = EXCLUDED.vlr_usd,
                vlr_eur = EXCLUDED.vlr_eur,
                vlr_gbp = EXCLUDED.vlr_gbp,
                dt_atualizacao = NOW();
        """
        cursor.execute(sql, (
            documento["Data"],
            _valor_valido(documento, "USD"),
            _valor_valido(documento, "EUR"),
            _valor_valido(documento, "GBP"),
        ))
        connection.commit()
        logger.info("Dados inseridos/atualizados no PostgreSQL: %s", documento)
        return True
    except Exception:
        if connection:
            connection.rollback()
        logger.exception("Erro ao inserir no PostgreSQL para %s", documento.get("Data"))
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def datas_existentes(data_inicio, data_fim) -> set:
    """Retorna o conjunto de datas já presentes em api.cotacoes no intervalo [data_inicio, data_fim]."""
    connection = pg_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT dt_cotacao FROM api.cotacoes WHERE dt_cotacao >= %s AND dt_cotacao <= %s ORDER BY dt_cotacao;",
            (data_inicio, data_fim),
        )
        return {row[0] for row in cursor.fetchall()}
    finally:
        connection.close()
