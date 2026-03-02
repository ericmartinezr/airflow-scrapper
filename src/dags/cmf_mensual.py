import logging
import pendulum
from itertools import chain
from scrapling import Fetcher
from datetime import timedelta
from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

INDICADORES = ["utm", "ipc"]
ENDPOINT = "https://api.cmfchile.cl/api-sbifv3/recursos_api/{indicador}/{year}/{month}"


@dag(
    "cmf_mensual",
    description="Scrapper DAG for CMF (mensual)",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, tz="UTC"),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=["scrapper"],
    default_args={
        "depends_on_past": False,
        "pool": "cmf_pool",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=5)
    }
)
def scrapper():

    @task()
    def extract_indicadores(indicador: str, **context):
        try:
            api_key = Variable.get("CMF_APIKEY", None)
            if not api_key:
                raise ValueError("¡Debes asignar la key!")

            logical_date = context["logical_date"]
            date = logical_date.format("YYYY-MM").split("-")
            year, month = date

            logger.info(f"Logical date: {logical_date}")

            params = {
                "apikey": api_key,
                "formato": "json"
            }

            endpoint = ENDPOINT.format(
                indicador=indicador,
                year=year,
                month=month
            )

            indicador_map = {
                "utm": "UTMs",
                "ipc": "IPCs"
            }

            result = Fetcher.get(endpoint, params=params)
            data = result.json()

            if "CodigoError" in data:
                logger.info(f"Data {data}")
                raise ValueError(
                    f"Error al consultar por indicador {indicador}")

            values = data[indicador_map[indicador]]
            if not values:
                raise ValueError(
                    f"No hay valores para el indicador {indicador} en {logical_date}")

            return [
                {
                    "indicador": indicador,
                    # Transformar el valor para que lo acepte la tabla, quitar el punto si tiene y transformar la coma en punto ti eiene
                    "valor": d["Valor"].replace(".", "").replace(",", "."),
                    "fecha_valor": d["Fecha"]
                }
                for d in values
            ]

        except Exception as e:
            logger.error(f"Error extrayendo indicador {indicador}")
            logger.error(e, exc_info=True)
            raise AirflowSkipException

    @task()
    def flatten_indicadores(indicadores):
        return list(chain.from_iterable(indicadores))

    upsert_indicadores = SQLExecuteQueryOperator.partial(
        task_id="upsert_indicadores",
        conn_id="scrap_db_con",
        sql="sql/cmf/upsert_indicadores.sql",
        autocommit=True,
    )

    _extract_indicadores = extract_indicadores.expand(indicador=INDICADORES)
    _flatten_indicadores = flatten_indicadores(
        indicadores=_extract_indicadores
    )
    _upsert_indicadores = upsert_indicadores.expand(
        parameters=_flatten_indicadores
    )
    _upsert_indicadores


scrapper()
