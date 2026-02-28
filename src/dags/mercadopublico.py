import logging
import pendulum
import time
import random
from scrapling import Fetcher
from datetime import timedelta
from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


logger = logging.getLogger(__name__)

ENDPOINT_LICITACION = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"


@dag(
    "mercadopublico",
    description="Scrapper DAG for MercadoPublico",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, 0, 0, 0, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["scrapper"],
    default_args={
        "depends_on_past": False,
        "pool": "mercadopublico_pool",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=5)
    }
)
def scrapper():

    @task()
    def extract_licitaciones(**context):
        """
        Extrae las licitaciones usando la API de MercadoPublico
        """
        try:
            start_date = context["logical_date"]
            yesterday = start_date.subtract(days=1)
            record_date = yesterday.format("DDMMYYYY")

            logger.info(f"Logical date: {start_date}")
            logger.info(f"Execution date: {record_date}")

            ticket = Variable.get("MP_TICKET", None)
            if not ticket:
                raise ValueError("¡Debes asignar el ticket!")

            params = {
                "fecha": record_date,
                "ticket": ticket
            }

            request = Fetcher.get(ENDPOINT_LICITACION, params=params)
            data = request.json()

            # Error al consultar, conexiones simulteaneas?
            if "Codigo" in data and "Mensaje" in data:
                raise ValueError(
                    f"Error al consultar: \nCodigo: {data["Codigo"]}\n{data["Mensaje"]}"
                )

            # Retorna solo el codigo externo
            # El detalle es en una tarea siguiente
            if not data["Listado"]:
                raise ValueError(
                    f"No existen licitaciones para la fecha {record_date}")

            # Filtra por estado
            # Debido a la cantidad de licitaciones que devuelve en ocasiones
            # la opcion es filtrar por codigo de estado. De esta forma reduzco el limite (configurable) de mapped tasks.
            # Publicada = "5"
            # Cerrada = "6"
            # Desierta = "7"
            # Adjudicada = "8"
            # Revocada = "18"
            # Suspendida = "19"

            # Otra opcion es trabajar con archivos pero es probablemente implicaría perder el paralelismo de airflow
            return [d["CodigoExterno"] for d in data["Listado"] if d["CodigoEstado"] == 8]

        except Exception as e:
            logger.error("Error extrayendo licitaciones")
            logger.error(e, exc_info=True)
            raise AirflowSkipException

    @task(max_active_tis_per_dag=1, retries=3)
    def extract_licitaciones_detalle(codigo: str, **context):
        """
        Extrae el detalle de la licitacion según su código

        Args:
        - codigo (str): Código de la licitación
        """
        rand_wait = random.uniform(2.5, 3.5)
        logger.info(f"Esperando {rand_wait} segundos porque el rate limit :)")
        time.sleep(rand_wait)

        logger.info(f"Procesando licitacion con codigo {codigo}")

        try:
            ticket = Variable.get("MP_TICKET", None)
            if not ticket:
                raise ValueError("¡Debes asignar el ticket!")

            params = {
                "codigo": codigo,
                "ticket": ticket
            }

            request = Fetcher.get(ENDPOINT_LICITACION, params=params)
            data = request.json()

            # Error al consultar, conexiones simulteaneas?
            if "Codigo" in data and "Mensaje" in data:
                raise ValueError(
                    f"Error al consultar: \nCodigo: {data["Codigo"]}\n{data["Mensaje"]}"
                )

            fecha_proceso = context["logical_date"]
            licitacion = data["Listado"][0]
            licitacion_comprador = licitacion["Comprador"]
            licitacion_fechas = licitacion["Fechas"]
            return {
                "codigo": codigo,
                "nombre": licitacion["Nombre"],
                "descripcion": licitacion["Descripcion"],
                "estado": licitacion["Estado"],
                "codigo_comprador": licitacion_comprador["CodigoOrganismo"],
                "nombre_comprador": licitacion_comprador["NombreOrganismo"],
                "rut_unidad_comprador": licitacion_comprador["RutUnidad"],
                "codigo_unidad_comprador": licitacion_comprador["CodigoUnidad"],
                "nombre_unidad_comprador": licitacion_comprador["NombreUnidad"],
                "direccion_unidad_comprador": licitacion_comprador["DireccionUnidad"],
                "comuna_unidad_comprador": licitacion_comprador["ComunaUnidad"],
                "region_unidad_comprador": licitacion_comprador["RegionUnidad"],
                "rut_usuario_comprador": licitacion_comprador["RutUsuario"],
                "codigo_usuario_comprador": licitacion_comprador["CodigoUsuario"],
                "nombre_usuario_comprador": licitacion_comprador["NombreUsuario"],
                "cargo_usuario_comprador": licitacion_comprador["CargoUsuario"],
                "codigo_tipo_licitacion": licitacion["CodigoTipo"],
                "tipo_licitacion": licitacion["Tipo"],
                "tipo_convocatoria": licitacion["TipoConvocatoria"],
                "estimacion": licitacion["Estimacion"],
                "monto_estimado": licitacion["MontoEstimado"],
                "moneda": licitacion["Moneda"],
                "modalidad": licitacion["Modalidad"],
                "unidad_tiempo_duracion_contrato": licitacion["UnidadTiempoDuracionContrato"],
                "tiempo_duracion_contrato": licitacion["TiempoDuracionContrato"],
                "fecha_creacion": licitacion_fechas["FechaCreacion"],
                "fecha_cierre": licitacion_fechas["FechaCierre"],
                "fecha_inicio": licitacion_fechas["FechaInicio"],
                "fecha_final": licitacion_fechas["FechaFinal"],
                "fecha_proceso": fecha_proceso,
                "listado_items": licitacion["Items"]["Listado"]
            }

        except Exception as e:
            logger.error(
                f"Error extrayendo el detalle de la licitacion {codigo}")
            logger.error(e, exc_info=True)
            raise AirflowSkipException

    @task()
    def extract_licitacion_items(licitaciones, **context):
        all_items = []
        fecha_proceso = context["logical_date"]
        for licitacion in licitaciones:
            logger.info(
                f"Se encontraron {len(licitacion['listado_items'])} items en la licitacion")
            all_items.extend([
                {
                    "correlativo": item.get("Correlativo", 0),
                    "codigo_licitacion": licitacion["codigo"],
                    "codigo_producto": item.get("CodigoProducto", ""),
                    "cantidad": item.get("Cantidad", 0.0),
                    "categoria": item.get("Categoria", ""),
                    "codigo_categoria": item.get("CodigoCategoria", 0),
                    "descripcion": item.get("Descripcion", ""),
                    "unidad_medida": item.get("UnidadMedida", ""),
                    "nombre_producto": item.get("NombreProducto", ""),
                    "rut_proveedor": (item.get("Adjudicacion") or {}).get("RutProveedor", ""),
                    "nombre_proveedor": (item.get("Adjudicacion") or {}).get("NombreProveedor", ""),
                    "cantidad_adjudicada": (item.get("Adjudicacion") or {}).get("NombreProveedor", 0),
                    "monto_unitario": (item.get("Adjudicacion") or {}).get("MontoUnitario", 0),
                    "fecha_proceso": fecha_proceso
                }
                # Considera solo 1 item ya que pueden ser muchos y supera el limite de mapped tasks de Airflow
                for item in licitacion["listado_items"][:1]
            ])
        return all_items

    upsert_licitacion = SQLExecuteQueryOperator.partial(
        task_id="upsert_licitacion",
        conn_id="scrap_db_con",
        sql="sql/mercadopublico/upsert_licitacion.sql",
        autocommit=True,
    )

    upsert_licitacion_items = SQLExecuteQueryOperator.partial(
        task_id="upsert_licitacion_items",
        conn_id="scrap_db_con",
        sql="sql/mercadopublico/upsert_licitacion_items.sql",
        autocommit=True,
    )

    _extract_licitaciones = extract_licitaciones()
    _extract_licitaciones_detalle = extract_licitaciones_detalle.expand(
        codigo=_extract_licitaciones
    )
    _extract_licitacion_items = extract_licitacion_items(
        licitaciones=_extract_licitaciones_detalle
    )
    _upsert_licitacion = upsert_licitacion.expand(
        parameters=_extract_licitaciones_detalle
    )
    _upsert_licitacion_items = upsert_licitacion_items.expand(
        parameters=_extract_licitacion_items
    )

    _upsert_licitacion
    _upsert_licitacion_items


scrapper()
