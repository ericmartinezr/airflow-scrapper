import logging
import pendulum
from itertools import chain
from scrapling import Fetcher
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


# TODO: Revisar cuando sale la nueva version
# OTEL no logra enviar todas las métricas ya que las envia en lotes
# y algunas metricas viven poco tiempo quedando fuera de esos lotes
# Se corrigio en este PR que ya está mergeado pero aun no se librea
# https://github.com/apache/airflow/pull/61808

ENDPOINTS = [
    {
        "categoria": "nacional",
        "url": "https://newsapi.ecn.cl/NewsApi/emol/seccionFiltrada/nacional/0"
    },
    {
        "categoria": "internacional",
        "url": "https://newsapi.ecn.cl/NewsApi/emol/seccionFiltrada/internacional/0"
    },
    {
        "categoria": "tecnologia",
        "url": "https://newsapi.ecn.cl/NewsApi/emol/seccionFiltrada/tecnología/0"
    },
    {
        "categoria": "educacion",
        "url": "https://newsapi.ecn.cl/NewsApi/emol/temaFiltrado/79,109/0"
    },
    {
        "categoria": "multimedia",
        "url": "https://newsapi.ecn.cl/NewsApi/emol/temaFiltrado/960/0"
    }
]


@dag(
    "emol",
    description="Scrapper DAG for Emol",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, 0, 0, 0, tz="UTC"),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=["scrapper"],
    default_args={
        "depends_on_past": False,
        "pool": "emol_pool",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=5)
    }
)
def scrapper():

    @task()
    def extract_news_links(endpoint: dict, **context):
        try:
            categoria = endpoint["categoria"]
            url = endpoint["url"]

            # Fecha en formato YYYY-MM-DD
            start_date = context["logical_date"]
            yesterday = start_date.subtract(days=1)
            record_date = yesterday.format("YYYY-MM-DD")
            params = {
                "size": 5,
                "from": 0,
                "fechaPublicacion": record_date
            }

            logger.info(f"Logical date: {start_date}")
            logger.info(f"Execution date: {record_date}")

            def fetch_data(params):
                news_links = []
                request = Fetcher.get(url, params=params)
                data = request.json()

                hits_news = data["hits"]["hits"]
                if not hits_news:
                    return []

                for hit in hits_news:
                    id = hit["_source"]["id"]
                    permalink = hit["_source"]["permalink"]
                    fecha_publicacion = hit["_source"]["fechaPublicacion"]
                    fecha_modificacion = hit["_source"]["fechaModificacion"]
                    news_links.append({
                        "id": id,
                        "categoria": categoria,
                        "fecha_publicacion": fecha_publicacion,
                        "fecha_modificacion": fecha_modificacion,
                        "link": permalink.replace("http://", "https://")
                    })

                return news_links

            # No se que son los numeros al final, parece que no afectan
            news_links = fetch_data(params)
            if not news_links:
                raise ValueError(
                    f"No news found for date {params['fechaPublicacion']}"
                )

            logger.info(f"Found {len(news_links)} news.")

            data_dates = [
                d["fecha_publicacion"].split("T")[0]
                for d in news_links
            ]

            # Query loop until no date is found in the records
            counter = 1
            while params["fechaPublicacion"] in data_dates:
                logger.debug(f"Iteration {counter}")

                params["from"] += counter * (params["size"] + 1)
                data_news = fetch_data(params)
                if not data_news:
                    raise ValueError(
                        f"No news found for date {params['fechaPublicacion']}. Iteration {counter}"
                    )

                data_dates = [
                    d["fecha_publicacion"].split("T")[0]
                    for d in data_news
                ]

                # In case there's another iteration and the wanted date is not in the returned data
                if params["fechaPublicacion"] not in data_dates:
                    break

                news_links.extend(data_news)
                counter += 1

            return news_links
        except Exception as e:
            logger.error("Error extracting news links")
            logger.error(e, exc_info=True)
            raise AirflowSkipException

    @task()
    def flatten_links(links):
        return list(chain.from_iterable(links))

    @task(max_active_tis_per_dag=2, retries=3)
    def extract_news_data(link_data: dict[str, str]):
        logger.info(f"Link data: {link_data}")
        id = link_data["id"]
        link = link_data["link"]
        categoria = link_data["categoria"]
        fecha_publicacion = link_data["fecha_publicacion"]
        fecha_modificacion = link_data["fecha_modificacion"]

        logger.info(f"Scrapping {link}, with publish date {fecha_publicacion}")

        try:
            page = Fetcher.get(link)
            titulo_noticia = page.css(
                "h1#cuDetalle_cuTitular_tituloNoticia")[0].text
            bajada_noticia = page.css(
                "h2#cuDetalle_cuTitular_bajadaNoticia")[0].text
            texto_noticia = "\n".join([
                tag.get_all_text(strip=True)
                for tag in page.css(
                    "div#cuDetalle_cuTexto_textoNoticia > div"
                )
                if tag.get_all_text(strip=True)
            ])

            return {
                "id": id,
                "categoria": categoria,
                "titulo": titulo_noticia,
                "bajada": bajada_noticia,
                "noticia": texto_noticia,
                "fecha_publicacion": fecha_publicacion,
                "fecha_modificacion": fecha_modificacion
            }

        except Exception as e:
            logger.error("Error extracting news data")
            logger.error(e, exc_info=True)
            raise AirflowSkipException

    @task()
    def save_data(data: dict):
        id = data["id"]
        categoria = data["categoria"]
        titulo = data["titulo"]
        bajada = data["bajada"]
        noticia = data["noticia"]
        fecha_publicacion = data["fecha_publicacion"]
        fecha_modificacion = data["fecha_modificacion"]

        query_insert = f"""
        INSERT INTO emol (id, categoria, titulo, bajada, noticia, fecha_publicacion, fecha_modificacion, fecha_proceso)
        VALUES (%(id)s, %(categoria)s, %(titulo)s, %(bajada)s, %(noticia)s, %(fecha_publicacion)s, %(fecha_modificacion)s, NOW())
        ON CONFLICT (id) DO UPDATE SET titulo = EXCLUDED."titulo", categoria = EXCLUDED."categoria",
        bajada = EXCLUDED."bajada", noticia = EXCLUDED."noticia", 
        fecha_publicacion = EXCLUDED."fecha_publicacion", fecha_modificacion = EXCLUDED."fecha_modificacion", fecha_proceso = EXCLUDED.fecha_proceso
        """

        try:
            pg_hook = PostgresHook(postgres_conn_id="news_db_con")
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            cur.execute(
                query_insert, {
                    "id": id,
                    "categoria": categoria,
                    "titulo": titulo,
                    "bajada": bajada,
                    "noticia": noticia,
                    "fecha_publicacion": fecha_publicacion,
                    "fecha_modificacion": fecha_modificacion
                })
            conn.commit()
            return 0
        except Exception as e:
            logger.error("Error saving data")
            logger.error(e, exc_info=True)
            return 1

    _extract_news_links = extract_news_links.expand(endpoint=ENDPOINTS)
    _flattened_links = flatten_links(links=_extract_news_links)
    _extract_news_data = extract_news_data.expand(
        link_data=_flattened_links
    )
    _save_data = save_data.expand(data=_extract_news_data)
    _save_data


scrapper()
