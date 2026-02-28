# Documentación de DAGs (Scrappers)

Este proyecto implementa orquestación de procesos de extracción de datos (web scraping y consumo de APIs) utilizando **Apache Airflow 3.1.7**. Se han desarrollado dos DAGs principales orientados a obtener, procesar y almacenar información diariamente.

---

## 1. DAG: `emol.py` (Noticias Emol)

**Objetivo:** 
Extraer diariamente noticias del portal Emol (El Mercurio) categorizadas en secciones específicas (Nacional, Internacional, Tecnología, Educación y Multimedia). 

**Flujo de trabajo (Tasks):**
1. **`extract_news_links`**: Consulta la API interna (`newsapi.ecn.cl`) para el día de ejecución correspondiente (`logical_date - 1 día`). Itera mediante paginación hasta agotar los resultados de la fecha buscada para cada portal y extrae los enlaces a las noticias. Esta tarea se ejecuta dinámicamente usando Dynamic Task Mapping (`.expand()`) por cada categoría (endpoint).
2. **`flatten_links`**: Aplana la lista de listas de enlaces obtenida de las distintas ejecuciones mapeadas.
3. **`extract_news_data`**: Utilizando los enlaces extraídos, realiza web scraping tradicional obteniendo el HTML de la noticia (a través de `scrapling.Fetcher`). Extrae el título (`titulo_noticia`), la bajada (`bajada_noticia`) y el cuerpo de la noticia analizando la estructura del DOM mediante selectores CSS. Se ejecuta dinámicamente por cada noticia.
4. **`save_data`**: Utiliza un hook de base de datos de manera programática (`PostgresHook`) para conectarse a `news_db_con` e insertar la información extraída en la tabla `emol`. Implementa lógica de *UPSERT* (`ON CONFLICT (id) DO UPDATE`) para evitar registros duplicados.

**Conceptos de Airflow demostrados / Aprendizajes:**
* **TaskFlow API:** Uso fluido de los decoradores `@dag` y `@task`.
* **Dynamic Task Mapping avanzado:** Uso de `.expand()` para paralelizar el descubrimiento de URLs, el recabado del contenido HTML, y la subida transaccional a la base de datos de forma paralela y eficiente.
* Uso de condicionales con repagincación directa dentro de Tasks (bucle `while`).
* Uso directo de *Hooks* (`PostgresHook`) de forma ad-hoc en un decorador `@task` en vez de usar *Operators*, manejando cursores explícitamente.
* Limitación de sobrecarga en ejecución paralela (`max_active_tis_per_dag=2` en el task de scraping).

---

## 2. DAG: `mercadopublico.py` (Licitaciones)

**Objetivo:**
Consultar diariamente la API oficial de Mercado Público (Chile) para obtener las licitaciones generadas el día anterior, extrayendo tanto la cabecera (información principal y del comprador) como el detalle de los ítems involucrados.

**Flujo de trabajo (Tasks):**
1. **`extract_licitaciones`**: Llama a la API general de Mercado Público para la fecha de extracción. Filtra la respuesta retornando solo aquellas licitaciones en estado "Adjudicada" (`CodigoEstado == 8`) con la intención de reducir el límite predeterminado de las tareas mapeadas.
2. **`extract_licitaciones_detalle`**: Tarea paralelizada dinámicamente por cada código de licitación obtenido en el paso anterior. Obtiene campos exhaustivos accediendo nuevamente a la API. Incorpora una ralentización aleatoria (`time.sleep`) para evitar errores por *Rate Limiting* (límite de peticiones) contra el servidor gubernamental.
3. **`extract_licitacion_items`**: Extrae, limpia y expone de manera aplanada en un array los ítems incluidos en la licitación. Limita estricta y temporalmente el retorno al primer ítem del listado (`listado_items[:1]`) para bordear con los topes de tareas dinámicas permitidas por cada DAG en Airflow.
4. **`upsert_licitacion` y `upsert_licitacion_items`**: Ejecutan las sentencias de inserción/actualización de licitaciones y sus ítems respectivamente hacia la base de datos `scrap_db_con`. Utilizan plantillas de SQL (`sql/mercadopublico/...`).

**Conceptos de Airflow demostrados / Aprendizajes:**
* **Uso mixto de APIs API / Operadores Tradicionales:** Pasa los XComs resueltos desde funciones con `@task` a un operador clásico (`SQLExecuteQueryOperator`) que es configurado mediante `.partial()` e invocado usando `.expand()`.
* **Manejo de Secretos/Credenciales Integrado:** Empleo de `Variable.get("MP_TICKET")` dentro del runtime local de Airflow para autenticar las peticiones a la API oficial.
* Patrones de retención intencional (`max_active_tis_per_dag=1` junto con `time.sleep`) como estrategia pasiva *anti-baneo*.

---

> **⚠️ Nota Importante sobre Observabilidad**
> 
> Actualmente la observabilidad (OpenTelemetry - OTEL) en este proyecto no se encuentra completa. OTEL no logra enviar todas las métricas puesto que el envío se realiza en lotes por diseño, y resulta que algunas métricas tienen una latencia o un ciclo de vida tan corto que quedan fuera de dichos lotes, desvaneciéndose antes de su emisión.
>
> Este comportamiento de Apache Airflow ya fue corregido en el repositorio oficial, sin embargo la versión **3.1.7** que se utiliza actualmente aún no contiene este parche. Esta problemática será reevaluada tan pronto la siguiente revisión del orquestador (**presumiblemente la 3.1.8**) sea liberada incluyendo los cambios del pull request [PR #61808](https://github.com/apache/airflow/pull/61808).

---

*Nota: Esta documentación ha sido generada con asistencia de Inteligencia Artificial y posteriormente revisada manualmente para garantizar su exactitud y completitud.*
