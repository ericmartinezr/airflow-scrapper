# Documentación de DAGs (Scrappers)

Este proyecto implementa orquestación de procesos de extracción de datos (web scraping y consumo de APIs) utilizando **Apache Airflow 3.1.8**. Se han desarrollado dos DAGs principales orientados a obtener, procesar y almacenar información diariamente.

---

## 1. DAG: `emol.py` (Noticias Emol)

**Objetivo:**
Extraer diariamente noticias del portal Emol (El Mercurio) categorizadas en secciones específicas (Nacional, Internacional, Tecnología, Educación y Multimedia).

**Flujo de trabajo (Tasks):**

1. **`extract_news_links`**: Consulta la API interna (`newsapi.ecn.cl`) para el día de ejecución correspondiente (`logical_date - 1 día`). Itera mediante paginación hasta agotar los resultados de la fecha buscada para cada portal y extrae los enlaces a las noticias. Esta tarea se ejecuta dinámicamente usando Dynamic Task Mapping (`.expand()`) por cada categoría (endpoint).
2. **`flatten_links`**: Aplana la lista de listas de enlaces obtenida de las distintas ejecuciones mapeadas.
3. **`extract_news_data`**: Utilizando los enlaces extraídos, realiza web scraping tradicional obteniendo el HTML de la noticia (a través de `scrapling.Fetcher`). Extrae el título (`titulo_noticia`), la bajada (`bajada_noticia`) y el cuerpo de la noticia analizando la estructura del DOM mediante selectores CSS. Se ejecuta dinámicamente por cada noticia.
4. **`save_data`**: Utiliza un hook de base de datos de manera programática (`PostgresHook`) para conectarse a `news_db_con` e insertar la información extraída en la tabla `emol`. Implementa lógica de _UPSERT_ (`ON CONFLICT (id) DO UPDATE`) para evitar registros duplicados.

**Conceptos de Airflow demostrados / Aprendizajes:**

- **TaskFlow API:** Uso fluido de los decoradores `@dag` y `@task`.
- **Dynamic Task Mapping avanzado:** Uso de `.expand()` para paralelizar el descubrimiento de URLs, el recabado del contenido HTML, y la subida transaccional a la base de datos de forma paralela y eficiente.
- Uso de condicionales con repagincación directa dentro de Tasks (bucle `while`).
- Uso directo de _Hooks_ (`PostgresHook`) de forma ad-hoc en un decorador `@task` en vez de usar _Operators_, manejando cursores explícitamente.
- Limitación de sobrecarga en ejecución paralela (`max_active_tis_per_dag=2` en el task de scraping).

---

## 2. DAG: `mercadopublico.py` (Licitaciones)

**Objetivo:**
Consultar diariamente la API oficial de Mercado Público (Chile) para obtener las licitaciones generadas el día anterior, extrayendo tanto la cabecera (información principal y del comprador) como el detalle de los ítems involucrados.

**Flujo de trabajo (Tasks):**

1. **`extract_licitaciones`**: Llama a la API general de Mercado Público para la fecha de extracción. Filtra la respuesta retornando solo aquellas licitaciones en estado "Adjudicada" (`CodigoEstado == 8`) con la intención de reducir el límite predeterminado de las tareas mapeadas.
2. **`extract_licitaciones_detalle`**: Tarea paralelizada dinámicamente por cada código de licitación obtenido en el paso anterior. Obtiene campos exhaustivos accediendo nuevamente a la API. Incorpora una ralentización aleatoria (`time.sleep`) para evitar errores por _Rate Limiting_ (límite de peticiones) contra el servidor gubernamental.
3. **`extract_licitacion_items`**: Extrae, limpia y expone de manera aplanada en un array los ítems incluidos en la licitación. Limita estricta y temporalmente el retorno al primer ítem del listado (`listado_items[:1]`) para bordear con los topes de tareas dinámicas permitidas por cada DAG en Airflow.
4. **`upsert_licitacion` y `upsert_licitacion_items`**: Ejecutan las sentencias de inserción/actualización de licitaciones y sus ítems respectivamente hacia la base de datos `scrap_db_con`. Utilizan plantillas de SQL (`sql/mercadopublico/...`).

**Conceptos de Airflow demostrados / Aprendizajes:**

- **Uso mixto de APIs API / Operadores Tradicionales:** Pasa los XComs resueltos desde funciones con `@task` a un operador clásico (`SQLExecuteQueryOperator`) que es configurado mediante `.partial()` e invocado usando `.expand()`.
- **Manejo de Secretos/Credenciales Integrado:** Empleo de `Variable.get("MP_TICKET")` dentro del runtime local de Airflow para autenticar las peticiones a la API oficial.
- Patrones de retención intencional (`max_active_tis_per_dag=1` junto con `time.sleep`) como estrategia pasiva _anti-baneo_.

---

## 3. DAG: `cmf_diario.py` (Indicadores Económicos Diarios — CMF)

**Objetivo:**
Consultar diariamente la API pública de la Comisión para el Mercado Financiero (CMF Chile) para obtener el valor del **Dólar**, el **Euro** y la **UF** correspondientes al día de ejecución. Almacena los valores en base de datos para su consulta histórica.

**Flujo de trabajo (Tasks):**

1. **`extract_indicadores`**: Llama al endpoint diario de la API de la CMF (`/api-sbifv3/recursos_api/{indicador}/{year}/{month}/dias/{day}`) para cada indicador. Autentica mediante la Variable de Airflow `CMF_APIKEY`. Transforma el campo `Valor` (formato chileno con punto de miles y coma decimal) a un número con punto decimal estándar. Retorna una lista de diccionarios con los campos `indicador`, `valor` y `fecha_valor`. Esta tarea se ejecuta dinámicamente (`.expand()`) una vez por cada indicador: `dolar`, `euro` y `uf`. En caso de error (sin API Key, respuesta con `CodigoError`, lista vacía o fallo de red), la tarea es omitida con `AirflowSkipException`.
2. **`flatten_indicadores`**: Aplana la lista de listas resultante de las ejecuciones paralelas de `extract_indicadores` en una sola lista plana, lista para ser expandida en el siguiente operador.
3. **`upsert_indicadores`**: Ejecuta la sentencia SQL de inserción/actualización (`sql/cmf/upsert_indicadores.sql`) en la base de datos `scrap_db_con` a través de `SQLExecuteQueryOperator`. Se expande dinámicamente para procesar cada registro de indicador por separado.

**Conceptos de Airflow demostrados / Aprendizajes:**

- **Consumo de API REST autenticada:** Uso de `Variable.get("CMF_APIKEY")` para inyectar de forma segura la llave de API en cada petición.
- **Dynamic Task Mapping con indicadores heterogéneos:** Expansión paralela de un mismo task sobre una lista fija de strings (`INDICADORES`), donde cada instancia consulta un recurso distinto de la misma API.
- **Integración TaskFlow + SQLExecuteQueryOperator:** Paso de resultados de un `@task` Python directamente como parámetros de un operador clásico usando `.partial()` + `.expand()`.
- **Transformación de formatos numéricos regionales:** Normalización de valores en formato chileno (`1.050,25` → `105025`) dentro del propio task de extracción antes de persistir.
- **Manejo defensivo de errores con skip:** Uso de `AirflowSkipException` como mecanismo de control de flujo para días sin datos (feriados, fines de semana) sin marcar el DAGRun como fallido.

---

## 4. DAG: `cmf_mensual.py` (Indicadores Económicos Mensuales — CMF)

**Objetivo:**
Consultar mensualmente la API pública de la CMF Chile para obtener el valor de la **UTM** (Unidad Tributaria Mensual) y el **IPC** (Índice de Precios al Consumidor) correspondientes al mes de ejecución. Almacena los valores en base de datos para consulta histórica y referencia tributaria.

**Flujo de trabajo (Tasks):**

1. **`extract_indicadores`**: Llama al endpoint mensual de la API de la CMF (`/api-sbifv3/recursos_api/{indicador}/{year}/{month}`) para cada indicador. Autentica mediante la Variable de Airflow `CMF_APIKEY`. Aplica la misma transformación de valores que el DAG diario (puntos de miles eliminados, coma decimal convertida a punto). Retorna una lista de diccionarios con `indicador`, `valor` y `fecha_valor`. Se expande dinámicamente sobre los indicadores `utm` e `ipc`. Ante cualquier error lanza `AirflowSkipException`.
2. **`flatten_indicadores`**: Aplana las listas de resultados de ambos indicadores en una lista plana única.
3. **`upsert_indicadores`**: Inserta o actualiza los registros en la base de datos `scrap_db_con` usando `SQLExecuteQueryOperator` con la misma plantilla SQL que el DAG diario (`sql/cmf/upsert_indicadores.sql`).

**Diferencias clave respecto a `cmf_diario.py`:**
| Aspecto | `cmf_diario` | `cmf_mensual` |
|---|---|---|
| **Schedule** | `0 0 * * *` (diario) | `@monthly` |
| **Indicadores** | `dolar`, `euro`, `uf` | `utm`, `ipc` |
| **Endpoint** | `.../dias/{day}` | Solo año y mes |
| **Granularidad de fecha** | `YYYY-MM-DD` | `YYYY-MM` |

**Conceptos de Airflow demostrados / Aprendizajes:**

- **Reutilización de patrones:** Estructura idéntica a `cmf_diario` demostrando cómo un mismo patrón de diseño (extract → flatten → upsert con Dynamic Task Mapping) se adapta fácilmente a distintas frecuencias y fuentes de datos.
- **`catchup=True` en ejecuciones mensuales:** Permite rellenar períodos históricos retroactivamente desde la `start_date`, útil para poblar bases de datos con series de tiempo de indicadores como el IPC.
- **Separación de responsabilidades por frecuencia:** Los indicadores diarios y mensuales se gestionan en DAGs independientes con sus propios pools y schedules, evitando acoplamiento entre lógicas de distinta cadencia.

_Nota: Esta documentación ha sido generada con asistencia de Inteligencia Artificial y posteriormente revisada manualmente para garantizar su exactitud y completitud._
