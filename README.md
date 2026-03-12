# Airflow Scrapper

Proyecto de scraping de noticias y licitaciones utilizando **Apache Airflow 3.1.8**.
Pensado para ser ejecutado en un entorno local con **WSL2**.
En un futuro se podrían agregar más DAGs para extraer información de otros sitios de interés.

> Para ver el detalle técnico y la justificación de los DAGs, revisa el archivo [DAGS.md](DAGS.md).

---

## 🚀 Requisitos e Instalación

### 1. Prerrequisitos

Asegúrate de contar con Python y PostgreSQL en tu entorno local (además de Docker, en caso de querer levantar el stack de observabilidad local).

### 2. Instalación de Airflow y dependencias

Para instalar la versión exacta del orquestador y los paquetes esenciales de este proyecto, ejecuta:

```sh
AIRFLOW_VERSION=3.1.8
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow[postgres,fab,otel]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install psycopg2==2.9.11
pip install "scrapling[fetchers]==0.4"
```

---

## ⚙️ Configuración del Entorno

### Actualizar configuración (Opcional, si vienes de instalación previa)

Si experimentas errores solicitando la variable `api_auth/jwt_secret`, se recomienda respaldar la carpeta actual `~/airflow` e iniciar el servidor nuevamente para generar la configuración limpia. Alternativamente, puedes intentar:

```sh
airflow config update --fix
```

### Habilitar Métricas (OpenTelemetry)

Para la recolección, almacenamiento y visualización integral de la telemetría, este proyecto hace uso de la imagen oficial de Docker [docker-otel-lgtm](https://github.com/grafana/docker-otel-lgtm) provista por Grafana. Esta imagen preconfigura en un solo entorno un ecosistema de observabilidad robusto (Loki, Grafana, Tempo y Mimir) nativamente compatible con OpenTelemetry.

#### 1. Gestión del Stack de Observabilidad

La configuración de la imagen se encuentra estructurada y provisionada en el archivo `compose.yml` en la raíz del repositorio. Para interactuar con él, requiere tener Docker activo en el sistema host.

- **Para desplegar e inicializar los servicios en segundo plano:**
  ```sh
  docker compose up -d
  ```
  _Una vez levantados los servicios, puedes acceder a la interfaz de Grafana desde tu navegador ingresando a [http://localhost:3000](http://localhost:3000) (Usuario: `admin` / Contraseña: `admin`). Ver **Generación de usuario administrador** para más información._

- **Para detener la ejecución (cancelar) y remover los recursos generados:**
  ```sh
  docker compose down
  ```

#### 2. Configurar la exportación en Airflow

Una vez que el receptor local se encuentra listo y escuchando peticiones, debes indicarle al orquestador que comience a emitir su telemetría. Para habilitarlo, debes modificar o añadir el siguiente bloque de configuración en tu archivo `~/airflow/airflow.cfg`:

```ini
[metrics]
otel_on = True
otel_host = localhost
# Puertos estándar configurados en docker-otel-lgtm: HTTP = 4318, gRPC = 4317
otel_port = 4318
otel_prefix = airflow
otel_interval_milliseconds = 30000
otel_ssl_active = False
```

> **Referencias:**
>
> - [Setup OpenTelemetry (Airflow Docs)](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html#setup-opentelemetry)
> - [Docker OTEL LGTM (Grafana)](https://github.com/grafana/docker-otel-lgtm)

---

## 🗄️ Preparación de Base de Datos

### 1. Configurar Cadena de Conexión principal

Primero, identifica la conexión actual utilizada por Airflow para su metabase:

```sh
airflow config get-value database sql_alchemy_conn
```

Edita tu archivo de configuración `~/airflow/airflow.cfg` y ajusta la cadena según tu base de datos de scraping:

```ini
# Buscar la siguiente variable y asignar los valores de tu entorno:
sql_alchemy_conn=postgresql+psycopg2://postgres:p4ssw0rd@localhost:5432/airflow_scrapper
```

### 2. Configurar Backend XCom Personalizado (Opcional, Recomendado)

Debido al volumen de datos (HTML extraído y listas largas), es prudente derivar los objetos XCom al disco en lugar de la base de datos principal, para evitar colapsarla.

```sh
# Crea el directorio de almacenamiento
mkdir -p ~/airflow/xcom
```

Luego, en `~/airflow/airflow.cfg`:

```ini
[core]
# Utiliza el Object Storage Backend nativo
xcom_backend=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
xcom_objectstorage_path=file:///home/eric/airflow/xcom

# 0 para almacenar TODO el XCom en disco.
# -1 para almacenar en la Base de Datos.
# Un valor > 0 genera un sistema híbrido (menor a X bytes va a BD, mayor a X va al disco)
xcom_objectstorage_threshold=0

# Límite de tareas dinámicas mapeadas por DAG
# max_map_length=1024
```

> **Referencia:** [XCom Backend (Airflow Docs)](https://airflow.apache.org/docs/apache-airflow-providers-common-io/stable/xcom_backend.html)

### 3. Ejecutar Migraciones

Una vez configurado todo el acceso a base de datos, ejecuta:

```sh
airflow db migrate
```

---

## 🔐 Generación de Usuario Administrador

Este proyecto utiliza el gestor por defecto (`Simple Auth Manager`). Al inicializar Airflow en modo standalone, se creará automáticamente un usuario y una contraseña local.

1. Identifica el usuario generado en `~/airflow/airflow.cfg` (formato `<usuario>:<rol>`):

   ```sh
   grep simple_auth_manager_users ~/airflow/airflow.cfg
   # Ejemplo: simple_auth_manager_users = admin:admin
   ```

2. Obtén la contraseña en el archivo secreto generado:
   ```sh
   cat ~/airflow/simple_auth_manager_passwords.json.generated
   # Ejemplo: {"admin": "tu_password_autogenerado"}
   ```

_(Nota: Puedes editar libremente tu contraseña en dicho archivo JSON)_

> **Referencia:** [Manage Users - Simple Auth Manager](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html#manage-users)

En caso de problemas persistentes con el usuario o la estructura de la base de datos, puedes reiniciar todo **(Atención: destruirá tus datos de ejecución locales)**:

```sh
airflow db reset
```

---

## ▶️ Ejecución del Servidor

Puedes levantar todos los servicios de Airflow rápidamente para desarrollo con:

```sh
airflow standalone
```

**Opción Manual:**
Si prefieres levantar los servicios individualmente para tener mejor control de los logs o instancias:

```sh
airflow api-server --port 8080 -D
airflow scheduler -D
airflow dag-processor -D
# airflow triggerer -D # Solo si usas Asynchronous Operators
```

### ⏹️ Detener Servicios Manuales

Si utilizaste el modo manual y necesitas detener los procesos en segundo plano:

```sh
# Mediante los archivos PID (Método limpio)
kill $(cat ~/airflow/airflow-webserver.pid)
kill $(cat ~/airflow/airflow-scheduler.pid)

# Forzado (Si quedan procesos huérfanos)
ps aux | grep airflow | grep -v grep | awk '{print $2}' | xargs kill -9
lsof -i :8793 | sed 1d |  awk '{print $2}' | xargs kill -9
```

---

## 🛠️ Utilidades Adicionales

### Re-ejecutar DAGs fallidos

Se incluye un script utilitario en Python para buscar y reiniciar tareas o DAG runs que hayan quedado en estado fallido:

```sh
python src/utils/run_failed_dags.py
```

---

_Nota: La presente documentación ha sido verificada y reestructurada con asistencia de Inteligencia Artificial para maximizar su claridad y legibilidad, preservando y respetando íntegramente las instrucciones técnicas dictadas por el desarrollador original para la versión 3.1.8._

_Nota adicional: Los archivos de testing (`tests/dags/...`) fueron generados con IA._
