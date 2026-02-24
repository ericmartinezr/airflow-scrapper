## Instalación

```sh
AIRFLOW_VERSION=3.1.7
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[postgres,fab,otel]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install psycopg2==2.9.11
```

## Configuración

### Actualizar configuración (si se viene desde una versión anterior)

Esto no funcionó del todo. Pedía que le diera valor a la variable api_auth/jwt_secret. No había nada similar en el .cfg, así que fue más fácil respaldar la carpeta anterior e iniciar nuevamente el servidor que crea el directorio limpio.

```sh
airflow config update --fix
```

### Métricas (OpenTelemetry)

```sh
[metrics]
otel_on = True
otel_host = localhost
# HTTP = 4318, gRPC = 4317
otel_port = 4318
otel_prefix = airflow
otel_interval_milliseconds = 30000
otel_ssl_active = False
```

- Referencia

* https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html#setup-opentelemetry
* https://github.com/grafana/docker-otel-lgtm

## Preparación base de datos

### Obtener cadena de conexión y modificarla

```sh
airflow config get-value database sql_alchemy_conn
```

### Modificar cadena de conexión

```sh
nano ~/airflow/airflow.cfg

# Buscar y modificar la siguiente variable
sql_alchemy_conn=postgresql+psycopg2://postgres:p4ssw0rd@localhost:5432/airflow_scrapper
```

```sh
airflow db migrate
```

### Generación de usuario

La instalación por defecto utiliza Simple Auth Manager y es el que usa este proyecto.

Al ejecutar `airflow standalone` se genera un usuario por defecto en `~/airflow/airflow.cfg`, con el formato <usuario>:<rol>

```sh
grep simple_auth_manager_users ~/airflow/airflow.cfg

# Imprime algo similar a
# simple_auth_manager_users = admin:admin
# Formato <usuario>:<rol>
```

Luego este usuario se debe buscar en el archivo `~/airflow/simple_auth_manager_passwords.json.generated` generado automáticamente

```sh
cat ~/airflow/simple_auth_manager_passwords.json.generated

# Imprime algo similar a
# {"admin": "<password>"}
# Ese password se puede editar directamente en el archivo
```

- Referencia

* https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html#manage-users

### Resetear usuario admin (opcional si hay problemas)

```sh
airflow db reset
```

## Iniciar el servidor

```sh
# Standalone
airflow standalone


# O individualmente
airflow api-server --port 8080 -D
airflow scheduler -D
airflow dag-processor
airflow triggerer -D
```

## Para detener el webserver

Solo si se inicia de forma individual. Si usa el modo `standalone` con `Ctrl+C` es suficiente.

```sh
# Detener el webserver
kill $(cat ~/airflow/airflow-webserver.pid)

# Detener el scheduler
kill $(cat ~/airflow/airflow-scheduler.pid)

# Detener lo que quede vivo del webserver
ps aux | grep airflow | grep -v grep | awk '{print $2}' | xargs kill -9

# Detener lo que quede vivo del scheduler
lsof -i :8793 | sed 1d |  awk '{print $2}' | xargs kill -9
```

## Otros

### Script para re-ejecutar dags fallidos

```sh
python src/utils/run_failed_dags.py
```
