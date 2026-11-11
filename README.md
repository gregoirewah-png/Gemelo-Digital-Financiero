# Gemelo Digital Financiero Personal

La carpeta principal para revisar y editar el proyecto es `/Users/santiagogregoire/Gemelo-Digital-Financiero 23.21.16`. La copia de trabajo de este chat está en `/Users/santiagogregoire/Documents/ChatGPT/Gemelo Digital Financiero BBVA`. Ambas contienen los archivos sincronizados de esta entrega. En Docker Desktop aparece como el proyecto **gemelo-bbva**. La imagen **gemelo-bbva-runtime:2026.11.10-presentacion** contiene el código, los DAG, contratos, tema de Streamlit y archivos CSV de las fuentes.

## Entrega para presentación

El usuario conversa con un acompañante financiero en español sencillo. Puede entender su mes, revisar gastos y deudas, preparar una meta o explorar una compra. Las respuestas sobre cifras y escenarios se calculan con herramientas verificadas. Groq atiende las preguntas abiertas usando el perfil publicado.

La demostración utiliza **Alex, una persona ficticia**, con movimientos, tarjeta, préstamo y ahorro conciliados bajo una identidad común. Los tres datasets originales permanecen separados en el panel ejecutivo. El proyecto incluye Delta Lake, segmentación, anomalías y un modelo supervisado de impago entrenado con 30,000 registros públicos etiquetados de UCI.

- [Presentación ejecutiva en Markdown](docs/PRESENTACION_EJECUTIVA.md)
- [Guion de demostración y preguntas](docs/GUION_DEMO.md)
- [Alcance de la entrega y decisiones](docs/ENTREGA.md)
- [Validación reproducible](docs/VALIDACION.md)

## Abrir las pantallas

| Pantalla | Enlace en este equipo | Servicio en Docker Desktop | Credenciales del `.env` |
| --- | --- | --- | --- |
| Chat del usuario | [Abrir chat](http://localhost:18501) | `streamlit_app` | Alex · persona ficticia con 12 meses conciliados |
| Ejecutivos y desarrolladores | [Abrir panel del equipo](http://localhost:18502) | `operations` | `OPERATIONS_USER`, `OPERATIONS_PASSWORD` |
| Orquestación | [Abrir Airflow](http://localhost:18080) | `airflow` | `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` |
| Almacenamiento | [Abrir consola MinIO](http://localhost:19101) | `minio` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| Administración SQL | [Abrir pgAdmin](http://localhost:15050) | `pgadmin` | `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` |

Docker ejecuta estos servicios y publica sus puertos en el equipo. `localhost` identifica el equipo desde el que abres el navegador. La configuración de puertos está en `.env`.

La carpeta original ya contiene `src/dashboard/ui.py`, `src/dashboard/operations.py` y el resto de la versión integrada. El nombre de la carpeta es `dashboard`. Los accesos vigentes del proyecto Docker son **18501** y **18502**. Los procesos antiguos de aplicación se detuvieron durante la unificación.

En el panel del equipo verás la versión instalada debajo del título. La edición de esta entrega es `2026.11.10-presentacion`. Si el navegador conserva una pantalla anterior después de actualizar, recarga con **Cmd + Shift + R** y comprueba el puerto.

## Levantar la instalación preparada

Ejecuta estos comandos desde la carpeta principal.

```bash
cd "/Users/santiagogregoire/Gemelo-Digital-Financiero 23.21.16"
docker compose build airflow
docker compose up -d
docker compose ps -a
```

La imagen se construye una vez y los cuatro servicios Python la comparten. PostgreSQL, MinIO, pgAdmin, logs y modelos utilizan volúmenes administrados por Docker. Los servicios de aplicación arrancan después de que `platform_init` prepare los esquemas y buckets. Un estado `Exited (0)` en `platform_init` indica que esa preparación terminó correctamente. Airflow usa un worker web y una tarea pesada simultánea para limitar el consumo de memoria.

El código se copia a la imagen al construirla. Cuando cambies Python, los DAG, los contratos, los CSV o `.streamlit/config.toml`, vuelve a ejecutar la construcción y el arranque anteriores. Las contraseñas y claves se suministran con `.env` al crear los contenedores.

## Primera instalación usando Docker

Docker Desktop debe estar en ejecución. Estos comandos permiten preparar la imagen y generar `.env` utilizando Python dentro de un contenedor.

```bash
docker build -t gemelo-bbva-runtime:2026.11.10-presentacion .
docker run --rm --entrypoint python \
  --user "$(id -u):0" \
  --env PYTHONUSERBASE=/home/airflow/.local \
  -v "$PWD:/workspace" -w /workspace \
  gemelo-bbva-runtime:2026.11.10-presentacion scripts/configure_env.py
```

Completa `GROQ_API_KEY` en `.env` y revisa `GROQ_MODEL`. Después inicia los servicios.

```bash
docker compose up -d
```

En esta instalación, `.env` ya está configurado. El generador completa secretos vacíos y conserva los valores existentes.

## Acceso desde otro equipo o un servidor Docker

En otro equipo de la misma red utiliza la dirección del equipo que ejecuta Docker.

| Servicio | Dirección usando el host de Docker |
| --- | --- |
| Chat | `http://<IP_O_DOMINIO_DEL_HOST>:18501` |
| Panel del equipo | `http://<IP_O_DOMINIO_DEL_HOST>:18502` |
| Airflow | `http://<IP_O_DOMINIO_DEL_HOST>:18080` |
| MinIO | `http://<IP_O_DOMINIO_DEL_HOST>:19101` |
| pgAdmin | `http://<IP_O_DOMINIO_DEL_HOST>:15050` |

Configura `PUBLIC_HOST` en `.env` con esa IP o dominio y ejecuta `docker compose up -d`. Airflow usará ese host para generar los enlaces de sus logs. La apertura de los puertos del servidor o de su red depende del entorno donde se despliegue.

La imagen y Compose se pueden ejecutar en otro host Docker. Para trasladar el estado ya cargado también se necesitan las copias de los volúmenes PostgreSQL y MinIO y el mismo archivo de configuración. Un arranque con volúmenes nuevos prepara una instalación vacía que se carga ejecutando el DAG maestro.

Entre contenedores, los servicios utilizan nombres internos de Docker.

| Componente | Dirección interna | Dirección desde este equipo |
| --- | --- | --- |
| PostgreSQL | `postgres:5432` | `localhost:15432` |
| MinIO API | `http://minio:9000` | [MinIO API](http://localhost:19100) |
| Chat | `http://streamlit_app:8501` | [Chat](http://localhost:18501) |
| Panel del equipo | `http://operations:8501` | [Panel](http://localhost:18502) |

Para registrar PostgreSQL en pgAdmin utiliza el host `postgres`, puerto `5432` y las variables `POSTGRES_*` de `.env`.

## Ubicación de los cambios

| Funcionalidad | Archivos de esta carpeta | Dónde revisarla |
| --- | --- | --- |
| Tema y contraste | [.streamlit/config.toml](.streamlit/config.toml), [ui.py](src/dashboard/ui.py) | Ambas pantallas Streamlit |
| Experiencia del usuario | [app.py](src/dashboard/app.py) | Chat, puerto 18501 |
| Resúmenes, gráficas, rechazo y logs | [operations.py](src/dashboard/operations.py) | Panel, puerto 18502 |
| Perfil personal y asistente sencillo | [personal_agent.py](src/ml/personal_agent.py), [personal_finance.py](src/analytics/personal_finance.py) | Chat del usuario |
| Modelo supervisado de impago | [credit_default_model.py](src/ml/credit_default_model.py), [train_credit_default.py](src/ml/train_credit_default.py) | Modelo de impago en panel |
| Identidad y conciliación personal | [personal_profile.py](src/transformation/personal_profile.py), [generate_demo_profile.py](scripts/generate_demo_profile.py) | Perfil personal |
| Lectura Gold y herramientas de IA | [agent_service.py](src/ml/agent_service.py) | Consultas y simulaciones por chat |
| Motor Monte Carlo | [monte_carlo.py](src/ml/monte_carlo.py), [run_monte_carlo.py](src/analytics/run_monte_carlo.py) | Chat y pestaña Escenarios |
| Limpieza, validación y cuarentena Silver | [silver_pipeline.py](src/transformation/silver_pipeline.py) y las entradas `bronze_to_silver_dataset*_pyspark.py` | Tareas Silver del DAG |
| Modelo dimensional y features | [publish_gold.py](src/analytics/publish_gold.py), [platform.sql](sql/platform.sql) | Detalle técnico y PostgreSQL |
| Métricas e ingesta | [observability.py](src/common/observability.py), [pipeline.py](src/pipeline.py) | Calidad, logs y Airflow |
| DAG maestro y recálculo | [gemelo_financiero_dag.py](dags/gemelo_financiero_dag.py) | Airflow |
| Empaquetado y servicios | [Dockerfile](Dockerfile), [docker-compose.yml](docker-compose.yml) | Proyecto `gemelo-bbva` en Docker Desktop |
| Supuestos y versiones | [Documento de Arquitectura](Documento%20de%20Arquitectura.md) | Documentación |
| Pruebas ejecutadas | [VALIDACION.md](docs/VALIDACION.md) | Informe de validación |

## Corrida integral

El DAG maestro tiene 19 tareas y programación diaria. Una instalación nueva arranca con los DAG pausados para permitir la preparación antes de cargar datos. Puedes disparar otra corrida desde Airflow o mediante la CLI del contenedor.

```bash
docker compose exec airflow airflow dags unpause dag_maestro_medallion
docker compose exec airflow airflow dags trigger dag_maestro_medallion
```

El panel muestra cada tarea, intento, conteos y logs. El agente consulta la publicación SQL confirmada por la tarea `publish`. La simulación automática usa los valores `MC_*` y guarda resultados con un identificador propio.

## Recalcular Monte Carlo

En el chat puedes pedir “¿Qué pasa si gasto 2,000 pesos más al mes durante 12 meses?”. El saldo inicial personal se toma de los ahorros conciliados de Alex. También puedes indicar ahorro inicial, semilla y cantidad de simulaciones. El formulario del equipo conserva el análisis independiente de Dataset 1. El equipo dispone de un formulario en Escenarios.

El DAG independiente acepta parámetros JSON.

```bash
docker compose exec airflow airflow dags unpause recalcular_monte_carlo
docker compose exec airflow airflow dags trigger recalcular_monte_carlo \
  --conf '{"initial_savings":10000,"months":12,"simulations":5000,"seed":17}'
```

Para una publicación histórica añade `source_run_id`. Puedes definir hasta cinco escenarios mediante `scenarios`, con campos `name`, `income_change_pct`, `expense_change_pct` y `extra_monthly_expense`. El campo opcional `one_time_expense` descuenta una compra una sola vez en el primer mes. Para simular a Alex desde el DAG añade `"profile_id":"persona_demo"`; al omitirlo se usa Dataset 1. El cambio se expresa en puntos porcentuales, de modo que **10 significa 10%**.

## Verificación dentro de Docker

```bash
docker compose exec streamlit_app python -m pytest tests -q
docker compose exec streamlit_app python -m tests.integration_checks
docker compose exec streamlit_app python -m tests.personal_integration_checks
docker compose exec airflow python tests/test_features.py
docker compose exec streamlit_app printenv GEMELO_VERSION
docker compose logs --tail 100 streamlit_app operations
```

La comprobación de integración requiere una publicación Gold y crea una simulación de prueba. La suite verifica el motor, las relaciones dimensionales, el agente, el login y las pantallas.

Para comprobar la salud HTTP de los dashboards usa los enlaces [chat](http://localhost:18501/_stcore/health) y [panel](http://localhost:18502/_stcore/health). Compose también muestra su estado de salud.

Los archivos fuente están incorporados en la imagen desde `data/raw` y `data/demo`. El perfil se regenera con `python scripts/generate_demo_profile.py` y la configuración `DEMO_*`, antes de reconstruir la imagen y ejecutar una corrida nueva. La procedencia y licencia del dataset etiquetado están en [credit_default_source.json](configs/credit_default_source.json). Los CSV con errores controlados están en `data/test`. Las claves se configuran en `.env`, excluido de la imagen y de Git.

## Configuración de usuarios y claves

Los usuarios, contraseñas, claves API, propietario del DAG y datos de la cuenta administradora se leen de `.env`. `.env.example` enumera la configuración disponible. `scripts/configure_env.py` agrega las variables ausentes desde esa plantilla y conserva las credenciales configuradas. Los endpoints PostgreSQL y MinIO también se obtienen de `.env`.

Las pruebas de configuración verifican que las conexiones utilicen el entorno, que una contraseña ausente detenga la conexión y que el generador conserve claves con caracteres especiales. Una prueba estática comprueba los scripts ejecutables para detectar credenciales literales o valores alternativos embebidos.

El respaldo previo a la sincronización se encuentra en `.backups/` dentro de la carpeta original. Contiene sus archivos de código y configuración anteriores, incluido el `.env` previo, y tiene permisos restringidos. Ese directorio está excluido de Git y de la imagen Docker. Los usuarios `root` y `airflow` del Dockerfile identifican las cuentas del sistema de la imagen base. Las cuentas de acceso a los servicios se configuran en `.env`.
