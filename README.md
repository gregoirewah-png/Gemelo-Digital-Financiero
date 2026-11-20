# Gemelo Digital Financiero Personal

La carpeta principal para revisar y editar el proyecto es `/Users/santiagogregoire/Gemelo-Digital-Financiero 23.21.16`. La copia de trabajo de este chat está en `/Users/santiagogregoire/Documents/ChatGPT/Gemelo Digital Financiero BBVA`. Ambas contienen los archivos sincronizados de esta entrega. En Docker Desktop aparece como el proyecto **gemelo-bbva**. La imagen **gemelo-bbva-runtime:2026.11.19-movil-rag** contiene el código, los DAG, contratos, tema de Streamlit y archivos CSV de las fuentes.

## Entrega para presentación

La versión final del material está en [presentacion_final_20261119](docs/presentacion_final_20261119/LEEME_PRIMERO.txt): [PowerPoint editable](docs/presentacion_final_20261119/Gemelo_Financiero_Final.pptx), [PDF del deck](docs/presentacion_final_20261119/Gemelo_Financiero_Final.pdf), [guion de diez minutos](docs/presentacion_final_20261119/Guion_10_minutos.pdf) y [defensa técnica con veinte fichas](docs/presentacion_final_20261119/Defensa_tecnica_y_fichas.pdf). Contiene 16 diapositivas principales para 9:40 y 16 de apoyo. Considera la rúbrica recibida el 19 de noviembre y los requisitos de la propuesta. El diseño utiliza la paleta BBVA y recursos originales suministrados; consulta su [inventario](docs/presentacion_final_20261119/RECURSOS_DISENO_BBVA.txt).

Para mostrar la malla y los tableros internos desde CDMX hace falta llevar la Mac o preparar un acceso remoto seguro. El enlace móvil publica solo al usuario; el escritorio remoto todavía no está configurado. Los comandos de ensayo están en esa carpeta. El deck del 17 de noviembre queda como antecedente.

El usuario conversa con un acompañante financiero en español sencillo. Puede entender su mes, revisar gastos y deudas, preparar una meta o explorar una compra. Las respuestas sobre cifras y escenarios se calculan con herramientas verificadas. Groq atiende las preguntas abiertas usando el perfil publicado.

La demostración utiliza **Alex, una persona ficticia**, con movimientos, tarjeta, préstamo y ahorro conciliados bajo una identidad común. Los tres datasets originales permanecen separados en el panel ejecutivo. El proyecto incluye Delta Lake, segmentación, anomalías y un modelo supervisado de impago entrenado con 30,000 registros públicos etiquetados de UCI.

- [Presentación de producto de diez minutos: PowerPoint, PDF y guion](docs/presentacion_20261117/entrega/PREPARACION_Y_REQUISITOS.md)
- [Presentación ejecutiva en Markdown](docs/PRESENTACION_EJECUTIVA.md)
- [Guion de demostración y preguntas](docs/GUION_DEMO.md)
- [Alcance de la entrega y decisiones](docs/ENTREGA.md)
- [Validación reproducible](docs/VALIDACION.md)

## Abrir las pantallas

| Pantalla | Enlace en este equipo | Servicio en Docker Desktop | Credenciales del `.env` |
| --- | --- | --- | --- |
| Chat del usuario | [Abrir chat](http://localhost:18501) | `streamlit_app` | Alex · persona ficticia con 12 meses conciliados |
| Vista ejecutiva | [Tendencias, alertas y modelos](http://localhost:18502) | `executive` | `EXECUTIVE_USER`, `EXECUTIVE_PASSWORD` |
| Data Engineering | [Calidad, fallos y logs](http://localhost:18503) | `operations` | `OPERATIONS_USER`, `OPERATIONS_PASSWORD` |
| Orquestación | [Abrir Airflow](http://localhost:18080) | `airflow` | `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` |
| Almacenamiento | [Abrir consola MinIO](http://localhost:19101) | `minio` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| Administración SQL | [Abrir pgAdmin](http://localhost:15050) | `pgadmin` | `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` |

Docker ejecuta estos servicios y publica sus puertos en el equipo. `localhost` identifica el equipo desde el que abres el navegador. La configuración de puertos está en `.env`.

La carpeta original ya contiene `src/dashboard/ui.py`, `src/dashboard/operations.py` y el resto de la versión integrada. El nombre de la carpeta es `dashboard`. Los accesos vigentes del proyecto Docker son **18501**, **18502** y **18503**. Los procesos antiguos de aplicación se detuvieron durante la unificación.

En el panel del equipo verás la versión instalada debajo del título. La edición de esta entrega es `2026.11.19-movil-rag`. Si el navegador conserva una pantalla anterior después de actualizar, recarga con **Cmd + Shift + R** y comprueba el puerto.

## Levantar la instalación preparada

Ejecuta estos comandos desde la carpeta principal.

```bash
cd "/Users/santiagogregoire/Gemelo-Digital-Financiero 23.21.16"
docker compose build airflow
docker compose up -d
docker compose ps -a
```

La imagen se construye una vez y los servicios Python la comparten. PostgreSQL, MinIO, pgAdmin, logs y modelos utilizan volúmenes administrados por Docker. Los servicios de aplicación arrancan después de que `platform_init` prepare los esquemas y buckets. Un estado `Exited (0)` en `platform_init` indica que esa preparación terminó correctamente. Airflow usa un worker web y una tarea pesada simultánea para limitar el consumo de memoria.

El código se copia a la imagen al construirla. Cuando cambies Python, los DAG, los contratos, los CSV o `.streamlit/config.toml`, vuelve a ejecutar la construcción y el arranque anteriores. Las contraseñas y claves se suministran con `.env` al crear los contenedores.

## Primera instalación usando Docker

Docker Desktop debe estar en ejecución. Estos comandos permiten preparar la imagen y generar `.env` utilizando Python dentro de un contenedor.

```bash
docker build -t gemelo-bbva-runtime:2026.11.19-movil-rag .
docker run --rm --entrypoint python \
  --user "$(id -u):0" \
  --env PYTHONUSERBASE=/home/airflow/.local \
  -v "$PWD:/workspace" -w /workspace \
  gemelo-bbva-runtime:2026.11.19-movil-rag scripts/configure_env.py
```

Completa `GROQ_API_KEY` en `.env` y revisa `GROQ_MODEL`. Después inicia los servicios.

```bash
docker compose up -d
```

En esta instalación, `.env` ya está configurado. El generador completa secretos vacíos y conserva los valores existentes.

## Abrir en iPhone y presentar desde CDMX

La vista del teléfono contiene únicamente el acompañante del usuario. En la misma Wi-Fi abre `http://<IP_DE_LA_MAC>:18501` en Safari. Puedes consultar la IP con `ipconfig getifaddr en0` en la Mac. Para abrir desde datos móviles o desde CDMX usa el enlace HTTPS temporal:

```bash
python3 scripts/mobile_demo.py start
python3 scripts/mobile_demo.py status
```

El primer comando inicia el perfil Docker `mobile`, imprime la dirección y la guarda en `.mobile-demo/url.txt`. En Safari introduce la clave **`DEMO_ACCESS_CODE` de `.env`**. El enlace no contiene la clave. La interfaz se adapta al ancho del iPhone, con tarjetas compactas, preguntas de acceso rápido y gráficos interactivos. [Guía para el teléfono y capturas](docs/ACCESO_MOVIL.md).

**La Mac debe permanecer despierta, conectada a corriente e internet y con la tapa abierta.** El ayudante inicia `caffeinate -i -s`: permite apagar la pantalla sin suspender Docker. Verifica que Docker Desktop siga abierto. No dependas del modo reposo. Si se corta la luz o la conexión, el servicio dejará de responder.

Cloudflare Quick Tunnel entrega una dirección temporal sin garantía de disponibilidad; un reinicio puede cambiarla. Ejecuta `start` y revisa `status` antes de compartirla. Solo se publica `mobile_user`, que exige clave antes de leer el perfil. Las vistas ejecutiva y técnica, PostgreSQL, Airflow, MinIO y pgAdmin permanecen accesibles por `localhost` en la Mac. No hay que abrir puertos del router. [Funcionamiento y límites de Quick Tunnels](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/).

Para apagar el acceso externo y restaurar la política normal de reposo:

```bash
python3 scripts/mobile_demo.py stop
```

La imagen puede trasladarse a otro host Docker. Para conservar la instalación cargada también se necesitan copias de los volúmenes PostgreSQL y MinIO y el mismo archivo de configuración. Con volúmenes nuevos, ejecuta el DAG maestro para crear la primera publicación.

Entre contenedores, los servicios utilizan nombres internos de Docker.

| Componente | Dirección interna | Dirección desde este equipo |
| --- | --- | --- |
| PostgreSQL | `postgres:5432` | `localhost:15432` |
| MinIO API | `http://minio:9000` | [MinIO API](http://localhost:19100) |
| Chat | `http://streamlit_app:8501` | [Chat](http://localhost:18501) |
| Ejecutivo | `http://executive:8501` | [Ejecutivo](http://localhost:18502) |
| Data Engineering | `http://operations:8501` | [Data Engineering](http://localhost:18503) |

Para registrar PostgreSQL en pgAdmin utiliza el host `postgres`, puerto `5432` y las variables `POSTGRES_*` de `.env`.

## Ubicación de los cambios

| Funcionalidad | Archivos de esta carpeta | Dónde revisarla |
| --- | --- | --- |
| Tema y contraste | [.streamlit/config.toml](.streamlit/config.toml), [ui.py](src/dashboard/ui.py) | Las tres vistas Streamlit |
| Experiencia del usuario | [app.py](src/dashboard/app.py) | Chat, puerto 18501 |
| Tendencias, alertas e interpretación de modelos | [executive.py](src/dashboard/executive.py), [executive_metrics.py](src/analytics/executive_metrics.py) | Ejecutivo, puerto 18502 |
| Calidad, rechazo y logs | [operations.py](src/dashboard/operations.py) | Data Engineering, puerto 18503 |
| RAG y corpus financiero | [knowledge_rag.py](src/ml/knowledge_rag.py), [knowledge_store.py](src/analytics/knowledge_store.py), [knowledge](knowledge/catalog.json) | Fuentes de explicación en el chat; corpus en Data Engineering |
| Acceso móvil protegido | [mobile_demo.py](scripts/mobile_demo.py), [access.py](src/dashboard/access.py) | iPhone con HTTPS y clave |
| Perfil personal y asistente sencillo | [personal_agent.py](src/ml/personal_agent.py), [personal_finance.py](src/analytics/personal_finance.py) | Chat del usuario |
| Modelo supervisado de impago | [credit_default_model.py](src/ml/credit_default_model.py), [train_credit_default.py](src/ml/train_credit_default.py) | Modelo de impago en panel |
| Identidad y conciliación personal | [personal_profile.py](src/transformation/personal_profile.py), [generate_demo_profile.py](scripts/generate_demo_profile.py) | Perfil personal |
| Lectura Gold y herramientas de IA | [agent_service.py](src/ml/agent_service.py) | Consultas y simulaciones por chat |
| Motor Monte Carlo | [monte_carlo.py](src/ml/monte_carlo.py), [run_monte_carlo.py](src/analytics/run_monte_carlo.py) | Chat y DAG de recálculo |
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

En el chat puedes pedir “¿Qué pasa si gasto 2,000 pesos más al mes durante 12 meses?”. El saldo inicial personal se toma de los ahorros conciliados de Alex. También puedes indicar ahorro inicial, semilla y cantidad de simulaciones. La vista ejecutiva permite consultar los escenarios guardados. El recálculo independiente de Dataset 1 se realiza mediante el DAG siguiente.

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
docker compose exec streamlit_app python -m tests.knowledge_integration_checks
python3 scripts/audit_repo.py
docker compose exec airflow python tests/test_features.py
docker compose exec streamlit_app printenv GEMELO_VERSION
docker compose logs --tail 100 streamlit_app executive operations
```

La comprobación de integración requiere una publicación Gold y crea una simulación de prueba. La suite verifica el motor, las relaciones dimensionales, el agente, el login y las pantallas.

Para comprobar la salud HTTP de los dashboards usa los enlaces [chat](http://localhost:18501/_stcore/health), [ejecutivo](http://localhost:18502/_stcore/health) y [Data Engineering](http://localhost:18503/_stcore/health). Compose también muestra su estado de salud.

Los archivos fuente están incorporados en la imagen desde `data/raw` y `data/demo`. El perfil se regenera con `python scripts/generate_demo_profile.py` y la configuración `DEMO_*`, antes de reconstruir la imagen y ejecutar una corrida nueva. La procedencia y licencia del dataset etiquetado están en [credit_default_source.json](configs/credit_default_source.json). Los CSV con errores controlados están en `data/test`. Las claves se configuran en `.env`, excluido de la imagen y de Git.

## Configuración de usuarios y claves

Los usuarios, contraseñas, claves API, propietario del DAG y datos de la cuenta administradora se leen de `.env`. `.env.example` enumera la configuración disponible. `scripts/configure_env.py` agrega las variables ausentes desde esa plantilla y conserva las credenciales configuradas. Los endpoints PostgreSQL y MinIO también se obtienen de `.env`.

Las pruebas de configuración verifican que las conexiones utilicen el entorno, que una contraseña ausente detenga la conexión y que el generador conserve claves con caracteres especiales. Una prueba estática comprueba los scripts ejecutables para detectar credenciales literales o valores alternativos embebidos.

El respaldo previo a la sincronización se encuentra en `.backups/` dentro de la carpeta original. Contiene sus archivos de código y configuración anteriores, incluido el `.env` previo, y tiene permisos restringidos. Ese directorio está excluido de Git y de la imagen Docker. Los usuarios `root` y `airflow` del Dockerfile identifican las cuentas del sistema de la imagen base. Las cuentas de acceso a los servicios se configuran en `.env`.

## Conocimiento financiero y base adicional de anomalías

El RAG recupera fragmentos de ocho guías curadas mediante TF-IDF y similitud coseno. Los documentos públicos citan CONDUSEF y Banco de México; las reglas del gemelo se identifican como guías internas. Las fuentes se muestran junto a la explicación. El corpus se publica en el esquema `knowledge` de PostgreSQL, con versión y hashes; si SQL no está disponible, se utiliza la copia curada de la imagen. Las cifras del usuario proceden siempre de Gold. Configuración: `RAG_ENABLED`, `RAG_TOP_K`, `RAG_MIN_SCORE`. No se realizan búsquedas web arbitrarias ni se admiten archivos privados en el corpus.

Los datos de **Isolation Forest** se manejan como una base analítica derivada adicional: `gold.credit_card_anomalies`, con vistas activas e históricas. Incluye todos los clientes válidos, scores, ratios y procedencia. Sus claves son publicación y cliente. No es una nueva fuente de personas ni un modelo Random Forest. [Definición completa](configs/data_catalog.json).

## Qué subir a Git

Sube código, DAG, SQL, contratos, pruebas, documentación, corpus y fixtures públicos de `data/raw`, `data/demo` y `data/test`, respetando sus licencias. Excluye `.env`, respaldos, `.mobile-demo`, volúmenes PostgreSQL, capas generadas, logs, modelos, cachés y entornos virtuales. `.env.example` conserva los nombres de configuración, con secretos vacíos.

`python3 scripts/audit_repo.py` comprueba el índice Git y la plantilla antes de subir. Quitar archivos del índice conserva los archivos locales y los volúmenes Docker. No modifica los commits anteriores. [Política y limpieza del repositorio](docs/CONTRIBUIR.md).
