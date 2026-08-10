# Arquitectura Empresarial
## Proyecto: Gemelo Digital Financiero Personal

---

# 1. Definición del Problema y Usuario

## Problema de negocio

Actualmente, muchas personas administran sus finanzas utilizando únicamente aplicaciones bancarias que muestran información histórica sobre ingresos, gastos y deudas, pero ofrecen poca capacidad para interpretar el impacto de futuras decisiones financieras.

Los usuarios suelen preguntarse:

- ¿Me conviene realizar esta compra?
- ¿Puedo adquirir este producto a crédito sin afectar mi estabilidad financiera?
- ¿Estoy gastando demasiado en alguna categoría?
- ¿Cómo puedo mejorar mi capacidad de ahorro?
- ¿Qué impacto tendría solicitar un nuevo préstamo?

Responder estas preguntas requiere analizar simultáneamente ingresos, gastos, hábitos de consumo, préstamos y uso del crédito, información que normalmente se encuentra distribuida en diferentes fuentes y no se analiza de forma integral.

El proyecto busca resolver este problema mediante la construcción de un **Gemelo Digital Financiero Personal**, capaz de integrar múltiples fuentes de información financiera para generar una representación digital del estado financiero del usuario y apoyar la toma de decisiones mediante análisis de datos e Inteligencia Artificial.

---

## Usuario objetivo

La solución está orientada a personas que desean administrar mejor sus finanzas personales y comprender el impacto de sus decisiones económicas antes de realizarlas.

El usuario podrá consultar información relacionada con:

- Gastos personales
- Hábitos de consumo
- Uso de tarjetas de crédito
- Endeudamiento
- Capacidad de ahorro
- Riesgo financiero
- Conveniencia de realizar compras a crédito

---

## Valor esperado

El proyecto permitirá al usuario:

- Centralizar su información financiera.
- Conocer su situación financiera actual.
- Detectar patrones de gasto.
- Analizar el impacto de futuras compras.
- Evaluar el riesgo asociado al uso del crédito.
- Obtener recomendaciones financieras mediante consultas en lenguaje natural.

---

# 2. Datos

## Dataset candidato

### Dataset 1 — Personal Transactions

**Propósito**

Registrar el comportamiento financiero diario del usuario.

**Información principal**

- ingresos
- gastos
- categorías
- fechas
- método de pago
- comercios

**Justificación**

Este dataset representa la principal fuente para analizar hábitos financieros y calcular indicadores de gasto y ahorro.

---

### Dataset 2 — Synthetic Personal Finance Dataset

**Propósito**

Representar la situación financiera general del usuario.

**Información principal**

- ingreso mensual
- ahorro
- score crediticio
- préstamos
- deuda
- patrimonio
- relación deuda-ingreso

**Justificación**

Permite construir el perfil financiero del usuario y evaluar su capacidad de pago y nivel de riesgo.

---

### Dataset 3 — Personal ML Dataset

**Propósito**

Servir como conjunto de datos para el desarrollo y evaluación del modelo de Machine Learning del proyecto. Este dataset contiene variables financieras y demográficas que permiten identificar patrones en el comportamiento financiero de los usuarios y generar recomendaciones personalizadas.

**Información principal**

- Edad
- Ingreso mensual
- Gasto mensual
- Ahorro
- Score crediticio
- Relación deuda/ingreso
- Estado laboral
- Nivel educativo
- Tipo de préstamo
- Tasa de interés
- Pago mensual del préstamo

**Justificación**

Este dataset será utilizado para entrenar el modelo de Machine Learning que apoyará al Gemelo Digital Financiero Personal en la generación de recomendaciones y en la evaluación del riesgo financiero de un usuario. Sus variables permiten construir modelos de clasificación o regresión para estimar el comportamiento financiero y complementar el análisis realizado sobre los datos transaccionales.

---

## Viabilidad

Los tres datasets son públicos, ampliamente utilizados para fines académicos y permiten desarrollar un Producto Mínimo Viable (MVP) sin utilizar información sensible de usuarios reales.

---

## KPIs mínimos (MVP)

### 1. Capacidad de ahorro

Calcula el porcentaje del ingreso mensual que permanece disponible después de cubrir los gastos.

---

### 2. Utilización del crédito

Porcentaje de utilización del límite de crédito disponible.

---

### 3. Índice de riesgo financiero

Indicador calculado considerando variables como:

- deuda
- ingresos
- score crediticio
- utilización del crédito

---

# 3. Flujo de datos

```
                    Datasets Públicos
──────────────────────────────────────────────────────

 Personal Transactions

 Synthetic Personal Finance

 Personal Finance ML Dataset

                │
                ▼

        Ingesta Automatizada

                │
                ▼

             Bronze

 Almacenamiento de datos originales
 sin modificaciones.

                │
                ▼

             Silver

 Limpieza
 Normalización
 Conversión de tipos
 Eliminación de duplicados
 Integración de datasets

                │
                ▼

              Gold

 KPIs
 Perfil financiero
 Indicadores
 Dataset analítico

                │
                ▼

 Dashboard + Asistente IA
```

---

# 4. Estrategia Bronze / Silver / Gold

## Bronze

Se almacenarán los datasets originales sin realizar modificaciones, preservando su estructura para garantizar trazabilidad.

---

## Silver

En esta capa se realizarán procesos de:

- limpieza
- normalización
- validación
- eliminación de duplicados
- estandarización de formatos
- integración de la información

---

## Gold

Se construirán tablas analíticas optimizadas para el consumo por dashboards y modelos de Inteligencia Artificial.

En esta capa se calcularán:

- KPIs financieros
- indicadores de riesgo
- utilización del crédito
- capacidad de ahorro
- perfil financiero consolidado

---

# 5. Riesgos y Supuestos

## Riesgos técnicos y operativos

- Los datasets utilizan diferentes identificadores de usuario, por lo que será necesario definir una estrategia de integración.
- Los datos pueden contener valores faltantes o inconsistentes que requieran procesos adicionales de limpieza.
- La curva de aprendizaje de herramientas como Apache Spark, Airflow y Docker puede incrementar los tiempos de desarrollo.
- El procesamiento distribuido puede requerir optimización para ejecutarse correctamente en un entorno local.

---

## Supuestos

- Los datasets públicos representan adecuadamente escenarios de finanzas personales.
- El usuario consultará información mediante un entorno controlado.
- El MVP estará orientado al análisis financiero personal y no sustituirá asesoría financiera profesional.
- Los modelos de IA ofrecerán recomendaciones basadas en los datos disponibles y no tomarán decisiones automáticas por el usuario.

---

## Stack
Docker, Apache Airflow, MinIO, PostgreSQL, PySpark, Pandas,
Great Expectations, Delta Lake/Parquet, Ollama y Streamlit.

## Levantamiento
```bash
cp .env.example .env
docker compose build
docker compose up -d
docker compose ps
```

## Interfaces
- Airflow: http://localhost:8080
- MinIO Console: http://localhost:9001
- Streamlit: http://localhost:8501
- Ollama API: http://localhost:11434
- PostgreSQL: localhost:5432
