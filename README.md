# SECOP II: análisis con Spark y Kafka

**Autora:** María Paula Gómez Guerrero  
**Curso:** Big Data – 202016911, UNAD  
**Actividad:** Tarea 3

## Objetivo

Analizar los registros de contratos electrónicos de SECOP II firmados en 2025 mediante procesamiento por lotes con PySpark y simular su llegada en flujo usando Kafka y Spark Structured Streaming.

## Datos

Fuente pública: https://www.datos.gov.co/resource/jbjy-vk9h.json

Campos utilizados: departamento, identificador del contrato, tipo de contrato, fecha de firma y valor del contrato.

La descarga utilizada en la prueba contenía 1.017.075 registros y aproximadamente 192 MB. El archivo de datos no se incluye en este repositorio. La fuente puede actualizarse y producir cantidades distintas en futuras descargas.

## Archivos

| Archivo | Función |
|---|---|
| descargar_datos.py | Descarga los registros de 2025 en formato JSONL. |
| analisis_batch.py | Limpia, transforma y agrupa los datos con Spark. |
| graficas_secop.py | Genera gráficas a partir de los resúmenes CSV. |
| productor_secop.py | Publica 300 registros históricos en Kafka. |
| consumidor_secop.py | Consume y procesa los mensajes en micro lotes. |

## Requisitos

- Ubuntu con Python 3.10 y Java 11.
- PySpark 3.5.3.
- kafka-python 3.0.12.
- Matplotlib.
- Apache Kafka 3.9.2, distribución Scala 2.12.
- Conexión a internet para descargar datos y el conector de Spark con Kafka.

## Preparación

Descargar o clonar el repositorio y abrir una terminal dentro de su carpeta.

Crear y activar el entorno virtual:

```bash
python3 -m venv ~/bigdata-env
source ~/bigdata-env/bin/activate
python -m pip install pyspark==3.5.3 kafka-python==3.0.12 matplotlib
mkdir -p datos resultados
```

Los comandos siguientes deben ejecutarse desde la carpeta del proyecto.

## Procesamiento por lotes

```bash
python descargar_datos.py
python analisis_batch.py
python graficas_secop.py
```

El análisis convierte la fecha a timestamp y el valor a decimal. Acepta filas con identificador no vacío, fecha válida de 2025 y valor numérico no negativo.

Genera conteos y sumas del valor por departamento, tipo de contrato y mes. Guarda los resúmenes CSV y las gráficas dentro de `resultados/`.

Los conteos representan filas. El programa no elimina duplicados por identificador.

## Preparación de Kafka

Descargar y extraer la distribución oficial en la carpeta personal:

```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.9.2/kafka_2.12-3.9.2.tgz
tar -xzf kafka_2.12-3.9.2.tgz
```

Iniciar los servicios:

```bash
~/kafka_2.12-3.9.2/bin/zookeeper-server-start.sh -daemon ~/kafka_2.12-3.9.2/config/zookeeper.properties
sleep 5
~/kafka_2.12-3.9.2/bin/kafka-server-start.sh -daemon ~/kafka_2.12-3.9.2/config/server.properties
sleep 8
jps
```

Crear el tópico:

```bash
~/kafka_2.12-3.9.2/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic secop_contratos
```

## Simulación de procesamiento en flujo

Volver a la carpeta del proyecto. En una primera terminal, activar el entorno e iniciar el consumidor:

```bash
source ~/bigdata-env/bin/activate
python consumidor_secop.py
```

Esperar el mensaje `CONSUMIDOR INICIADO`.

En una segunda terminal, ubicada también en la carpeta del proyecto:

```bash
source ~/bigdata-env/bin/activate
python productor_secop.py
```

El productor envía 300 registros con una pausa de 0,2 segundos entre mensajes. Cada ejecución incorpora un identificador y una secuencia.

El consumidor procesa micro lotes con un disparador configurado cada cinco segundos. Muestra conteos y sumas por departamento y guarda los registros en JSON dentro de `resultados/streaming/`.

Los datos son históricos. Se simula su llegada en flujo; no se reciben contratos en vivo desde SECOP II.

Para detener el consumidor, pulsar Ctrl + C.

## Resultados de la prueba

- Batch: 1.017.075 filas aceptadas y cero excluidas con los filtros aplicados.
- Bogotá: 282.844 registros.
- Prestación de servicios: 869.583 registros.
- Febrero: mayor cantidad mensual, con 159.758 registros.
- Flujo: 300 mensajes guardados, 300 secuencias únicas, cero duplicados y cero faltantes en la ejecución verificada.

## Reinicios y limitaciones

Antes de repetir el flujo, verificar que ZooKeeper, Kafka y el tópico estén disponibles.

Si se recrea el tópico, utilizar una nueva carpeta de checkpoint en el consumidor. Las salidas usan nombres de lote y modo overwrite: respaldar los resultados anteriores antes de iniciar una consulta con un checkpoint nuevo.

El laboratorio utiliza un broker y factor de replicación uno, sin redundancia entre brokers. La validación de los 300 mensajes corresponde a una ejecución concreta.

## Presentación

https://docs.google.com/presentation/d/1w9c5AZJx6OLZgJVWbZmueCAO21oe-KG7SqvsD7mbKF4/edit
