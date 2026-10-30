from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

spark = (SparkSession.builder
    .master("local[2]")
    .appName("SECOP_Streaming")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

esquema = StructType([
    StructField("id_contrato", StringType()),
    StructField("departamento", StringType()),
    StructField("tipo_de_contrato", StringType()),
    StructField("fecha_de_firma", StringType()),
    StructField("valor_del_contrato", StringType()),
    StructField("ejecucion_id", StringType()),
    StructField("secuencia", LongType())
])

entrada = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "secop_contratos")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 100)
    .load())

datos = (entrada
    .select(
        F.from_json(
            F.col("value").cast("string"), esquema
        ).alias("dato"),
        F.col("timestamp").alias("momento_kafka")
    )
    .select("dato.*", "momento_kafka")
    .withColumn(
        "valor",
        F.col("valor_del_contrato").cast("decimal(20,2)")
    ))

def procesar(lote, numero):
    if lote.isEmpty():
        return

    lote.persist()
    try:
        print(f"\n=== MICROLOTE {numero} ===", flush=True)
        print("Mensajes recibidos:", lote.count(), flush=True)

        lote.groupBy("ejecucion_id", "departamento").agg(
            F.count("*").alias("mensajes"),
            F.sum("valor").alias("valor_total_COP")
        ).orderBy(F.desc("mensajes")).show(truncate=25)

        lote.write.mode("overwrite").json(
            f"resultados/streaming/lote_{numero}"
        )
    finally:
        lote.unpersist()

consulta = (datos.writeStream
    .foreachBatch(procesar)
    .option("checkpointLocation", "resultados/checkpoint_secop_1793315884")
    .trigger(processingTime="5 seconds")
    .start())

print("\nCONSUMIDOR INICIADO. Esperando mensajes...", flush=True)

try:
    consulta.awaitTermination()
except KeyboardInterrupt:
    consulta.stop()
finally:
    spark.stop()
