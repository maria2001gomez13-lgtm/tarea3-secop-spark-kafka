from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .master("local[2]")
         .appName("SECOP_Batch")
         .config("spark.driver.memory", "1g")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

original = spark.read.json("datos/secop_2025.jsonl")

datos = original.select(
    F.trim("id_contrato").alias("id_contrato"),
    F.trim("departamento").alias("departamento"),
    F.trim("tipo_de_contrato").alias("tipo_de_contrato"),
    F.to_timestamp("fecha_de_firma").alias("fecha"),
    F.col("valor_del_contrato").cast("decimal(20,2)").alias("valor")
)

validos = datos.filter(
    F.col("id_contrato").isNotNull() &
    (F.col("id_contrato") != "") &
    F.col("fecha").isNotNull() &
    (F.year("fecha") == 2025) &
    F.col("valor").isNotNull() &
    (F.col("valor") >= 0)
)

total = original.count()
cantidad_validos = validos.count()

print("\nREGISTROS ORIGINALES:", total)
print("REGISTROS VALIDOS:", cantidad_validos)
print("REGISTROS EXCLUIDOS:", total - cantidad_validos)

def resumen(campo):
    return validos.groupBy(campo).agg(
        F.count("*").alias("registros"),
        F.sum("valor").alias("valor_total_COP")
    )

por_departamento = resumen("departamento")
por_tipo = resumen("tipo_de_contrato")
por_mes = (validos
           .withColumn("mes", F.date_format("fecha", "yyyy-MM"))
           .groupBy("mes")
           .agg(F.count("*").alias("registros"),
                F.sum("valor").alias("valor_total_COP")))

for nombre, tabla, orden in [
    ("por_departamento", por_departamento, "registros"),
    ("por_tipo", por_tipo, "registros"),
    ("por_mes", por_mes, "mes")
]:
    salida = tabla.orderBy(
        F.col(orden).asc() if orden == "mes"
        else F.col(orden).desc()
    )
    print("\nRESULTADOS:", nombre)
    salida.show(15, truncate=32)
    salida.coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv("resultados/" + nombre)

print("\nANALISIS BATCH TERMINADO. Resultados guardados.")
spark.stop()
