"""
Spark Structured Streaming Consumer.

Lee precios de criptomonedas desde un topic de Kafka y los persiste
en formato Parquet siguiendo la arquitectura Medallion (Bronze).
"""
import logging
import os
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
from pyspark.sql import SparkSession


# ============== Configuración ==============
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# ============== Logging ==============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("crypto-consumer")

#==== Funciones de parseo======
def get_crypto_schema() -> StructType:
    """
    Define el esquema esperado de los mensajes JSON desde Kafka.
    Debe coincidir exactamente con la estructura del Producer.
    """
    return StructType([
        StructField("coin", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("market_cap_usd", DoubleType(), True),
        StructField("volume_24h_usd", DoubleType(), True),
        StructField("change_24h_pct", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ])


def parse_messages(raw_stream, schema):
    """
    Parsea los mensajes JSON y aplica el esquema definido.
    
    Returns:
        DataFrame con columnas tipadas + event_time + ingestion_time.
    """
    return (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("ingestion_time", current_timestamp())
    )
#=== Funciones principales ====
def create_spark_session() -> SparkSession:
    """Crea una sesión Spark configurada con el conector de Kafka."""
    spark = (
        SparkSession.builder
        .appName("CryptoStreamingPipeline")
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session creada (versión {spark.version})")
    return spark

def read_kafka_stream(spark: SparkSession):
    """
    Lee mensajes desde Kafka como un stream continuo.
    
    Returns:
        DataFrame con columnas: key, value, topic, partition, offset, timestamp.
        Los valores vienen como bytes; el parsing lo hacemos después.
    """
    logger.info(f"Conectando al topic '{KAFKA_TOPIC}' en {KAFKA_BOOTSTRAP_SERVERS}")
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
def main():
    logger.info("Iniciando Crypto Consumer (Spark Streaming)")
    spark = create_spark_session()
    # Leer el stream desde Kafka
    raw_stream = read_kafka_stream(spark)
    
    # Verificar el esquema (esto es eager, se ejecuta inmediatamente)
    logger.info("Esquema del stream de Kafka:")
    raw_stream.printSchema()
    
    #Validar lectura imprimiendo a consola

    query = (
        raw_stream
        .selectExpr(
            "CAST(key AS STRING)",
            "CAST(value AS STRING)",
            "topic",
            "partition",
            "offset",
            "timestamp",
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Deteniendo stream...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()