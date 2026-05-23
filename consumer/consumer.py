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
# Rutas de datos
DATA_PATH = os.getenv("DATA_PATH", "/app/data")
BRONZE_PATH = f"{DATA_PATH}/bronze"
CHECKPOINT_BRONZE = f"{DATA_PATH}/_checkpoints/bronze"

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
def write_to_bronze(raw_stream):
    """
    Persiste los datos crudos en la capa Bronze (Parquet).
    
    Bronze guarda los mensajes tal como llegaron, sin transformaciones.
    Permite reprocesar todo el pipeline si cambia la lógica de Silver.
    """
    logger.info(f"Bronze writer escribiendo a: {BRONZE_PATH}")
    
    bronze_df = raw_stream.selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp",
    )
    
    return (
        bronze_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_BRONZE)
        .trigger(processingTime="10 seconds")
        .queryName("bronze_writer")
        .start()
    )
def main():
    logger.info("Iniciando Crypto Consumer (Spark Streaming)")
    spark = create_spark_session()
    schema = get_crypto_schema()
    
    raw_stream = read_kafka_stream(spark)
    
    # Bronze: datos crudos
    bronze_query = write_to_bronze(raw_stream)
    logger.info("✓ Bronze writer iniciado")
    
    # Console: para debugging visual
    parsed_stream = parse_messages(raw_stream, schema)
    console_query = (
        parsed_stream
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )
    logger.info("✓ Console writer iniciado")
    
    logger.info("Pipeline en ejecución. Ctrl+C para detener.")
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Deteniendo streams...")
        for query in spark.streams.active:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()