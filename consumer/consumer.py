"""
Spark Structured Streaming Consumer.

Lee precios de criptomonedas desde un topic de Kafka y los persiste
en formato Parquet siguiendo la arquitectura Medallion (Bronze).
"""

import logging
import os

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


def main():
    logger.info("Iniciando Crypto Consumer (Spark Streaming)")
    spark = create_spark_session()
    
    # Mantenemos vivo el contexto para verificar que arranca bien
    logger.info("Spark session activa. Presiona Ctrl+C para detener.")
    
    try:
        # Por ahora solo mantenemos viva la sesión
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Cerrando Spark session...")
        spark.stop()


if __name__ == "__main__":
    main()