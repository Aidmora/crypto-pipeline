"""
Crypto Price Producer.

Consulta los precios de criptomonedas desde CoinGecko y los publica
en un topic de Kafka en formato JSON.
"""
import signal
import json
import time
import os
import requests
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, UnrecognizedBrokerVersion, KafkaConnectionError
# Flag global para apagado limpio
running = True
# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("crypto-producer")

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "10"))

COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = ["bitcoin", "ethereum", "cardano", "solana", "polkadot"]
VS_CURRENCY = "usd"


def handle_shutdown(signum, frame):
    """Maneja Ctrl+C / SIGTERM para apagar el producer limpiamente."""
    global running
    logger.info(f"Señal {signum} recibida. Cerrando producer...")
    running = False

def fetch_crypto_prices():
    """Consulta los precios actuales desde CoinGecko."""
    params = {
        "ids": ",".join(COINS),
        "vs_currencies": VS_CURRENCY,
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
    }
    response = requests.get(COINGECKO_API_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def is_valid_record(record):
    """Valida que el registro tenga los campos críticos no nulos."""
    if record.get("price_usd") is None:
        return False
    if record["price_usd"] <= 0:
        return False
    if not record.get("coin"):
        return False
    return True


def build_records(api_data):
    """Transforma la respuesta de la API en registros normalizados y validados."""
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    records = []
    
    for coin, metrics in api_data.items():
        record = {
            "coin": coin,
            "price_usd": metrics.get("usd"),
            "market_cap_usd": metrics.get("usd_market_cap"),
            "volume_24h_usd": metrics.get("usd_24h_vol"),
            "change_24h_pct": metrics.get("usd_24h_change"),
            "timestamp": timestamp,
        }
        
        if is_valid_record(record):
            records.append(record)
        else:
            logger.warning(f"Registro inválido descartado: {coin}")
    
    return records


def create_producer(retries=10, base_delay=2):
    """
    Crea un Kafka producer con reintentos y backoff exponencial.
    
    Espera a que Kafka esté disponible antes de continuar.
    El delay crece exponencialmente: 2s, 4s, 8s, 16s, ... (máx 30s).
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=100,
            )
            logger.info(f"Producer conectado a {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except (NoBrokersAvailable, UnrecognizedBrokerVersion, KafkaConnectionError):
            delay = min(base_delay * (2 ** (attempt - 1)), 30)
            logger.warning(
                f"Kafka no disponible (intento {attempt}/{retries}). "
                f"Reintentando en {delay}s..."
            )
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a Kafka tras varios intentos.")

def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    # Crear producer
    producer = create_producer()
    logger.info(f"Producer conectado a {KAFKA_BOOTSTRAP_SERVERS}")

    # Loop principal
    while running:
        try:
            api_data = fetch_crypto_prices()
            records = build_records(api_data)
            for record in records:
                producer.send(KAFKA_TOPIC, value=record)
                logger.info(f"Enviado: {record['coin']} = ${record['price_usd']}")
            producer.flush()
            time.sleep(FETCH_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()