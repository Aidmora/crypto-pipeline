"""
Crypto Price Producer.

Consulta los precios de criptomonedas desde CoinGecko y los publica
en un topic de Kafka en formato JSON.
"""

import json
import time

import requests
from kafka import KafkaProducer


# Configuración
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "crypto-prices"
FETCH_INTERVAL_SECONDS = 10

COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = ["bitcoin", "ethereum", "cardano", "solana", "polkadot"]
VS_CURRENCY = "usd"


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


def build_records(api_data):
    """Transforma la respuesta de la API en registros normalizados."""
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    records = []
    for coin, metrics in api_data.items():
        records.append({
            "coin": coin,
            "price_usd": metrics.get("usd"),
            "market_cap_usd": metrics.get("usd_market_cap"),
            "volume_24h_usd": metrics.get("usd_24h_vol"),
            "change_24h_pct": metrics.get("usd_24h_change"),
            "timestamp": timestamp,
        })
    return records


def main():
    # Crear producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Producer conectado a {KAFKA_BOOTSTRAP_SERVERS}")

    # Loop principal
    while True:
        try:
            api_data = fetch_crypto_prices()
            records = build_records(api_data)
            for record in records:
                producer.send(KAFKA_TOPIC, value=record)
                print(f"Enviado: {record['coin']} = ${record['price_usd']}")
            producer.flush()
            time.sleep(FETCH_INTERVAL_SECONDS)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()