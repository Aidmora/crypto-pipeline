# Producer — Crypto Price Ingestion

Servicio que consulta los precios de criptomonedas desde la API pública de **CoinGecko** y los publica en un topic de **Apache Kafka** en formato JSON.

## Stack

- Python 3.11
- `requests` para llamadas HTTP
- `kafka-python` para integración con Kafka

## Configuración

El producer se configura mediante variables de entorno:

| Variable | Default | Descripción |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Brokers de Kafka |
| `KAFKA_TOPIC` | `crypto-prices` | Topic destino |
| `FETCH_INTERVAL_SECONDS` | `10` | Intervalo entre consultas a CoinGecko |

## Ejecución local

```bash
# Crear entorno virtual
python -m venv venv
source venv/Scripts/activate  # En Linux/Mac: source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# Asegúrate que Kafka esté corriendo (docker compose up -d desde la raíz)

# Correr el producer
python producer.py
```

## Formato del mensaje publicado

```json
{
  "coin": "bitcoin",
  "price_usd": 67432.18,
  "market_cap_usd": 1328540000000,
  "volume_24h_usd": 28394521900,
  "change_24h_pct": 2.34,
  "timestamp": "2026-05-15T21:34:12Z"
}
```

## Monedas consultadas

- Bitcoin (BTC)
- Ethereum (ETH)
- Cardano (ADA)
- Solana (SOL)
- Polkadot (DOT)