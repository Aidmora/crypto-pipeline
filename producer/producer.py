"""
Crypto Price Producer.

Consulta los precios de criptomonedas desde la API de CoinGecko.
"""

import requests


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


def main():
    print("Consultando precios de criptomonedas...")
    data = fetch_crypto_prices()
    print(data)


if __name__ == "__main__":
    main()