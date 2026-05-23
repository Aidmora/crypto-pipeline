# Consumer — Spark Structured Streaming

Servicio que consume mensajes de Kafka usando **Apache Spark Structured Streaming**, parsea el JSON con un esquema definido y persiste los datos siguiendo la **arquitectura Medallion**.

## 🏗️ Arquitectura

```
Kafka topic (crypto-prices)
        │
        ▼
   Spark Streaming
        │
        └──► Bronze (Parquet)  ← datos crudos
        
        [Silver y Gold próximamente]
```

## 🛠️ Stack

- **Apache Spark 3.5.1** (Structured Streaming)
- **PySpark** como interfaz Python
- **Apache Parquet** (formato columnar)
- **Imagen base**: `apache/spark:3.5.1` (oficial de Apache Software Foundation)

## ⚙️ Configuración

El consumer se configura mediante variables de entorno:

| Variable | Default | Descripción |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Brokers de Kafka (red interna Docker) |
| `KAFKA_TOPIC` | `crypto-prices` | Topic a consumir |
| `SPARK_MASTER` | `local[*]` | Master URL de Spark |
| `DATA_PATH` | `/app/data` | Ruta base para los Parquet |

## 🚀 Ejecución

El consumer corre dentro de Docker, junto con el resto del stack:

```bash
# Desde la raíz del proyecto
docker compose up --build -d

# Ver logs del consumer en tiempo real
docker compose logs -f consumer

# Bajar el stack
docker compose down
```

## 📂 Estructura de salida

```
consumer/data/
├── bronze/                          # Datos crudos desde Kafka
│   ├── part-00000-xxx.parquet
│   ├── part-00001-xxx.parquet
│   └── _spark_metadata/
└── _checkpoints/                    # Estado del stream (NO borrar)
    └── bronze/
        ├── commits/
        ├── offsets/
        └── sources/
```

## 🧪 Inspeccionar los Parquet

Desde tu máquina local (con pandas instalado):

```python
import pandas as pd

# Leer toda la capa Bronze
df = pd.read_parquet("./consumer/data/bronze/")
print(df.head())
print(f"Total registros: {len(df)}")
print(df.dtypes)
```

## 📊 Schema de Bronze

| Columna | Tipo | Descripción |
|---|---|---|
| `key` | string | Clave del mensaje Kafka (puede ser null) |
| `value` | string | JSON crudo del mensaje |
| `topic` | string | Topic de origen |
| `partition` | int | Partición de Kafka |
| `offset` | long | Offset único del mensaje |
| `kafka_timestamp` | timestamp | Cuándo Kafka recibió el mensaje |

## 🔄 Flujo de procesamiento

1. **Lectura**: Spark se conecta al topic `crypto-prices` y consume los mensajes en micro-batches cada 10 segundos.
2. **Parseo**: Los mensajes JSON se convierten en columnas tipadas usando un schema explícito.
3. **Bronze**: Los datos crudos se persisten en formato Parquet con checkpointing.
4. **Consola**: En paralelo, los datos parseados se imprimen en consola para debugging.

## 🎯 Decisiones técnicas

### ¿Por qué `apache/spark:3.5.1` y no `bitnami/spark`?

Bitnami movió sus imágenes versionadas a un esquema de suscripción de pago en agosto de 2025. La imagen oficial de Apache Software Foundation es la alternativa más estable, mantenida y libre de licencias restrictivas.

### ¿Por qué arquitectura Medallion?

- **Reprocesabilidad**: si cambio la lógica de Silver, puedo recalcularla desde Bronze sin volver a consultar la API.
- **Auditoría**: Bronze guarda los datos tal como llegaron, útil para debugging.
- **Separación de responsabilidades**: Bronze = ingesta sin opinión; Silver = lógica de negocio.

### ¿Por qué Parquet?

- **Compresión columnar**: hasta 10x más eficiente que CSV.
- **Schema embebido**: el archivo es autodescriptivo.
- **Lecturas selectivas**: solo lee las columnas que necesitas.
- **Compatibilidad universal**: Spark, Pandas, BigQuery, DuckDB, Athena.

### ¿Por qué checkpointing?

Spark Streaming usa los checkpoints para reanudar desde el último offset procesado en caso de fallo. Sin checkpoint, podrías duplicar o perder mensajes. Con checkpoint, se garantiza procesamiento **exactly-once**.

## 🔮 Próximas capas (en desarrollo)

- **Silver**: validación de datos, enriquecimiento con campo `trend` (BULLISH/BEARISH/NEUTRAL).
- **Gold**: agregaciones por ventanas de tiempo (precio promedio, máximos, mínimos).