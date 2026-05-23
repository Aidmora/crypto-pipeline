FROM apache/spark:3.5.1

# Cambiar a root para instalar dependencias
USER root

WORKDIR /app

# Instalar pip si no está y actualizar
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip

# Copiar e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del consumer
COPY consumer.py .

# Crear directorio para datos con permisos amplios
RUN mkdir -p /app/data && chmod -R 777 /app/data

# Arrancar con spark-submit para incluir el conector Kafka
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "--master", "local[*]", \
     "consumer.py"]