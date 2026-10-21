Big Data (Batch y Streaming con Spark y Kafka)
Repositorio que contiene la implementación práctica del procesamiento de datos en Batch
 (analítica estática) y en Tiempo Real / Streaming (arquitectura orientada a eventos) 
utilizando Apache Spark, Python y Apache Kafka, enfocado en un caso de estudio de música (Spotify).

📂 Estructura del Proyecto
spotify_tarea.py: Script de procesamiento en Batch. 
Carga el dataset masivo de Spotify (spotify_real.csv), realiza limpieza, 
traducción de cabeceras al español, transformación de variables (duración a minutos) y
 consultas analíticas clave (canciones más largas, artistas con mayor frecuencia y filtrado avanzado de bailabilidad y tiempo).

kafka_producer.py: Script productor de streaming.
 Simula la llegada constante de reproducciones musicales de Spotify segundo a segundo y las publica en un topic de Kafka.

spark_streaming_consumer.py: Script consumidor de Spark Streaming. 
Se conecta a Kafka, parsea los flujos en formato JSON, los agrupa en ventanas de tiempo de 1 minuto y 
calcula los acumulados de reproducciones en tiempo real mediante Structured Streaming.

⚙️ Instrucciones de Ejecución
1. Procesamiento en Batch
Para ejecutar el análisis estático sobre el conjunto de datos de Spotify:
spark-submit spotify_tarea.py

2. Procesamiento en Tiempo Real (Streaming con Kafka)
Asegúrate de tener encendidos los servicios de coordinación y broker de Kafka en tu entorno (Ubuntu/VirtualBox).

Paso A: Iniciar ZooKeeper
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties

Paso B: Iniciar el servidor de Kafka
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties

Paso C: Ejecutar el Productor
python3 kafka_producer.py

Paso D: Ejecutar el Consumidor (Spark Streaming)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
