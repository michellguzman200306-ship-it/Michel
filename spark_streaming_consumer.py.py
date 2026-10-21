from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("SpotifyStreamingRealTime") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema estructurado para los datos de música
schema = StructType([
    StructField("nombre", StringType()),
    StructField("artista", StringType()),
    StructField("duracion_min", DoubleType()),
    StructField("reproducciones", IntegerType()),
    StructField("timestamp", TimestampType())
])

# Configurar el lector de streaming conectándose a Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Parsear el valor binario a texto JSON y aplicarle el esquema
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Agrupar por ventana de tiempo (1 minuto), nombre de canción y artista sumando las reproducciones
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "nombre", "artista") \
    .agg(_sum("reproducciones").alias("total_reproducciones"))

# Configurar la salida para que se visualice dinámicamente en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()