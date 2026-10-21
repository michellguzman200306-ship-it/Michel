from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Analisis_Spotify_Espanol').getOrCreate()

# Ruta del archivo real
file_path = 'spotify_real.csv'
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Renombramos columnas al español y calculamos la duración en minutos
df_espanol = df.withColumnRenamed('name', 'nombre') \
               .withColumnRenamed('artists', 'artista') \
               .withColumnRenamed('danceability', 'bailable') \
               .withColumnRenamed('tempo', 'tiempo') \
               .withColumn('duracion_minutos', F.round(F.col('duration_ms') / 60000, 2))

print("==================================================")
print("--- 1. ESQUEMA GENERAL DEL CONJUNTO DE DATOS ---")
print("==================================================")
df_espanol.printSchema()

print("\n==================================================")
print("--- 2. CANCIONES ORDENADAS DE LA MÁS LARGA A LA MÁS CORTA ---")
print("==================================================")
canciones_largas = df_espanol.select('nombre', 'artista', 'duracion_minutos', 'tiempo').orderBy(F.col('duracion_minutos').desc())
canciones_largas.show(10, truncate=False)

print("\n==================================================")
print("--- 3. ARTISTAS CON MAYOR CANTIDAD DE CANCIONES ---")
print("==================================================")
artistas_frecuentes = df_espanol.groupBy('artista').count().orderBy(F.col('count').desc())
artistas_frecuentes.show(10, truncate=False)

print("\n==================================================")
print("--- 4. CONSULTA: CANCIONES MUY BAILABLES Y RÁPIDAS ---")
print("==================================================")
analisis_avanzado = df_espanol.filter(
    (F.col('bailable') > 0.75) & (F.col('tiempo') > 120.0)
).select('nombre', 'artista', 'bailable', 'tiempo', 'duracion_minutos')
analisis_avanzado.show(10, truncate=False)

# Pausa estratégica para mantener activa la interfaz web
input("\nProcesamiento finalizado. Presiona Enter para cerrar la sesión de Spark...")