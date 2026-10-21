import time
import json
import random
from kafka import KafkaProducer

# Lista de canciones y artistas musicales basados en tu temática de Spotify
canciones_spotify = [
    {"nombre": "Shape of You", "artista": "Ed Sheeran", "duracion_min": 3.53},
    {"nombre": "Blinding Lights", "artista": "The Weeknd", "duracion_min": 3.20},
    {"nombre": "Dance Monkey", "artista": "Tones and I", "duracion_min": 3.30},
    {"nombre": "Despacito", "artista": "Luis Fonsi", "duracion_min": 3.48},
    {"nombre": "Levitating", "artista": "Dua Lipa", "duracion_min": 3.23}
]

def generate_spotify_data():
    track = random.choice(canciones_spotify)
    return {
        "nombre": track["nombre"],
        "artista": track["artista"],
        "duracion_min": track["duracion_min"],
        "reproducciones": random.randint(1, 50),
        "timestamp": int(time.time())
    }

# Configuración del productor conectándose al servidor local de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Bucle infinito para enviar datos segundo a segundo
while True:
    data = generate_spotify_data()
    producer.send('sensor_data', value=data)
    print(f"Reproducción enviada: {data}")
    time.sleep(1)