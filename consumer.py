import json

from kafka import KafkaConsumer

resultado = {}

consumer = KafkaConsumer(
  bootstrap_servers=["localhost:19092"],
  group_id="demo-group",
  auto_offset_reset="earliest",
  enable_auto_commit=False,
  consumer_timeout_ms=1000, 
  value_deserializer=lambda m: json.loads(m.decode('ascii')),
)

consumer.subscribe("stock-updates")

try:
    for message in consumer:
        tr = message.value
        resultado.setdefault(tr["simbolo"], {
            "precio_ponderado": 0, 
            "volumen_total": 0, 
                })
        resultado[tr["simbolo"]]["precio_ponderado"] += tr["precio"] * tr["volumen"]
        resultado[tr["simbolo"]]["volumen_total"] += tr["volumen"]
except Exception as e:
    print(f"Error al consumir mensajes: {e}")
finally:
    for clave in resultado:
        ppp = resultado[clave]["precio_ponderado"] / resultado[clave]["volumen_total"]
        print(f'El precio promedio ponderado para {clave}: {ppp}')
    consumer.close()
