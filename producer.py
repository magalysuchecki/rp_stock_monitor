import websocket, json, os

from kafka import KafkaProducer

import utils as u

producer = KafkaProducer(
    bootstrap_servers="localhost:19092", 
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
    )

topic = "stock-updates"

# redpanda handlers

def on_success(metadata):
  print(f"Mensaje #{metadata.offset} producido en '{metadata.topic}'")

def on_error(e):
  print(f"Error produciendo mensaje: {e}")


# websocket handlers

def on_ws_message(ws, message):
    datos = json.loads(message)["data"]
    registros = [{
            "simbolo": d["s"],
            "precio": d["p"],
            "volumen": d["v"],
            "fecha_hora": u.formatear_fecha(d["t"]),
        } for d in datos ]
    
    for registro in registros:
        future = producer.send(topic, value=registro)
        future.add_callback(on_success)
        future.add_errback(on_error)

def on_ws_error(ws, error):
    print(error)


def on_ws_close(ws, close_status_code, close_msg):
    # cuando matamos el programa
    producer.flush()
    producer.close()
    print("--- conexión cerrada ---")


def on_ws_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

# producir mensajes
if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        "wss://ws.finnhub.io?token=%s" % os.environ.get('FN_API_KEY'),
        on_message=on_ws_message,
        on_error=on_ws_error,
        on_close=on_ws_close,
    )
    ws.on_open = on_ws_open
    ws.run_forever()
