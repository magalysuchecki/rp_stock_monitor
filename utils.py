from datetime import datetime as d

def formatear_fecha(timestamp):
    return d.utcfromtimestamp(
        timestamp / 1000).strftime('%d/%m/%Y %H:%M:%S') 

