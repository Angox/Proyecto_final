import json
import os
import time
import requests
import pandas as pd
import numpy as np
import boto3
from io import StringIO

# Configuración
S3_BUCKET = os.environ.get('S3_BUCKET_NAME')
BASE_BINANCE = "https://api.binance.com"
s3 = boto3.client('s3')

def get_top_symbols(limit=50):
    """Obtiene los pares con mayor volumen para asegurar relevancia."""
    try:
        r = requests.get(f"{BASE_BINANCE}/api/v3/ticker/24hr", params={"type":"MINI"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        # Filtramos solo USDT y ordenamos por volumen
        filtered = [x for x in data if x['symbol'].endswith("USDT") and float(x['quoteVolume']) > 0]
        filtered.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
        return [x['symbol'] for x in filtered[:limit]]
    except Exception as e:
        print(f"Error obteniendo simbolos: {e}")
        raise e

def fetch_market_data(symbol):
    """Descarga velas de 1m para las ultimas 24h."""
    now = int(time.time() * 1000)
    start = now - (24 * 60 * 60 * 1000)
    try:
        # Optimizacion: Una sola llamada de 1000 velas cubre 16 horas, suficiente para la demo.
        # Para produccion real, hacer paginación si se requieren 24h exactas (1440 mins).
        params = {"symbol": symbol, "interval": "1m", "startTime": start, "limit": 1000}
        r = requests.get(f"{BASE_BINANCE}/api/v3/klines", params=params, timeout=5)
        data = r.json()
        
        df = pd.DataFrame(data, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","quote_volume","n_trades","taker_base","taker_quote","ignore"
        ])
        df["ts"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close"] = df["close"].astype(float)
        return df.set_index("ts")["close"]
    except Exception as e:
        print(f"Error descargando {symbol}: {e}")
        return pd.Series(dtype=float)

def calculate_correlations(price_df, threshold=0.75):
    """Calcula matriz de correlacion y lag optimo."""
    edges = []
    cols = price_df.columns
    # Iterar sobre pares
    for i in range(len(cols)):
        for j in range(i+1, len(cols)):
            a, b = cols[i], cols[j]
            # Simplificación: Correlación directa de Pearson para velocidad
            # En prod: Usar window sliding o cross-correlation completa
            corr = price_df[a].corr(price_df[b])
            
            if abs(corr) > threshold:
                edges.append({
                    "~from": a,
                    "~to": b,
                    "~label": "CORRELATES_WITH",
                    "weight:Double": round(abs(corr), 4),
                    "raw_corr:Double": round(corr, 4),
                    "timestamp:Long": int(time.time())
                })
    return edges

def upload_csv_s3(df, filename):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    # Guardamos en /upload para disparar el evento S3
    key = f"upload/{filename}"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
    print(f"Subido: s3://{S3_BUCKET}/{key}")

def lambda_handler(event, context):
    print("Fase 1: ETL Iniciado")
    
    symbols = get_top_symbols(limit=50) # 50 top coins
    prices = {}
    
    for sym in symbols:
        clean_name = sym.replace("USDT", "")
        series = fetch_market_data(sym)
        if not series.empty:
            prices[clean_name] = series
            
    if not prices:
        return {"status": "Error", "message": "No data fetched"}

    price_df = pd.DataFrame(prices).dropna(how='all')
    
    # 1. Crear Nodos CSV (Formato Gremlin Load)
    nodes_data = [{"~id": col, "~label": "Crypto", "name": col} for col in price_df.columns]
    df_nodes = pd.DataFrame(nodes_data)
    upload_csv_s3(df_nodes, "nodes.csv")
    
    # 2. Crear Aristas CSV
    edges_list = calculate_correlations(price_df)
    df_edges = pd.DataFrame(edges_list)
    upload_csv_s3(df_edges, "edges.csv")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Procesado: {len(df_nodes)} nodos y {len(df_edges)} relaciones.")
    }
