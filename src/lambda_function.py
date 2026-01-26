import time
import json
import os
import requests
import pandas as pd
import numpy as np
import boto3
from io import StringIO

# Configuración desde variables de entorno (inyectadas por Terraform)
S3_BUCKET = os.environ.get('S3_BUCKET_NAME')
NEPTUNE_ENDPOINT = os.environ.get('NEPTUNE_ENDPOINT')
NEPTUNE_LOADER_IAM_ROLE = os.environ.get('NEPTUNE_LOADER_IAM_ROLE')
REGION = "eu-west-1"

s3 = boto3.client('s3')
neptune = boto3.client('neptunedata', endpoint_url=f"https://{NEPTUNE_ENDPOINT}:8182", region_name=REGION)

BASE = "https://api.binance.com"

def top100_usdt_by_quote_volume():
    try:
        r = requests.get(f"{BASE}/api/v3/ticker/24hr", params={"type":"MINI"}, timeout=20)
        r.raise_for_status()
        data = r.json()
        rows = []
        for t in data:
            sym = t["symbol"]
            if sym.endswith("USDT") and "quoteVolume" in t:
                rows.append((sym, float(t["quoteVolume"])))
        rows.sort(key=lambda x: x[1], reverse=True)
        return [s for s,_ in rows[:50]] # Limitado a 50 para evitar timeout en pruebas, subir a 100 en prod
    except Exception as e:
        print(f"Error fetching top 100: {e}")
        return []

def fetch_1m_24h(symbol):
    now = int(time.time() * 1000)
    start = now - 24*60*60*1000
    try:
        # Simplificación: 1 sola llamada de 1000 velas para velocidad
        p1 = {"symbol": symbol, "interval": "1m", "startTime": start, "limit": 1000}
        k1 = requests.get(f"{BASE}/api/v3/klines", params=p1, timeout=20).json()
        
        df = pd.DataFrame(k1, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","quote_volume","n_trades","taker_base","taker_quote","ignore"
        ])
        df["ts"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close"] = df["close"].astype(float)
        return df.set_index("ts")["close"]
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return pd.Series(dtype=float)

def best_lag_minutes(x_ret: pd.Series, y_ret: pd.Series, max_lag=60):
    best = (0, np.nan)
    # Optimización: Vectorizar si fuera posible, pero el loop es claro
    for lag in range(-max_lag, max_lag+1):
        try:
            c = x_ret.corr(y_ret.shift(lag))
            if pd.notna(c) and (pd.isna(best[1]) or abs(c) > abs(best[1])):
                best = (lag, c)
        except:
            continue
    return best

def upload_to_s3(df, filename):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=f"raw/{filename}", Body=csv_buffer.getvalue())
    print(f"Uploaded {filename} to S3")

def lambda_handler(event, context):
    print("Iniciando proceso ETL Crypto...")
    
    symbols = top100_usdt_by_quote_volume()
    prices = {}
    
    # 1. Descarga de datos
    print(f"Descargando datos para {len(symbols)} simbolos...")
    for s in symbols:
        ser = fetch_1m_24h(s)
        if not ser.empty:
            prices[s.replace("USDT","")] = ser # Quitamos USDT para nodos limpios

    price_df = pd.DataFrame(prices).sort_index()
    ret = price_df.pct_change().dropna(how="all")

    # 2. Generación de CSVs para Neptune
    # Nodos: :ID, :LABEL, name
    nodes_list = []
    for col in ret.columns:
        nodes_list.append({"~id": col, "~label": "Crypto", "name": col})
    df_nodes = pd.DataFrame(nodes_list)
    
    # Aristas: :START_ID, :END_ID, :TYPE, weight:Double, lag:Int
    edges_list = []
    cols = ret.columns
    # Matriz de correlación simplificada (solo pares fuertes > 0.7 o < -0.7)
    threshold = 0.7 
    
    print("Calculando correlaciones...")
    # Iteración optimizada
    for i in range(len(cols)):
        for j in range(i+1, len(cols)):
            a = cols[i]
            b = cols[j]
            lag, corr = best_lag_minutes(ret[a], ret[b])
            
            if abs(corr) > threshold:
                # Si lag > 0, B sigue a A (A -> B). Si lag < 0, A sigue a B (B -> A)
                source, target = (a, b) if lag >= 0 else (b, a)
                edges_list.append({
                    "~from": source,
                    "~to": target,
                    "~label": "CORRELATES_WITH",
                    "weight:Double": abs(corr),
                    "lag:Int": abs(lag),
                    "raw_corr:Double": corr
                })

    df_edges = pd.DataFrame(edges_list)

    # 3. Subir a S3
    upload_to_s3(df_nodes, "nodes.csv")
    upload_to_s3(df_edges, "edges.csv")

    # 4. Trigger Neptune Bulk Load (Opcional, requiere configuración de IAM compleja)
    # Aquí simplemente retornamos éxito. La carga se suele hacer con otro evento o llamada directa.
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Proceso completado. Nodos: {len(df_nodes)}, Aristas: {len(df_edges)}')
    }
