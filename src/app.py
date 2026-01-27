import os
import json
import ccxt
import pandas as pd
import boto3
import time
from datetime import datetime
from gremlin_python.driver import client, serializer

# Configuración
NEPTUNE_ENDPOINT = os.environ.get('NEPTUNE_ENDPOINT')
S3_BUCKET = os.environ.get('S3_BUCKET')
TIMEFRAME = '1m'
LIMIT = 1000 

def get_binance_data():
    print("Iniciando conexión con Binance...")
    exchange = ccxt.binance({
        'timeout': 30000, 
        'enableRateLimit': True
    })
    
    try:
        markets = exchange.load_markets()
    except Exception as e:
        print(f"ERROR CRÍTICO cargando mercados: {e}")
        return pd.DataFrame()

    symbols = [s for s in markets if s.endswith('/USDC')]
    # Seleccionamos top 10 para asegurar datos pero no saturar
    selected_symbols = symbols[:10] 
    
    data = {}
    print(f"Descargando datos para: {selected_symbols}")
    
    for sym in selected_symbols:
        try:
            ohlcv = exchange.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=LIMIT)
            if not ohlcv: continue
                
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df = df[~df.index.duplicated(keep='first')] # Quitar duplicados
            
            coin_name = sym.split('/')[0]
            data[coin_name] = df['close']
            
        except Exception as e:
            print(f"Error en {sym}: {e}")
            
    if not data: return pd.DataFrame()

    full_df = pd.DataFrame(data)
    # Rellenar huecos (ffill) y luego borrar si quedan nulos al inicio
    full_df = full_df.fillna(method='ffill').dropna()
    
    return full_df

def calculate_correlations(df):
    results = []
    columns = df.columns
    THRESHOLD = 0.6
    
    for asset_a in columns:
        for asset_b in columns:
            if asset_a == asset_b: continue
            
            best_corr = 0
            best_lag = 0
            
            for lag in range(-15, 16):
                if lag == 0: continue
                df_shifted = df[asset_b].shift(-lag)
                corr = df[asset_a].corr(df_shifted)
                
                if pd.isna(corr): continue
                if abs(corr) > abs(best_corr):
                    best_corr = corr
                    best_lag = lag
            
            if abs(best_corr) > THRESHOLD:
                results.append({
                    'leader': asset_a,
                    'follower': asset_b,
                    'correlation': float(best_corr),
                    'lag_minutes': int(best_lag)
                })
    return results

def update_neptune(relationships):
    if not relationships:
        print("No hay relaciones para guardar.")
        return []

    print(f"Conectando a Neptune: {NEPTUNE_ENDPOINT}")
    g_client = None
    try:
        g_client = client.Client(f'wss://{NEPTUNE_ENDPOINT}:8182/gremlin', 'g')
        
        # --- PASO 1: Identificar Monedas Únicas ---
        # Extraemos set único de monedas involucradas para crearlas primero
        unique_coins = set()
        for rel in relationships:
            unique_coins.add(rel['leader'])
            unique_coins.add(rel['follower'])
            
        print(f"Asegurando {len(unique_coins)} nodos (vertices)...")
        
        # --- PASO 2: Crear/Asegurar Vértices (Uno a uno de forma síncrona) ---
        for coin in unique_coins:
            query_vertex = f"""
            g.V().has('coin', 'symbol', '{coin}')
             .fold()
             .coalesce(unfold(), addV('coin').property('symbol', '{coin}'))
            """
            # .all().result() FORZA a esperar que termine antes de seguir
            g_client.submit(query_vertex).all().result()
            
        print("Vértices sincronizados. Insertando aristas...")

        # --- PASO 3: Crear Aristas (Edges) ---
        count = 0
        for rel in relationships:
            # Query más limpia: Busca A, Busca B, Crea arista entre ellos
            query_edge = f"""
            g.V().has('coin', 'symbol', '{rel['leader']}').as('a')
             .V().has('coin', 'symbol', '{rel['follower']}').as('b')
             .coalesce(
                outE('leads').where(inV().as('b')), 
                addE('leads').from('a').to('b')
             )
             .property('correlation', {rel['correlation']})
             .property('lag', {rel['lag_minutes']})
             .property('updated_at', '{datetime.now().isoformat()}')
            """
            
            try:
                # Ejecución síncrona para evitar sobrecarga y conflictos
                g_client.submit(query_edge).all().result()
                count += 1
            except Exception as e:
                print(f"Error insertando arista {rel['leader']}->{rel['follower']}: {e}")

        print(f"Proceso Neptune finalizado. {count} relaciones procesadas.")
        return relationships
        
    except Exception as e:
        print(f"ERROR GLOBAL NEPTUNE: {e}")
        return relationships
    finally:
        if g_client:
            g_client.close()

def save_to_s3(data):
    if not data: return
    s3 = boto3.client('s3')
    filename = f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"output/{filename}",
            Body=json.dumps(data)
        )
        print(f"Guardado en S3: {filename}")
    except Exception as e:
        print(f"ERROR S3: {e}")

def handler(event, context):
    print("--- INICIO ---")
    df = get_binance_data()
    if df.empty: return {"statusCode": 200, "body": "No Data"}
    
    correlations = calculate_correlations(df)
    update_neptune(correlations)
    save_to_s3(correlations)
    
    print("--- FIN ---")
    return {"statusCode": 200, "body": "OK"}
