import os
import json
import ccxt
import pandas as pd
import boto3
import time
from io import StringIO
from datetime import datetime
from gremlin_python.driver import client, serializer

# Configuración
NEPTUNE_ENDPOINT = os.environ.get('NEPTUNE_ENDPOINT')
S3_BUCKET = os.environ.get('S3_BUCKET')
CSV_FILENAME = "market_leaders_history.csv" # Nombre del archivo único
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
    # Top 15 para tener más posibilidades de encontrar relaciones
    selected_symbols = symbols[:15] 
    
    data = {}
    print(f"Descargando datos para: {selected_symbols}")
    
    for sym in selected_symbols:
        try:
            ohlcv = exchange.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=LIMIT)
            if not ohlcv: continue
                
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df = df[~df.index.duplicated(keep='first')]
            
            coin_name = sym.split('/')[0]
            data[coin_name] = df['close']
            
        except Exception as e:
            print(f"Error en {sym}: {e}")
            
    if not data: return pd.DataFrame()

    full_df = pd.DataFrame(data)
    # Rellenar y limpiar
    full_df = full_df.ffill().dropna()
    
    return full_df

def calculate_correlations(df):
    results = []
    columns = df.columns
    THRESHOLD = 0.65 # Subimos un poco la exigencia
    
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
        return

    print(f"Conectando a Neptune: {NEPTUNE_ENDPOINT}")
    g_client = None
    try:
        g_client = client.Client(f'wss://{NEPTUNE_ENDPOINT}:8182/gremlin', 'g')
        
        unique_coins = set()
        for rel in relationships:
            unique_coins.add(rel['leader'])
            unique_coins.add(rel['follower'])
            
        # 1. Asegurar Vértices
        for coin in unique_coins:
            g_client.submit(f"""
            g.V().has('coin', 'symbol', '{coin}')
             .fold().coalesce(unfold(), addV('coin').property('symbol', '{coin}'))
            """).all().result()
            
        # 2. Insertar/Actualizar Aristas
        for rel in relationships:
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
            g_client.submit(query_edge).all().result()

    except Exception as e:
        print(f"ERROR NEPTUNE UPDATE: {e}")
    finally:
        if g_client: g_client.close()

def get_leaders_analytics():
    """
    Consulta Gremlin avanzada para obtener estadísticas detalladas por Líder
    """
    print("--- CONSULTANDO ANALÍTICAS EN NEPTUNE ---")
    g_client = None
    leaders_data = []
    
    try:
        g_client = client.Client(f'wss://{NEPTUNE_ENDPOINT}:8182/gremlin', 'g')
        
        # CONSULTA AVANZADA:
        # Para cada moneda que tiene aristas salientes ('leads'):
        # 1. Agrupa por símbolo del líder
        # 2. Proyecta una lista de sus seguidores con sus propiedades (corr, lag)
        query = """
        g.V().where(outE('leads')).project('leader', 'followers_info')
         .by('symbol')
         .by(outE('leads').project('symbol', 'corr', 'lag')
             .by(inV().values('symbol'))
             .by('correlation')
             .by('lag')
             .fold())
        """
        
        results = g_client.submit(query).all().result()
        
        timestamp = datetime.now().isoformat()
        
        for r in results:
            leader = r['leader']
            followers = r['followers_info']
            count = len(followers)
            
            # Calcular promedios
            avg_corr = sum([abs(f['corr']) for f in followers]) / count if count > 0 else 0
            avg_lag = sum([f['lag'] for f in followers]) / count if count > 0 else 0
            
            # Crear lista legible: "ETH(0.85), SOL(0.70)"
            followers_str = "; ".join([f"{f['symbol']}({f['corr']:.2f})" for f in followers])
            
            leaders_data.append({
                'timestamp': timestamp,
                'leader': leader,
                'follower_count': count,
                'avg_correlation': round(avg_corr, 4),
                'avg_lag_minutes': round(avg_lag, 2), # Positivo: Líder va X min por delante
                'followers_list': followers_str
            })
            
    except Exception as e:
        print(f"Error consultando líderes: {e}")
    finally:
        if g_client: g_client.close()
        
    return pd.DataFrame(leaders_data)

def update_csv_in_s3(new_df):
    if new_df.empty:
        print("No hay nuevos líderes para guardar.")
        return

    s3 = boto3.client('s3')
    
    try:
        # 1. Intentar leer el CSV existente
        print(f"Buscando archivo histórico: {CSV_FILENAME}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"output/{CSV_FILENAME}")
        existing_df = pd.read_csv(obj['Body'])
        print(f"Archivo encontrado con {len(existing_df)} registros.")
        
        # 2. Concatenar
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
    except s3.exceptions.NoSuchKey:
        print("Archivo no existe. Creando uno nuevo.")
        combined_df = new_df
    except Exception as e:
        print(f"Error leyendo S3 (posiblemente corrupto, creando nuevo): {e}")
        combined_df = new_df

    # 3. Guardar de vuelta en S3
    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"output/{CSV_FILENAME}",
        Body=csv_buffer.getvalue()
    )
    print(f"CSV actualizado guardado. Total filas: {len(combined_df)}")

def handler(event, context):
    print("--- INICIO ---")
    df = get_binance_data()
    
    if not df.empty:
        correlations = calculate_correlations(df)
        if correlations:
            update_neptune(correlations)
            
            # Obtener analíticas avanzadas desde el Grafo
            leaders_df = get_leaders_analytics()
            
            # Guardar en el CSV acumulativo
            update_csv_in_s3(leaders_df)
    
    print("--- FIN ---")
    return {"statusCode": 200, "body": "OK"}
