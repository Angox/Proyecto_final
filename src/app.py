import os
import json
import ccxt
import pandas as pd
import boto3
from datetime import datetime
from gremlin_python.driver import client, serializer

# Configuración
NEPTUNE_ENDPOINT = os.environ.get('NEPTUNE_ENDPOINT')
S3_BUCKET = os.environ.get('S3_BUCKET')
TIMEFRAME = '1m'
LIMIT = 1440 # 24 horas * 60 minutos

def get_binance_data():
    exchange = ccxt.binance()
    markets = exchange.load_markets()
    # Filtramos pares USDC
    symbols = [s for s in markets if s.endswith('/USDC')]
    
    # Para producción, limita esto a top 10-20 monedas para evitar timeout en Lambda
    # o aumenta la memoria del Lambda al máximo.
    selected_symbols = symbols[:10] 
    
    data = {}
    print("Descargando datos de Binance...")
    for sym in selected_symbols:
        try:
            ohlcv = exchange.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=LIMIT)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            data[sym.split('/')[0]] = df['close'] # Guardamos solo el precio de cierre y el nombre de la moneda (BTC, ETH)
        except Exception as e:
            print(f"Error descargando {sym}: {e}")
            
    return pd.DataFrame(data).dropna()

def calculate_correlations(df):
    results = []
    columns = df.columns
    print("Calculando correlaciones y lags...")
    
    for asset_a in columns:
        for asset_b in columns:
            if asset_a == asset_b:
                continue
            
            best_corr = 0
            best_lag = 0
            
            # Probamos lags de -30 a +30 minutos
            for lag in range(-30, 31):
                # Shift positivo: asset_b se mueve DESPUES de asset_a
                df_shifted = df[asset_b].shift(-lag) 
                corr = df[asset_a].corr(df_shifted)
                
                if abs(corr) > abs(best_corr):
                    best_corr = corr
                    best_lag = lag
            
            # Filtramos correlaciones fuertes (> 0.7 o < -0.7)
            if abs(best_corr) > 0.7:
                results.append({
                    'leader': asset_a,
                    'follower': asset_b,
                    'correlation': float(best_corr),
                    'lag_minutes': int(best_lag)
                })
    return results

def update_neptune(relationships):
    print(f"Conectando a Neptune: {NEPTUNE_ENDPOINT}")
    # Conexión Gremlin
    g_client = client.Client(f'wss://{NEPTUNE_ENDPOINT}:8182/gremlin', 'g')
    
    try:
        # Limpiar grafo anterior (opcional, depende de tu lógica de negocio)
        g_client.submit("g.V().drop()")
        
        for rel in relationships:
            # Query Gremlin para crear vertices y arista si no existen
            query = f"""
            g.V().has('coin', 'symbol', '{rel['leader']}').fold().coalesce(unfold(), addV('coin').property('symbol', '{rel['leader']}')).as('a').
              V().has('coin', 'symbol', '{rel['follower']}').fold().coalesce(unfold(), addV('coin').property('symbol', '{rel['follower']}')).as('b').
              addE('leads').from('a').to('b')
                .property('correlation', {rel['correlation']})
                .property('lag', {rel['lag_minutes']})
            """
            g_client.submit(query)
            
        print("Grafo actualizado.")
        
        # CONSULTA FINAL: Buscar rutas de líderes fuertes
        # Ejemplo: Dame quien lidera a quien con más de 0.8 de correlación
        query_analysis = "g.V().outE('leads').has('correlation', gt(0.8)).inV().path().by('symbol').by(valueMap())"
        result_set = g_client.submit(query_analysis)
        results = result_set.all().result()
        return results
        
    except Exception as e:
        print(f"Error en Neptune: {e}")
        raise e
    finally:
        g_client.close()

def save_to_s3(data):
    s3 = boto3.client('s3')
    filename = f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"output/{filename}",
        Body=json.dumps(str(data))
    )
    print(f"Guardado en S3: {filename}")

def handler(event, context):
    df = get_binance_data()
    if df.empty:
        return {"statusCode": 200, "body": "No data"}
        
    correlations = calculate_correlations(df)
    graph_results = update_neptune(correlations)
    save_to_s3(graph_results)
    
    return {
        "statusCode": 200,
        "body": json.dumps("Proceso completado correctamente")
    }
