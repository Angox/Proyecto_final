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
LIMIT = 1000 # Reducido ligeramente para pruebas

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
    # Seleccionamos algunos más para tener variedad
    selected_symbols = symbols[:10] 
    
    data = {}
    print(f"Descargando datos para: {selected_symbols}")
    
    for sym in selected_symbols:
        try:
            ohlcv = exchange.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=LIMIT)
            if not ohlcv:
                continue
                
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Limpiamos duplicados por si acaso
            df = df[~df.index.duplicated(keep='first')]
            
            coin_name = sym.split('/')[0]
            data[coin_name] = df['close']
            print(f"-> {coin_name}: {len(df)} filas.")
            
        except Exception as e:
            print(f"Error en {sym}: {e}")
            
    print("Generando DataFrame combinado...")
    full_df = pd.DataFrame(data)
    
    print(f"Dimensiones antes de limpiar: {full_df.shape}")
    
    # MEJORA CRÍTICA: En lugar de borrar todo si falta un dato (dropna),
    # rellenamos con el valor anterior (ffill)
    full_df = full_df.fillna(method='ffill')
    
    # Si al principio hay NaNs (porque una moneda empezó más tarde), los borramos ahora
    full_df = full_df.dropna()
    
    print(f"Dimensiones finales para análisis: {full_df.shape}")
    return full_df

def calculate_correlations(df):
    results = []
    columns = df.columns
    print("Calculando correlaciones...")
    
    # Umbral de correlación (bájalo a 0.5 para pruebas si no salen resultados)
    THRESHOLD = 0.6 
    
    for asset_a in columns:
        for asset_b in columns:
            if asset_a == asset_b:
                continue
            
            best_corr = 0
            best_lag = 0
            
            # Reducimos rango de lag para ir más rápido en pruebas (-15 a +15 min)
            for lag in range(-15, 16):
                if lag == 0: continue # Opcional: ignorar correlación instantánea si buscas predicción
                
                df_shifted = df[asset_b].shift(-lag)
                
                # Correlación simple de Pearson
                corr = df[asset_a].corr(df_shifted)
                
                # Gestión de NaN resultantes del shift
                if pd.isna(corr): continue

                if abs(corr) > abs(best_corr):
                    best_corr = corr
                    best_lag = lag
            
            if abs(best_corr) > THRESHOLD:
                print(f"¡HALLAZGO! {asset_a} -> {asset_b} (Corr: {best_corr:.2f}, Lag: {best_lag})")
                results.append({
                    'leader': asset_a,
                    'follower': asset_b,
                    'correlation': float(best_corr),
                    'lag_minutes': int(best_lag)
                })
    
    print(f"Total correlaciones encontradas: {len(results)}")
    return results

def update_neptune(relationships):
    if not relationships:
        print("No hay relaciones para guardar en Neptune.")
        return []

    print(f"Conectando a Neptune: {NEPTUNE_ENDPOINT}")
    try:
        # IMPORTANTE: Usar wss:// y puerto 8182
        g_client = client.Client(f'wss://{NEPTUNE_ENDPOINT}:8182/gremlin', 'g')
        
        # Limpiar grafo previo (opcional, cuidado en producción)
        # g_client.submit("g.V().drop()")
        
        count = 0
        for rel in relationships:
            query = f"""
            g.V().has('coin', 'symbol', '{rel['leader']}').fold().coalesce(unfold(), addV('coin').property('symbol', '{rel['leader']}')).as('a').
              V().has('coin', 'symbol', '{rel['follower']}').fold().coalesce(unfold(), addV('coin').property('symbol', '{rel['follower']}')).as('b').
              addE('leads').from('a').to('b')
                .property('correlation', {rel['correlation']})
                .property('lag', {rel['lag_minutes']})
                .property('updated_at', '{datetime.now().isoformat()}')
            """
            g_client.submit(query)
            count += 1
            
        print(f"Insertadas {count} aristas en Neptune.")
        
        # Query de prueba
        query_check = "g.E().count()"
        res = g_client.submit(query_check).all().result()
        print(f"Total aristas en la DB: {res}")
        
        g_client.close()
        return relationships
        
    except Exception as e:
        print(f"ERROR NEPTUNE: {e}")
        # No relanzamos el error para que al menos guarde en S3 lo que tenga
        return relationships

def save_to_s3(data):
    if not data:
        print("Nada que guardar en S3.")
        return

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
    print("--- INICIO EJECUCIÓN ---")
    df = get_binance_data()
    
    if df.empty:
        print("El DataFrame está vacío. Abortando.")
        return {"statusCode": 200, "body": "No Data"}
        
    correlations = calculate_correlations(df)
    
    # Intentamos guardar en Neptune, si falla, seguimos a S3
    update_neptune(correlations)
    
    # Guardamos resultados en S3
    save_to_s3(correlations)
    
    print("--- FIN EJECUCIÓN ---")
    return {
        "statusCode": 200,
        "body": json.dumps(f"Procesado. Relaciones encontradas: {len(correlations)}")
    }
