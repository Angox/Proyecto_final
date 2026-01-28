import os
import json
import ccxt
import pandas as pd
import numpy as np
import boto3
from io import StringIO
from datetime import datetime
from gremlin_python.driver import client, serializer

# Configuración
NEPTUNE_ENDPOINT = os.environ.get('NEPTUNE_ENDPOINT')
S3_BUCKET = os.environ.get('S3_BUCKET')
CSV_FILENAME = "market_leaders_history.csv"
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
        return pd.DataFrame(), {}

    # Filtramos pares USDC
    symbols = [s for s in markets if s.endswith('/USDC')]
    # Aumentamos un poco el límite para tener mejor visión de mercado
    selected_symbols = symbols[:60] 
    
    close_data = {}
    volume_data = {}
    metadata = {}
    
    print(f"Descargando datos para: {len(selected_symbols)} activos")
    
    for sym in selected_symbols:
        try:
            # Traemos velas
            ohlcv = exchange.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=LIMIT)
            if not ohlcv: continue
                
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Limpieza básica
            df = df[~df.index.duplicated(keep='first')]
            
            coin_name = sym.split('/')[0]
            close_data[coin_name] = df['close']
            
            # --- NUEVO: CÁLCULO DE MÉTRICAS INDIVIDUALES ---
            # 1. Volatilidad (Desviación estándar de los retornos logarítmicos de los últimos 60 min)
            df['log_ret'] = np.log(df['close'] / df['close'].shift(1))
            volatility = df['log_ret'].tail(60).std() * 100 # Porcentaje
            
            # 2. Volumen Relativo (Volumen actual vs Promedio 20 periodos)
            avg_vol = df['volume'].rolling(20).mean().iloc[-1]
            cur_vol = df['volume'].iloc[-1]
            vol_ratio = round(cur_vol / avg_vol, 2) if avg_vol > 0 else 0
            
            metadata[coin_name] = {
                'volatility': float(volatility) if not pd.isna(volatility) else 0.0,
                'volume_ratio': float(vol_ratio)
            }
            
        except Exception as e:
            print(f"Error en {sym}: {e}")
            
    if not close_data: return pd.DataFrame(), {}

    full_df = pd.DataFrame(close_data)
    full_df = full_df.ffill().dropna()
    
    return full_df, metadata

def calculate_correlations(df):
    results = []
    columns = df.columns
    THRESHOLD = 0.70 # Subimos un poco el umbral para calidad
    
    print("Calculando matriz de correlación y lags...")
    
    for asset_a in columns:
        for asset_b in columns:
            if asset_a == asset_b: continue
            
            best_corr = 0
            best_lag = 0
            
            # Buscamos quién mueve a quién en una ventana de +/- 15 minutos
            for lag in range(-15, 16):
                if lag == 0: continue # Ignoramos movimiento instantáneo exacto para buscar causalidad
                
                # Si desplazamos B hacia el futuro (lag negativo) y correlaciona con A hoy,
                # significa que A se movió ANTES que B. A lidera.
                df_shifted = df[asset_b].shift(-lag) 
                corr = df[asset_a].corr(df_shifted)
                
                if pd.isna(corr): continue
                if abs(corr) > abs(best_corr):
                    best_corr = corr
                    best_lag = lag
            
            # Filtramos solo correlaciones fuertes
            if abs(best_corr) > THRESHOLD:
                results.append({
                    'leader': asset_a,
                    'follower': asset_b,
                    'correlation': float(best_corr),
                    'lag_minutes': int(best_lag)
                })
    return results

def update_neptune(relationships, metadata):
    """
    Actualiza el grafo enriqueciendo Vértices (Metadata) y Aristas (Relaciones).
    """
    if not relationships:
        print("No hay relaciones para guardar.")
        return

    print(f"Conectando a Neptune: {NEPTUNE_ENDPOINT}")
    g_client = None
    try:
        g_client = client.Client(f'wss://{NEPTUNE_ENDPOINT}:8182/gremlin', 'g')
        
        # 1. ACTUALIZAR VÉRTICES CON METADATA (Volatilidad y Volumen)
        # Esto es clave: Ahora el grafo sabe si la moneda es volátil o tiene volumen.
        unique_coins = set(metadata.keys())
        
        for coin in unique_coins:
            meta = metadata.get(coin, {'volatility': 0, 'volume_ratio': 0})
            
            # Upsert del vértice con propiedades nuevas
            g_client.submit(f"""
            g.V().has('coin', 'symbol', '{coin}')
             .fold().coalesce(
                unfold()
                .property('volatility', {meta['volatility']})
                .property('volume_ratio', {meta['volume_ratio']})
                .property('last_seen', '{datetime.now().isoformat()}'), 
                addV('coin')
                .property('symbol', '{coin}')
                .property('volatility', {meta['volatility']})
                .property('volume_ratio', {meta['volume_ratio']})
                .property('last_seen', '{datetime.now().isoformat()}')
             )
            """).all().result()
            
        # 2. INSERTAR ARISTAS (Relaciones Líder -> Seguidor)
        for rel in relationships:
            # Borrar relación vieja para no duplicar fuerza
            drop_query = f"""
            g.V().has('coin', 'symbol', '{rel['leader']}')
             .outE('leads').where(inV().has('coin', 'symbol', '{rel['follower']}')).drop()
            """
            g_client.submit(drop_query).all().result()
            
            # Crear nueva relación
            add_query = f"""
            g.V().has('coin', 'symbol', '{rel['leader']}').as('a')
             .V().has('coin', 'symbol', '{rel['follower']}').as('b')
             .addE('leads').from('a').to('b')
             .property('correlation', {rel['correlation']})
             .property('lag', {rel['lag_minutes']})
             .property('updated_at', '{datetime.now().isoformat()}')
            """
            g_client.submit(add_query).all().result()

    except Exception as e:
        print(f"ERROR NEPTUNE UPDATE: {e}")
    finally:
        if g_client: g_client.close()

def get_leaders_analytics():
    """
    Consulta avanzada a Neptune para extraer inteligencia de mercado.
    """
    print("--- CONSULTANDO ANALÍTICAS AVANZADAS EN NEPTUNE ---")
    g_client = None
    leaders_data = []
    
    try:
        g_client = client.Client(f'wss://{NEPTUNE_ENDPOINT}:8182/gremlin', 'g')
        
        # QUERY AVANZADA:
        # 1. Busca nodos que tienen aristas salientes ('leads')
        # 2. Proyecta sus datos de mercado (volatilidad, volumen)
        # 3. Calcula su 'influence_score' (OutDegree)
        # 4. Calcula su 'independence_score' (InDegree - si es 0, nadie lo manda)
        query = """
        g.V().where(outE('leads')).project('leader', 'volatility', 'volume_ratio', 'influence_score', 'independence_score', 'followers_info')
         .by('symbol')
         .by(coalesce(values('volatility'), constant(0.0)))
         .by(coalesce(values('volume_ratio'), constant(0.0)))
         .by(outE('leads').count())
         .by(inE('leads').count())
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
            volatility = r['volatility']
            vol_ratio = r['volume_ratio']
            influence = r['influence_score'] # A cuántos lidero
            independence = r['independence_score'] # Cuántos me lideran a mí (Menor es mejor para un líder puro)
            
            followers_raw = r['followers_info']
            
            # Deduplicación lógica en Python
            unique_followers_dict = {}
            for f in followers_raw:
                unique_followers_dict[f['symbol']] = f
            followers_clean = list(unique_followers_dict.values())
            
            count = len(followers_clean)
            if count == 0: continue

            # Cálculos de grupo
            avg_corr = sum([abs(f['corr']) for f in followers_clean]) / count
            avg_lag = sum([f['lag'] for f in followers_clean]) / count
            
            followers_str = "; ".join([f"{f['symbol']}({f['corr']:.2f})" for f in followers_clean])
            
            # Lógica de "Calidad de Líder"
            # Un buen líder tiene alta influencia (mueve a muchos) y alta independencia (nadie lo mueve a él)
            leader_quality = "WEAK"
            if influence >= 3 and independence == 0:
                leader_quality = "ALPHA" # Líder puro
            elif influence >= 5:
                leader_quality = "STRONG"
            
            leaders_data.append({
                'timestamp': timestamp,
                'leader': leader,
                'leader_quality': leader_quality, # NUEVO
                'volatility_score': round(volatility, 4), # NUEVO
                'volume_momentum': round(vol_ratio, 2), # NUEVO
                'influence_score': influence, # NUEVO
                'independence_score': independence, # NUEVO
                'follower_count': count,
                'avg_correlation': round(avg_corr, 4),
                'avg_lag_minutes': round(avg_lag, 2),
                'followers_list': followers_str
            })
            
    except Exception as e:
        print(f"Error consultando líderes: {e}")
    finally:
        if g_client: g_client.close()
        
    return pd.DataFrame(leaders_data)

def update_csv_in_s3(new_df):
    if new_df.empty:
        print("No hay datos para guardar.")
        return

    s3 = boto3.client('s3')
    
    try:
        print(f"Buscando archivo histórico: {CSV_FILENAME}")
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=f"output/{CSV_FILENAME}")
            existing_df = pd.read_csv(obj['Body'])
            # Concatenar
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # MANTENIMIENTO:
            # Si el archivo crece demasiado, guardamos solo las últimas 5000 filas para no saturar memoria
            if len(combined_df) > 5000:
                combined_df = combined_df.tail(5000)
                
        except s3.exceptions.NoSuchKey:
            combined_df = new_df

    except Exception as e:
        print(f"Error gestionando S3: {e}")
        combined_df = new_df

    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"output/{CSV_FILENAME}",
        Body=csv_buffer.getvalue()
    )
    print(f"CSV actualizado. Filas totales: {len(combined_df)}")

def handler(event, context):
    print("--- INICIO PIPELINE DE GRAFO ---")
    
    # 1. Obtener Datos y Metadatos de Mercado
    df, metadata = get_binance_data()
    
    if not df.empty:
        # 2. Calcular Relaciones Matemáticas
        correlations = calculate_correlations(df)
        
        if correlations:
            # 3. Enriquecer el Grafo en Neptune (Topología + Datos de Mercado)
            update_neptune(correlations, metadata)
            
            # 4. Extraer Inteligencia (Insights)
            leaders_df = get_leaders_analytics()
            
            # 5. Exportar a S3 para el siguiente Lambda
            update_csv_in_s3(leaders_df)
    
    print("--- FIN ---")
    return {"statusCode": 200, "body": "OK"}
