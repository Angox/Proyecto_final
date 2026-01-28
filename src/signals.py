import os
import boto3
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime

# Configuración
SIGNALS_BUCKET = os.environ.get('SIGNALS_BUCKET')
INPUT_BUCKET = os.environ.get('INPUT_BUCKET')
CSV_OUTPUT_NAME = "trading_signals.csv"
DEFAULT_INPUT_KEY = "output/market_leaders_history.csv"

s3 = boto3.client('s3')

def get_latest_data(bucket, key):
    print(f"--- Leyendo archivo: s3://{bucket}/{key} ---")
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(response['Body'])
    except Exception as e:
        print(f"Error leyendo S3: {e}")
        return pd.DataFrame()

def detect_strategies(row):
    """
    Analiza una fila y genera señales.
    """
    signals = []
    
    leader = row['leader']
    corr = row['avg_correlation']
    lag = row['avg_lag_minutes']
    followers_count = row['follower_count']
    followers_str = row['followers_list']
    timestamp = row['timestamp']
    
    # Obtener datos enriquecidos (con seguridad por si faltan columnas)
    quality = row['leader_quality'] if 'leader_quality' in row else 'WEAK'
    volatility = float(row['volatility_score']) if 'volatility_score' in row and not pd.isna(row['volatility_score']) else 0.0
    volume_mom = float(row['volume_momentum']) if 'volume_momentum' in row and not pd.isna(row['volume_momentum']) else 1.0

    # --- ESTRATEGIA 1: ALPHA PREDATOR (Calidad + Volumen) ---
    if quality == 'ALPHA' and volume_mom > 1.1:
        signals.append({
            'strategy': 'ALPHA_PREDATOR',
            'signal_strength': 'CRITICAL',
            'description': f"ALPHA Leader {leader} moving with Volume ({volume_mom}x).",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS_AGGRESSIVE',
            'condition': 'Immediate Entry'
        })

    # --- ESTRATEGIA 2: VOLATILITY BREAKOUT ---
    if volatility > 0.4 and lag > 0.1 and corr > 0.6:
        signals.append({
            'strategy': 'VOL_BREAKOUT',
            'signal_strength': 'HIGH',
            'description': f"{leader} High Volatility ({volatility:.2f}%) breakout.",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS',
            'condition': 'Breakout Catch'
        })

    # --- ESTRATEGIA 3: LEADER MOMENTUM (Estándar) ---
    if lag > 0.15 and corr > 0.55:
        signals.append({
            'strategy': 'LEADER_MOMENTUM',
            'signal_strength': 'MEDIUM',
            'description': f"Standard lead: {leader} is {lag}m ahead.",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS', 
            'condition': 'Scalp'
        })

    # --- ESTRATEGIA 4: VOLUME LOADING ---
    if volume_mom > 2.0 and abs(lag) < 0.5:
        signals.append({
            'strategy': 'VOLUME_LOADING',
            'signal_strength': 'HIGH',
            'description': f"High Volume ({volume_mom}x) on {leader} preparing move.",
            'action_asset': leader,
            'trade_asset': leader,
            'condition': 'Anticipate'
        })

    # --- ESTRATEGIA 5: LAG CATCH-UP ---
    if lag < -1.0 and corr > 0.60:
        signals.append({
            'strategy': 'LAG_CATCHUP',
            'signal_strength': 'MEDIUM',
            'description': f"{leader} lagging behind group.",
            'action_asset': 'FOLLOWERS', 
            'trade_asset': leader, 
            'condition': 'Reversion'
        })

    # --- ESTRATEGIA 6: INSTANT SYNC ---
    if abs(lag) < 0.08 and corr > 0.92:
        signals.append({
            'strategy': 'INSTANT_SYNC',
            'signal_strength': 'HFT',
            'description': f"Perfect sync {leader}. Arbitrage lock.",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS',
            'condition': 'HFT'
        })

    # Formatear salida
    final_output = []
    for s in signals:
        s.update({
            'generated_at': datetime.now().isoformat(),
            'data_timestamp': timestamp,
            'leader_symbol': leader,
            'leader_quality': quality,
            'volatility': volatility,
            'volume_ratio': volume_mom
        })
        final_output.append(s)
        
    return final_output

def process_signals(df):
    if df.empty: 
        print("DataFrame vacío, no se puede procesar.")
        return pd.DataFrame()
    
    # Asegurar orden cronológico y tomar último snapshot
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    last_timestamp = df['timestamp'].iloc[-1].isoformat()
    
    current_market_state = df[df['timestamp'] == df['timestamp'].iloc[-1]]
    
    all_signals = []
    
    # 1. Procesar todas las filas
    for _, row in current_market_state.iterrows():
        row_signals = detect_strategies(row)
        all_signals.extend(row_signals)
        
    # 2. LOGICA DE RELLENO: Si no hay señales, crear registro de actividad nula
    if not all_signals:
        print("⚠️ No se detectaron estrategias activas. Generando log de 'MARKET_QUIET'.")
        all_signals.append({
            'generated_at': datetime.now().isoformat(),
            'data_timestamp': last_timestamp,
            'leader_symbol': 'NONE',
            'strategy': 'NO_SIGNALS_DETECTED', # Para que sepas que el lambda corrió
            'signal_strength': 'INFO',
            'description': 'Market analyzed but no thresholds were met.',
            'action_asset': '-',
            'trade_asset': '-',
            'condition': '-',
            'leader_quality': '-',
            'volatility': 0.0,
            'volume_ratio': 0.0
        })
        
    return pd.DataFrame(all_signals)

def update_signals_csv(new_signals_df):
    # Ya no verificamos si está vacío, porque process_signals siempre devuelve algo
    
    try:
        print(f"Buscando histórico en {SIGNALS_BUCKET}...")
        obj = s3.get_object(Bucket=SIGNALS_BUCKET, Key=CSV_OUTPUT_NAME)
        existing_df = pd.read_csv(obj['Body'])
        combined_df = pd.concat([existing_df, new_signals_df], ignore_index=True)
        
        # Mantenimiento: Limpiar logs viejos si crece mucho (opcional)
        if len(combined_df) > 5000:
             combined_df = combined_df.tail(5000)
             
    except Exception:
        print("Creando archivo de señales desde cero.")
        combined_df = new_signals_df

    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    
    s3.put_object(
        Bucket=SIGNALS_BUCKET,
        Key=CSV_OUTPUT_NAME,
        Body=csv_buffer.getvalue()
    )
    print(f"¡CSV actualizado! Filas agregadas: {len(new_signals_df)}")

def handler(event, context):
    print("--- INICIO ANÁLISIS V3 (ALWAYS LOG) ---")
    
    src_bucket = INPUT_BUCKET
    src_key = DEFAULT_INPUT_KEY
    
    # Intento de lectura del evento S3
    try:
        if 'Records' in event and len(event['Records']) > 0:
            if 's3' in event['Records'][0]:
                src_bucket = event['Records'][0]['s3']['bucket']['name']
                src_key = event['Records'][0]['s3']['object']['key']
    except Exception:
        pass

    try:
        df_history = get_latest_data(src_bucket, src_key)
        
        if df_history.empty:
            print("Error: CSV de entrada vacío.")
            return {"statusCode": 404, "body": "Empty Input"}

        # Procesar (ahora siempre devuelve al menos una fila)
        signals_df = process_signals(df_history)
        
        # Guardar siempre
        update_signals_csv(signals_df)
            
    except Exception as e:
        print(f"ERROR CRÍTICO: {e}")
        return {"statusCode": 500, "body": str(e)}
        
    return {"statusCode": 200, "body": "Analysis Complete"}
