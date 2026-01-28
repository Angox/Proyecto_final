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

    # --- ESTRATEGIA 1: ALPHA PREDATOR ---
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

    # --- ESTRATEGIA 3: LEADER MOMENTUM (Relajado) ---
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
        print("DataFrame vacío.")
        return pd.DataFrame()
    
    # Asegurar orden cronológico y tomar último snapshot
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        last_timestamp = df['timestamp'].iloc[-1].isoformat()
        current_market_state = df[df['timestamp'] == df['timestamp'].iloc[-1]]
    else:
        # Fallback si el CSV está corrupto o es muy simple
        last_timestamp = datetime.now().isoformat()
        current_market_state = df
    
    all_signals = []
    
    # 1. Procesar todas las filas
    for _, row in current_market_state.iterrows():
        row_signals = detect_strategies(row)
        all_signals.extend(row_signals)
        
    # 2. LOGICA DE SEGURIDAD: Si no hay señales, generar LOG para CSV
    if not all_signals:
        print("⚠️ No se detectaron estrategias activas. Generando fila de control.")
        all_signals.append({
            'generated_at': datetime.now().isoformat(),
            'data_timestamp': last_timestamp,
            'leader_symbol': 'NONE',
            'strategy': 'NO_SIGNALS_DETECTED', 
            'signal_strength': 'INFO',
            'description': 'Market analyzed successfully but no patterns met thresholds.',
            'action_asset': '-',
            'trade_asset': '-',
            'condition': '-',
            'leader_quality': '-',
            'volatility': 0.0,
            'volume_ratio': 0.0
        })
        
    return pd.DataFrame(all_signals)

def update_signals_csv(new_signals_df):
    # Sin condiciones: SIEMPRE intenta escribir
    try:
        print(f"Buscando histórico en {SIGNALS_BUCKET}...")
        try:
            obj = s3.get_object(Bucket=SIGNALS_BUCKET, Key=CSV_OUTPUT_NAME)
            existing_df = pd.read_csv(obj['Body'])
            combined_df = pd.concat([existing_df, new_signals_df], ignore_index=True)
            
            # Limpieza para no saturar disco
            if len(combined_df) > 3000:
                 combined_df = combined_df.tail(3000)
        except s3.exceptions.NoSuchKey:
            print("Archivo no existe. Creando nuevo.")
            combined_df = new_signals_df
        except Exception as e:
            print(f"Error leyendo histórico (creando nuevo): {e}")
            combined_df = new_signals_df

        csv_buffer = StringIO()
        combined_df.to_csv(csv_buffer, index=False)
        
        s3.put_object(
            Bucket=SIGNALS_BUCKET,
            Key=CSV_OUTPUT_NAME,
            Body=csv_buffer.getvalue()
        )
        print(f"¡ÉXITO! CSV actualizado con {len(new_signals_df)} filas nuevas.")
        
    except Exception as e:
        print(f"ERROR CRÍTICO escribiendo en S3: {e}")

def handler(event, context):
    print("--- INICIO ANÁLISIS ---")
    
    # 1. DETERMINAR ORIGEN DE DATOS (Solución al KeyError)
    src_bucket = INPUT_BUCKET
    src_key = DEFAULT_INPUT_KEY
    
    try:
        # Verificamos si viene de un trigger S3 real
        if event and 'Records' in event and len(event['Records']) > 0:
            if 's3' in event['Records'][0]:
                src_bucket = event['Records'][0]['s3']['bucket']['name']
                src_key = event['Records'][0]['s3']['object']['key']
                print(f"Trigger S3 detectado: {src_key}")
        else:
            print("Trigger manual o desconocido. Usando bucket/key por defecto.")
    except Exception as e:
        print(f"Advertencia analizando evento: {e}. Usando defaults.")

    # 2. PROCESO PRINCIPAL
    try:
        df_history = get_latest_data(src_bucket, src_key)
        
        if df_history.empty:
            print("El CSV de entrada está vacío. No se puede procesar.")
            return {"statusCode": 200, "body": "Input Empty"}

        # Procesar (Esto ahora siempre devuelve un DataFrame, aunque sea con NO_SIGNALS)
        signals_df = process_signals(df_history)
        
        # Guardar (Esto escribe incondicionalmente)
        update_signals_csv(signals_df)
            
    except Exception as e:
        print(f"ERROR FATAL: {e}")
        # Retornamos 200 para que Lambda no reintente infinitamente si es un error lógico
        return {"statusCode": 200, "body": str(e)}
        
    return {"statusCode": 200, "body": "Analysis Complete"}
