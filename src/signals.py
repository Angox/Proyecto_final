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
        # Pandas inferirá las nuevas columnas automáticamente
        return pd.read_csv(response['Body'])
    except Exception as e:
        print(f"Error leyendo S3: {e}")
        return pd.DataFrame()

def detect_strategies(row):
    """
    Analiza una fila y genera señales usando DATOS AVANZADOS (Volumen, Volatilidad, Calidad).
    """
    signals = []
    
    # Datos Básicos
    leader = row['leader']
    corr = row['avg_correlation']
    lag = row['avg_lag_minutes']
    followers_count = row['follower_count']
    timestamp = row['timestamp']
    
    # --- NUEVOS DATOS (Con manejo de errores por si hay filas viejas) ---
    # Usamos .get() o verificamos si la columna existe en la row
    quality = row['leader_quality'] if 'leader_quality' in row else 'WEAK'
    volatility = float(row['volatility_score']) if 'volatility_score' in row and not pd.isna(row['volatility_score']) else 0.0
    volume_mom = float(row['volume_momentum']) if 'volume_momentum' in row and not pd.isna(row['volume_momentum']) else 1.0
    influence = int(row['influence_score']) if 'influence_score' in row and not pd.isna(row['influence_score']) else followers_count

    # ---------------------------------------------------------
    # ESTRATEGIA 1: ALPHA PREDATOR (NUEVA - MÁXIMA CALIDAD)
    # ---------------------------------------------------------
    # Un líder 'ALPHA' (independiente) que se mueve con VOLUMEN (>1.1x promedio).
    # Esta es la señal más fiable del sistema.
    if quality == 'ALPHA' and volume_mom > 1.1:
        signals.append({
            'strategy': 'ALPHA_PREDATOR',
            'signal_strength': 'CRITICAL', # Máxima prioridad
            'description': f"ALPHA Leader {leader} moving with Volume ({volume_mom}x). Pure trend origin.",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS_AGGRESSIVE',
            'condition': 'Immediate Entry'
        })

    # ---------------------------------------------------------
    # ESTRATEGIA 2: VOLATILITY BREAKOUT (NUEVA)
    # ---------------------------------------------------------
    # El líder tiene alta volatilidad (>0.4%) y arrastra a otros.
    # Indica que el movimiento es explosivo, no ruido lateral.
    if volatility > 0.4 and lag > 0.1 and corr > 0.6:
        signals.append({
            'strategy': 'VOL_BREAKOUT',
            'signal_strength': 'HIGH',
            'description': f"{leader} High Volatility ({volatility:.2f}%) breakout detected.",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS',
            'condition': 'Breakout Catch'
        })

    # ---------------------------------------------------------
    # ESTRATEGIA 3: LEADER MOMENTUM (CLÁSICA - Permisiva)
    # ---------------------------------------------------------
    # Mantenemos umbrales bajos para asegurar flujo de datos
    if lag > 0.15 and corr > 0.55:
        signals.append({
            'strategy': 'LEADER_MOMENTUM',
            'signal_strength': 'MEDIUM',
            'description': f"Standard lead: {leader} is {lag}m ahead.",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS', 
            'condition': 'Standard Scalp'
        })

    # ---------------------------------------------------------
    # ESTRATEGIA 4: VOLUME DIVERGENCE (NUEVA)
    # ---------------------------------------------------------
    # El líder tiene mucho volumen pero el precio apenas se mueve (lag bajo)
    # Esto suele anticipar un movimiento explosivo inminente.
    if volume_mom > 2.0 and abs(lag) < 0.5:
        signals.append({
            'strategy': 'VOLUME_LOADING',
            'signal_strength': 'HIGH',
            'description': f"Huge Volume ({volume_mom}x) on {leader} without huge lag yet. Loading phase.",
            'action_asset': leader,
            'trade_asset': leader, # Operar el líder directamente
            'condition': 'Anticipate Breakout'
        })

    # ---------------------------------------------------------
    # ESTRATEGIA 5: LAG CATCH-UP (Recuperación)
    # ---------------------------------------------------------
    if lag < -1.0 and corr > 0.60:
        signals.append({
            'strategy': 'LAG_CATCHUP',
            'signal_strength': 'MEDIUM',
            'description': f"{leader} lagging behind group.",
            'action_asset': 'FOLLOWERS', 
            'trade_asset': leader, 
            'condition': 'Reversion'
        })

    # ---------------------------------------------------------
    # ESTRATEGIA 6: INSTANT SYNC (Arbitraje)
    # ---------------------------------------------------------
    if abs(lag) < 0.08 and corr > 0.92:
        signals.append({
            'strategy': 'INSTANT_SYNC',
            'signal_strength': 'HFT',
            'description': f"Perfect sync {leader}. Arbitrage/Bot lock.",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS',
            'condition': 'HFT Execution'
        })

    # Formatear salida común
    final_output = []
    for s in signals:
        s.update({
            'generated_at': datetime.now().isoformat(),
            'data_timestamp': timestamp,
            'leader_symbol': leader,
            'leader_quality': quality,    # Dato útil en el output
            'volatility': volatility,     # Dato útil en el output
            'volume_ratio': volume_mom    # Dato útil en el output
        })
        final_output.append(s)
        
    return final_output

def process_signals(df):
    if df.empty: return pd.DataFrame()
    
    # Tomamos solo el último snapshot de tiempo disponible
    # (Aseguramos que ordenamos por timestamp por si acaso viene desordenado)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    last_timestamp = df['timestamp'].iloc[-1]
    
    current_market_state = df[df['timestamp'] == last_timestamp]
    
    all_signals = []
    
    for _, row in current_market_state.iterrows():
        row_signals = detect_strategies(row)
        all_signals.extend(row_signals)
        
    return pd.DataFrame(all_signals)

def update_signals_csv(new_signals_df):
    if new_signals_df.empty:
        print("No se encontraron patrones operables.")
        return

    try:
        print(f"Buscando histórico en {SIGNALS_BUCKET}...")
        obj = s3.get_object(Bucket=SIGNALS_BUCKET, Key=CSV_OUTPUT_NAME)
        existing_df = pd.read_csv(obj['Body'])
        combined_df = pd.concat([existing_df, new_signals_df], ignore_index=True)
        
        # Mantenimiento: Limitar tamaño archivo señales
        if len(combined_df) > 2000:
             combined_df = combined_df.tail(2000)
             
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
    print(f"¡Éxito! {len(new_signals_df)} nuevas señales guardadas.")

def handler(event, context):
    print("--- INICIANDO ANÁLISIS DE SEÑALES V2 (NEPTUNE ENHANCED) ---")
    
    src_bucket = INPUT_BUCKET
    src_key = DEFAULT_INPUT_KEY
    
    # Detección Robust de Evento
    try:
        if 'Records' in event and len(event['Records']) > 0:
            if 's3' in event['Records'][0]:
                src_bucket = event['Records'][0]['s3']['bucket']['name']
                src_key = event['Records'][0]['s3']['object']['key']
    except Exception:
        pass # Fallback a defaults

    try:
        df_history = get_latest_data(src_bucket, src_key)
        
        if df_history.empty:
            print("CSV de entrada vacío.")
            return {"statusCode": 404, "body": "Empty Input"}

        signals_df = process_signals(df_history)
        
        if not signals_df.empty:
            print(f"Señales Generadas: {len(signals_df)}")
            # Loguear las mejores señales
            best_signals = signals_df[signals_df['signal_strength'].isin(['CRITICAL', 'HIGH'])]
            if not best_signals.empty:
                print("--- TOP SEÑALES ---")
                print(best_signals[['strategy', 'leader_symbol', 'volume_ratio']].head())
            
            update_signals_csv(signals_df)
        else:
            print("Sin señales (Mercado tranquilo).")
            
    except Exception as e:
        print(f"ERROR: {e}")
        return {"statusCode": 500, "body": str(e)}
        
    return {"statusCode": 200, "body": "Signals Processed"}
