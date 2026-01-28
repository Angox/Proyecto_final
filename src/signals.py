import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

# Configuración
SIGNALS_BUCKET = os.environ.get('SIGNALS_BUCKET')
INPUT_BUCKET = os.environ.get('INPUT_BUCKET')
CSV_OUTPUT_NAME = "trading_signals.csv"

s3 = boto3.client('s3')

def get_latest_data(bucket, key):
    print(f"Leyendo archivo: s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(response['Body'])

def detect_strategies(row):
    """
    Analiza una fila de datos y devuelve una lista de señales detectadas.
    VERSIÓN PERMISIVA: Umbrales reducidos para garantizar señales.
    """
    signals = []
    
    leader = row['leader']
    corr = row['avg_correlation']
    lag = row['avg_lag_minutes']
    followers_count = row['follower_count']
    followers_str = row['followers_list']
    timestamp = row['timestamp']

    # --- ESTRATEGIA 1: LEADER MOMENTUM (Scalping Rápido) ---
    # CAMBIO: Lag reducido de 1.0 -> 0.2 para captar BTC/ETH moviendo el mercado.
    # CAMBIO: Correlación reducida de 0.75 -> 0.70.
    if lag > 0.2 and corr > 0.70:
        strength = 'HIGH' if corr > 0.85 else 'MEDIUM'
        signals.append({
            'strategy': 'LEADER_MOMENTUM',
            'signal_strength': strength,
            'description': f"Leader {leader} moves {lag}m ahead (Corr: {corr:.2f}). Scalp followers.",
            'action_asset': leader,
            'trade_asset': 'FOLLOWERS', 
            'condition': 'Quick Scalp / Breakout'
        })

    # --- ESTRATEGIA 2: LAG CATCH-UP (Reversión / Lentos) ---
    # CAMBIO: Lag negativo de -2.0 -> -1.5. 
    # CAMBIO: Correlación bajada a 0.65. Muchos de tus datos tienen lag -13, esto debe entrar sí o sí.
    if lag < -1.5 and corr > 0.65:
        signals.append({
            'strategy': 'LAG_CATCHUP',
            'signal_strength': 'HIGH',
            'description': f"{leader} is lagging {abs(lag)}m behind group. Expect catch-up move.",
            'action_asset': 'FOLLOWERS', 
            'trade_asset': leader, # Operar este activo que va lento
            'condition': 'Entry on Lag'
        })

    # --- ESTRATEGIA 3: INVERSE HEDGE (Correlación Negativa) ---
    # CAMBIO: Umbral negativo de -0.70 -> -0.60.
    if "(-" in followers_str:
        pairs = followers_str.split(';')
        for p in pairs:
            try:
                clean_p = p.strip()
                if '(' not in clean_p: continue
                symbol = clean_p.split('(')[0]
                val_str = clean_p.split('(')[1].replace(')', '')
                val = float(val_str)
                
                # Umbral más permisivo para detectar coberturas
                if val < -0.60:
                    signals.append({
                        'strategy': 'INVERSE_PAIR',
                        'signal_strength': 'MEDIUM',
                        'description': f"{leader} vs {symbol} inverse correlation ({val}). Hedge opportunity.",
                        'action_asset': leader,
                        'trade_asset': symbol,
                        'condition': 'Trade Opposite'
                    })
            except:
                continue

    # --- ESTRATEGIA 4: HIGH CORRELATION CLUSTER (Cluster Masivo) ---
    # NUEVA ESTRATEGIA: Si tienes muchos seguidores (>10) con correlación perfecta (1.0),
    # como en tu caso de WIN/BAT/TFUEL, cualquier movimiento es señal crítica.
    if followers_count >= 10 and corr > 0.95:
        signals.append({
            'strategy': 'CLUSTER_BREAKOUT',
            'signal_strength': 'CRITICAL',
            'description': f"{leader} is part of a massive sync cluster ({followers_count} assets).",
            'action_asset': leader,
            'trade_asset': 'ANY_IN_CLUSTER',
            'condition': 'Cluster Move'
        })
        
    # --- ESTRATEGIA 5: MARKET DRIVER (Tendencia General) ---
    # CAMBIO: Correlación bajada a 0.70.
    elif followers_count >= 5 and corr > 0.70:
        signals.append({
            'strategy': 'MARKET_DRIVER',
            'signal_strength': 'HIGH',
            'description': f"{leader} driving market sentiment ({followers_count} pairs).",
            'action_asset': leader,
            'trade_asset': 'MARKET_ETFs',
            'condition': 'Trend Confirmation'
        })

    # Formatear salida
    final_output = []
    for s in signals:
        s.update({
            'generated_at': datetime.now().isoformat(),
            'data_timestamp': timestamp,
            'leader_symbol': leader,
            'avg_lag': lag,
            'avg_corr': corr
        })
        final_output.append(s)
        
    return final_outpu

def process_signals(df):
    # Tomamos solo el último snapshot de tiempo disponible en el CSV
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
    except Exception as e:
        print(f"Archivo nuevo o error leyendo: {e}. Creando desde cero.")
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
    print("--- INICIANDO ANÁLISIS DE SEÑALES (MODO PERMISIVO) ---")
    
    try:
        record = event['Records'][0]
        src_bucket = record['s3']['bucket']['name']
        src_key = record['s3']['object']['key']
        
        df_history = get_latest_data(src_bucket, src_key)
        signals_df = process_signals(df_history)
        
        if not signals_df.empty:
            print(f"Señales generadas:\n{signals_df[['strategy', 'leader_symbol', 'action_asset']].head()}")
            update_signals_csv(signals_df)
        else:
            print("Mercado sin anomalías detectables (incluso con filtros bajos).")
            
    except Exception as e:
        print(f"ERROR FATAL: {e}")
        raise e
        
    return {"statusCode": 200, "body": "Permissive Analysis Complete"}
