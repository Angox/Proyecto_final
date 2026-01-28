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
    VERSION PERMISIVA: Umbrales reducidos para captar más movimientos.
    """
    signals = []
    
    leader = row['leader']
    corr = row['avg_correlation']
    lag = row['avg_lag_minutes']
    followers_count = row['follower_count']
    followers_str = row['followers_list']
    timestamp = row['timestamp']

    # --- ESTRATEGIA 1: LEADER MOMENTUM ---
    # CAMBIO: Lag > 0.5 (antes 1.0) y Corr > 0.60 (antes 0.75)
    # Riesgo: Puede dar señales en movimientos pequeños o menos claros.
    if lag > 0.5 and corr > 0.60:
        signals.append({
            'strategy': 'LEADER_MOMENTUM',
            'signal_strength': 'HIGH' if corr > 0.80 else 'MEDIUM',
            'description': f"Leader {leader} moves {lag}m ahead (Permissive Mode). Watch followers.",
            'action_asset': leader, 
            'trade_asset': 'FOLLOWERS', 
            'condition': 'Wait for Leader Breakout'
        })

    # --- ESTRATEGIA 2: LAG CATCH-UP (Reversión / Retraso) ---
    # CAMBIO: Lag < -1.0 (antes -2.0) y Corr > 0.70 (antes 0.80)
    # Riesgo: Entrar en activos que simplemente son lentos, no necesariamente retrasados.
    if lag < -1.0 and corr > 0.70:
        signals.append({
            'strategy': 'LAG_CATCHUP',
            'signal_strength': 'MEDIUM',
            'description': f"{leader} is lagging {abs(lag)}m behind. Catch-up potential.",
            'action_asset': 'FOLLOWERS', 
            'trade_asset': leader, 
            'condition': 'Immediate Entry if Divergence'
        })

    # --- ESTRATEGIA 3: INVERSE HEDGE (Correlación Negativa) ---
    # CAMBIO: Detecta desde -0.50 (antes -0.70)
    if "(-" in followers_str: 
        pairs = followers_str.split(';')
        for p in pairs:
            try:
                clean_p = p.strip()
                symbol = clean_p.split('(')[0]
                val_str = clean_p.split('(')[1].replace(')', '')
                val = float(val_str)
                
                # Umbral más relajado para correlación inversa
                if val < -0.50:
                    signals.append({
                        'strategy': 'INVERSE_PAIR',
                        'signal_strength': 'LOW' if val > -0.7 else 'MEDIUM',
                        'description': f"{leader} moves OPPOSITE to {symbol} (Corr: {val}).",
                        'action_asset': leader,
                        'trade_asset': symbol,
                        'condition': 'Trade Opposite Direction'
                    })
            except:
                continue

    # --- ESTRATEGIA 4: MARKET DRIVER (Sentimiento General) ---
    # CAMBIO: Solo 3 seguidores requeridos (antes 5) y Corr > 0.70
    if followers_count >= 3 and corr > 0.70:
        signals.append({
            'strategy': 'MARKET_DRIVER',
            'signal_strength': 'HIGH',
            'description': f"{leader} is driving a cluster of {followers_count} assets.",
            'action_asset': leader,
            'trade_asset': 'ALL_MARKET',
            'condition': 'Trend Confirmation'
        })

    # Formatear salida común
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
        
    return final_output

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
