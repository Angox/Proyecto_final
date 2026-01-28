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
    """
    signals = []
    
    leader = row['leader']
    corr = row['avg_correlation']
    lag = row['avg_lag_minutes']
    followers_count = row['follower_count']
    followers_str = row['followers_list']
    timestamp = row['timestamp']

    # --- ESTRATEGIA 1: LEADER MOMENTUM (La original) ---
    # El líder se mueve antes (Lag positivo) y tiene fuerte correlación.
    # Acción: Observar al líder. Si se mueve fuerte, entrar en los seguidores.
    if lag > 1.0 and corr > 0.75:
        signals.append({
            'strategy': 'LEADER_MOMENTUM',
            'signal_strength': 'HIGH' if corr > 0.85 else 'MEDIUM',
            'description': f"Leader {leader} moves {lag}m ahead. Watch followers for delayed reaction.",
            'action_asset': leader, # Mirar este
            'trade_asset': 'FOLLOWERS', # Operar estos
            'condition': 'Wait for Leader Breakout'
        })

    # --- ESTRATEGIA 2: LAG CATCH-UP (Reversión / Retraso) ---
    # El "líder" tiene Lag NEGATIVO fuerte. Esto significa que en realidad 
    # este activo está reaccionando TARDÍAMENTE a los activos en su lista de seguidores.
    # Acción: Si los "seguidores" (que en realidad van delante) se han movido, operar este activo inmediatamente.
    if lag < -2.0 and corr > 0.80:
        signals.append({
            'strategy': 'LAG_CATCHUP',
            'signal_strength': 'HIGH',
            'description': f"{leader} is lagging {abs(lag)}m behind its pairs. Check if pairs moved recently.",
            'action_asset': 'FOLLOWERS', # Si estos se movieron...
            'trade_asset': leader, # ...Operar este (Catch-up)
            'condition': 'Immediate Entry if Divergence'
        })

    # --- ESTRATEGIA 3: INVERSE HEDGE (Correlación Negativa) ---
    # Buscamos en la lista de seguidores aquellos con correlación negativa (ej: -0.8).
    # Útil para coberturas (Hedging) o pares de arbitraje.
    if "(-" in followers_str: # Detección rápida de negativos en el string
        # Parseamos la lista para encontrar el negativo específico
        pairs = followers_str.split(';')
        for p in pairs:
            try:
                # Formato esperado: SYMBOL(CORR)
                clean_p = p.strip()
                symbol = clean_p.split('(')[0]
                val = float(clean_p.split('(')[1].replace(')', ''))
                
                if val < -0.70:
                    signals.append({
                        'strategy': 'INVERSE_PAIR',
                        'signal_strength': 'MEDIUM',
                        'description': f"{leader} moves OPPOSITE to {symbol} (Corr: {val}).",
                        'action_asset': leader,
                        'trade_asset': symbol,
                        'condition': 'Trade Opposite Direction'
                    })
            except:
                continue

    # --- ESTRATEGIA 4: MARKET DRIVER (Sentimiento General) ---
    # Un activo con muchísimos seguidores (>5) define la tendencia del mercado.
    # No es para operar un par específico, sino para definir si somos Bullish o Bearish en general.
    if followers_count >= 5 and corr > 0.8:
        signals.append({
            'strategy': 'MARKET_DRIVER',
            'signal_strength': 'CRITICAL',
            'description': f"{leader} is driving the market with {followers_count} correlated assets.",
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
        # Intentar leer histórico de señales para hacer append
        print(f"Buscando histórico en {SIGNALS_BUCKET}...")
        obj = s3.get_object(Bucket=SIGNALS_BUCKET, Key=CSV_OUTPUT_NAME)
        existing_df = pd.read_csv(obj['Body'])
        combined_df = pd.concat([existing_df, new_signals_df], ignore_index=True)
    except Exception as e:
        print(f"Archivo nuevo o error leyendo: {e}. Creando desde cero.")
        combined_df = new_signals_df

    # Guardar
    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    
    s3.put_object(
        Bucket=SIGNALS_BUCKET,
        Key=CSV_OUTPUT_NAME,
        Body=csv_buffer.getvalue()
    )
    print(f"¡Éxito! {len(new_signals_df)} nuevas señales guardadas.")

def handler(event, context):
    print("--- INICIANDO ANÁLISIS DE SEÑALES AVANZADO ---")
    
    try:
        # Obtener bucket y key del evento S3
        record = event['Records'][0]
        src_bucket = record['s3']['bucket']['name']
        src_key = record['s3']['object']['key']
        
        # 1. Leer datos crudos
        df_history = get_latest_data(src_bucket, src_key)
        
        # 2. Procesar estrategias
        signals_df = process_signals(df_history)
        
        # 3. Guardar resultados
        if not signals_df.empty:
            print(f"Señales generadas:\n{signals_df[['strategy', 'leader_symbol', 'action_asset']].head()}")
            update_signals_csv(signals_df)
        else:
            print("Mercado sin anomalías detectables.")
            
    except Exception as e:
        print(f"ERROR FATAL: {e}")
        raise e
        
    return {"statusCode": 200, "body": "Advanced Analysis Complete"}
