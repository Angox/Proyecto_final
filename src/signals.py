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
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(response['Body'])

def generate_signals(df):
    """
    Analiza las últimas filas para generar señales.
    Estrategia: 'Scalping de Seguimiento'
    """
    # Tomamos solo los datos del último timestamp registrado
    last_timestamp = df['timestamp'].iloc[-1]
    latest_df = df[df['timestamp'] == last_timestamp].copy()
    
    signals = []
    
    for index, row in latest_df.iterrows():
        # LÓGICA DE TRADING
        # 1. Fuerza de correlación alta (> 0.80)
        # 2. El líder se mueve ANTES que los seguidores (Lag positivo > 0.5 min)
        # 3. Tiene al menos 3 seguidores (para confirmar tendencia de mercado)
        
        if row['avg_correlation'] > 0.80 and row['avg_lag_minutes'] > 0.5 and row['follower_count'] >= 3:
            
            signal_type = "WATCH_FOLLOWERS_MOMENTUM"
            priority = "HIGH"
            
            # Si la correlación es extrema, la prioridad es crítica
            if row['avg_correlation'] > 0.90:
                priority = "CRITICAL"

            signals.append({
                'generated_at': datetime.now().isoformat(),
                'signal_source_time': row['timestamp'],
                'asset_to_watch': row['leader'], # El líder que se mueve primero
                'action': signal_type,
                'priority': priority,
                'rationale': f"Leader moves {row['avg_lag_minutes']}m ahead of {row['follower_count']} assets with {row['avg_correlation']} corr.",
                'target_assets': row['followers_list'] # Estos son los que deberías comprar/vender según lo que haga el líder
            })
            
    return pd.DataFrame(signals)

def update_signals_csv(new_signals_df):
    if new_signals_df.empty:
        print("No se generaron señales nuevas.")
        return

    try:
        # Intentar leer el histórico de señales
        obj = s3.get_object(Bucket=SIGNALS_BUCKET, Key=CSV_OUTPUT_NAME)
        existing_df = pd.read_csv(obj['Body'])
        combined_df = pd.concat([existing_df, new_signals_df], ignore_index=True)
    except Exception:
        print("Creando nuevo archivo de señales.")
        combined_df = new_signals_df

    # Guardar
    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    
    s3.put_object(
        Bucket=SIGNALS_BUCKET,
        Key=CSV_OUTPUT_NAME,
        Body=csv_buffer.getvalue()
    )
    print(f"Señales guardadas en {SIGNALS_BUCKET}/{CSV_OUTPUT_NAME}")

def handler(event, context):
    print("--- PROCESANDO SEÑALES ---")
    
    # Obtener detalles del evento S3 trigger
    record = event['Records'][0]
    src_bucket = record['s3']['bucket']['name']
    src_key = record['s3']['object']['key'] # output/market_leaders_history.csv
    
    print(f"Archivo detectado: {src_key} en {src_bucket}")
    
    try:
        df_history = get_latest_data(src_bucket, src_key)
        signals_df = generate_signals(df_history)
        
        if not signals_df.empty:
            update_signals_csv(signals_df)
        else:
            print("Criterios de estrategia no cumplidos. Sin señales.")
            
    except Exception as e:
        print(f"ERROR GENERANDO SEÑALES: {e}")
        raise e
        
    return {"statusCode": 200, "body": "Signals Processed"}
