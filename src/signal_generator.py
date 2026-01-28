import boto3
import csv
import io
import os
from datetime import datetime

s3 = boto3.client('s3')

OUTPUT_BUCKET = os.environ['SIGNALS_BUCKET_NAME']
OUTPUT_FILE_KEY = 'trading_signals_log.csv'

def get_latest_data(bucket, key):
    """Descarga y parsea solo el último bloque de tiempo del CSV de entrada"""
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(content))
    
    data = list(reader)
    if not data:
        return []

    # Ordenamos por fecha y nos quedamos con el último timestamp registrado
    latest_ts = data[-1]['timestamp']
    latest_rows = [row for row in data if row['timestamp'] == latest_ts]
    return latest_rows

def generate_signals(rows):
    """Aplica la lógica de trading"""
    signals = []
    
    for row in rows:
        leader = row['leader']
        followers = int(row['follower_count'])
        corr = float(row['avg_correlation'])
        lag = float(row['avg_lag_minutes'])
        ts = row['timestamp']

        # --- ESTRATEGIA DE TRADING ---
        signal_type = "NEUTRAL"
        reason = ""

        # Regla 1: Compra por Momentum de Mercado (Líder Fuerte)
        if followers >= 4 and corr > 0.80:
            signal_type = "BUY_STRONG"
            reason = f"Market Mover: Arrastra a {followers} monedas con alta confianza."
        
        # Regla 2: Compra Especulativa (Alta correlación en nicho)
        elif followers >= 2 and corr > 0.95:
            signal_type = "BUY_NICHE"
            reason = f"Niche Leader: Correlacion perfecta ({corr}) en grupo pequeño."

        # Regla 3: Venta/Precaución (Si la correlación baja mucho, el líder pierde fuerza)
        elif followers > 5 and corr < 0.5:
            signal_type = "SELL_WEAKNESS"
            reason = "Divergencia: Muchos seguidores pero correlación rota."

        if signal_type != "NEUTRAL":
            signals.append([ts, signal_type, leader, followers, corr, lag, reason])
            
    return signals

def append_to_signals_file(new_signals):
    """Añade las nuevas señales al CSV histórico en el nuevo bucket"""
    if not new_signals:
        print("No hay señales nuevas.")
        return

    # 1. Intentar descargar el archivo existente
    existing_data = ""
    header = "timestamp,signal,asset,strength,correlation,lag,reason\n"
    
    try:
        resp = s3.get_object(Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILE_KEY)
        existing_data = resp['Body'].read().decode('utf-8')
        # Si ya existe, no necesitamos cabecera nueva, solo los datos
    except s3.exceptions.ClientError:
        # Si no existe, usamos la cabecera inicial
        existing_data = header

    # 2. Preparar nuevas líneas
    new_lines = io.StringIO()
    writer = csv.writer(new_lines)
    writer.writerows(new_signals)
    
    # 3. Concatenar y Subir
    final_content = existing_data
    # Asegurar que hay un salto de línea antes de añadir
    if existing_data and not existing_data.endswith('\n'):
        final_content += "\n"
    
    final_content += new_lines.getvalue()

    s3.put_object(Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILE_KEY, Body=final_content)
    print(f"✅ Señales guardadas en s3://{OUTPUT_BUCKET}/{OUTPUT_FILE_KEY}")

def handler(event, context):
    try:
        # Obtener bucket y archivo que disparó el evento
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = event['Records'][0]['s3']['object']['key']
        
        print(f"Procesando actualización en: {source_key}")
        
        # 1. Leer datos nuevos
        rows = get_latest_data(source_bucket, source_key)
        
        # 2. Generar Señales
        signals = generate_signals(rows)
        
        # 3. Guardar
        if signals:
            append_to_signals_file(signals)
        
        return {"statusCode": 200, "body": "OK"}
        
    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": str(e)}
