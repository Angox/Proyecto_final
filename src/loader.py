import json
import os
import boto3
import requests
from datetime import datetime

# Variables de entorno
NEPTUNE_ENDPOINT = os.environ.get('NEPTUNE_ENDPOINT')
NEPTUNE_PORT = os.environ.get('NEPTUNE_PORT', '8182')
NEPTUNE_LOAD_ROLE_ARN = os.environ.get('NEPTUNE_LOAD_ROLE_ARN')
REGION = os.environ.get('AWS_REGION', 'eu-west-1')

def lambda_handler(event, context):
    print("Fase 2: Neptune Loader Iniciado")
    
    # Obtener info del evento S3
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] # ej: upload/nodes.csv
        
        source = f"s3://{bucket}/{key}"
        print(f"Procesando archivo: {source}")

        # Endpoint del Bulk Loader de Neptune
        loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"
        
        # Payload para la API de carga
        payload = {
            "source": source,
            "format": "csv",
            "iamRoleArn": NEPTUNE_LOAD_ROLE_ARN,
            "region": REGION,
            "failOnError": "FALSE",
            "parallelism": "MEDIUM",
            "updateSingleCardinalityProperties": "TRUE",
            "queueRequest": "FALSE"
        }
        
        # headers vacíos porque estamos dentro de la VPC y usamos IAM auth luego si fuera necesario,
        # pero normalmente dentro de VPC se permite acceso por Security Group.
        try:
            response = requests.post(loader_url, json=payload)
            print(f"Respuesta Neptune: {response.status_code} - {response.text}")
            
            if response.status_code == 200:
                load_id = response.json().get('payload', {}).get('loadId')
                print(f"Carga iniciada exitosamente. Load ID: {load_id}")
            else:
                print("Error iniciando la carga en Neptune")
                raise Exception(response.text)
                
        except Exception as e:
            print(f"Excepción conectando a Neptune: {e}")
            raise e

    return {'statusCode': 200, 'body': 'Carga solicitada a Neptune'}
