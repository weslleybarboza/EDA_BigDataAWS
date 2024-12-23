import json
import csv
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Nome do bucket e chave do arquivo CSV
    bucket_source = event['Records'][0]['s3']['bucket']['name']
    bucket_destination = 'eda-raw-zone-us-east-2-<Account ID>'
    csv_key = event['Records'][0]['s3']['object']['key']
    
    # Nome do arquivo JSON de saida
    json_key = csv_key.replace('.csv', '.json')
    
    # Baixar o arquivo CSV do S3
    csv_file = s3.get_object(Bucket=bucket_source, Key=csv_key)
    csv_content = csv_file['Body'].read().decode('utf-8').splitlines()
    
    # Converter CSV para JSON
    reader = csv.DictReader(csv_content)
    json_data = [row for row in reader]
    
    # Salvar o arquivo JSON no S3
    s3.put_object(
        Bucket=bucket_destination,
        Key=json_key,
        Body=json.dumps(json_data),
        ContentType='application/json'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully converted {csv_key} to {json_key}')
    }