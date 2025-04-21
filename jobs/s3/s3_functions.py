import os
import boto3
from dotenv import load_dotenv, find_dotenv
from moto import mock_s3
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Configuração inicial do cliente S3
def create_s3_client():
    load_dotenv(find_dotenv())
    """
    Cria e retorna um cliente S3 configurado com as credenciais da AWS.
    """
    try:
        s3_client = boto3.client("s3")
        print("Cliente S3 criado com sucesso.")
        return s3_client
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Erro ao configurar o cliente S3: {e}")
        raise


def upload_to_s3(file_path, bucket_name, s3_key):
    try:
        s3_client = create_s3_client()
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Arquivo {file_path} enviado com sucesso para s3://{bucket_name}/{s3_key}.")
        return f"Upload concluído: s3://{bucket_name}/{s3_key}"
    except FileNotFoundError:
        print(f"Erro: O arquivo {file_path} não foi encontrado.")
        return f"Erro: Arquivo {file_path} não encontrado."
    except Exception as e:
        print(f"Erro ao fazer upload para o S3: {e}")
        return f"Erro ao fazer upload: {e}"


def download_from_s3(bucket_name, s3_key, local_path):
    try:
        s3_client = create_s3_client()
        s3_client.download_file(bucket_name, s3_key, local_path)
        print(f"Arquivo baixado com sucesso de s3://{bucket_name}/{s3_key} para {local_path}.")
        return f"Download concluído: {local_path}"
    except FileNotFoundError:
        print(f"Erro: O diretório local {os.path.dirname(local_path)} não existe.")
        return f"Erro: Diretório local {os.path.dirname(local_path)} não encontrado."
    except Exception as e:
        print(f"Erro ao baixar arquivo do S3: {e}")
        return f"Erro ao baixar arquivo: {e}"


def delete_from_s3(bucket_name, s3_key):
    try:
        s3_client = create_s3_client()
        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        print(f"Arquivo s3://{bucket_name}/{s3_key} deletado com sucesso.")
        return f"Arquivo deletado: s3://{bucket_name}/{s3_key}"
    except Exception as e:
        print(f"Erro ao deletar arquivo do S3: {e}")
        return f"Erro ao deletar arquivo: {e}"


def delete_folder_from_s3(bucket_name, prefix):
    """
    Deleta todos os arquivos de um "diretório" no S3 (prefixo).
    """
    try:
        s3_client = create_s3_client()
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        deleted_files = 0
        for page in pages:
            if 'Contents' in page:
                delete_us = {'Objects': [{'Key': obj['Key']} for obj in page['Contents']] }
                response = s3_client.delete_objects(Bucket=bucket_name, Delete=delete_us)
                deleted_files += len(response.get('Deleted', []))

        print(f"{deleted_files} arquivos deletados de s3://{bucket_name}/{prefix}")
        return f"Total de arquivos deletados: {deleted_files}"

    except Exception as e:
        print(f"Erro ao deletar arquivos do S3: {e}")
        return f"Erro: {e}"


def read_s3_file(bucket_name, s3_key):
    try:
        s3_client = create_s3_client()
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        file_content = response["Body"].read().decode("utf-8")
        print("Arquivo lido com sucesso!")
        return file_content
    except Exception as e:
        print(f"Erro ao ler arquivo do S3: {e}")
        return None



def lendo(is_mock=False):
    if is_mock:
        mock_context = mock_s3()
        mock_context.start()
        
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='yellow_taxi_files')
        
        s3_uri = 's3a://yellow_taxi_files/yellow_taxi_files/'
        print(f"Lendo de: {s3_uri}")
        
        mock_context.stop()
        return s3_uri
    else:
        return 's3a://yellow_taxi_files/yellow_taxi_files/'


def salvando(is_mock=False):
    if is_mock:
        mock_context = mock_s3()
        mock_context.start()

        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='prd_yellow_taxi_table')

        output_path = 's3a://prd_yellow_taxi_table/yellow_taxi_output/'
        print(f"Salvando em: {output_path}")

        mock_context.stop()
        return output_path
    else:
        return 's3a://prd_yellow_taxi_table/yellow_taxi_output/'
