# Importações externas
import os
import re
import sys
import time
import json
import requests
import traceback
from unicodedata import normalize
from io import StringIO
from dotenv import load_dotenv, find_dotenv
import duckdb

# Importações de bibliotecas PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, count, col, udf, from_utc_timestamp, current_timestamp, lit, input_file_name,
    monotonically_increasing_id, substring_index, when, explode, regexp, regexp_extract, concat, 
    year, month, hour, to_timestamp, avg, dayofmonth
)

# Funções personalizadas de logging e S3
from jobs.s3.s3_functions import *
from jobs.log_utils.logging_function import *

import boto3
#import pytest
from pyspark.sql.functions import col


def ETL():

    s3_uri = lendo(is_mock = True)
    print("Caminho do S3 para leitura:", s3_uri)

    #load_dotenv("./.env")
    # encontra e carrega o .env na árvore de pastas
    load_dotenv(find_dotenv())


    # Configura o logger
    logger = configurar_logger("📥 Transformando dados ETL")
    logger.info("ETL iniciando")
    notificar_falha("📥 Início do processamento do ETL")

    def __normalize_str(_str):
        return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                    .encode('ASCII', 'ignore')
                    .decode('ASCII')
                    .replace(' ', '_')
                    .replace('-', '_')
                    .replace('/', '_')
                    .replace('.', '_')
                    .replace('$', 'S')
                    .upper())

    # Caminho para o diretório de destino
    table_dir = '/home/lenovo/airflow/tmp/s3-us-east-1.amazonaws.com/prd_yellow_taxi_table/'

    if not os.path.exists(table_dir):
        os.makedirs(table_dir)
        logger.info(f'Diretório criado: {table_dir}')
    else:
        logger.info(f'O diretório já existe: {table_dir}')

    base_path = '/home/lenovo/airflow/tmp/s3-us-east-1.amazonaws.com/yellow_taxi_files'

    regex = re.compile(r'trip.*\.parquet')

    # Cria a sessão Spark
    spark = SparkSession.builder \
        .appName("JobAirflowPySpark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    try:
        dataframes = []
        for nome in os.listdir(base_path):
            subdir_path = os.path.join(base_path, nome)

            if os.path.isdir(subdir_path):
                logger.info(f'📁 Verificando diretório: {subdir_path}')
 
                for arquivo in os.listdir(subdir_path):
                    if regex.search(arquivo):
                        caminho_arquivo = os.path.join(subdir_path, arquivo)
                        filename = arquivo.split('/')[-1]
                        basename = filename.split('.')[0]
                        logger.info(f'🔍 Lendo arquivo: {basename}')

                        df = (spark.read
                                .option("header", "true")
                                .option("encoding", "utf-8")
                                .parquet(f"{caminho_arquivo}")).withColumn("controls_column", lit(f"{basename}"))

                        df = df.withColumn("current_time", current_timestamp())

                        for column in df.columns:
                            df = df.withColumnRenamed(column, __normalize_str(column))

                        dataframes.append(df)
                    else:
                        logger.warning(f'⚠️ Nenhum arquivo compatível em: {subdir_path}')

        if dataframes:
            final_df = dataframes[0]
            for df in dataframes[1:]:
                final_df = final_df.unionByName(df, allowMissingColumns=True)

            df_yellow = final_df.withColumn("TPEP_DROPOFF_DATETIME", to_timestamp(col("TPEP_DROPOFF_DATETIME")))
            df_yellow = df_yellow.withColumn("YEAR", year(col("TPEP_DROPOFF_DATETIME"))) \
                                 .withColumn("TPEP_MONTH", month(col("TPEP_DROPOFF_DATETIME"))) \
                                 .withColumn("HOUR", hour(col("TPEP_DROPOFF_DATETIME")))
            df_yellow = df_yellow.withColumn("MONTH", regexp_extract("CONTROLS_COLUMN", r"\d{4}-(\d{2})", 1))
            df_yellow = df_yellow.filter(col("YEAR") == 2023)
            df_yellow = df_yellow.filter(col("TPEP_MONTH").isin(1, 2, 3, 4, 5))

            df_yellow.write.mode("overwrite").parquet(f'{table_dir}')
            logger.info("✅ Dados processados e salvos com sucesso.")
            notificar_falha("✅ Trusted finalizado com sucesso - dados padronizados e salvos.")
        else:
            logger.warning("⚠️ Nenhum DataFrame válido encontrado para consolidar.")
            notificar_falha("⚠️ Nenhum dado encontrado para consolidação.")
                # Importante: encerre a sessão ao finalizar

        output_path = salvando(is_mock = True)
        print("Caminho do S3 para escrita:", output_path)
        spark.stop()
        return

    except Exception as e:
        logger.exception("❌ Erro durante o processamento dos dados Trusted")
        notificar_falha(f"❌ Erro durante o processamento Trusted: {str(e)}")
                # Certifique-se de encerrar o Spark mesmo em caso de erro
        if 'spark' in locals():
            spark.stop()
