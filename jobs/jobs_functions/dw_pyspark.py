# Importações externas
import os
import re
import sys
import time
import json
import duckdb
import requests
import traceback
from unicodedata import normalize
from io import StringIO
from dotenv import load_dotenv, find_dotenv

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



def dw_pyspark():

    s3_uri = lendo(is_mock = True)
    print("Caminho do S3 para leitura:", s3_uri)

    #load_dotenv("./.env")
    # encontra e carrega o .env na árvore de pastas
    load_dotenv(find_dotenv())

    # Cria a sessão Spark
    spark = SparkSession.builder \
        .appName("JobAirflowPySpark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Configura o logger
    logger = configurar_logger("📥 Query ")
    logger.info("Padronizando os dados")
    notificar_falha("📥 Início do processamento ")

    df = spark.read.option("header", "true").option("encoding", "utf-8").parquet(f"/home/lenovo/airflow/tmp/s3-us-east-1.amazonaws.com/prd_yellow_taxi_table")

    df_pyspark = df.select([
        'VENDORID', 'PASSENGER_COUNT', 'TOTAL_AMOUNT', 
        'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME',
        'CONTROLS_COLUMN', 'YEAR', 'MONTH', 'TPEP_MONTH', 'HOUR'])

    df_grouped = df_pyspark.groupBy("YEAR", "MONTH").agg(
        sum("TOTAL_AMOUNT").alias("SOMATORIA_TOTAL_AMOUNT"),
        count("*").alias("NUMERO_TOTAL_DE_TRIPS")
    )

    df_grouped = df_grouped.withColumn(
        "avg_fare_per_trip",
        col("SOMATORIA_TOTAL_AMOUNT") / col("NUMERO_TOTAL_DE_TRIPS")
    )

    old_stdout = sys.stdout
    sys.stdout = mystdout = StringIO()

    df_grouped.show()

    sys.stdout = old_stdout
    output_string = mystdout.getvalue()
    logger.info("📊 Resultado da agregação por mês:\n" + output_string)
    notificar_falha("📊 Resultado da agregação por mês enviado.")

    table_name = "ext_avg_total_amount"
    external_path = "/home/lenovo/airflow/tmp/external_tables/avg_total_amount"

    df_grouped.write \
        .mode("overwrite") \
        .option("path", external_path) \
        .option("external", "true") \
        .saveAsTable(table_name)

    logger.info(f"✅ Tabela externa '{table_name}' salva com sucesso em: {external_path}")
    notificar_falha(f"✅ Tabela externa '{table_name}' salva com sucesso em: {external_path}")



    df = df_pyspark.withColumn("TPEP_PICKUP_DATETIME", to_timestamp("TPEP_PICKUP_DATETIME"))
    df = df.withColumn("TPEP_DROPOFF_DATETIME", to_timestamp("TPEP_DROPOFF_DATETIME"))
    df = df.withColumn("DAY", dayofmonth("TPEP_PICKUP_DATETIME"))
    df = df.withColumn("PICKUP_HOUR", hour("TPEP_PICKUP_DATETIME"))

    avg_by_hour_day = df.groupBy("MONTH", "DAY", "PICKUP_HOUR") \
                        .agg(avg("PASSENGER_COUNT").alias("AVG_PASSENGER_COUNT")) \
                        .orderBy("DAY", "PICKUP_HOUR")

    table_name = "ext_avg_passenger_count"
    external_path = "/home/lenovo/airflow/tmp/external_tables/avg_passenger_count"

    avg_by_hour_day.write \
        .mode("overwrite") \
        .option("path", external_path) \
        .option("external", "true") \
        .saveAsTable(table_name)

    logger.info(f"✅ Tabela externa '{table_name}' salva com sucesso em: {external_path}")
    notificar_falha(f"✅ Tabela externa '{table_name}' salva com sucesso em: {external_path}")

    output_path = salvando(is_mock = True)
    print("Caminho do S3 para escrita:", output_path)

    spark.stop()
    return