import os
import sys
import traceback
from dotenv import load_dotenv, find_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from jobs.log_utils.logging_function import configurar_logger, notificar_falha

def dw_metastore():
    # Carrega variáveis de ambiente
    load_dotenv(find_dotenv())

    '''
    # Cria sessão Spark com suporte a Hive
    spark = SparkSession.builder \
        .appName("JobAirflowPySpark") \
        .enableHiveSupport() \
        .getOrCreate()
    
    '''
    # Cria sessão Spark com suporte a Hive
    spark = SparkSession.builder \
        .appName("JobAirflowPySpark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "/home/lenovo/airflow/spark-warehouse") \
        .config("hive.metastore.uris", "") \
        .enableHiveSupport() \
        .getOrCreate()
    # .config("spark.sql.catalogImplementation", "hive") # importando para hive metastore


    # Configura o logger
    logger = configurar_logger("📥 DW SparkMetaStore")
    logger.info("📥 Iniciando o processamento")
    notificar_falha("📥 Início do processamento ")

    try:
        logger.info("📥 Lendo dados parquet")
        table_path = '/home/lenovo/airflow/tmp/s3-us-east-1.amazonaws.com/prd_yellow_taxi_table'
        df = spark.read.option("header", "true").parquet(table_path)
        logger.info("✅ Leitura realizada com sucesso")
        notificar_falha("✅ Leitura da trusted concluída com sucesso")
    except Exception as e:
        logger.error(f"❌ Falha ao ler parquet: {e}")
        traceback.print_exc()
        notificar_falha(f"❌ Erro ao ler trusted: {str(e)}")
        sys.exit(1)

    df = df.select([
        'VENDORID', 'PASSENGER_COUNT', 'TOTAL_AMOUNT',
        'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME',
        'CONTROLS_COLUMN', 'YEAR', 'MONTH', 'TPEP_MONTH', 'HOUR'
    ])

    nome_banco = "dw"
    tabela_nome = "yellow_taxi_table"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {nome_banco}")
    spark.catalog.setCurrentDatabase(nome_banco)

    logger.info(f"💾 Salvando tabela '{tabela_nome}' no metastore")
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f'{nome_banco}.{tabela_nome}')

    logger.info(f"✅ Tabela '{tabela_nome}' salva com sucesso")

    # Criação e preenchimento das views
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw.VM_total_amount_mes (
            YEAR INT,
            MONTH INT,
            SOMATORIA_TOTAL_AMOUNT DOUBLE,
            NUMERO_TOTAL_DE_TRIPS INT,
            avg_fare_per_trip DOUBLE
        )
    """)

    spark.sql("""
        INSERT OVERWRITE TABLE dw.VM_total_amount_mes
        SELECT
            YEAR,
            CAST(MONTH AS INT) AS MONTH,
            SUM(TOTAL_AMOUNT) AS SOMATORIA_TOTAL_AMOUNT,
            COUNT(*) AS NUMERO_TOTAL_DE_TRIPS,
            SUM(TOTAL_AMOUNT) / COUNT(*) AS avg_fare_per_trip
        FROM dw.yellow_taxi_table
        GROUP BY YEAR, CAST(MONTH AS INT)
        ORDER BY MONTH
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw.VM_passenger_count_hora_dia (
            YEAR INT,
            MONTH INT,
            HOUR INT,
            SOMATORIA_PASSENGER_COUNT INT,
            NUMERO_TOTAL_DE_TRIPS INT,
            avg_PASSENGER_COUNT DOUBLE
        )
    """)

    spark.sql("""
        INSERT OVERWRITE TABLE dw.VM_passenger_count_hora_dia
        SELECT
            YEAR,
            CAST(MONTH AS INT),
            HOUR,
            SUM(PASSENGER_COUNT),
            COUNT(*),
            SUM(PASSENGER_COUNT) / COUNT(*)
        FROM dw.yellow_taxi_table
        GROUP BY YEAR, CAST(MONTH AS INT), HOUR
    """)

    logger.info("✅ Tabelas agregadas criadas com sucesso")
    spark.stop()
if __name__ == "__main__":
    dw_metastore()
# Apenas define a função. spark-submit executa o arquivo inteiro, então não chame aqui diretamente.
