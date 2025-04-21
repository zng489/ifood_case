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

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, count, col, udf, from_utc_timestamp, current_timestamp, lit, input_file_name,
    monotonically_increasing_id, substring_index, when, explode, regexp, regexp_extract, concat, 
    year, month, hour, to_timestamp, avg, dayofmonth
)

from jobs.s3.s3_functions import *
from jobs.log_utils.logging_function import configurar_logger, notificar_falha


def dw_motherduck():
    load_dotenv(find_dotenv())
    motherduck_token = os.getenv("TOKEN")

    logger = configurar_logger("📥 DW MotherDuck")
    logger.info("📥 Padronizando os dados")
    notificar_falha("📥 Início do processamento")

    try:
        con = duckdb.connect(f"md:?token={motherduck_token}")

        existing_tables = con.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in existing_tables]
        table_name = "prd_yellow_taxi_table"

        if table_name not in table_names:
            con.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_parquet('/home/lenovo/airflow/tmp/s3-us-east-1.amazonaws.com/prd_yellow_taxi_table/*.parquet')
            """)
            logger.info(f"✅ Dados carregados na tabela MotherDuck '{table_name}' com sucesso!")
            notificar_falha(f"✅ Dados carregados na tabela MotherDuck '{table_name}' com sucesso!")
        else:
            logger.warning(f"⚠️ Tabela '{table_name}' já existe. Ignorando criação.")
            notificar_falha(f"⚠️ Tabela '{table_name}' já existe. Nenhuma ação tomada.")

        df = con.execute("SELECT * FROM prd_yellow_taxi_table LIMIT 10").fetchdf()
        print(df)

    except Exception as e:
        logger.error(f"❌ Erro no DW_motherduck: {e}")
        notificar_falha(f"❌ Erro no DW_motherduck:\n{str(e)}")
        raise e
