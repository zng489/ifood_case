import os
import re
import requests
import shutil
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from jobs.log_utils.logging_function import *
from jobs.s3.s3_functions import *

def run_scraper():
    s3_uri = lendo(is_mock = True)
    print("Caminho do S3 para leitura:", s3_uri)

    logger = configurar_logger("✅ SCRAPING")
    logger.info("Aplicação iniciada")
    notificar_falha("🟡 Scraping iniciado...")

    BUCKET_NAME = "yellow_taxi_files"
    REGIAO = "us-east-1"
    PASTA_LOCAL = "/home/lenovo/airflow/tmp/s3-us-east-1.amazonaws.com/yellow_taxi_files/"

    s3 = boto3.client("s3", region_name=REGIAO)

    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    notificar_falha("🟡 Navegador iniciado e scraping começou...")

    try:
        logger.info("Acessando a página da NYC TLC...")
        driver.get("https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
        wait = WebDriverWait(driver, 10)

        logger.info("Esperando botão de expansão de 2023...")
        div_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'div[data-answer="faq2023"]')))
        div_element.click()
        logger.info("Expansão da seção de 2023 realizada com sucesso.")

        logger.info("Buscando todos os links de arquivos .parquet de 2023...")
        link_elements = wait.until(EC.presence_of_all_elements_located((
            By.XPATH, "//a[contains(@href, 'yellow_tripdata_2023-') and contains(@href, '.parquet')]"
        )))

        logger.info(f"{len(link_elements)} arquivos encontrados para 2023.")

        for link_element in link_elements:
            url = link_element.get_attribute("href")
            file_name = url.split("/")[-1]

            match = re.search(r'yellow_tripdata_2023-(\d{1,2})\.parquet', file_name)

            if match:
                mes = int(match.group(1))
                if mes > 5:
                    logger.info(f"🟡 Ignorando {file_name} (mês {mes} > 5)")
                    continue

            month_str = f"2023_{mes:02d}"
            pasta_destino = os.path.join(PASTA_LOCAL, month_str)
            os.makedirs(pasta_destino, exist_ok=True)

            local_path = os.path.join(pasta_destino, file_name)

            if not os.path.exists(local_path):
                logger.info(f"📥 Baixando {file_name}...")
                resposta = requests.get(url, stream=True)
                with open(local_path, "wb") as f:
                    for chunk in resposta.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"✅ Download concluído: {file_name}")
                notificar_falha(f"📁 Arquivo baixado: {file_name}")
            else:
                logger.warning(f"⚠️ Arquivo já existe: {file_name}")

            s3_key = f"{month_str}/{file_name}"
            logger.info(f"🚀 Simulando envio para S3: s3://{BUCKET_NAME}/{s3_key}")
            print(f"🚀 Enviando para S3: s3://{BUCKET_NAME}/{s3_key}")
            logger.info("Simulando criação do path s3.upload_file(local_path, BUCKET_NAME, s3_key)")

            notificar_falha('''
            # Código real de envio para o S3 (comentado)
            s3_key = f"{month_str}/{file_name}"
            s3.upload_file(local_path, BUCKET_NAME, s3_key)

            # Limpeza de arquivos locais (comentada)
            print(f"🧹 Removendo local: {local_path}")
            os.remove(local_path)
            if not os.listdir(pasta_destino):
                shutil.rmtree(pasta_destino)
            ''')

            output_path = salvando(is_mock = True)
            print("Caminho do S3 para escrita:", output_path)

        logger.info("✅ Processo finalizado: arquivos até 2023-05 processados.")
        notificar_falha("✅ Scraping finalizado com sucesso! Arquivos até 2023-05 foram processados.")

    except Exception as e:
        logger.error(f"❌ Erro durante execução: {str(e)}")
        notificar_falha(f"❌ Falha no scraping: {str(e)}")

    finally:
        driver.quit()
        logger.info("🧹 Navegador fechado.")
        notificar_falha("🔚 Navegador encerrado e processo finalizado.")