from datetime import datetime, timedelta
import os, sys, time
import requests
from functools import partial

# 1) pega o caminho deste arquivo (…/airflow/dags/scraping.py)
here = os.path.dirname(__file__)
# 2) sobe um nível para /home/lenovo/airflow
project_root = os.path.abspath(os.path.join(here, ".."))
# 3) insere no início do path para o Python olhar primeiro lá
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# === AGORA OS IMPORTS FUNCIONAM ===
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# importa sua lógica
from jobs.jobs_functions.get_data_scraping import *
from jobs.jobs_functions.pyspark_ETL import *
from jobs.jobs_functions.dw_pyspark import *
from jobs.jobs_functions.dw_motherduck import *
from jobs.log_utils.logging_function import *


# Ajuste o start_date para o futuro
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'IFOOD_CASE',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False  # <-- isso impede execuções retroativas
)
# Função de callback
def notificar_falha(context):
    webhook_url = "https://discord.com/api/webhooks/1363142723191832636/yvw9WP_SXdr_N6RgJXvljsUqyh6Puz-sO7Mf86s1D31xIpmfhM7kZN0JjEmYV4i49KBR"
    mensagem = {"content": f"{context}"}
    try:
        response = requests.post(webhook_url, json=mensagem)
        response.raise_for_status()
        print("Mensagem enviada com sucesso.")
    except Exception as e:
        print(f"Falha ao enviar webhook: {e}")

# Usar partial para evitar execução direta
callback_scraping_falha = partial(notificar_falha, 'Scraping pegando os dados DEU PROBLEMA!!')

# Função de espera
def espera():
    time.sleep(60)

# Tasks
t1 = BashOperator(
    task_id='iniciando_pipeline',
    bash_command='echo "🚀 Iniciando o pipeline..."',
    dag=dag
)

t2 = PythonOperator(
    task_id='run_scraper',
    python_callable=run_scraper,
    retries=3,
    retry_delay=timedelta(minutes=1),
    on_failure_callback=callback_scraping_falha,
    dag=dag
)


t3 = PythonOperator(
    task_id='ETL',
    python_callable=ETL,
    retries=3,
    retry_delay=timedelta(minutes=1),
    on_failure_callback=callback_scraping_falha,
    dag=dag
)

t4 = PythonOperator(
    task_id='dw_pyspark',
    python_callable=dw_pyspark,
    retries=3,
    retry_delay=timedelta(minutes=1),
    on_failure_callback=callback_scraping_falha,
    dag=dag
)

t5 = PythonOperator(
    task_id='dw_motherduck',
    python_callable=dw_motherduck,
    retries=3,
    retry_delay=timedelta(minutes=1),
    on_failure_callback=callback_scraping_falha,
    dag=dag
)


# Esperas entre as tarefas
wait_1 = PythonOperator(task_id='wait_1', python_callable=espera, dag=dag)


t1 >> t2 >> t3 >> [t4 , wait_1, t5]
wait_1  >> t4