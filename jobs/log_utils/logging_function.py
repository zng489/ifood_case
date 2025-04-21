import os
import sys
import logging
import requests
from pathlib import Path
from logging.handlers import RotatingFileHandler

caminho_padrao_log = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "registro_log", "app.log")
)

def configurar_logger(
    nome_logger: str = "app",
    nivel=logging.DEBUG,
    log_para_arquivo: bool = True,
    caminho_log: str = caminho_padrao_log,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3
) -> logging.Logger:
    
    logger = logging.getLogger(nome_logger)
    logger.setLevel(nivel)

    if logger.hasHandlers():
        return logger

    formato = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formato)
    logger.addHandler(console_handler)

    if log_para_arquivo:
        Path(caminho_log).parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            filename=caminho_log,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8"
        )
        file_handler.setFormatter(formato)
        logger.addHandler(file_handler)

    return logger


def notificar_falha(context):
    webhook_url = "https://discord.com/api/webhooks/1363142723191832636/yvw9WP_SXdr_N6RgJXvljsUqyh6Puz-sO7Mf86s1D31xIpmfhM7kZN0JjEmYV4i49KBR"

    mensagem = {
        "content": f"{context}"
    }

    try:
        response = requests.post(webhook_url, json=mensagem)
        response.raise_for_status()
        #print("Mensagem enviada com sucesso.")
    except Exception as e:
        print(f"Falha ao enviar webhook: {e}")