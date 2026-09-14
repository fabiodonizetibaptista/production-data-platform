import json
import logging
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path


LOG_DIR = Path("logs")
AUDIT_DIR = Path("data/audit")

LOG_FILE = LOG_DIR / "pipeline.log"
AUDIT_FILE = AUDIT_DIR / "pipeline_runs.jsonl"


def utc_now() -> datetime:
    """
    Retorna o horário atual em UTC.

    Usar UTC evita ambiguidades entre servidores,
    ambientes e fusos horários diferentes.
    """
    return datetime.now(timezone.utc)


def create_run_id() -> str:
    """
    Cria um identificador único para cada execução.
    """
    return uuid.uuid4().hex


def configure_logger(name: str) -> logging.Logger:
    """
    Configura logging para console e arquivo.

    O arquivo usa rotação para impedir crescimento
    indefinido do log local.
    """

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Evita adicionar handlers duplicados caso
    # a função seja chamada novamente.
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )

    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def write_audit_record(record: dict) -> None:
    """
    Persiste um registro de auditoria por execução.

    JSONL significa JSON Lines:
    cada linha do arquivo representa um JSON independente.
    """

    AUDIT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    with AUDIT_FILE.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(
                record,
                ensure_ascii=False,
                default=str,
            )
        )

        file.write("\n")