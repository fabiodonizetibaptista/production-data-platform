import logging
from pathlib import Path
from urllib.request import Request, urlopen


# -------------------------------------------------------------------------
# Configuração
# -------------------------------------------------------------------------

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_2026-01.parquet"
)

RAW_DIR = Path("data/raw")

OUTPUT_FILE = RAW_DIR / "yellow_tripdata_2026-01.parquet"

TEMP_FILE = RAW_DIR / "yellow_tripdata_2026-01.parquet.part"

CHUNK_SIZE = 1024 * 1024  # 1 MB


# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# Validações
# -------------------------------------------------------------------------

def is_valid_parquet(file_path: Path) -> bool:
    """
    Faz uma validação simples do arquivo Parquet.

    Arquivos Parquet possuem a assinatura PAR1
    tanto no início quanto no final do arquivo.

    Esta validação não garante que todo o conteúdo esteja correto,
    mas detecta arquivos vazios, incompletos ou claramente inválidos.
    """

    if not file_path.exists():
        return False

    if file_path.stat().st_size < 8:
        return False

    with file_path.open("rb") as file:
        header = file.read(4)

        file.seek(-4, 2)
        footer = file.read(4)

    return header == b"PAR1" and footer == b"PAR1"


# -------------------------------------------------------------------------
# Ingestão
# -------------------------------------------------------------------------

def download_file() -> None:
    """
    Faz o download do dataset do NYC Taxi para a camada RAW.

    Características importantes:

    - não baixa novamente um arquivo já válido;
    - baixa primeiro para um arquivo temporário;
    - valida o tamanho recebido;
    - valida a assinatura Parquet;
    - somente depois publica o arquivo definitivo.
    """

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Primeira proteção de idempotência:
    # se já temos um arquivo válido, não repetimos o download.
    if is_valid_parquet(OUTPUT_FILE):
        logger.info(
            "Arquivo já existe e é válido. Download ignorado: %s",
            OUTPUT_FILE,
        )
        return

    # Remove eventual arquivo temporário deixado por uma execução anterior.
    if TEMP_FILE.exists():
        logger.warning(
            "Arquivo temporário encontrado. Removendo: %s",
            TEMP_FILE,
        )
        TEMP_FILE.unlink()

    logger.info("Iniciando download.")
    logger.info("Origem: %s", SOURCE_URL)

    request = Request(
        SOURCE_URL,
        headers={
            "User-Agent": "production-data-platform/1.0",
        },
    )

    total_bytes = 0

    with urlopen(request, timeout=60) as response:
        status_code = response.status

        if status_code != 200:
            raise RuntimeError(
                f"Download falhou. HTTP status: {status_code}"
            )

        content_length = response.headers.get("Content-Length")

        expected_bytes = (
            int(content_length)
            if content_length is not None
            else None
        )

        with TEMP_FILE.open("wb") as output:
            while True:
                chunk = response.read(CHUNK_SIZE)

                if not chunk:
                    break

                output.write(chunk)
                total_bytes += len(chunk)

    logger.info(
        "Download concluído: %.2f MB",
        total_bytes / (1024 * 1024),
    )

    # Quando o servidor informa Content-Length,
    # comparamos o esperado com o efetivamente recebido.
    if expected_bytes is not None and total_bytes != expected_bytes:
        raise RuntimeError(
            "Download incompleto. "
            f"Esperado={expected_bytes} bytes, "
            f"recebido={total_bytes} bytes."
        )

    if not is_valid_parquet(TEMP_FILE):
        raise RuntimeError(
            "O arquivo recebido não passou na validação Parquet."
        )

    # O arquivo só recebe o nome definitivo depois de validado.
    TEMP_FILE.replace(OUTPUT_FILE)

    logger.info(
        "Arquivo RAW publicado com sucesso: %s",
        OUTPUT_FILE,
    )


if __name__ == "__main__":
    download_file()