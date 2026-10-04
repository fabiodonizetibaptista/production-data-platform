import pytest

pytestmark = pytest.mark.unit

import pyarrow as pa
import pyarrow.parquet as pq

import src.ingestion.download_nyc_taxi as ingestion
from src.ingestion.download_nyc_taxi import is_valid_parquet


def test_is_valid_parquet_returns_false_when_file_does_not_exist(tmp_path):
    file_path = tmp_path / "missing.parquet"

    assert is_valid_parquet(file_path) is False


def test_is_valid_parquet_returns_false_for_invalid_file(tmp_path):
    file_path = tmp_path / "invalid.parquet"
    file_path.write_bytes(b"this-is-not-a-parquet-file")

    assert is_valid_parquet(file_path) is False


def test_is_valid_parquet_returns_true_for_valid_parquet(tmp_path):
    file_path = tmp_path / "valid.parquet"

    table = pa.table(
        {
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        }
    )

    pq.write_table(table, file_path)

    assert is_valid_parquet(file_path) is True


def test_download_file_skips_download_when_valid_raw_already_exists(
    tmp_path,
    monkeypatch,
):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    output_file = raw_dir / "yellow_tripdata_2026-01.parquet"
    temp_file = raw_dir / "yellow_tripdata_2026-01.parquet.part"

    # Criamos um Parquet válido sem depender de rede.
    table = pa.table({"id": [1, 2, 3]})
    pq.write_table(table, output_file)

    # Redirecionamos os paths usados pelo módulo para o diretório
    # temporário criado pelo pytest.
    monkeypatch.setattr(ingestion, "RAW_DIR", raw_dir)
    monkeypatch.setattr(ingestion, "OUTPUT_FILE", output_file)
    monkeypatch.setattr(ingestion, "TEMP_FILE", temp_file)

    # Se o código tentar acessar a internet, o teste falha.
    def fail_if_urlopen_is_called(*args, **kwargs):
        raise AssertionError(
            "urlopen não deveria ser chamado quando o RAW já é válido."
        )

    monkeypatch.setattr(
        ingestion,
        "urlopen",
        fail_if_urlopen_is_called,
    )

    modified_before = output_file.stat().st_mtime_ns

    ingestion.download_file()

    modified_after = output_file.stat().st_mtime_ns

    assert output_file.exists()
    assert modified_after == modified_before
    assert not temp_file.exists()


import io


class FakeHttpResponse:
    """Resposta HTTP controlada para testes sem acesso à rede."""

    def __init__(
        self,
        payload,
        *,
        status=200,
        content_length=None,
    ):
        self.status = status
        self.headers = {}

        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

        self._stream = io.BytesIO(payload)

    def read(self, size=-1):
        return self._stream.read(size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


def build_parquet_bytes():
    sink = pa.BufferOutputStream()

    pq.write_table(
        pa.table({"id": [1, 2, 3]}),
        sink,
    )

    return sink.getvalue().to_pybytes()


def configure_download_paths(
    tmp_path,
    monkeypatch,
):
    raw_dir = tmp_path / "raw"
    output_file = raw_dir / "yellow_tripdata_2026-01.parquet"
    temp_file = raw_dir / "yellow_tripdata_2026-01.parquet.part"

    monkeypatch.setattr(
        ingestion,
        "RAW_DIR",
        raw_dir,
    )
    monkeypatch.setattr(
        ingestion,
        "OUTPUT_FILE",
        output_file,
    )
    monkeypatch.setattr(
        ingestion,
        "TEMP_FILE",
        temp_file,
    )

    return output_file, temp_file


def test_download_file_publishes_valid_download(
    tmp_path,
    monkeypatch,
):
    output_file, temp_file = configure_download_paths(
        tmp_path,
        monkeypatch,
    )

    payload = build_parquet_bytes()

    monkeypatch.setattr(
        ingestion,
        "urlopen",
        lambda *args, **kwargs: FakeHttpResponse(
            payload,
            content_length=len(payload),
        ),
    )

    ingestion.download_file()

    assert output_file.exists()
    assert ingestion.is_valid_parquet(output_file) is True

    # O temporário foi promovido atomicamente para o RAW.
    assert not temp_file.exists()


def test_download_file_rejects_incomplete_download(
    tmp_path,
    monkeypatch,
):
    output_file, _ = configure_download_paths(
        tmp_path,
        monkeypatch,
    )

    payload = build_parquet_bytes()

    monkeypatch.setattr(
        ingestion,
        "urlopen",
        lambda *args, **kwargs: FakeHttpResponse(
            payload,
            content_length=len(payload) + 100,
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="Download incompleto",
    ):
        ingestion.download_file()

    # Um download incompleto jamais pode virar RAW oficial.
    assert not output_file.exists()


def test_download_file_rejects_invalid_parquet(
    tmp_path,
    monkeypatch,
):
    output_file, _ = configure_download_paths(
        tmp_path,
        monkeypatch,
    )

    payload = b"arquivo-corrompido"

    monkeypatch.setattr(
        ingestion,
        "urlopen",
        lambda *args, **kwargs: FakeHttpResponse(
            payload,
            content_length=len(payload),
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="não passou na validação Parquet",
    ):
        ingestion.download_file()

    # Arquivo corrompido nunca deve ser publicado.
    assert not output_file.exists()
