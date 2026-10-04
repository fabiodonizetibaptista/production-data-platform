import pytest

import src.pipeline.run_nyc_taxi_pipeline as pipeline


pytestmark = pytest.mark.unit


def test_pipeline_stops_when_raw_validation_fails(
    monkeypatch,
):
    monkeypatch.setattr(
        pipeline,
        "download_file",
        lambda: None,
    )

    monkeypatch.setattr(
        pipeline,
        "validate_dataset",
        lambda *_: False,
    )

    def fail_if_transformation_runs(*args, **kwargs):
        raise AssertionError(
            "Transformação não deveria executar "
            "após falha da RAW."
        )

    monkeypatch.setattr(
        pipeline,
        "transform_raw_to_silver",
        fail_if_transformation_runs,
    )

    with pytest.raises(
        pipeline.PipelineStepError,
        match="RAW validation failed",
    ):
        pipeline.run_pipeline()


def test_pipeline_fails_when_silver_validation_fails(
    monkeypatch,
):
    monkeypatch.setattr(
        pipeline,
        "download_file",
        lambda: None,
    )

    monkeypatch.setattr(
        pipeline,
        "validate_dataset",
        lambda *_: True,
    )

    monkeypatch.setattr(
        pipeline,
        "transform_raw_to_silver",
        lambda **kwargs: None,
    )

    monkeypatch.setattr(
        pipeline,
        "validate_silver",
        lambda: False,
    )

    with pytest.raises(
        pipeline.PipelineStepError,
        match="Silver validation failed",
    ):
        pipeline.run_pipeline()
