from bicam_collection.storage import _redact_error


def test_redact_error_removes_api_credentials_from_urls() -> None:
    error = "404 for https://api.govinfo.gov/packages/X/summary?api_key=secret-value&format=json"

    redacted = _redact_error(error)

    assert redacted == "404 for https://api.govinfo.gov/packages/X/summary?api_key=[REDACTED]&format=json"
    assert "secret-value" not in redacted


def test_redact_error_handles_other_token_parameter_names() -> None:
    assert _redact_error("request failed?access_token=abc123") == "request failed?access_token=[REDACTED]"
