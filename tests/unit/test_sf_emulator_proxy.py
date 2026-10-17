import gzip
import json

from static.snowflake_gzip_proxy import ProxyHandler


def test_decompress_gzip_login_request_with_content_encoding_header():
    body = b'{"data":{"LOGIN_NAME":"test"}}'

    assert ProxyHandler._decompress_gzip_body(gzip.compress(body), "gzip") == body


def test_decompress_gzip_request_without_content_encoding_header():
    body = b'{"data":{"LOGIN_NAME":"test"}}'

    assert ProxyHandler._decompress_gzip_body(gzip.compress(body), "") == body


def test_normalize_response_adds_empty_rowtype_for_no_result_statement():
    response = {
        "data": {
            "queryId": "test-query",
            "sqlState": "00000",
        }
    }

    normalized = json.loads(
        ProxyHandler.normalize_response(json.dumps(response).encode(), login=False)
    )

    assert normalized["data"]["rowtype"] == []
    assert normalized["data"]["rowset"] == []


def test_normalize_response_replaces_null_rowset_with_empty_list():
    response = {"data": {"rowtype": [], "rowset": None}}

    normalized = json.loads(
        ProxyHandler.normalize_response(json.dumps(response).encode(), login=False)
    )

    assert normalized["data"]["rowset"] == []


def test_normalize_response_preserves_rowtype_and_completes_column_metadata():
    response = {
        "data": {
            "rowtype": [{"name": "RESULT", "type": "fixed"}],
        }
    }

    normalized = json.loads(
        ProxyHandler.normalize_response(json.dumps(response).encode(), login=False)
    )

    assert normalized["data"]["rowtype"] == [
        {
            "name": "RESULT",
            "type": "fixed",
            "length": None,
            "precision": None,
            "scale": None,
        }
    ]
