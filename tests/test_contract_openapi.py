from fastapi.testclient import TestClient

from ingestion_service.main import app

client = TestClient(app)


def test_openapi_ingest_contract():
    """Verify /v1/ingest endpoint matches contract models in OpenAPI spec."""
    # 1. Fetch OpenAPI spec
    response = client.get("/openapi.json")
    assert response.status_code == 200
    spec = response.json()

    # 2. Ensure /v1/ingest POST exists
    path = "/v1/ingest"
    assert path in spec["paths"], f"{path} not in OpenAPI paths"
    post_op = spec["paths"][path]["post"]

    # 3. Check request body schema references IngestRequest
    req_body = post_op.get("requestBody")
    assert req_body is not None, "POST /v1/ingest must have requestBody"
    req_schema_ref = req_body["content"]["application/json"]["schema"]["$ref"]
    assert req_schema_ref.endswith("IngestRequest"), (
        f"Request schema should be IngestRequest, got {req_schema_ref}"
    )

    # 4. Check response schema references IngestResponse
    responses = post_op["responses"]
    resp_202 = responses.get("202")
    assert resp_202 is not None, "POST /v1/ingest must define 202 response"
    resp_schema_ref = resp_202["content"]["application/json"]["schema"]["$ref"]
    assert resp_schema_ref.endswith("IngestResponse"), (
        f"Response schema should be IngestResponse, got {resp_schema_ref}"
    )
    # 5. Optionally check 422 (validation error) uses ErrorResponse
    # resp_422 = responses.get("422")
    # if resp_422:
    #    error_schema_ref = resp_422["content"]["application/json"]["schema"]["$ref"]
    #    assert error_schema_ref.endswith("ErrorResponse"), \
    #         f"422 response should be ErrorResponse, got {error_schema_ref}"
