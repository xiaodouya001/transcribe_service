"""coverage: schemas.error_scenarios helpers and validation"""

import pytest
from starlette import status

from realtime_transcribe_service.schemas.error_codes import ErrorCode, WsCloseCode
from realtime_transcribe_service.schemas.error_scenarios import ProtocolErrorScenario


def test_protocol_error_scenarios_cover_all_matrix_entries():
    assert {
        scenario.matrix_id
        for scenario in ProtocolErrorScenario
        if scenario.matrix_id is not None
    } == {
        "E-01",
        "E-02",
        "E-03",
        "E-04",
        "E-05",
        "E-06",
        "E-07",
        "E-08",
        "E-09",
        "E-10",
        "E-11",
        "E-12",
        "E-13",
        "E-14",
        "E-15",
        "E-16",
        "E-17",
    }


def test_protocol_error_scenario_build_response_uses_defaults():
    scenario = ProtocolErrorScenario.MISSING_QUERY_CONVERSATION_ID

    assert scenario.require_http_status() == status.HTTP_400_BAD_REQUEST
    assert scenario.error_code == ErrorCode.E1003
    assert scenario.default_message == "Missing required field"
    assert scenario.default_log_reason == "Transport: Missing conversationId, rejecting connection"
    assert scenario.format_details() == "Query parameter 'conversationId' is required"

    response = scenario.build_response("conv-1")
    assert response["error"]["code"] == "E1003"
    assert response["error"]["message"] == "Missing required field"
    assert response["error"]["details"] == "Query parameter 'conversationId' is required"


def test_protocol_error_scenario_build_response_allows_detail_override():
    scenario = ProtocolErrorScenario.AUTHENTICATION_FAILED

    response = scenario.build_response("conv-1", details="Bearer token missing")
    assert response["error"]["code"] == "E1010"
    assert response["error"]["message"] == "Authentication failed"
    assert response["error"]["details"] == "Bearer token missing"


def test_protocol_error_scenario_formats_dynamic_details():
    scenario = ProtocolErrorScenario.CONNECTION_LIMIT_EXCEEDED

    details = scenario.format_details(active=3, max_connections=2)
    response = scenario.build_response("conv-1", details=details)

    assert response["error"]["code"] == "E1008"
    assert response["error"]["message"] == "Too many connections"
    assert response["error"]["details"] == "Active 3 >= limit 2"


def test_protocol_error_scenario_supports_ws_close_only_without_default_details():
    scenario = ProtocolErrorScenario.ORCHESTRATOR_INTERNAL_EXCEPTION

    assert scenario.require_ws_close_code() == WsCloseCode.INTERNAL_ERROR
    assert scenario.format_details() is None
    response = scenario.build_response("conv-1")
    assert response["error"]["details"] is None


def test_protocol_error_scenario_require_http_status_rejects_ws_only_scenario():
    with pytest.raises(ValueError, match="does not define an HTTP status"):
        ProtocolErrorScenario.INVALID_JSON.require_http_status()


def test_protocol_error_scenario_require_ws_close_code_rejects_http_only_scenario():
    with pytest.raises(ValueError, match="does not define a WebSocket close code"):
        ProtocolErrorScenario.SERVICE_DRAINING.require_ws_close_code()
