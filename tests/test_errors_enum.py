"""coverage: schemas.errors close_code_for_error branches"""

import pytest

from transcribe_service.schemas.errors import ErrorCode, WsCloseCode, close_code_for_error


@pytest.mark.parametrize(
    "code,expected",
    [
        (ErrorCode.E1001, WsCloseCode.INVALID_PAYLOAD),
        (ErrorCode.E1002, WsCloseCode.POLICY_VIOLATION),
        (ErrorCode.E1003, WsCloseCode.POLICY_VIOLATION),
        (ErrorCode.E1004, WsCloseCode.POLICY_VIOLATION),
        (ErrorCode.E1005, WsCloseCode.POLICY_VIOLATION),
        (ErrorCode.E1006, WsCloseCode.POLICY_VIOLATION),
        (ErrorCode.E1007, WsCloseCode.INTERNAL_ERROR),
        (ErrorCode.E1008, WsCloseCode.TRY_AGAIN_LATER),
        (ErrorCode.E1012, WsCloseCode.TRY_AGAIN_LATER),
        (ErrorCode.E1009, WsCloseCode.POLICY_VIOLATION),
        (ErrorCode.E1010, WsCloseCode.POLICY_VIOLATION),
        (ErrorCode.E1011, WsCloseCode.POLICY_VIOLATION),
    ],
)
def test_close_code_for_error(code: ErrorCode, expected: WsCloseCode):
    assert close_code_for_error(code) == expected
