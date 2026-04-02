"""Protocol-level error scenarios aligned with the contract matrix."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from starlette import status

from realtime_transcribe_service.schemas.error_codes import ErrorCode, WsCloseCode
from realtime_transcribe_service.schemas.response import build_error


@dataclass(frozen=True, slots=True)
class ProtocolErrorSpec:
    """Static protocol mapping for one documented error scenario."""

    matrix_id: str | None
    error_code: ErrorCode
    message: str
    details_template: str | None
    http_status: int | None = None
    ws_close_code: WsCloseCode | None = None
    log_reason: str | None = None


class ProtocolErrorScenario(Enum):
    """Single source of truth for protocol-level error scenarios."""

    MISSING_QUERY_CONVERSATION_ID = ProtocolErrorSpec(
        matrix_id="E-01",
        error_code=ErrorCode.E1003,
        message="Missing required field",
        details_template="Query parameter 'conversationId' is required",
        http_status=status.HTTP_400_BAD_REQUEST,
        log_reason="Transport: Missing conversationId, rejecting connection",
    )
    SERVICE_DRAINING = ProtocolErrorSpec(
        matrix_id="E-02",
        error_code=ErrorCode.E1008,
        message="Service draining",
        details_template="Server is shutting down, try again later",
        http_status=status.HTTP_503_SERVICE_UNAVAILABLE,
        log_reason="Transport: Service is draining, rejecting new connection",
    )
    CONNECTION_LIMIT_EXCEEDED = ProtocolErrorSpec(
        matrix_id="E-03",
        error_code=ErrorCode.E1008,
        message="Too many connections",
        details_template="Active {active} >= limit {max_connections}",
        http_status=status.HTTP_429_TOO_MANY_REQUESTS,
        log_reason="Transport: Connection limit reached, rejecting new connection",
    )
    INVALID_JSON = ProtocolErrorSpec(
        matrix_id="E-04",
        error_code=ErrorCode.E1001,
        message="Invalid JSON",
        details_template=None,
        ws_close_code=WsCloseCode.INVALID_PAYLOAD,
        log_reason="Transport: JSON decode failed",
    )
    INVALID_ENUM_VALUE = ProtocolErrorSpec(
        matrix_id="E-05",
        error_code=ErrorCode.E1002,
        message="Validation failed",
        details_template=None,
        ws_close_code=WsCloseCode.POLICY_VIOLATION,
        log_reason="Orchestrator: Schema validation failed",
    )
    MISSING_REQUIRED_FIELD = ProtocolErrorSpec(
        matrix_id="E-06",
        error_code=ErrorCode.E1003,
        message="Validation failed",
        details_template=None,
        ws_close_code=WsCloseCode.POLICY_VIOLATION,
        log_reason="Orchestrator: Schema validation failed",
    )
    INVALID_FIELD_TYPE = ProtocolErrorSpec(
        matrix_id="E-07",
        error_code=ErrorCode.E1004,
        message="Validation failed",
        details_template=None,
        ws_close_code=WsCloseCode.POLICY_VIOLATION,
        log_reason="Orchestrator: Schema validation failed",
    )
    INVALID_TIMESTAMP_FORMAT = ProtocolErrorSpec(
        matrix_id="E-08",
        error_code=ErrorCode.E1005,
        message="Validation failed",
        details_template=None,
        ws_close_code=WsCloseCode.POLICY_VIOLATION,
        log_reason="Orchestrator: Schema validation failed",
    )
    SEQUENCE_OUT_OF_ORDER = ProtocolErrorSpec(
        matrix_id="E-09",
        error_code=ErrorCode.E1006,
        message="Sequence number out of order",
        details_template="sequenceNumber={sequence_number} is not expected",
        ws_close_code=WsCloseCode.POLICY_VIOLATION,
        log_reason="Orchestrator: Sequence number out of order",
    )
    DOWNSTREAM_TIMEOUT = ProtocolErrorSpec(
        matrix_id="E-10",
        error_code=ErrorCode.E1011,
        message="Downstream timeout",
        details_template="Kafka send timed out",
        ws_close_code=WsCloseCode.TRY_AGAIN_LATER,
        log_reason="Orchestrator: Kafka timed out",
    )
    DOWNSTREAM_UNAVAILABLE = ProtocolErrorSpec(
        matrix_id="E-11",
        error_code=ErrorCode.E1008,
        message="Downstream unavailable",
        details_template=None,
        http_status=status.HTTP_503_SERVICE_UNAVAILABLE,
        ws_close_code=WsCloseCode.TRY_AGAIN_LATER,
        log_reason=None,
    )
    ORCHESTRATOR_INTERNAL_EXCEPTION = ProtocolErrorSpec(
        matrix_id="E-12",
        error_code=ErrorCode.E1007,
        message="Internal server error",
        details_template=None,
        ws_close_code=WsCloseCode.INTERNAL_ERROR,
        log_reason="Orchestrator: Unhandled exception",
    )
    TRANSPORT_INTERNAL_EXCEPTION = ProtocolErrorSpec(
        matrix_id="E-13",
        error_code=ErrorCode.E1007,
        message="Internal server error",
        details_template=None,
        ws_close_code=WsCloseCode.INTERNAL_ERROR,
        log_reason="Transport: Connection error",
    )
    CONVERSATION_ID_MISMATCH = ProtocolErrorSpec(
        matrix_id="E-14",
        error_code=ErrorCode.E1009,
        message="conversationId mismatch",
        details_template=(
            "metaData.conversationId must match query parameter "
            "'conversationId' ({expected_conversation_id!r})"
        ),
        ws_close_code=WsCloseCode.POLICY_VIOLATION,
        log_reason="Transport: metaData.conversationId does not match the handshake query",
    )
    BUSINESS_RULE_VIOLATION = ProtocolErrorSpec(
        matrix_id="E-15",
        error_code=ErrorCode.E1009,
        message="Validation failed",
        details_template=None,
        ws_close_code=WsCloseCode.POLICY_VIOLATION,
        log_reason="Orchestrator: Schema validation failed",
    )
    ACTIVE_SENDER_CONFLICT = ProtocolErrorSpec(
        matrix_id="E-16",
        error_code=ErrorCode.E1009,
        message="Only one sender connection is allowed",
        details_template="another connection is already sending messages for this conversation",
        http_status=status.HTTP_403_FORBIDDEN,
        ws_close_code=WsCloseCode.POLICY_VIOLATION,
        log_reason="Transport: Conversation already has an active sender",
    )
    AUTHENTICATION_FAILED = ProtocolErrorSpec(
        matrix_id="E-17",
        error_code=ErrorCode.E1010,
        message="Authentication failed",
        details_template=None,
        http_status=status.HTTP_401_UNAUTHORIZED,
        log_reason="Transport: Authentication failed during handshake",
    )

    @property
    def matrix_id(self) -> str | None:
        return self.value.matrix_id

    @property
    def error_code(self) -> ErrorCode:
        return self.value.error_code

    @property
    def default_message(self) -> str:
        return self.value.message

    @property
    def default_log_reason(self) -> str | None:
        return self.value.log_reason

    def format_details(self, **kwargs: object) -> str | None:
        """Render the scenario's details string from the static template."""
        template = self.value.details_template
        if template is None:
            return None
        return template.format(**kwargs)

    def build_response(
        self,
        conversation_id: str,
        *,
        details: str | None = None,
    ) -> dict[str, Any]:
        """Build an ``ERROR`` response using this scenario's defaults."""
        return build_error(
            conversation_id,
            self.error_code.value,
            self.default_message,
            self.format_details() if details is None else details,
        )

    def require_http_status(self) -> int:
        """Return the configured HTTP status or fail fast when absent."""
        http_status = self.value.http_status
        if http_status is None:
            raise ValueError(f"{self.name} does not define an HTTP status")
        return http_status

    def require_ws_close_code(self) -> WsCloseCode:
        """Return the configured WebSocket close code or fail fast when absent."""
        ws_close_code = self.value.ws_close_code
        if ws_close_code is None:
            raise ValueError(f"{self.name} does not define a WebSocket close code")
        return ws_close_code
