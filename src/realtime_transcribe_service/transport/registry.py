"""Connection registry for active WebSocket sessions."""

from __future__ import annotations

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from realtime_transcribe_service.constants import WS_CLOSE_REASON_GOING_AWAY
from realtime_transcribe_service.schemas.error_codes import WsCloseCode


class ConnectionRegistry:
    """Track active WebSocket connections and close them in bulk during shutdown."""

    def __init__(self) -> None:
        self._connections: dict[str, list[WebSocket]] = {}
        self._active_count = 0

    def add(self, conversation_id: str, ws: WebSocket) -> None:
        self._connections.setdefault(conversation_id, []).append(ws)
        self._active_count += 1

    def remove(self, conversation_id: str, ws: WebSocket | None = None) -> None:
        """Remove a registration.

        If ``ws`` is provided, remove it only when the registered object is still the
        same instance. This prevents an older connection's ``finally`` block from
        accidentally deleting a newer registration for the same conversation.
        """
        if ws is None:
            sockets = self._connections.pop(conversation_id, None)
            if sockets:
                self._active_count -= len(sockets)
            return
        sockets = self._connections.get(conversation_id)
        if not sockets:
            return
        remaining = [registered_ws for registered_ws in sockets if registered_ws is not ws]
        removed = len(sockets) - len(remaining)
        if removed:
            self._active_count -= removed
        if remaining:
            self._connections[conversation_id] = remaining
        else:
            self._connections.pop(conversation_id, None)

    @property
    def active_count(self) -> int:
        return self._active_count

    async def close_all(
        self,
        code: int = WsCloseCode.GOING_AWAY,
        reason: str = WS_CLOSE_REASON_GOING_AWAY,
    ) -> None:
        """Send Close frames to all currently active connections during graceful shutdown."""
        for cid, sockets in list(self._connections.items()):
            for ws in list(sockets):
                try:
                    if ws.client_state == WebSocketState.CONNECTED:
                        await ws.close(code=code, reason=reason)
                except Exception:
                    pass
            self._connections.pop(cid, None)
        self._active_count = 0
