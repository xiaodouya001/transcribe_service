"""ConnectorManager - manages multiple STT sessions via Webhook."""

import asyncio
from typing import Any

import structlog

from transcription_ingest.connector import get_connector_for_url
from transcription_ingest.connector.reconnect import run_with_reconnect

log = structlog.get_logger(__name__)


class ConnectorManager:
    """Manages multiple sessions: add_session creates Connector + run_session task."""

    def __init__(
        self,
        *,
        dedup: Any,
        cleaner: Any,
        producer: Any,
        settings: Any,
        shutdown: Any = None,
    ) -> None:
        self._dedup = dedup
        self._cleaner = cleaner
        self._producer = producer
        self._settings = settings
        self._shutdown = shutdown
        self._sessions: dict[str, asyncio.Task] = {}

    def add_session(self, metadata: dict, ws_url: str, sse_url: str) -> None:
        """Create Connector for session, start run_session task."""
        session_id = (metadata or {}).get("session_id", "")
        if not session_id:
            log.warning("ConnectorManager: 忽略无 session_id 的 Webhook", metadata=metadata)
            return

        if session_id in self._sessions:
            log.info("ConnectorManager: 会话已存在，忽略重复", session_id=session_id)
            return

        use_sse = (self._settings.transcribe_service_protocol or "sse").lower() == "sse"
        url = sse_url if use_sse else ws_url
        if not url:
            log.warning(
                "ConnectorManager: 无可用 URL",
                session_id=session_id,
                use_sse=use_sse,
                sse_url=sse_url,
                ws_url=ws_url,
            )
            return

        task = asyncio.create_task(
            self._run_session(session_id, url, use_sse, metadata),
            name=f"session-{session_id}",
        )
        self._sessions[session_id] = task
        log.info(
            "ConnectorManager: 已添加会话",
            session_id=session_id,
            url=url,
            use_sse=use_sse,
        )

    def remove_session(self, session_id: str) -> None:
        """Cancel and remove session task."""
        task = self._sessions.pop(session_id, None)
        if task and not task.done():
            task.cancel()
            log.info("ConnectorManager: 已移除会话", session_id=session_id)

    async def wait_for_sessions(self, timeout: float | None = None) -> None:
        """Wait for all session tasks to finish, or timeout."""
        if not self._sessions:
            return
        tasks = list(self._sessions.values())
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout)
        except asyncio.TimeoutError:
            for sid, t in list(self._sessions.items()):
                if not t.done():
                    t.cancel()
            log.warning("ConnectorManager: 等待会话超时", remaining=list(self._sessions.keys()))

    async def _run_session(
        self,
        session_id: str,
        url: str,
        use_sse: bool,
        metadata: dict,
    ) -> None:
        """Run single session: connect -> Dedup -> Cleaner -> Producer. Uses run_with_reconnect."""
        read_timeout = getattr(self._settings, "sse_read_timeout", None)
        ping_interval = getattr(self._settings, "ws_ping_interval", 20.0)
        ping_timeout = getattr(self._settings, "ws_ping_timeout", 20.0)

        last_event_id: str | None = None

        async def connect_fn(leid: str | None) -> str | None:
            nonlocal last_event_id
            connector = get_connector_for_url(
                url,
                use_sse=use_sse,
                last_event_id=leid,
                read_timeout=read_timeout,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
            )
            try:
                async for event, payload in connector.connect():
                    if self._shutdown and getattr(self._shutdown, "draining", False):
                        break
                    received_at = payload.pop("_ingest_received_at", None)
                    if await self._dedup.should_emit(
                        event.session_id,
                        event.seq_no,
                        processing_id=event.processing_id,
                        created_at=event.created_at,
                    ):
                        cleaned = self._cleaner.clean(payload, event)
                        log.info(
                            "ConnectorManager: 发送 transcript 到 Kafka",
                            session_id=event.session_id,
                            seq_no=event.seq_no,
                            transcript=event.transcript[:30] + "..." if len(event.transcript) > 30 else event.transcript,
                        )
                        await self._producer.send(
                            session_id=event.session_id,
                            seq_no=event.seq_no,
                            transcript=event.transcript,
                            role=event.role,
                            created_at=event.created_at,
                            processing_status=event.processing_status,
                            processing_id=event.processing_id,
                            raw_payload=cleaned.get("raw"),
                            cleaned=cleaned.get("cleaned"),
                        )
                    else:
                        log.info("Dedup: 已过滤重复", session_id=event.session_id, seq_no=event.seq_no)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("ConnectorManager: 会话异常", session_id=session_id, error=str(e))
                raise
            return getattr(connector, "last_event_id", None)

        try:
            if getattr(self._settings, "reconnect_enabled", True):
                await run_with_reconnect(connect_fn, self._settings, self._shutdown)
            else:
                await connect_fn(None)
        except asyncio.CancelledError:
            pass
        finally:
            self.remove_session(session_id)
