"""Echo producer for Demo mode - writes to console and/or JSONL file."""

import json
import os
from pathlib import Path
from typing import Any

import structlog

from asr_ingest.producer.base import ProducerBackend

log = structlog.get_logger()

def _default_output_file() -> str:
    """Read at runtime so tests can set DEMO_OUTPUT_FILE before creating producer."""
    return os.environ.get("DEMO_OUTPUT_FILE", "demo_output.jsonl")


class EchoProducer:
    """Demo producer: print to console and append to demo_output.jsonl."""

    def __init__(self, output_file: str | None = None) -> None:
        self._output_file = Path(output_file or _default_output_file())
        self._file_handle = None

    def _ensure_file(self) -> None:
        if self._file_handle is None or self._file_handle.closed:
            self._file_handle = open(self._output_file, "a", encoding="utf-8")

    async def send(
        self,
        session_id: str,
        seq_no: int,
        transcript: str,
        role: str = "",
        created_at: str = "",
        processing_status: str = "",
        *,
        raw_payload: dict | None = None,
        cleaned: dict | None = None,
        **kwargs: Any,
    ) -> None:
        """Emit to console and append JSONL line. When raw/cleaned provided, write that format."""
        if raw_payload is not None or cleaned is not None:
            payload = {"raw": raw_payload, "cleaned": cleaned or {}}
        else:
            extra = {k: v for k, v in kwargs.items() if k not in ("raw_payload", "cleaned", "source_json")}
            payload = {
                "raw": None,
                "cleaned": {
                    "session_id": session_id,
                    "seq_no": seq_no,
                    "transcript": transcript,
                    "role": role,
                    "created_at": created_at,
                    "processing_status": processing_status,
                    **extra,
                },
            }
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        log.info("EchoProducer emit", payload=payload)
        self._ensure_file()
        self._file_handle.write(line)
        self._file_handle.flush()

    async def flush(self) -> None:
        """No-op for echo; file is flushed on each send."""
        if self._file_handle and not self._file_handle.closed:
            self._file_handle.flush()

    def close(self) -> None:
        """Close the output file handle."""
        if self._file_handle and not self._file_handle.closed:
            self._file_handle.close()
            self._file_handle = None
