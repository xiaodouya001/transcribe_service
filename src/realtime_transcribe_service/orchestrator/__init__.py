"""Orchestrator 调度层 — 两阶段提交业务编排。"""

from realtime_transcribe_service.orchestrator.protocols import OrchestratorBackend, OrchestratorResult

__all__ = ["OrchestratorBackend", "OrchestratorResult"]

