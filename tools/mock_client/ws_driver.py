"""WebSocket 连接驱动 — 消息生成、场景引擎、并发压测。"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

import websockets
import websockets.exceptions

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 消息生成器
# ---------------------------------------------------------------------------

_TRANSCRIPTS_ZH = [
    "你好，请问有什么可以帮到你？",
    "我想查询一下我的账户余额。",
    "好的，请稍等，我帮您查询一下。",
    "您的账户余额是一千二百三十四元。",
    "请问还有其他问题吗？",
    "我还想了解一下信用卡的优惠活动。",
    "目前我们有消费满一百减二十的活动。",
    "这个活动的有效期是到月底。",
    "好的，谢谢你的解答。",
    "不客气，祝您生活愉快，再见。",
    "我想投诉一下上次的服务体验。",
    "非常抱歉给您带来不好的体验，请您描述一下具体情况。",
    "上次打电话等了半小时才接通。",
    "我们会改进排队系统，感谢您的反馈。",
    "请问密码忘记了怎么办？",
    "您可以通过手机验证码重置密码。",
]

_TRANSCRIPTS_EN = [
    "Hello, how can I help you today?",
    "I'd like to check my account balance please.",
    "Sure, let me look that up for you.",
    "Your current balance is twelve hundred dollars.",
    "Is there anything else I can help with?",
    "Can you tell me about the rewards program?",
    "We currently have a cashback promotion running.",
    "The promotion is valid until the end of this month.",
    "Great, thank you for the information.",
    "You're welcome. Have a nice day!",
    "I need to report an unauthorized transaction.",
    "I'm sorry to hear that. Let me escalate this for you.",
    "How long will the investigation take?",
    "Typically it takes three to five business days.",
    "Can I update my mailing address?",
    "Of course, what is your new address?",
]


def _random_hex(n: int = 4) -> str:
    return uuid.uuid4().hex[:n]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def random_transcript() -> str:
    pool = _TRANSCRIPTS_ZH + _TRANSCRIPTS_EN
    return random.choice(pool)


def generate_message(
    conversation_id: str,
    seq: int,
    *,
    event_type: str = "SESSION_ONGOING",
    agent_id: str | None = None,
    staff_id: str | None = None,
    customer_id: str | None = None,
    start_ts: str | None = None,
) -> dict[str, Any]:
    """生成一条符合 InboundMessage schema 的消息。"""
    now = _utc_now_iso()
    return {
        "metaData": {
            "conversationId": conversation_id,
            "agentId": agent_id or f"AGT-{_random_hex(4)}",
            "staffId": staff_id or f"STF-{_random_hex(4)}",
            "customerId": customer_id or f"CST-{_random_hex(4)}",
            "callStartTimeStamp": start_ts or now,
            "callEndTimeStamp": now if event_type == "SESSION_COMPLETE" else None,
            "eventType": event_type,
        },
        "payload": {
            "sequenceNumber": seq,
            "speaker": random.choice(["Agent", "Customer"]),
            "transcript": random_transcript(),
            "engineProvider": "FanoLabs",
            "isFinal": True,
            "createdAtTimeStamp": now,
        },
    }


def _session_message_split(total_messages: int) -> tuple[int, int]:
    """将「会话内 WebSocket 业务消息总数」拆成 ONGOING 条数与 COMPLETE 的 seq。

    ``total_messages`` 含最后一条 ``SESSION_COMPLETE``（至少为 1：仅发结束帧时 seq=0）。

    返回 ``(ongoing_count, complete_seq)``。
    """
    total = max(1, total_messages)
    ongoing_count = max(0, total - 1)
    complete_seq = total - 1
    return ongoing_count, complete_seq


# ---------------------------------------------------------------------------
# 统计
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    load_running: bool = False
    sent: int = 0
    ack: int = 0
    error: int = 0
    active_connections: int = 0
    latencies: list[float] = field(default_factory=list)
    server_latencies: list[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)
    end_time: float | None = None
    recent_errors: deque = field(default_factory=lambda: deque(maxlen=100))

    def record_load_error(
        self,
        *,
        stage: str,
        cid: str,
        detail: str,
        seq: int | None = None,
        event_type: str | None = None,
        server_resp: dict | None = None,
    ) -> None:
        """压测路径专用：累加 error，并记入环形列表 + 打日志。"""
        self.error += 1
        entry: dict[str, Any] = {
            "stage": stage,
            "cid": cid,
            "detail": detail[:800],
        }
        if seq is not None:
            entry["seq"] = seq
        if event_type is not None:
            entry["eventType"] = event_type
        if server_resp is not None:
            entry["server_resp"] = server_resp
        self.recent_errors.append(entry)
        log.warning("压测错误 %s", entry)

    def snapshot(self) -> dict[str, Any]:
        finished_at = self.end_time if self.end_time is not None else time.monotonic()
        elapsed = max(finished_at - self.start_time, 0.001)
        sorted_lat = sorted(self.latencies) if self.latencies else [0]
        sorted_srv_lat = sorted(self.server_latencies) if self.server_latencies else [0]
        def _pct(p: float) -> float:
            idx = int(len(sorted_lat) * p)
            idx = min(idx, len(sorted_lat) - 1)
            return round(sorted_lat[idx] * 1000, 2)
        def _srv_pct(p: float) -> float:
            idx = int(len(sorted_srv_lat) * p)
            idx = min(idx, len(sorted_srv_lat) - 1)
            return round(sorted_srv_lat[idx] * 1000, 2)
        return {
            "load_running": self.load_running,
            "sent": self.sent,
            "ack": self.ack,
            "error": self.error,
            "active_connections": self.active_connections,
            "tps": round(self.sent / elapsed, 1) if self.sent > 0 else 0.0,
            "p50_ms": _pct(0.5),
            "p95_ms": _pct(0.95),
            "p99_ms": _pct(0.99),
            "server_p50_ms": _srv_pct(0.5),
            "server_p95_ms": _srv_pct(0.95),
            "server_p99_ms": _srv_pct(0.99),
            "elapsed_sec": round(elapsed, 1),
            "recent_errors": list(self.recent_errors),
        }

    def finish(self) -> None:
        self.load_running = False
        if self.end_time is None:
            self.end_time = time.monotonic()

    def reset(self) -> None:
        self.load_running = False
        self.sent = 0
        self.ack = 0
        self.error = 0
        self.active_connections = 0
        self.latencies.clear()
        self.server_latencies.clear()
        self.recent_errors.clear()
        self.start_time = time.monotonic()
        self.end_time = None


# ---------------------------------------------------------------------------
# SSE 事件广播
# ---------------------------------------------------------------------------

EventCallback = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]


# ---------------------------------------------------------------------------
# 场景引擎
# ---------------------------------------------------------------------------

@dataclass
class ScenarioResult:
    name: str
    passed: bool
    steps: list[dict[str, Any]] = field(default_factory=list)


def _format_server_error(resp: dict | None) -> str:
    """从服务端 ERROR 帧中提取可读错误信息。"""
    if not resp:
        return ""
    err = resp.get("error") or {}
    code = err.get("code", "?")
    msg = err.get("message", "")
    details = err.get("details", "")
    parts = [f"[{code}] {msg}"]
    if details:
        parts.append(details[:300])
    return " — ".join(parts)


def _format_ws_connect_error(exc: BaseException) -> tuple[str, dict | None]:
    """拼握手失败详情，返回 (可读摘要, 服务端 JSON 响应 or None)。"""
    resp = getattr(exc, "response", None)
    if resp is not None:
        sc = getattr(resp, "status_code", None) or getattr(resp, "status", None)
        body = getattr(resp, "body", None)
        server_resp: dict | None = None
        body_text = ""
        if body:
            try:
                import json
                server_resp = json.loads(body)
                err = server_resp.get("error", {})
                body_text = f"[{err.get('code', '?')}] {err.get('message', '')}"
                if err.get("details"):
                    body_text += f" — {err['details']}"
            except (json.JSONDecodeError, ValueError, AttributeError):
                try:
                    body_text = body.decode("utf-8", errors="replace").strip()[:200]
                except Exception:
                    body_text = repr(body)[:120]
        status_map = {400: "请求无效", 429: "连接数超限", 503: "服务不可用"}
        reason = status_map.get(sc, str(exc))
        detail = f"HTTP {sc} — {reason}" + (f": {body_text}" if body_text else "")
        return detail, server_resp

    cause = exc.__cause__
    if cause:
        return f"{type(exc).__name__}: {exc} (cause: {type(cause).__name__}: {cause})", None
    return f"{type(exc).__name__}: {exc}", None


async def _open_ws(
    ws_url: str,
    conversation_id: str | None,
    retries: int = 3,
    retry_delay: float = 0.5,
) -> websockets.WebSocketClientProtocol:
    uri = ws_url if conversation_id is None else f"{ws_url}?conversationId={conversation_id}"
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return await websockets.connect(uri, open_timeout=30)
        except Exception as e:
            last_exc = e
            if attempt < retries:
                await asyncio.sleep(retry_delay * attempt)
    raise last_exc  # type: ignore[misc]


async def _send_and_recv(
    ws: websockets.WebSocketClientProtocol,
    msg: dict | str,
) -> dict | None:
    """发送消息并接收一条响应。如果连接已关闭返回 None。"""
    text = msg if isinstance(msg, str) else json.dumps(msg, ensure_ascii=False)
    await ws.send(text)
    try:
        resp = await asyncio.wait_for(ws.recv(), timeout=10)
        return json.loads(resp)
    except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError):
        return None


async def _send_expect_error_and_close(
    ws: websockets.WebSocketClientProtocol,
    msg: dict | str,
    *,
    action: str,
    expected_code: str,
    expected_close: int,
    result: ScenarioResult,
    emit: EventCallback,
) -> None:
    """发送一条预期触发 ERROR + Close 的消息，并校验错误码与关闭码。"""
    resp = await _send_and_recv(ws, msg)
    step = {"action": action, "resp_type": resp.get("metaData", {}).get("eventType") if resp else None}
    if resp and resp.get("metaData", {}).get("eventType") == "ERROR":
        err_code = resp.get("error", {}).get("code")
        step["error_code"] = err_code
        if err_code != expected_code:
            result.passed = False
            step["error"] = f"期望 {expected_code}，实际={err_code}"
    else:
        result.passed = False
        step["error"] = "期望 ERROR 帧"
    result.steps.append(step)
    await emit("scenario_step", {"scenario": result.name, "step": step})

    try:
        await asyncio.wait_for(ws.wait_closed(), timeout=5)
        close_code = ws.close_code
        step_c = {"action": "verify_close", "close_code": close_code}
        if close_code != expected_close:
            result.passed = False
            step_c["error"] = f"期望 close_code={expected_close}，实际={close_code}"
        result.steps.append(step_c)
        await emit("scenario_step", {"scenario": result.name, "step": step_c})
    except asyncio.TimeoutError:
        result.passed = False
        result.steps.append({"action": "verify_close", "error": "等待关闭超时"})


async def _session_ongoing_plus_complete_and_close(
    ws: Any,
    cid: str,
    meta_base: dict[str, Any],
    n_messages: int,
    result: ScenarioResult,
    emit: EventCallback,
) -> None:
    """按「会话消息总数」发送 ONGOING + SESSION_COMPLETE，并校验服务端 close 1000。"""
    ongoing_count, complete_seq = _session_message_split(n_messages)
    for seq in range(ongoing_count):
        msg = generate_message(cid, seq, event_type="SESSION_ONGOING", **meta_base)
        resp = await _send_and_recv(ws, msg)
        step = {
            "action": "send_ongoing",
            "seq": seq,
            "resp_type": resp.get("metaData", {}).get("eventType") if resp else None,
        }
        if not resp or resp.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
            result.passed = False
            step["error"] = "期望 TRANSCRIPT_ACK"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})

    msg = generate_message(cid, complete_seq, event_type="SESSION_COMPLETE", **meta_base)
    resp = await _send_and_recv(ws, msg)
    step = {
        "action": "send_complete",
        "seq": complete_seq,
        "resp_type": resp.get("metaData", {}).get("eventType") if resp else None,
    }
    if not resp or resp.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
        result.passed = False
        step["error"] = "期望 TRANSCRIPT_ACK"
    result.steps.append(step)
    await emit("scenario_step", {"scenario": result.name, "step": step})

    try:
        await asyncio.wait_for(ws.wait_closed(), timeout=5)
        close_code = ws.close_code
        step = {"action": "verify_close", "close_code": close_code}
        if close_code != 1000:
            result.passed = False
            step["error"] = f"期望 close_code=1000，实际={close_code}"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})
    except asyncio.TimeoutError:
        result.passed = False
        result.steps.append({"action": "verify_close", "error": "等待关闭超时"})


async def _session_ongoing_only(
    ws: Any,
    cid: str,
    meta_base: dict[str, Any],
    n_messages: int,
    result: ScenarioResult,
    emit: EventCallback,
) -> None:
    """按 N-01 仅发送 SESSION_ONGOING，并校验每条都返回 TRANSCRIPT_ACK。"""
    total = max(1, n_messages)
    for seq in range(total):
        msg = generate_message(cid, seq, event_type="SESSION_ONGOING", **meta_base)
        resp = await _send_and_recv(ws, msg)
        step = {
            "action": "send_ongoing",
            "seq": seq,
            "resp_type": resp.get("metaData", {}).get("eventType") if resp else None,
        }
        if not resp or resp.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
            result.passed = False
            step["error"] = "期望 TRANSCRIPT_ACK"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})


async def scenario_a_normal_flow(
    ws_url: str, emit: EventCallback, n_messages: int = 5,
) -> ScenarioResult:
    """N-01：会话中正常处理，仅发送 ``SESSION_ONGOING``。"""
    result = ScenarioResult(name="N-01", passed=True)
    cid = f"mock-N01-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        await _session_ongoing_only(
            ws, cid, meta_base, n_messages, result, emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_b_idempotent(
    ws_url: str,
    emit: EventCallback,
    n_messages: int = 5,
) -> ScenarioResult:
    """N-02：幂等重放，对每个 ``seq ∈ [0, n_messages)`` 先发 ``SESSION_ONGOING`` 再重放同一帧，两次都应 ACK。"""
    result = ScenarioResult(name="N-02", passed=True)
    cid = f"mock-N02-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        for seq in range(max(1, n_messages)):
            msg = generate_message(cid, seq, event_type="SESSION_ONGOING", **meta_base)
            resp1 = await _send_and_recv(ws, msg)
            step1 = {
                "action": "send_first",
                "seq": seq,
                "resp_type": resp1.get("metaData", {}).get("eventType") if resp1 else None,
            }
            if not resp1 or resp1.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
                result.passed = False
                step1["error"] = "首次发送期望 ACK"
            result.steps.append(step1)
            await emit("scenario_step", {"scenario": result.name, "step": step1})

            resp2 = await _send_and_recv(ws, msg)
            step2 = {
                "action": "send_duplicate",
                "seq": seq,
                "resp_type": resp2.get("metaData", {}).get("eventType") if resp2 else None,
            }
            if not resp2 or resp2.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
                result.passed = False
                step2["error"] = "重放期望 ACK（幂等）"
            result.steps.append(step2)
            await emit("scenario_step", {"scenario": result.name, "step": step2})
    finally:
        await ws.close()

    return result


async def scenario_c_out_of_order(
    ws_url: str,
    emit: EventCallback,
    n_messages: int = 5,
) -> ScenarioResult:
    """E-09：序列号乱序，seq 0 → 跳到 seq ``jump``（``jump=max(2,n_messages)``）→ 期望 E1006 + 断连 1008。"""
    result = ScenarioResult(name="E-09", passed=True)
    jump_seq = max(2, n_messages)
    cid = f"mock-E09-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        # seq 0 正常
        msg0 = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        resp0 = await _send_and_recv(ws, msg0)
        step0 = {"action": "send_seq0", "resp_type": resp0.get("metaData", {}).get("eventType") if resp0 else None}
        result.steps.append(step0)
        await emit("scenario_step", {"scenario": result.name, "step": step0})

        # 乱序：跳过中间 seq
        msg5 = generate_message(cid, jump_seq, event_type="SESSION_ONGOING", **meta_base)
        resp5 = await _send_and_recv(ws, msg5)
        step5 = {
            "action": f"send_seq{jump_seq}_ooo",
            "resp_type": resp5.get("metaData", {}).get("eventType") if resp5 else None,
        }
        if resp5 and resp5.get("metaData", {}).get("eventType") == "ERROR":
            err_code = resp5.get("error", {}).get("code")
            step5["error_code"] = err_code
            if err_code != "E1006":
                result.passed = False
                step5["error"] = f"期望 E1006，实际={err_code}"
        else:
            result.passed = False
            step5["error"] = "期望 ERROR 帧"
        result.steps.append(step5)
        await emit("scenario_step", {"scenario": result.name, "step": step5})

        # 验证关闭码
        try:
            await asyncio.wait_for(ws.wait_closed(), timeout=5)
            close_code = ws.close_code
            step_c = {"action": "verify_close", "close_code": close_code}
            if close_code != 1008:
                result.passed = False
                step_c["error"] = f"期望 close_code=1008，实际={close_code}"
            result.steps.append(step_c)
            await emit("scenario_step", {"scenario": result.name, "step": step_c})
        except asyncio.TimeoutError:
            result.passed = False
            result.steps.append({"action": "verify_close", "error": "等待关闭超时"})
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_d1_invalid_json(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-04：非法 JSON → E1001 + 断连 1007。"""
    result = ScenarioResult(name="E-04", passed=True)
    cid = f"mock-E04-{_random_hex(6)}"
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        resp = await _send_and_recv(ws, "this is not valid json!!!")
        step = {"action": "send_bad_json", "resp_type": resp.get("metaData", {}).get("eventType") if resp else None}
        if resp and resp.get("error", {}).get("code") == "E1001":
            step["error_code"] = "E1001"
        else:
            result.passed = False
            step["error"] = "期望 E1001"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})

        try:
            await asyncio.wait_for(ws.wait_closed(), timeout=5)
            close_code = ws.close_code
            step_c = {"action": "verify_close", "close_code": close_code}
            if close_code != 1007:
                result.passed = False
                step_c["error"] = f"期望 close_code=1007，实际={close_code}"
            result.steps.append(step_c)
            await emit("scenario_step", {"scenario": result.name, "step": step_c})
        except asyncio.TimeoutError:
            result.passed = False
            result.steps.append({"action": "verify_close", "error": "等待关闭超时"})
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e01_missing_query_conversation_id(
    ws_url: str,
    emit: EventCallback,
) -> ScenarioResult:
    """E-01：握手时缺少 query conversationId，期望 HTTP 400 / E1003。"""
    result = ScenarioResult(name="E-01", passed=True)

    try:
        ws = await _open_ws(ws_url, None, retries=1)
    except Exception as e:
        detail, srv_resp = _format_ws_connect_error(e)
        status_code = getattr(getattr(e, "response", None), "status_code", None) or getattr(
            getattr(e, "response", None), "status", None
        )
        step = {
            "action": "connect_without_query",
            "resp_type": f"HTTP {status_code}" if status_code is not None else "HTTP ?",
        }
        if srv_resp:
            step["error_code"] = (srv_resp.get("error") or {}).get("code")
        expected_code = "E1003"
        if status_code != 400 or step.get("error_code") != expected_code:
            result.passed = False
            step["error"] = f"期望 HTTP 400 / {expected_code}，实际：{detail}"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})
        return result

    result.passed = False
    result.steps.append({"action": "connect_without_query", "error": "握手意外成功，应返回 HTTP 400 / E1003"})
    await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
    try:
        await ws.close()
    except Exception:
        pass
    return result


async def scenario_d2_schema_error(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-06：缺少必填字段 → ERROR + 断连 1008。"""
    result = ScenarioResult(name="E-06", passed=True)
    cid = f"mock-E06-{_random_hex(6)}"
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = {"metaData": {"eventType": "SESSION_ONGOING"}, "payload": {}}
        resp = await _send_and_recv(ws, bad_msg)
        step = {"action": "send_bad_schema", "resp_type": resp.get("metaData", {}).get("eventType") if resp else None}
        if resp and resp.get("metaData", {}).get("eventType") == "ERROR":
            step["error_code"] = resp.get("error", {}).get("code")
        else:
            result.passed = False
            step["error"] = "期望 ERROR 帧"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})

        try:
            await asyncio.wait_for(ws.wait_closed(), timeout=5)
            close_code = ws.close_code
            step_c = {"action": "verify_close", "close_code": close_code}
            if close_code != 1008:
                result.passed = False
                step_c["error"] = f"期望 close_code=1008，实际={close_code}"
            result.steps.append(step_c)
            await emit("scenario_step", {"scenario": result.name, "step": step_c})
        except asyncio.TimeoutError:
            result.passed = False
            result.steps.append({"action": "verify_close", "error": "等待关闭超时"})
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e05_invalid_enum(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-05：枚举值非法，期望 E1002 + 断连 1008。"""
    result = ScenarioResult(name="E-05", passed=True)
    cid = f"mock-E05-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["metaData"]["eventType"] = "INVALID"
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_bad_enum",
            expected_code="E1002",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e07_wrong_type(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-07：字段类型不符，期望 E1004 + 断连 1008。"""
    result = ScenarioResult(name="E-07", passed=True)
    cid = f"mock-E07-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["metaData"]["conversationId"] = 123
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_wrong_type",
            expected_code="E1004",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e08_invalid_timestamp(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-08：时间格式无效，期望 E1005 + 断连 1008。"""
    result = ScenarioResult(name="E-08", passed=True)
    cid = f"mock-E08-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["payload"]["createdAtTimeStamp"] = "2025-03-21T18:32:20.000+08:00"
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_bad_timestamp",
            expected_code="E1005",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e14_conversation_id_mismatch(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-14：query/body conversationId 不一致，期望 E1009 + 断连 1008。"""
    result = ScenarioResult(name="E-14", passed=True)
    cid = f"mock-E14-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["metaData"]["conversationId"] = f"{cid}-other"
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_conversation_id_mismatch",
            expected_code="E1009",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e15_business_rule_violation(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-15：业务规则校验失败，期望 E1009 + 断连 1008。"""
    result = ScenarioResult(name="E-15", passed=True)
    cid = f"mock-E15-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["payload"]["isFinal"] = False
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_business_rule_violation",
            expected_code="E1009",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_g_session_complete(
    ws_url: str,
    emit: EventCallback,
    n_messages: int = 5,
) -> ScenarioResult:
    """N-03：会话结束场景，``n_messages`` 会话总数含 COMPLETE，最终 ACK + 断连 1000。"""
    result = ScenarioResult(name="N-03", passed=True)
    cid = f"mock-N03-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"agent_id": f"AGT-{_random_hex()}", "staff_id": f"STF-{_random_hex()}", "customer_id": f"CST-{_random_hex()}", "start_ts": start_ts}
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        await _session_ongoing_plus_complete_and_close(
            ws, cid, meta_base, n_messages, result, emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


SCENARIOS: dict[str, Any] = {
    "N-01": scenario_a_normal_flow,
    "N-02": scenario_b_idempotent,
    "N-03": scenario_g_session_complete,
    "E-01": scenario_e01_missing_query_conversation_id,
    "E-04": scenario_d1_invalid_json,
    "E-05": scenario_e05_invalid_enum,
    "E-06": scenario_d2_schema_error,
    "E-07": scenario_e07_wrong_type,
    "E-08": scenario_e08_invalid_timestamp,
    "E-09": scenario_c_out_of_order,
    "E-14": scenario_e14_conversation_id_mismatch,
    "E-15": scenario_e15_business_rule_violation,
}


# ---------------------------------------------------------------------------
# 并发压测驱动
# ---------------------------------------------------------------------------

async def _load_single_conversation(
    ws_url: str,
    stats: Stats,
    emit: EventCallback,
    n_messages: int,
    interval_ms: float,
    *,
    sse_register_cid: bool = False,
    stop_event: asyncio.Event | None = None,
) -> None:
    """单条对话压测：共 ``n_messages`` 条业务消息（含 1 条 SESSION_COMPLETE）。

    ``sse_register_cid``：是否向 UI 发 ``conversation_registered``。高压测会海量会话，默认关闭以免塞爆 SSE。
    """
    ongoing_count, complete_seq = _session_message_split(n_messages)
    cid = f"load-{_random_hex(8)}"
    if sse_register_cid:
        await emit("conversation_registered", {"conversation_id": cid})
    start_ts = _utc_now_iso()
    meta_base = {
        "agent_id": f"AGT-{_random_hex()}",
        "staff_id": f"STF-{_random_hex()}",
        "customer_id": f"CST-{_random_hex()}",
        "start_ts": start_ts,
    }
    interval = interval_ms / 1000.0

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        detail, srv_resp = _format_ws_connect_error(e)
        stats.record_load_error(stage="connect", cid=cid, detail=detail, server_resp=srv_resp)
        await emit("load_error", {"cid": cid, "stage": "connect", "error": detail, "server_resp": srv_resp})
        return

    stats.active_connections += 1
    try:
        for seq in range(ongoing_count):
            if stop_event and stop_event.is_set():
                break
            msg = generate_message(cid, seq, event_type="SESSION_ONGOING", **meta_base)
            t0 = time.monotonic()
            resp = await _send_and_recv(ws, msg)
            latency = time.monotonic() - t0
            stats.sent += 1
            stats.latencies.append(latency)
            if resp and resp.get("metaData", {}).get("eventType") == "TRANSCRIPT_ACK":
                stats.ack += 1
                srv_ms = (resp.get("payload") or {}).get("serverProcessingMs")
                if isinstance(srv_ms, (int, float)):
                    stats.server_latencies.append(float(srv_ms) / 1000.0)
            else:
                et = resp.get("metaData", {}).get("eventType") if resp else None
                if resp is None:
                    detail = "无响应(10s 超时或连接已关闭)"
                elif et == "ERROR":
                    detail = f"服务端错误: {_format_server_error(resp)}"
                else:
                    detail = f"期望 TRANSCRIPT_ACK，实际 eventType={et!r}"
                stats.record_load_error(
                    stage="ongoing",
                    cid=cid,
                    seq=seq,
                    detail=detail,
                    event_type=et,
                    server_resp=resp,
                )
                await emit(
                    "load_error",
                    {"cid": cid, "stage": "ongoing", "seq": seq, "error": detail, "server_resp": resp},
                )
            if interval > 0:
                await asyncio.sleep(interval)

        if stop_event and stop_event.is_set():
            return

        # SESSION_COMPLETE
        msg = generate_message(cid, complete_seq, event_type="SESSION_COMPLETE", **meta_base)
        t0 = time.monotonic()
        resp = await _send_and_recv(ws, msg)
        latency = time.monotonic() - t0
        stats.sent += 1
        stats.latencies.append(latency)
        if resp and resp.get("metaData", {}).get("eventType") == "TRANSCRIPT_ACK":
            stats.ack += 1
            srv_ms = (resp.get("payload") or {}).get("serverProcessingMs")
            if isinstance(srv_ms, (int, float)):
                stats.server_latencies.append(float(srv_ms) / 1000.0)
        else:
            et = resp.get("metaData", {}).get("eventType") if resp else None
            if resp is None:
                detail = "无响应(10s 超时或连接已关闭)"
            elif et == "ERROR":
                detail = f"服务端错误: {_format_server_error(resp)}"
            else:
                detail = f"期望 TRANSCRIPT_ACK，实际 eventType={et!r}"
            stats.record_load_error(
                stage="complete",
                cid=cid,
                seq=complete_seq,
                detail=detail,
                event_type=et,
                server_resp=resp,
            )
            await emit(
                "load_error",
                {
                    "cid": cid,
                    "stage": "complete",
                    "seq": complete_seq,
                    "error": detail,
                    "server_resp": resp,
                },
            )
    except Exception as e:
        stats.record_load_error(stage="exception", cid=cid, detail=repr(e))
        await emit("load_error", {"cid": cid, "stage": "exception", "error": str(e)})
    finally:
        stats.active_connections -= 1
        try:
            await ws.close()
        except Exception:
            pass


async def run_load_test(
    ws_url: str,
    stats: Stats,
    emit: EventCallback,
    concurrency: int = 10,
    messages_per_conv: int = 10,
    interval_ms: float = 20,
    ramp_up_ms: float = 0,
    stop_event: asyncio.Event | None = None,
) -> None:
    """并发压测：一轮 = **并发数** 路相互独立的会话（每路一条 WebSocket）。

    创建 ``concurrency`` 个 asyncio 任务；信号量同为 ``concurrency``，
    故这一批会话会**同时建连、同时在线**，直到各自发完消息后关闭。

    ``ramp_up_ms``: 若 > 0，在这段时间内均匀启动全部连接（线性爬坡），
    避免瞬间洪峰把服务端 TCP backlog 打满。0 = 不限速（所有连接同时发起）。
    """
    err_sse_cap = min(2000, max(100, concurrency + 200))
    err_sse_left = err_sse_cap

    async def emit_throttled(event_type: str, data: dict[str, Any]) -> None:
        nonlocal err_sse_left
        if event_type == "load_error":
            if err_sse_left <= 0:
                return
            err_sse_left -= 1
        await emit(event_type, data)

    await emit_throttled("stats", stats.snapshot())
    sem = asyncio.Semaphore(concurrency)
    tasks: list[asyncio.Task] = []

    ramp_interval = (ramp_up_ms / 1000.0 / concurrency) if ramp_up_ms > 0 and concurrency > 1 else 0

    async def _guarded(idx: int) -> None:
        if ramp_interval > 0:
            await asyncio.sleep(ramp_interval * idx)
        async with sem:
            if stop_event and stop_event.is_set():
                return
            await _load_single_conversation(
                ws_url,
                stats,
                emit_throttled,
                messages_per_conv,
                interval_ms,
                sse_register_cid=False,
                stop_event=stop_event,
            )

    batch_size = concurrency
    for i in range(batch_size):
        if stop_event and stop_event.is_set():
            break
        tasks.append(asyncio.create_task(_guarded(i)))

    await asyncio.gather(*tasks, return_exceptions=True)
    load_cancelled = bool(stop_event and stop_event.is_set())
    stats.finish()
    await emit_throttled("load_done", {**stats.snapshot(), "load_cancelled": load_cancelled})
