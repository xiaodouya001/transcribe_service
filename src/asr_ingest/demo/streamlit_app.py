"""Streamlit E2E Demo - visualize pipeline, dedup, and Kafka output."""

import asyncio
import json
import sys
from pathlib import Path

# Ensure project root in path
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import streamlit as st

from asr_ingest.demo.run_e2e_instrumented import run_instrumented, InstrumentedResult
from asr_ingest.demo.scenarios import SCENARIOS

st.set_page_config(page_title="ASR Ingest E2E Demo", layout="wide")
st.title("ASR Ingest E2E Demo")

# Sidebar config
with st.sidebar:
    scenario_id = st.selectbox(
        "场景",
        options=list(SCENARIOS.keys()),
        format_func=lambda k: SCENARIOS[k]["label"],
        index=0,
    )
    scenario = SCENARIOS[scenario_id]
    st.caption(f"输入源: {scenario['path']}")

    inject_duplicates = st.checkbox("注入重复以验证去重", value=False)
    st.caption("勾选后 Mock 会发送重复事件，Redis 视图将出现 filtered")

    transport = st.radio(
        "传输协议",
        options=["sse", "websocket"],
        format_func=lambda x: "SSE" if x == "sse" else "WebSocket",
        index=0,
    )
    st.caption("验证业务代码 SSE/WebSocket 两种接入方式的连通性")

# Session state for results and self-healing
if "last_result" not in st.session_state:
    st.session_state.last_result = None
if "run_error" not in st.session_state:
    st.session_state.run_error = None


def run_pipeline() -> InstrumentedResult:
    """Run instrumented pipeline (sync wrapper for asyncio)."""
    return asyncio.run(
        run_instrumented(
            inject_duplicates=inject_duplicates,
            shuffle_order=scenario["shuffle"],
            transcripts_path=scenario["path"],
            mode=transport,
        )
    )


if st.button("运行 E2E Demo", type="primary"):
    with st.spinner("运行中..."):
        result = run_pipeline()
        st.session_state.last_result = result
        st.session_state.run_error = result.error

# Show error and retry hint
if st.session_state.run_error:
    st.error(f"Pipeline 失败: {st.session_state.run_error}")
    st.info("请检查 Mock 端口 8765 是否被占用，或点击上方按钮重试。")
    st.session_state.run_error = None

result = st.session_state.last_result
if not result or result.error:
    st.info("点击「运行 E2E Demo」开始。")
    st.stop()

collector = result.collector

# Stats
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("总事件数", len(collector.redis_events))
with col2:
    st.metric("去重通过", collector.pass_count)
with col3:
    st.metric("去重过滤", collector.filtered_count)
with col4:
    st.metric("Kafka 发送", len(collector.kafka_events))

# Pipeline diagram (SVG - no external deps, always renders)
with st.expander("Pipeline 流程图"):
    svg = """
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 420 120" width="100%" height="120">
  <defs>
    <marker id="arrow" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
      <path d="M0,0 L10,4 L0,8 Z" fill="#666"/>
    </marker>
  </defs>
  <rect x="10" y="20" width="100" height="40" rx="4" fill="#e0e0e0" stroke="#999"/>
  <text x="60" y="45" text-anchor="middle" font-size="12">SSE/WebSocket</text>
  <line x1="110" y1="40" x2="150" y2="40" stroke="#666" stroke-width="2" marker-end="url(#arrow)"/>
  <rect x="150" y="20" width="80" height="40" rx="4" fill="#e0e0e0" stroke="#999"/>
  <text x="190" y="45" text-anchor="middle" font-size="12">Dedup</text>
  <line x1="230" y1="40" x2="270" y2="40" stroke="#666" stroke-width="2" marker-end="url(#arrow)"/>
  <text x="250" y="35" text-anchor="middle" font-size="10" fill="#666">pass</text>
  <rect x="270" y="20" width="100" height="40" rx="4" fill="#c8e6c9" stroke="#4caf50"/>
  <text x="320" y="45" text-anchor="middle" font-size="12">Kafka Producer</text>
  <line x1="190" y1="60" x2="190" y2="80" stroke="#666" stroke-width="2" marker-end="url(#arrow)"/>
  <text x="200" y="75" text-anchor="middle" font-size="10" fill="#666">filtered</text>
  <rect x="150" y="80" width="80" height="30" rx="4" fill="#ffcdd2" stroke="#f44336"/>
  <text x="190" y="98" text-anchor="middle" font-size="12">丢弃</text>
</svg>
"""
    st.markdown(svg, unsafe_allow_html=True)

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["输入源", "对话记录", "Redis 视图", "Kafka 视图"])

with tab1:
    path = scenario["path"]
    if path.is_file():
        if path.exists():
            st.caption(f"当前输入源: {path}")
            st.code(path.read_text(encoding="utf-8"), language="json")
        else:
            st.warning(f"未找到 {path}")
    else:
        if path.exists() and path.is_dir():
            files = sorted(path.glob("*.json"))
            st.caption(f"当前输入源: {path} (共 {len(files)} 个 JSON)")
            for f in files:
                with st.expander(f.name):
                    st.code(f.read_text(encoding="utf-8"), language="json")
        else:
            st.warning(f"未找到目录 {path}")

with tab2:
    events = sorted(
        collector.kafka_events,
        key=lambda e: (e.get("seq_no", 0), e.get("session_id", "")),
    )
    for ev in events:
        role = ev.get("role", "Agent")
        transcript = ev.get("transcript", "")
        with st.chat_message(role):
            st.write(transcript)
    if not events:
        st.caption("无对话记录")

with tab3:
    if collector.redis_events:
        display_rows = []
        for e in collector.redis_events:
            row = {
                "写入顺序": e.get("arrival_order", "-"),
                "key": e.get("key", ""),
                "value": e.get("value", ""),
                "result": e.get("result", ""),
                "session_id": e.get("session_id", ""),
                "transcription_seq_no": e.get("seq_no"),
            }
            display_rows.append(row)
        col_cfg = {
            "写入顺序": st.column_config.TextColumn("写入顺序"),
            "key": st.column_config.TextColumn("Key"),
            "value": st.column_config.TextColumn("Value"),
            "result": st.column_config.TextColumn("结果"),
            "session_id": st.column_config.TextColumn("Session ID"),
            "transcription_seq_no": st.column_config.NumberColumn("Transcription Seq No"),
        }
        st.dataframe(display_rows, column_config=col_cfg, use_container_width=True)
        st.caption("pass = 首次通过去重并写入 value=1，filtered = 重复 key 已存在无写入")
    else:
        st.caption("无 Redis 事件")

with tab4:
    if collector.kafka_events:
        display_rows = []
        for i, e in enumerate(collector.kafka_events):
            key = e.get("session_id", "")
            header = "-"
            raw_payload = e.get("raw_payload")
            cleaned = {
                k: v
                for k, v in e.items()
                if k not in ("raw_payload",)
            }
            payload = {"raw": raw_payload, "cleaned": cleaned}
            display_rows.append({
                "写入顺序": i + 1,
                "Key": key,
                "Header": header,
                "Payload": json.dumps(payload, ensure_ascii=False, indent=None),
            })
        st.dataframe(
            display_rows,
            column_config={
                "写入顺序": st.column_config.NumberColumn("Kafka 写入顺序"),
                "Key": st.column_config.TextColumn("Key"),
                "Header": st.column_config.TextColumn("Header"),
                "Payload": st.column_config.TextColumn("Payload"),
            },
            use_container_width=True,
        )
        st.caption("写入顺序 = 发往 Kafka 的先后（同 session 内按 seq_no 排序）")
    else:
        st.caption("无 Kafka 消息")
