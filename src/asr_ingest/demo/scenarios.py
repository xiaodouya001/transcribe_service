"""Scenario config for Streamlit Demo - path and shuffle per scenario."""

from pathlib import Path

# __file__ = demo/scenarios.py, .parent = demo/
DEMO_ROOT = Path(__file__).resolve().parent
SCENARIOS_ROOT = DEMO_ROOT / "scenarios"

SCENARIOS = {
    "single_response_multi_transcriptions": {
        "path": SCENARIOS_ROOT / "single_response_multi_transcriptions" / "transcripts.json",
        "shuffle": False,
        "label": "单响应多转录",
    },
    "multi_response_single_transcriptions": {
        "path": SCENARIOS_ROOT / "multi_response_single_transcriptions",
        "shuffle": False,
        "label": "多响应每响应单转录",
    },
    "shuffle_multi_response_single_transcriptions": {
        "path": SCENARIOS_ROOT / "shuffle_multi_response_single_transcriptions",
        "shuffle": True,
        "label": "乱序多响应每响应单转录",
    },
    "shuffle_multi_response_multi_transcriptions": {
        "path": SCENARIOS_ROOT / "shuffle_multi_response_multi_transcriptions",
        "shuffle": True,
        "label": "乱序多响应每响应多转录",
    },
}
