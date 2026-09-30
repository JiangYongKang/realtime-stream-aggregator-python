from __future__ import annotations

import pprint
from typing import Any


def log_section(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def log_input(label: str, payload: Any) -> None:
    print(f"[INPUT] {label}: {payload}")


def log_window_decision(result: dict[str, Any]) -> None:
    print(
        "[WINDOW] status={status} event={event_id} window_start={window_start_ms}"
        " watermark={watermark_ms} detail={detail}".format(**result)
    )


def log_state(label: str, state: Any) -> None:
    print(f"[STATE] {label}:")
    pprint.pp(state, sort_dicts=False)
