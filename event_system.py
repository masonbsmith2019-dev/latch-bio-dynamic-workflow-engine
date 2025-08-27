from __future__ import annotations
import json
import os
import time
import multiprocessing as mp
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Set, Tuple

class EventWriter:
    def __init__(self, out_path: str):
        self.out_path = out_path
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        open(self.out_path, "w").close()

    @staticmethod
    def _now_iso() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int((time.time()%1)*1000):03d}Z"

    #ev is the event we want to write
    def write(self, ev: Dict[str, Any]) -> None:
        # include timestamp of orchestration
        if "ts" not in ev:
            ev["ts"] = self._now_iso()
        # Convert to JSON-safe format
        json_safe_ev = self._to_jsonable(ev)
        with open(self.out_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(json_safe_ev, ensure_ascii=False) + "\n")

    @staticmethod
    def _to_jsonable(obj):
        """Recursively convert to JSON-safe types."""
        # dataclasses (including slots=True)
        if is_dataclass(obj):
            obj = asdict(obj)
        # dict
        if isinstance(obj, dict):
            return {k: EventWriter._to_jsonable(v) for k, v in obj.items()}
        # list/tuple
        if isinstance(obj, (list, tuple)):
            return [EventWriter._to_jsonable(x) for x in obj]
        # set -> sorted list (stable output is nice)
        if isinstance(obj, set):
            return sorted(EventWriter._to_jsonable(x) for x in obj)
        # primitives pass through
        if isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        # fallback: repr so we never crash logging
        return repr(obj)