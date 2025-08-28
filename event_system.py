from __future__ import annotations
import json
import os
import time
import multiprocessing as mp
from dataclasses import asdict, is_dataclass, dataclass
from typing import Any, Dict, Literal
from pathlib import Path
from datetime import datetime, timezone
from promise import PlanDiff
from uuid import UUID
from enum import Enum

#this was causing an error for some reason
# @dataclass(frozen=True, slots=True)
# class PlanDiffEvent:
#     type: Literal["plandiff"] = "plandiff"
#     planDiff: PlanDiff

# @dataclass(frozen=True, slots=True)
# class LogEvent:
#     type: Literal["log"] = "log"
#     msg: str

# @dataclass(frozen=True, slots=True)
# class EmitEvent:
#     type: Literal["emit"] = "emit"
#     planDiff: PlanDiff

#PlanDiffEvent needs PlanDiff
#other events: Log, Emit

class EventWriter:
    def __init__(self, out_path: Path):
        self.out_path = out_path
        #os.makedirs(os.path.dirname(out_path), exist_ok=True)
        #open(self.out_path, "w").close()
        self.out_path.parent.mkdir(exist_ok=True, parents=True)

    #ev is the event we want to write
    def write(self, ev: Dict[str, Any]) -> None:
        # include timestamp of orchestration
        if "ts" not in ev:
            ev["ts"] = datetime.now(timezone.utc).isoformat()
        # Convert to JSON-safe format
        json_safe_ev = self._to_jsonable(ev)
        with self.out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(json_safe_ev, ensure_ascii=False) + "\n")

    @staticmethod
    def _to_jsonable(obj):
        # Dataclasses (including slots=True) -> dict, then recurse
        if is_dataclass(obj):
            return EventWriter._to_jsonable(asdict(obj))
        # UUID -> string form
        if isinstance(obj, UUID):
            return str(obj)
        # Enum -> its value
        if isinstance(obj, Enum):
            return obj.value
        # pathlib.Path -> string
        if isinstance(obj, Path):
            return str(obj)
        # datetime -> ISO8601
        if isinstance(obj, datetime):
            return obj.isoformat()
        # dict -> recurse
        if isinstance(obj, dict):
            return {k: EventWriter._to_jsonable(v) for k, v in obj.items()}
        # list/tuple -> list recurse
        if isinstance(obj, (list, tuple)):
            return [EventWriter._to_jsonable(x) for x in obj]
        # set -> sorted list recurse (stable output)
        if isinstance(obj, set):
            return sorted(EventWriter._to_jsonable(x) for x in obj)
        # primitives
        if isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        # fallback: repr so we never crash logging
        return repr(obj)