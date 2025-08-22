from config import Config
from typing import List, Dict, Any
import os
import json
from datetime import datetime, timedelta


class SnapshotStore:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def write_snapshot(self, data: List[Dict[str, Any]]):
        path = os.path.join(self.cfg.SNAPSHOT_DIR, f"{self.cfg.RUN_DATE}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return path

    def prune(self):
        cutoff = datetime.now() - timedelta(days=self.cfg.SNAPSHOT_WINDOW_DAYS)
        for name in os.listdir(self.cfg.SNAPSHOT_DIR):
            if not name.endswith(".json"):
                continue
            stem = name.replace(".json", "")
            try:
                d = datetime.strptime(stem, "%Y-%m-%d")
            except Exception:
                continue
            if d < cutoff:
                os.remove(os.path.join(self.cfg.SNAPSHOT_DIR, name))