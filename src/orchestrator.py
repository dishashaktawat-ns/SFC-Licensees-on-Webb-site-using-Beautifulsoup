
import json
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pandas as pd
from config import Config
from .scraper_bsoup import ListPageParser, FirmDetailParser
from .transformer import Transformer
from .snapshot import SnapshotStore
from .utils import HttpClient
from .validator import Validator


class SFCPipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cfg.ensure_dirs()
        self.http = HttpClient(cfg)
        self.list_parser = ListPageParser(cfg)
        self.firm_parser = FirmDetailParser(self.http, cfg)
        self.transformer = Transformer()
        self.snapshot = SnapshotStore(cfg)

    def _early_filter(self, firms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cutoff = datetime.now() - timedelta(days=self.cfg.DAYS_FILTER)
        def keep(r: Dict[str, Any]) -> bool:
            s = r.get("licence_start_list", "")
            if not s:
                return True  # keep if missing; resolve on detail page
            try:
                return datetime.strptime(s, "%Y-%m-%d") >= cutoff
            except Exception:
                return True
        out = [f for f in firms if keep(f)]
        print(f"[FILTER] After {self.cfg.DAYS_FILTER}d window by list-page Licence start: {len(out)}")
        return out

    def ingest(self) -> List[Dict[str, Any]]:
        print("[INGEST] Fetching list page ...")
        resp = self.http.get(self.cfg.BASE_URL)
        if not resp:
            raise SystemExit("Unable to fetch list page.")

        firms = self.list_parser.parse(resp.text)
        print(f"[INGEST] Discovered firms on list page: {len(firms)}")

        firms = self._early_filter(firms)

        print("[INGEST] Fetching firm pages ...")
        results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.cfg.MAX_WORKERS) as ex:
            futures = [ex.submit(self.firm_parser.parse, f) for f in firms]
            for fut in as_completed(futures):
                rec = fut.result()
                if rec:
                    results.append(rec)

        with open(self.cfg.RAW_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"[INGEST] Raw saved -> {self.cfg.RAW_FILE} (firms: {len(results)})")
        return results

    def transform(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        print("[TRANSFORM] Normalizing records ...")
        norm = self.transformer.normalize(raw_records)
        with open(self.cfg.PROCESSED_FILE, "w", encoding="utf-8") as f:
            json.dump(norm, f, ensure_ascii=False, indent=2)
        print(f"[TRANSFORM] Processed saved -> {self.cfg.PROCESSED_FILE}")
        return norm

    def validate(self, records: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        print("[VALIDATE] Running validation and metrics ...")
        issues = Validator.validate(records)
        issues.to_csv(self.cfg.VALIDATION_FILE, index=False)
        metrics = Validator.metrics(records)
        metrics.to_csv(self.cfg.METRICS_FILE, index=False)
        print(f"[VALIDATE] Issues -> {self.cfg.VALIDATION_FILE} (rows: {len(issues)})")
        print(f"[VALIDATE] Metrics -> {self.cfg.METRICS_FILE}")
        return issues, metrics

    def snapshot_store(self, processed: List[Dict[str, Any]]):
        print("[SNAPSHOT] Writing snapshot & pruning old ones ...")
        self.snapshot.write_snapshot(processed)
        self.snapshot.prune()
        print("[SNAPSHOT] Done.")

    def run(self):
        raw = self.ingest()
        processed = self.transform(raw)
        self.validate(processed)
        self.snapshot_store(processed)
        print("[PIPELINE] Completed.")