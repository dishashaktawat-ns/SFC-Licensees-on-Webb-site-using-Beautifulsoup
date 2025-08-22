import os
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Config:
    BASE_URL: str = "https://webb-site.com/dbpub/SFClicount.asp"
    VERIFY_SSL: bool = False          # Set True if your machine trusts the cert; False avoids hostname mismatch errors
    MAX_WORKERS: int = 8              # Polite concurrency; reduce if server returns 500s
    REQ_TIMEOUT: int = 20
    MAX_RETRIES: int = 3
    BACKOFF_SECONDS: float = 2.0

    DAYS_FILTER: int = 365            # Early filter: only fetch firm detail if list-page Licence start within last N days
    FETCH_LICENSEE_HISTORY: bool = False  # Follow person link to parse "SFC licenses" history (heavier)
    SNAPSHOT_WINDOW_DAYS: int = 90

    RUN_DATE: str = datetime.now().strftime("%Y-%m-%d")

    # Folders
    RAW_DIR: str = "data/raw"
    PROCESSED_DIR: str = "data/processed"
    LOGS_DIR: str = "data/logs"
    SNAPSHOT_DIR: str = "data/snapshots"

    # Filenames (derived)
    @property
    def RAW_FILE(self) -> str:
        return os.path.join(self.RAW_DIR, f"firms_raw_{self.RUN_DATE}.json")

    @property
    def PROCESSED_FILE(self) -> str:
        return os.path.join(self.PROCESSED_DIR, f"firms_processed_{self.RUN_DATE}.json")

    @property
    def METRICS_FILE(self) -> str:
        return os.path.join(self.LOGS_DIR, f"metrics_{self.RUN_DATE}.csv")

    @property
    def VALIDATION_FILE(self) -> str:
        return os.path.join(self.LOGS_DIR, f"validation_{self.RUN_DATE}.csv")

    def ensure_dirs(self):
        for d in (self.RAW_DIR, self.PROCESSED_DIR, self.LOGS_DIR, self.SNAPSHOT_DIR):
            os.makedirs(d, exist_ok=True)