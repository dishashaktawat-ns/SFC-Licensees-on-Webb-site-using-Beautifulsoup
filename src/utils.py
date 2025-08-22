import re
import time
from typing import  Optional
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from config import Config


class HttpClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = self._make_session()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _make_session(self) -> requests.Session:
        s = requests.Session()
        retry = Retry(
            total=self.cfg.MAX_RETRIES,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(self.cfg.MAX_RETRIES):
            try:
                r = self.session.get(url, timeout=self.cfg.REQ_TIMEOUT, verify=self.cfg.VERIFY_SSL)
                r.raise_for_status()
                return r
            except Exception as e:
                if attempt == self.cfg.MAX_RETRIES - 1:
                    print(f"[HTTP ERR] {url}: {e}")
                    return None
                time.sleep(self.cfg.BACKOFF_SECONDS * (attempt + 1))


class DateTools:
    @staticmethod
    def parse_date(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return ""
        # Try common formats observed on Webb-site
        for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        m = re.search(r"\d{4}-\d{2}-\d{2}", s)
        if m:
            return m.group(0)
        return ""

    @staticmethod
    def is_active(until_yyyy_mm_dd: str) -> bool:
        if not until_yyyy_mm_dd:
            return True
        try:
            d = datetime.strptime(until_yyyy_mm_dd, "%Y-%m-%d")
            return d >= datetime.now()
        except ValueError:
            return False