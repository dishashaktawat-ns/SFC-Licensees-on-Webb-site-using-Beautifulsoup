
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from config import Config
from .utils import DateTools, HttpClient





class ListPageParser:
    """
    Parses the list page:
    Columns resemble:
    Row | Name | (prev RO/Rep/Total) | (curr RO/Rep/Total) | Change | Rep% | End% | Licence start | Licence end
    We extract: firm_name, firm_url, licence_start (last-2 col), licence_end (last col).
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg

    @staticmethod
    def _find_table_by_headers(soup: BeautifulSoup, must_have: List[str]) -> Optional[BeautifulSoup]:
        for tbl in soup.find_all("table"):
            # find header row
            head_tr = None
            for tr in tbl.find_all("tr"):
                if tr.find("th"):
                    head_tr = tr
                    break
            if not head_tr:
                continue
            headers = [th.get_text(" ", strip=True).lower() for th in head_tr.find_all(["th", "td"])]
            joined = " ".join(headers)
            if all(tok in joined for tok in must_have):
                return tbl
        return None

    def parse(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        # Heuristic: table that mentions name + licence
        table = self._find_table_by_headers(
            soup, must_have=["name", "licence", "ro", "rep", "total"]
        ) or soup.find("table")

        firms: List[Dict[str, Any]] = []
        if not table:
            return firms

        rows = table.find_all("tr")
        if len(rows) <= 1:
            return firms

        for tr in rows[1:]:
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            name_td = tds[1]
            firm_name = name_td.get_text(strip=True)
            a = name_td.find("a")
            firm_url = urljoin(self.cfg.BASE_URL, a["href"]) if a and a.get("href") else ""

            lic_start = DateTools.parse_date(tds[-2].get_text(strip=True)) if len(tds) >= 2 else ""
            lic_end = DateTools.parse_date(tds[-1].get_text(strip=True)) if len(tds) >= 1 else ""

            if firm_name and firm_url:
                firms.append({
                    "firm_name": firm_name,
                    "firm_url": firm_url,
                    "licence_start_list": lic_start,
                    "licence_end_list": lic_end
                })
        return firms


class PersonHistoryParser:
    """
    Parses a person's page for 'SFC licenses' history:
    Headers: Organisation | Role | Activity | From | Until
    """
    def __init__(self, http: HttpClient):
        self.http = http

    @staticmethod
    def _find_table_by_headers(soup: BeautifulSoup, must_have: List[str]) -> Optional[BeautifulSoup]:
        for tbl in soup.find_all("table"):
            head_tr = None
            for tr in tbl.find_all("tr"):
                if tr.find("th"):
                    head_tr = tr
                    break
            if not head_tr:
                continue
            headers = [th.get_text(" ", strip=True).lower() for th in head_tr.find_all(["th", "td"])]
            joined = " ".join(headers)
            if all(tok in joined for tok in must_have):
                return tbl
        return None

    def parse_history(self, person_url: str) -> List[Dict[str, str]]:
        r = self.http.get(person_url)
        if not r:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        tbl = self._find_table_by_headers(soup, must_have=["organisation", "role", "activity", "from", "until"])
        if not tbl:
            return []

        # identify header row
        head_tr = None
        for tr in tbl.find_all("tr"):
            if tr.find("th"):
                head_tr = tr
                break
        headers = [th.get_text(" ", strip=True).lower() for th in head_tr.find_all(["th", "td"])] if head_tr else []

        def get_idx(names: List[str]) -> Optional[int]:
            for i, h in enumerate(headers):
                if any(n in h for n in names):
                    return i
            return None

        idx_org = get_idx(["organisation", "organization"])
        idx_role = get_idx(["role"])
        idx_act = get_idx(["activity"])
        idx_from = get_idx(["from"])
        idx_until = get_idx(["until"])

        out = []
        for tr in tbl.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if not tds:
                continue
            org = tds[idx_org].get_text(strip=True) if idx_org is not None and idx_org < len(tds) else ""
            role = tds[idx_role].get_text(strip=True) if idx_role is not None and idx_role < len(tds) else ""
            act = tds[idx_act].get_text(strip=True) if idx_act is not None and idx_act < len(tds) else ""
            frm = DateTools.parse_date(tds[idx_from].get_text(strip=True)) if idx_from is not None and idx_from < len(tds) else ""
            until = DateTools.parse_date(tds[idx_until].get_text(strip=True)) if idx_until is not None and idx_until < len(tds) else ""
            out.append({
                "organisation": org,
                "role": role,
                "activity": act,
                "from": frm,
                "until": until
            })
        return out


class FirmDetailParser:
    """
    Parses the firm detail page:
    Headers: Name | (Age ...) | (⚥) | SFC ID | Role | From | Until
    Derives status from Until.
    """
    def __init__(self, http: HttpClient, cfg: Config):
        self.http = http
        self.cfg = cfg
        self.person_parser = PersonHistoryParser(http)

    @staticmethod
    def _find_table_by_headers(soup: BeautifulSoup, must_have: List[str]) -> Optional[BeautifulSoup]:
        for tbl in soup.find_all("table"):
            head_tr = None
            for tr in tbl.find_all("tr"):
                if tr.find("th"):
                    head_tr = tr
                    break
            if not head_tr:
                continue
            headers = [th.get_text(" ", strip=True).lower() for th in head_tr.find_all(["th", "td"])]
            joined = " ".join(headers)
            if all(tok in joined for tok in must_have):
                return tbl
        return None

    def parse(self, firm_stub: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        r = self.http.get(firm_stub["firm_url"])
        if not r:
            return None
        soup = BeautifulSoup(r.text, "html.parser")

        # Find licensees table by header names
        lic_tbl = self._find_table_by_headers(soup, must_have=["name", "sfc id", "role", "from", "until"])
        licensees: List[Dict[str, Any]] = []

        if lic_tbl:
            # header names
            head_tr = None
            for tr in lic_tbl.find_all("tr"):
                if tr.find("th"):
                    head_tr = tr
                    break
            headers = [th.get_text(" ", strip=True).lower() for th in head_tr.find_all(["th", "td"])] if head_tr else []

            def idx(names: List[str]) -> Optional[int]:
                for i, h in enumerate(headers):
                    if any(n in h for n in names):
                        return i
                return None

            i_name = idx(["name"])
            i_sfc = idx(["sfc id", "sfcid", "id"])
            i_role = idx(["role"])
            i_from = idx(["from"])
            i_until = idx(["until"])

            for tr in lic_tbl.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if not tds or None in (i_name, i_role, i_from, i_until):
                    continue

                name_td = tds[i_name] if i_name < len(tds) else None
                name = name_td.get_text(strip=True) if name_td else ""

                person_url = ""
                if name_td:
                    a = name_td.find("a")
                    if a and a.get("href"):
                        person_url = urljoin(firm_stub["firm_url"], a["href"])

                sfc_id = tds[i_sfc].get_text(strip=True) if (i_sfc is not None and i_sfc < len(tds)) else ""
                role = tds[i_role].get_text(strip=True) if i_role < len(tds) else ""
                start = DateTools.parse_date(tds[i_from].get_text(strip=True)) if i_from < len(tds) else ""
                until = DateTools.parse_date(tds[i_until].get_text(strip=True)) if i_until < len(tds) else ""
                status = "Active" if DateTools.is_active(until) else "Inactive"

                history = []
                if self.cfg.FETCH_LICENSEE_HISTORY and person_url:
                    history = self.person_parser.parse_history(person_url)

                licensees.append({
                    "licensee_id": sfc_id,
                    "name": name,
                    "role": role,
                    "status": status,
                    "licence_start": start,
                    "licence_end": until,
                    "history": history,
                    "person_url": person_url
                })

        # Attempt to read firm-level Licence start/end labels on page; fallback to list-page values
        def label_value(label_text: str) -> str:
            node = soup.find(string=re.compile(fr"^{re.escape(label_text)}$", re.I))
            if not node:
                return ""
            nxt = node.find_next()
            return nxt.get_text(strip=True) if nxt else ""

        firm_lic_start = DateTools.parse_date(label_value("Licence start")) or firm_stub.get("licence_start_list", "")
        firm_lic_end = DateTools.parse_date(label_value("Licence end")) or firm_stub.get("licence_end_list", "")

        return {
            "firm_id": "",
            "firm_name": firm_stub["firm_name"],
            "firm_url": firm_stub["firm_url"],
            "licence_start": firm_lic_start,
            "licence_end": firm_lic_end,
            "last_updated": Config.RUN_DATE,
            "current_licensees_count": len(licensees),
            "licensees": licensees
        }