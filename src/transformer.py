from config import Config
from typing import List, Any, Dict
from .utils import DateTools

class Transformer:
    """Normalize dates; standardize role/status casing; ensure schema consistency."""
    @staticmethod
    def normalize(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for r in records:
            firm = {
                "firm_id": str(r.get("firm_id", "") or ""),
                "firm_name": str(r.get("firm_name", "") or "").strip(),
                "firm_url": str(r.get("firm_url", "") or "").strip(),
                "licence_start": DateTools.parse_date(r.get("licence_start", "")),
                "licence_end": DateTools.parse_date(r.get("licence_end", "")),
                "last_updated": DateTools.parse_date(r.get("last_updated", "")) or Config.RUN_DATE,
                "licensees": []
            }
            lics = r.get("licensees", []) or []
            for l in lics:
                firm["licensees"].append({
                    "licensee_id": str(l.get("licensee_id", "") or ""),
                    "name": str(l.get("name", "") or "").strip(),
                    "role": str(l.get("role", "") or "").strip().title(),
                    "status": str(l.get("status", "") or "").strip().title(),
                    "licence_start": DateTools.parse_date(l.get("licence_start", "")),
                    "licence_end": DateTools.parse_date(l.get("licence_end", "")),
                    "history": l.get("history", []) or [],
                    "person_url": str(l.get("person_url", "") or "")
                })
            firm["current_licensees_count"] = int(r.get("current_licensees_count", len(firm["licensees"])) or 0)
            out.append(firm)
        return out