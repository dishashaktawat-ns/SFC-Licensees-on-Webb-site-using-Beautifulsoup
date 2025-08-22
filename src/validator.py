from typing import List, Dict, Any
import pandas as pd

class Validator:
    """Simple validation + metrics collection."""
    REQUIRED_FIRM_FIELDS = ["firm_name", "firm_url", "last_updated", "licensees"]
    REQUIRED_LICENSEE_FIELDS = ["name", "role", "licence_start"]  # minimal set

    @classmethod
    def validate(cls, records: List[Dict[str, Any]]) -> pd.DataFrame:
        issues = []
        for i, rec in enumerate(records):
            for f in cls.REQUIRED_FIRM_FIELDS:
                if f not in rec or rec[f] in (None, ""):
                    issues.append({"row": i, "level": "firm", "field": f, "issue": "missing"})
            if isinstance(rec.get("licensees"), list):
                for j, lic in enumerate(rec["licensees"]):
                    for f in cls.REQUIRED_LICENSEE_FIELDS:
                        if f not in lic or lic[f] in (None, ""):
                            issues.append({"row": i, "level": "licensee", "index": j, "field": f, "issue": "missing"})
        return pd.DataFrame(issues)

    @staticmethod
    def metrics(records: List[Dict[str, Any]]) -> pd.DataFrame:
        total_firms = len(records)
        missing_start = sum(1 for r in records if not r.get("licence_start"))
        missing_end = sum(1 for r in records if not r.get("licence_end"))
        empty_lics = sum(1 for r in records if not r.get("licensees"))
        return pd.DataFrame([{
            "total_firms": total_firms,
            "firms_missing_licence_start": missing_start,
            "firms_missing_licence_end": missing_end,
            "firms_with_no_licensees": empty_lics
        }])
