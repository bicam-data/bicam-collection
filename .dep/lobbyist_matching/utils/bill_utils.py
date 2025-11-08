import re


def year_to_congress(year: int) -> int:
    """Convert a calendar year to congress number.
    Congress starts in odd years: 107th: 2001-2002, 108th: 2003-2004, etc.
    """
    return ((year - 1789) // 2) + 1


def standardize_bill_type(
    chamber: str, res_type: str | None = None, leg_type: str | None = None
) -> str | None:
    """Standardize bill type based on chamber and resolution type.
    Returns: hr, hres, hconres, hjres, s, sres, sconres, sjres or None.
    """
    if not chamber:
        return None
    chamber = chamber.lower().replace("-", "").strip()
    res_type = res_type.lower().replace("-", "").strip() if res_type else None
    leg_type = leg_type.lower().replace("-", "").strip() if leg_type else None
    if chamber.startswith("h"):
        if not res_type:
            if not leg_type or leg_type.startswith("b"):
                return "hr"
            elif leg_type.startswith("r"):
                return "hres"
        elif res_type.startswith("c"):
            return "hconres"
        elif res_type.startswith("j"):
            return "hjres"
    elif chamber.startswith("s"):
        if not res_type:
            if not leg_type or leg_type.startswith("b"):
                return "s"
            elif leg_type.startswith("r"):
                return "sres"
        elif res_type.startswith("c"):
            return "sconres"
        elif res_type.startswith("j"):
            return "sjres"
    return None


def standardize_law_number(law_text: str) -> str:
    """Standardize law number format to 'PL{digits-hyphen}'."""
    nums = re.sub(r"[^\d-]", "", law_text or "")
    return f"PL{nums}" if nums else ""
