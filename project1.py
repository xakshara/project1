#!/usr/bin/env python3
"""
project1.py — Part 1

Files expected in the SAME folder as this script:
  - air_quality.csv  columns: UHF Geo ID, Geo description, YYYY/MM/DD, pm2.5
  - uhf.csv          either:
        (A) with header, columns containing 'borough' and 'zip', or
        (B) no header, rows like: Borough,UHF42,101,10463,10471,...

Measurement tuple used throughout:
  (date_str, uhf_id:int, uhf_name:str, value:float)
"""

from pathlib import Path
from typing import Dict, List, Tuple
import csv

# ---------- Paths ----------
HERE = Path(__file__).resolve().parent
AIR_QUALITY_FILE = HERE / "air_quality.csv"
UHF_FILE = HERE / "uhf.csv"

Measurement = Tuple[str, int, str, float]


# ---------- Utilities ----------

def _to_int_safe(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

def _to_float_safe(x):
    try:
        return float(str(x).strip())
    except Exception:
        return None

def _norm_borough(name: str) -> str:
    return str(name).strip().title()

def _expand_uhf_code(uhf_code: str) -> List[int]:
    """
    Expand UHF34 concatenations like '105106107' into [105, 106, 107].
    Return [205] for a normal code '205'.
    """
    s = str(uhf_code).strip()
    if not s:
        return []
    if s.isdigit() and len(s) > 3 and len(s) % 3 == 0:
        return [int(s[i:i+3]) for i in range(0, len(s), 3)]
    v = _to_int_safe(s)
    return [v] if v is not None else []

def _sniff_delimiter(path: Path, default=","):
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            sample = f.read(4096)
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return default


# ---------- Loaders (Part 1a and 1b) ----------

def read_pollution():
    """
    Read air_quality.csv with strict column order:
      0: UHF Geo ID (int)
      1: Geo description (str)
      2: date 'YYYY/MM/DD' (str)
      3: pm2.5 (float)
    Returns:
      by_uhf: Dict[int, List[Measurement]]
      by_date: Dict[str, List[Measurement]]
    """
    by_uhf: Dict[int, List[Measurement]] = {}
    by_date: Dict[str, List[Measurement]] = {}

    if not AIR_QUALITY_FILE.exists():
        print(f"Could not find {AIR_QUALITY_FILE}")
        return by_uhf, by_date

    delim = _sniff_delimiter(AIR_QUALITY_FILE, ",")
    with AIR_QUALITY_FILE.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=delim)
        rows = list(reader)

    if not rows:
        return by_uhf, by_date

    # Treat first row as header only if first cell is not an integer
    start_idx = 1 if rows and rows[0] and not str(rows[0][0]).strip().isdigit() else 0

    for row in rows[start_idx:]:
        if len(row) < 4:
            continue
        uhf_id = _to_int_safe(row[0])
        uhf_name = str(row[1]).strip()
        date_str = str(row[2]).strip()
        value = _to_float_safe(row[3])
        if uhf_id is None or not uhf_name or not date_str or value is None:
            continue
        m: Measurement = (date_str, uhf_id, uhf_name, value)
        by_uhf.setdefault(uhf_id, []).append(m)
        by_date.setdefault(date_str, []).append(m)

    return by_uhf, by_date


def read_uhf():
    """
    Build:
      zip_to_uhfs: Dict[str, List[int]]
      borough_to_uhfs: Dict[str, List[int]]

    Handles two formats:
      A) Header present with columns containing 'borough' and 'zip'
      B) No header, rows like: Borough,UHF42,101,10463,10471,...
         where column 0=borough, 1=marker (UHF42/UHF34), 2=code, 3+=ZIPs
    """
    zip_to_uhfs: Dict[str, List[int]] = {}
    borough_to_uhfs: Dict[str, List[int]] = {}

    if not UHF_FILE.exists():
        print(f"Could not find {UHF_FILE}")
        return zip_to_uhfs, borough_to_uhfs

    delim = _sniff_delimiter(UHF_FILE, ",")
    with UHF_FILE.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=delim)
        rows = [[c.strip() for c in r] for r in reader if any(c.strip() for c in r)]

    if not rows:
        return zip_to_uhfs, borough_to_uhfs

    # Detect header by presence of keywords
    header_kw = ("uhf", "code", "id", "borough", "boro", "zip")
    first_lower = [c.lower() for c in rows[0]]
    # Only accept real headers (borough/zip/uhf_id), not data like 'UHF42'
    has_header = any(
    ("borough" in c or "boro" in c or "zip" in c or "uhf_id" in c or "uhf id" in c)
    for c in first_lower
)


    if has_header:
        header = first_lower
        data = rows[1:]

        def find_col(keys, default=None):
            for i, h in enumerate(header):
                if any(k in h for k in keys):
                    return i
            return default

        i_bor = find_col(["borough", "boro"], 0)
        i_uhf = find_col(["uhf", "code", "id"], 1)
        i_zip_start = find_col(["zip"], 2)
        if i_zip_start is None:
            i_zip_start = 2

        for r in data:
            if len(r) <= max(i for i in [i_bor, i_uhf, i_zip_start] if i is not None):
                continue
            borough = _norm_borough(r[i_bor]) if i_bor is not None else ""
            uhf_ids = _expand_uhf_code(r[i_uhf]) if i_uhf is not None else []
            zip_cells = r[i_zip_start:] if i_zip_start is not None else []

            zips = []
            for z in zip_cells:
                if len(z) >= 5 and z[:5].isdigit():
                    zips.append(z[:5])
            seen = set()
            zips = [z for z in zips if not (z in seen or seen.add(z))]

            if borough and uhf_ids:
                borough_to_uhfs.setdefault(borough, [])
                for u in uhf_ids:
                    if u not in borough_to_uhfs[borough]:
                        borough_to_uhfs[borough].append(u)
            for z in zips:
                zip_to_uhfs.setdefault(z, [])
                for u in uhf_ids:
                    if u not in zip_to_uhfs[z]:
                        zip_to_uhfs[z].append(u)

        return zip_to_uhfs, borough_to_uhfs

    # No header: expect Borough,UHF42,101,10463,10471,...
    for r in rows:
        if len(r) < 3:
            continue
        borough = _norm_borough(r[0])
        marker = r[1].upper()
        code_cell = r[2]  # after marker
        uhf_ids = _expand_uhf_code(code_cell)

        start = 3  # ZIPs begin after code
        zip_cells = r[start:]

        zips = []
        for z in zip_cells:
            if len(z) >= 5 and z[:5].isdigit():
                zips.append(z[:5])
        seen = set()
        zips = [z for z in zips if not (z in seen or seen.add(z))]

        if borough and uhf_ids:
            borough_to_uhfs.setdefault(borough, [])
            for u in uhf_ids:
                if u not in borough_to_uhfs[borough]:
                    borough_to_uhfs[borough].append(u)
        for z in zips:
            zip_to_uhfs.setdefault(z, [])
            for u in uhf_ids:
                if u not in zip_to_uhfs[z]:
                    zip_to_uhfs[z].append(u)

    return zip_to_uhfs, borough_to_uhfs


# ---------- Query helpers (Part 1c) ----------

def _format_measurement(m: Measurement) -> str:
    d, uhf_id, uhf_name, val = m
    return f"{d} UHF {uhf_id} {uhf_name} {val:.2f} mcg/m^3"

def search_by_zip(zip_code: str, zip_to_uhfs, by_uhf) -> List[Measurement]:
    out = []
    for u in zip_to_uhfs.get(str(zip_code), []):
        out.extend(by_uhf.get(u, []))
    return out

def search_by_uhf(uhf_id, by_uhf) -> List[Measurement]:
    u = _to_int_safe(uhf_id)
    return list(by_uhf.get(u, [])) if u is not None else []

def search_by_borough(borough: str, borough_to_uhfs, by_uhf) -> List[Measurement]:
    b = _norm_borough(borough)
    out = []
    for u in borough_to_uhfs.get(b, []):
        out.extend(by_uhf.get(u, []))
    return out

def search_by_date(date_str: str, by_date) -> List[Measurement]:
    return list(by_date.get(date_str.strip(), []))


# ---------- CLI ----------

def main():
    print("Loading data...")
    by_uhf, by_date = read_pollution()
    zip_to_uhfs, borough_to_uhfs = read_uhf()
    total = sum(len(v) for v in by_uhf.values())
    print("Loaded.")
    print(
        f"Records: {total} | UHF ids: {len(by_uhf)} | Dates: {len(by_date)} | "
        f"Zip codes: {len(zip_to_uhfs)} | Boroughs: {len(borough_to_uhfs)}"
    )

    menu = (
        "\nChoose a search type:\n"
        "  1) zip\n"
        "  2) uhf\n"
        "  3) borough\n"
        "  4) date\n"
        "  q) quit\n"
    )

    while True:
        print(menu)
        choice = input("Enter choice: ").strip().lower()
        if choice in ("q", "quit", "exit"):
            print("Goodbye.")
            return

        if choice in ("1", "zip"):
            term = input("Enter 5-digit zip: ").strip()
            results = search_by_zip(term, zip_to_uhfs, by_uhf)
        elif choice in ("2", "uhf"):
            term = input("Enter UHF id: ").strip()
            results = search_by_uhf(term, by_uhf)
        elif choice in ("3", "borough"):
            term = input("Enter borough name: ").strip()
            results = search_by_borough(term, borough_to_uhfs, by_uhf)
        elif choice in ("4", "date"):
            term = input("Enter date as YYYY/MM/DD: ").strip()
            results = search_by_date(term, by_date)
        else:
            print("Invalid choice.\n")
            continue

        if not results:
            print("No matching records.\n")
            continue

        for m in results:
            print(_format_measurement(m))
        print(f"\nReturned {len(results)} measurements.\n")


if __name__ == "__main__":
    main()
