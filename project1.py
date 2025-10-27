#!/usr/bin/env python3
"""
project1.py — Part 1

Goal:
  - Read and organize NYC air quality data from CSV files.
  - Let users query pollution data by zip code, borough, UHF ID, or date.

Files expected in the SAME folder as this script:
  - air_quality.csv  → UHF Geo ID, Geo description, YYYY/MM/DD, pm2.5
  - uhf.csv          → either:
        (A) header with 'borough'/'zip' columns
        (B) raw format like: Borough,UHF42,101,10463,10471,...

Measurement tuple used throughout:
  (date_str, uhf_id:int, uhf_name:str, value:float)
"""

from pathlib import Path
from typing import Dict, List, Tuple
import csv

# ---------- Paths ----------
# We use Pathlib for clean and OS-independent file path handling.
HERE = Path(__file__).resolve().parent
AIR_QUALITY_FILE = HERE / "air_quality.csv"
UHF_FILE = HERE / "uhf.csv"

# A measurement tuple stores the relevant info for each reading.
Measurement = Tuple[str, int, str, float]


# ---------- Utility Helper Functions ----------

# We define several small “utility” functions to simplify data cleaning.

def _to_int_safe(x):
    """Convert any value to int safely, returning None if it fails.
    This helper ensures that values like ' 42 ', '042', or even numeric strings
    from CSVs can be converted cleanly. 
    """
    try:
        return int(str(x).strip())
    except Exception:
        return None

def _to_float_safe(x):
    """Convert any value to float safely, returning None if it fails.
    This helper behaves similarly to _to_int_safe but for floating-point values.
    It is useful when reading numerical data from CSV files that may contain
    missing, corrupted, or placeholder entries."""
    try:
        return float(str(x).strip())
    except Exception:
        return None

def _norm_borough(name: str) -> str:
    """
    Normalize borough names for consistent lookups.
    Example: 'manhattan' → 'Manhattan'
        ' BROOKLYN '  -> 'Brooklyn'

    This ensures that inconsistent capitalization or extra spaces in the source
    data don't affect dictionary key matching when grouping or filtering by
    borough name.
    """
    return str(name).strip().title()

def _expand_uhf_code(uhf_code: str) -> List[int]:
    """
    Handle special UHF34-style codes that concatenate multiple IDs.
    Example: '105106107' → [105, 106, 107]
    Rationale: UHF34 groups multiple neighborhoods into one region.
    """
    s = str(uhf_code).strip()
    if not s:
        return []
    # If the string is a long numeric chain divisible by 3, split into 3-digit chunks
    if s.isdigit() and len(s) > 3 and len(s) % 3 == 0:
        return [int(s[i:i+3]) for i in range(0, len(s), 3)]
    v = _to_int_safe(s)
    return [v] if v is not None else []

def _sniff_delimiter(path: Path, default=","):
    """
    Automatically detect CSV delimiter (comma, semicolon, tab, etc.).
    Reason: Some data exports might use different delimiters.
    """
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
    Load and index data from air_quality.csv.

    We create TWO dictionaries:
      1) by_uhf[UHF_ID] → list of measurement tuples
      2) by_date[date] → list of measurement tuples

    This double indexing makes lookups fast for both geography and time.
    """

    # Initialize two lookup dictionaries: one keyed by UHF ID, one by date
    by_uhf: Dict[int, List[Measurement]] = {}
    by_date: Dict[str, List[Measurement]] = {}

    # Verify that the CSV file exists before proceeding
    if not AIR_QUALITY_FILE.exists():
        print(f"Could not find {AIR_QUALITY_FILE}")
        return by_uhf, by_date

    # Detect delimiter automatically
    delim = _sniff_delimiter(AIR_QUALITY_FILE, ",")
    with AIR_QUALITY_FILE.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=delim)
        rows = list(reader)

    if not rows:
        return by_uhf, by_date

    # Skip header if first cell is not numeric (i.e., 'Geo ID')
    start_idx = 1 if rows and rows[0] and not str(rows[0][0]).strip().isdigit() else 0

    for row in rows[start_idx:]:
        if len(row) < 4:
            continue
        # Extract and clean each field
        uhf_id = _to_int_safe(row[0])
        uhf_name = str(row[1]).strip()
        date_str = str(row[2]).strip()
        value = _to_float_safe(row[3])

        # Validate all fields before adding
        if uhf_id is None or not uhf_name or not date_str or value is None:
            continue
        # Create the measurement tuple
        m: Measurement = (date_str, uhf_id, uhf_name, value)

        # Store in both lookup dictionaries
        by_uhf.setdefault(uhf_id, []).append(m)
        by_date.setdefault(date_str, []).append(m)

    return by_uhf, by_date


def read_uhf():
    """
    Load and map data from uhf.csv.

    We produce:
      - zip_to_uhfs: maps zip → list of UHF IDs
      - borough_to_uhfs: maps borough → list of UHF IDs

    Why:
      These dictionaries let users search by borough or zip easily.
      The function automatically handles two possible file formats:
        A) With headers containing "borough"/"zip" columns.
        B) Raw format (e.g., 'Bronx,UHF42,101,10463,10471,...')
    """
    zip_to_uhfs: Dict[str, List[int]] = {}
    borough_to_uhfs: Dict[str, List[int]] = {}

    if not UHF_FILE.exists():
        print(f"Could not find {UHF_FILE}")
        return zip_to_uhfs, borough_to_uhfs

    # Read and strip whitespace from all cells
    delim = _sniff_delimiter(UHF_FILE, ",")
    with UHF_FILE.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=delim)
        rows = [[c.strip() for c in r] for r in reader if any(c.strip() for c in r)]

    if not rows:
        return zip_to_uhfs, borough_to_uhfs

    # --- Header detection ---
    # We decide if the file has a header by checking for certain keywords.
    first_lower = [c.lower() for c in rows[0]]
    has_header = any(
        ("borough" in c or "boro" in c or "zip" in c or "uhf_id" in c or "uhf id" in c)
        for c in first_lower
    )

    # --- Case A: Header format ---
    if has_header:
        header = first_lower
        data = rows[1:]

        # Helper to find column indices by keyword
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

        # Iterate through each row and populate our dictionaries
        for r in data:
            if len(r) <= max(i for i in [i_bor, i_uhf, i_zip_start] if i is not None):
                continue

            borough = _norm_borough(r[i_bor]) if i_bor is not None else ""
            uhf_ids = _expand_uhf_code(r[i_uhf]) if i_uhf is not None else []
            zip_cells = r[i_zip_start:] if i_zip_start is not None else []

            # Extract valid 5-digit ZIPs only
            zips = []
            for z in zip_cells:
                if len(z) >= 5 and z[:5].isdigit():
                    zips.append(z[:5])
            # Remove duplicates while preserving order
            seen = set()
            zips = [z for z in zips if not (z in seen or seen.add(z))]

            # Map borough → UHF
            if borough and uhf_ids:
                borough_to_uhfs.setdefault(borough, [])
                for u in uhf_ids:
                    if u not in borough_to_uhfs[borough]:
                        borough_to_uhfs[borough].append(u)

            # Map ZIP → UHF
            for z in zips:
                zip_to_uhfs.setdefault(z, [])
                for u in uhf_ids:
                    if u not in zip_to_uhfs[z]:
                        zip_to_uhfs[z].append(u)

        return zip_to_uhfs, borough_to_uhfs

    # --- Case B: No header (raw format) ---
    # Example row: Bronx, UHF42, 101, 10463, 10471
    for r in rows:
        if len(r) < 3:
            continue
        borough = _norm_borough(r[0])
        marker = r[1].upper()  # "UHF42" / "UHF34" indicator (not used directly)
        code_cell = r[2]       # column 2 = numeric UHF ID(s)
        uhf_ids = _expand_uhf_code(code_cell)

        # ZIP codes start from column 3 onward
        zip_cells = r[3:]

        # Extract valid 5-digit ZIPs and deduplicate
        zips = []
        for z in zip_cells:
            if len(z) >= 5 and z[:5].isdigit():
                zips.append(z[:5])
        seen = set()
        zips = [z for z in zips if not (z in seen or seen.add(z))]

        # Populate borough and ZIP dictionaries
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


# ---------- Query Helpers (Part 1c) ----------

def _format_measurement(m: Measurement) -> str:
    """Convert one measurement tuple into a readable string for printing."""
    d, uhf_id, uhf_name, val = m
    return f"{d} UHF {uhf_id} {uhf_name} {val:.2f} mcg/m^3"

def search_by_zip(zip_code: str, zip_to_uhfs, by_uhf) -> List[Measurement]:
    """
    Given a ZIP, find all UHF IDs in that ZIP, and return all related measurements.
    This uses both dictionaries built earlier.
    """
    out = []
    for u in zip_to_uhfs.get(str(zip_code), []):
        out.extend(by_uhf.get(u, []))
    return out

def search_by_uhf(uhf_id, by_uhf) -> List[Measurement]:
    """Return all measurements for a specific UHF ID."""
    u = _to_int_safe(uhf_id)
    return list(by_uhf.get(u, [])) if u is not None else []

def search_by_borough(borough: str, borough_to_uhfs, by_uhf) -> List[Measurement]:
    """
    Return all measurements for a given borough.
    Uses borough_to_uhfs to expand to all its UHF codes first.
    """
    b = _norm_borough(borough)
    out = []
    for u in borough_to_uhfs.get(b, []):
        out.extend(by_uhf.get(u, []))
    return out

def search_by_date(date_str: str, by_date) -> List[Measurement]:
    """Return all measurements recorded on a specific date."""
    return list(by_date.get(date_str.strip(), []))


# ---------- CLI (Main interactive interface) ----------

def main():
    """
    Main entry point for the program
    Loads data once at startup, then repeatedly asks user for search criteria.
    This command-line interface allows flexible exploration of the dataset.
    """
    print("Loading data...")
    by_uhf, by_date = read_pollution()
    zip_to_uhfs, borough_to_uhfs = read_uhf()
    total = sum(len(v) for v in by_uhf.values())
    print("Loaded.")
    print(
        f"Records: {total} | UHF ids: {len(by_uhf)} | Dates: {len(by_date)} | "
        f"Zip codes: {len(zip_to_uhfs)} | Boroughs: {len(borough_to_uhfs)}"
    )

    # Simple text-based menu for usability
    menu = (
        "\nChoose a search type:\n"
        "  1) zip\n"
        "  2) uhf\n"
        "  3) borough\n"
        "  4) date\n"
        "  q) quit\n"
    )

  #Keep prompting user until they quit
    while True:
        print(menu)
        choice = input("Enter choice: ").strip().lower()
        if choice in ("q", "quit", "exit"):
            print("Goodbye.")
            return

        # Each option uses the search functions we defined earlier
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

        # Print all results neatly formatted
        for m in results:
            print(_format_measurement(m))
        print(f"\nReturned {len(results)} measurements.\n")


if __name__ == "__main__":
    main()
