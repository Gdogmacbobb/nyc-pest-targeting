#!/usr/bin/env python3
"""
nyc_live_pipeline.py

NYC Multifamily Property Manager Targeting Pipeline
Phase 1: Build ranked managing-agent list for 3 NYC neighborhoods
Phase 2: Add scoring + tiers

Inputs (in --data-dir):
  - hpd_contacts.csv
  - hpd_registrations.csv
  - pluto.csv
  - hpd_violations.csv   (optional; if missing, compliance score = 0)

Outputs (to --output-dir):
  - NYC_Multifamily_TargetManagers_Phase1.xlsx
  - NYC_Multifamily_TargetManagers_Phase2_Scored.xlsx

Hard-fail:
  - Missing required inputs (contacts/registrations/pluto)
  - Cannot derive BBL
  - Cannot identify a usable managing-agent name field in contacts

This script is designed to survive Socrata schema/casing differences by:
  - lowercasing column names
  - accepting multiple column name variants
  - constructing BBL from boro/block/lot when needed
  - deriving a managing-agent name from the "best available" contacts columns
"""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set, Tuple

import pandas as pd


# -----------------------------
# Config
# -----------------------------
NEIGHBORHOODS = [
    {"label": "UES", "name": "Upper East Side (Manhattan)", "zips": {"10021", "10028", "10075"}},
    {"label": "BAY_RIDGE", "name": "Bay Ridge (Brooklyn)", "zips": {"11209"}},
    {"label": "FH_REGO", "name": "Forest Hills / Rego Park (Queens)", "zips": {"11375", "11374"}},
]

UNIT_MIN = 25
UNIT_MAX = 120

PHASE1_FILENAME = "NYC_Multifamily_TargetManagers_Phase1.xlsx"
PHASE2_FILENAME = "NYC_Multifamily_TargetManagers_Phase2_Scored.xlsx"

REQUIRED_FILES = ["hpd_contacts.csv", "hpd_registrations.csv", "pluto.csv"]
OPTIONAL_FILES = ["hpd_violations.csv"]


# -----------------------------
# Utilities
# -----------------------------
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).lower().strip() for c in df.columns]
    return df


def first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def zfill_zip(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    # drop obvious "nan"
    s = s.mask(s.str.lower().isin({"nan", "none", ""}), "")
    return s.str.zfill(5)


def all_target_zips() -> Set[str]:
    z: Set[str] = set()
    for n in NEIGHBORHOODS:
        z |= set(n["zips"])
    return z


def assign_neighborhood(zipcode: str) -> Optional[str]:
    for n in NEIGHBORHOODS:
        if zipcode in n["zips"]:
            return n["name"]
    return None


def ensure_bbl(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """
    Ensure 'bbl' exists. Accept variants or construct from components.
    """
    df = norm_cols(df)

    if "bbl" in df.columns:
        df["bbl"] = df["bbl"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        return df

    alt = first_existing_col(df, ["borough_block_lot", "boro_block_lot"])
    if alt:
        df["bbl"] = df[alt].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        return df

    # Construct from components (common in PLUTO/other exports)
    if {"boro", "block", "lot"}.issubset(df.columns):
        df["bbl"] = (
            df["boro"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(1)
            + df["block"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)
            + df["lot"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(4)
        )
        return df

    if {"borough", "block", "lot"}.issubset(df.columns):
        df["bbl"] = (
            df["borough"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(1)
            + df["block"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)
            + df["lot"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(4)
        )
        return df

    raise ValueError(
        f"{df_name}: cannot find/construct BBL. First 80 columns: {list(df.columns)[:80]}"
    )


def pick_best_text_column(df: pd.DataFrame, exclude: Set[str]) -> Optional[str]:
    """
    Heuristic fallback: choose a likely "name" column from contacts by scanning columns.
    Preference order:
      - contains 'name'
      - contains 'owner'
      - contains 'agent'
      - contains 'contact'
    Must not be in exclude.
    """
    cols = [c for c in df.columns if c not in exclude]
    if not cols:
        return None

    def score(col: str) -> int:
        s = 0
        lc = col.lower()
        if "name" in lc:
            s += 50
        if "owner" in lc:
            s += 30
        if "agent" in lc:
            s += 25
        if "contact" in lc:
            s += 15
        if "company" in lc or "corp" in lc:
            s += 10
        if "address" in lc or "phone" in lc:
            s -= 25
        if "id" in lc:
            s -= 10
        return s

    cols_sorted = sorted(cols, key=score, reverse=True)
    return cols_sorted[0] if score(cols_sorted[0]) > 0 else None


# -----------------------------
# Stats
# -----------------------------
@dataclass
class RunStats:
    contacts_rows: int = 0
    registrations_rows: int = 0
    pluto_rows: int = 0
    violations_rows: int = 0
    merged_contacts_regs_rows: int = 0
    merged_all_rows: int = 0
    filtered_rows: int = 0
    managers_rows: int = 0


# -----------------------------
# Load inputs
# -----------------------------
def load_inputs(data_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
    missing = [f for f in REQUIRED_FILES if not (data_dir / f).exists()]
    if missing:
        raise FileNotFoundError(f"Missing required input files in {data_dir}: {missing}")

    contacts = pd.read_csv(data_dir / "hpd_contacts.csv", dtype=str, low_memory=False)
    regs = pd.read_csv(data_dir / "hpd_registrations.csv", dtype=str, low_memory=False)
    pluto = pd.read_csv(data_dir / "pluto.csv", dtype=str, low_memory=False)

    viol_path = data_dir / "hpd_violations.csv"
    violations = pd.read_csv(viol_path, dtype=str, low_memory=False) if viol_path.exists() else None

    return contacts, regs, pluto, violations


# -----------------------------
# Prep datasets
# -----------------------------
def prepare_pluto(pluto: pd.DataFrame) -> pd.DataFrame:
    pluto = norm_cols(pluto)
    pluto = ensure_bbl(pluto, "PLUTO")

    zip_col = first_existing_col(pluto, ["zipcode", "zip", "zip_code"])
    if not zip_col:
        raise ValueError(f"PLUTO: cannot find zipcode column. First 80 columns: {list(pluto.columns)[:80]}")

    units_col = first_existing_col(pluto, ["unitsres", "units_res", "residential_units"])
    if not units_col:
        # as a last resort, try anything that looks like unitsres
        candidates = [c for c in pluto.columns if "units" in c and "res" in c]
        if candidates:
            units_col = candidates[0]
        else:
            raise ValueError(f"PLUTO: cannot find UnitsRes column. First 80 columns: {list(pluto.columns)[:80]}")

    pluto["zipcode"] = zfill_zip(pluto[zip_col])
    pluto["unitsres"] = pd.to_numeric(pluto[units_col], errors="coerce")

    # Optional Phase 2 fields
    year_col = first_existing_col(pluto, ["yearbuilt", "year_built"])
    pluto["yearbuilt"] = pd.to_numeric(pluto[year_col], errors="coerce") if year_col else pd.NA

    landuse_col = first_existing_col(pluto, ["landuse", "land_use"])
    pluto["landuse"] = pluto[landuse_col] if landuse_col else pd.NA

    return pluto


def prepare_contacts_and_regs(contacts: pd.DataFrame, regs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    contacts = norm_cols(contacts)
    regs = norm_cols(regs)

    # registration id fields
    regid_c = first_existing_col(contacts, ["registrationid", "registration_id"])
    regid_r = first_existing_col(regs, ["registrationid", "registration_id"])
    if not regid_c or not regid_r:
        raise ValueError(
            "HPD datasets missing registrationid. "
            f"Contacts cols sample: {list(contacts.columns)[:40]} | Regs cols sample: {list(regs.columns)[:40]}"
        )

    if regid_c != "registrationid":
        contacts = contacts.rename(columns={regid_c: "registrationid"})
    if regid_r != "registrationid":
        regs = regs.rename(columns={regid_r: "registrationid"})

    # Ensure BBL is available on registrations
    regs = ensure_bbl(regs, "HPD Registrations")

    # Primary set of known name fields (covering many Socrata variants)
    agent_name_col = first_existing_col(
        contacts,
        [
            "managing_agent",
            "managingagent",
            "agent_name",
            "agentname",
            "contact_name",
            "ownername",
            "owner_name",
            "ownercorpname",
            "corporateownername",
            "registrant_name",
            "business_name",
            "company_name",
            "name",
            "entity_name",
        ],
    )

    # If still not found, pick best likely text column
    if not agent_name_col:
        exclude = {"registrationid"}
        agent_name_col = pick_best_text_column(contacts, exclude=exclude)

    if not agent_name_col:
        # Hard fail: without a name field, there is nothing to aggregate
        raise ValueError(
            "HPD Contacts: cannot determine managing agent / owner name field. "
            f"First 120 columns: {list(contacts.columns)[:120]}"
        )

    contacts = contacts.rename(columns={agent_name_col: "managing_agent"})

    # Address fields
    addr_col = first_existing_col(
        contacts,
        [
            "mailing_address",
            "mailingaddress",
            "businessaddress",
            "business_address",
            "address",
            "contact_address",
            "streetaddress",
            "street_address",
        ],
    )
    # Phone fields
    phone_col = first_existing_col(
        contacts,
        [
            "phone",
            "phone1",
            "businessphone",
            "business_phone",
            "contact_phone",
            "phonenumber",
            "phone_number",
        ],
    )

    if addr_col:
        contacts = contacts.rename(columns={addr_col: "mailing_address"})
    else:
        contacts["mailing_address"] = ""

    if phone_col:
        contacts = contacts.rename(columns={phone_col: "phone"})
    else:
        contacts["phone"] = ""

    # Normalize values a bit
    contacts["managing_agent"] = contacts["managing_agent"].astype(str).str.strip()
    contacts["mailing_address"] = contacts["mailing_address"].astype(str).str.strip()
    contacts["phone"] = contacts["phone"].astype(str).str.strip()

    return contacts, regs


def prepare_violations(violations: pd.DataFrame) -> pd.DataFrame:
    violations = norm_cols(violations)
    violations = ensure_bbl(violations, "HPD Violations")
    violations["bbl"] = violations["bbl"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    return violations


# -----------------------------
# Build raw building table
# -----------------------------
def build_raw_buildings(
    contacts: pd.DataFrame,
    regs: pd.DataFrame,
    pluto: pd.DataFrame,
    violations: Optional[pd.DataFrame],
    stats: RunStats,
) -> pd.DataFrame:
    # Contacts -> Registrations to get BBL
    merged = contacts.merge(regs[["registrationid", "bbl"]], on="registrationid", how="left")
    stats.merged_contacts_regs_rows = len(merged)

    merged["bbl"] = merged["bbl"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    pluto["bbl"] = pluto["bbl"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    # Join to PLUTO for zipcode + units
    merged = merged.merge(pluto[["bbl", "zipcode", "unitsres", "yearbuilt", "landuse"]], on="bbl", how="inner")
    stats.merged_all_rows = len(merged)

    tz = all_target_zips()

    # Filter ZIP + units range
    merged = merged[merged["zipcode"].isin(tz)]
    merged = merged[(merged["unitsres"] >= UNIT_MIN) & (merged["unitsres"] <= UNIT_MAX)]

    merged["neighborhood"] = merged["zipcode"].apply(assign_neighborhood)
    merged = merged.dropna(subset=["neighborhood"])
    stats.filtered_rows = len(merged)

    # Violations count (optional)
    if violations is not None:
        v_counts = violations.groupby("bbl").size().reset_index(name="violation_count")
        merged = merged.merge(v_counts, on="bbl", how="left")
        merged["violation_count"] = pd.to_numeric(merged["violation_count"], errors="coerce").fillna(0).astype(int)
    else:
        merged["violation_count"] = 0

    # Mixed-use heuristic via PLUTO LandUse == "04"
    merged["mixed_use_flag"] = merged["landuse"].astype(str).str.strip().eq("04")

    # Density heuristic: 3+ unique buildings by same manager within same ZIP
    merged["_zip_buildings_for_manager"] = merged.groupby(["managing_agent", "zipcode"])["bbl"].transform("nunique")
    merged["density_flag"] = merged["_zip_buildings_for_manager"] >= 3

    return merged


# -----------------------------
# Aggregate managers (Phase 1)
# -----------------------------
def aggregate_managers(raw: pd.DataFrame, stats: RunStats) -> pd.DataFrame:
    raw["managing_agent_norm"] = (
        raw["managing_agent"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    )

    def first_nonempty(series: pd.Series) -> str:
        for v in series.astype(str).tolist():
            if v and v.strip() and v.strip().lower() not in {"nan", "none"}:
                return v.strip()
        return ""

    managers = raw.groupby("managing_agent_norm").agg(
        **{
            "Managing Agent Name": ("managing_agent_norm", "first"),
            "Mailing Address": ("mailing_address", first_nonempty),
            "Phone": ("phone", first_nonempty),
            "Neighborhood": ("neighborhood", lambda x: ", ".join(sorted(set(map(str, x))))),
            "ZIP Code(s)": ("zipcode", lambda x: ", ".join(sorted(set(map(str, x))))),
            "# Buildings Managed": ("bbl", "nunique"),
            "Total Units": ("unitsres", "sum"),
            "Avg Units per Building": ("unitsres", "mean"),
            "Violation Count (Total)": ("violation_count", "sum"),
            "Mixed-use Buildings Count": ("mixed_use_flag", "sum"),
            "Density Flag": ("density_flag", "max"),
        }
    ).reset_index(drop=True)

    managers["Avg Units per Building"] = managers["Avg Units per Building"].round(1)

    managers["Portfolio Operator Flag (5+)"] = managers["# Buildings Managed"] >= 5
    managers["Multi-ZIP Flag"] = managers["ZIP Code(s)"].astype(str).apply(
        lambda s: len([z for z in s.split(",") if z.strip()]) >= 2
    )

    managers["Notes"] = (
        "UnitsRes filtered 25-120; Density flag uses same-ZIP heuristic (>=3 buildings/ZIP). "
        "Mixed-use inferred from PLUTO LandUse=='04'."
    )

    managers = managers.sort_values(["# Buildings Managed", "Total Units"], ascending=[False, False])
    stats.managers_rows = len(managers)
    return managers


# -----------------------------
# Phase 2 scoring
# -----------------------------
def score_phase2(managers: pd.DataFrame) -> pd.DataFrame:
    scored = managers.copy()

    # Portfolio Strength (30)
    portfolio_strength = (
        (scored["# Buildings Managed"] >= 5).astype(int) * 15
        + (scored["Total Units"] >= 300).astype(int) * 15
    )

    # Compliance Risk (25): top quartile of violations/building
    violation_rate = (scored["Violation Count (Total)"] / scored["# Buildings Managed"]).fillna(0)
    q75 = violation_rate.quantile(0.75) if len(violation_rate) else 0
    compliance_risk = (violation_rate >= q75).astype(int) * 25 if q75 > 0 else 0

    # Revenue Potential (25)
    avg_units = scored["Avg Units per Building"]
    rev_potential = ((avg_units >= 40) & (avg_units <= 100)).astype(int) * 15
    rev_potential += (scored["Mixed-use Buildings Count"] > 0).astype(int) * 10

    # Route Efficiency (20)
    density_score = scored["Density Flag"].astype(int) * 20

    scored["Violation Score"] = compliance_risk
    scored["Revenue Potential Score"] = rev_potential
    scored["Density Score"] = density_score

    scored["Total Priority Score (0-100)"] = (
        portfolio_strength + compliance_risk + rev_potential + density_score
    ).clip(upper=100)

    def tier(score: float) -> str:
        if score >= 75:
            return "A"
        if score >= 50:
            return "B"
        return "C"

    scored["Recommended Tier (A/B/C)"] = scored["Total Priority Score (0-100)"].apply(tier)
    scored = scored.sort_values(["Total Priority Score (0-100)", "# Buildings Managed"], ascending=[False, False])

    return scored


# -----------------------------
# Write outputs
# -----------------------------
def write_excels(
    output_dir: Path,
    phase1_managers: pd.DataFrame,
    phase1_raw: pd.DataFrame,
    phase2_managers: pd.DataFrame,
    phase2_raw: pd.DataFrame,
    data_notes: List[List[str]],
    run_log: List[List[str]],
) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    p1_path = output_dir / PHASE1_FILENAME
    p2_path = output_dir / PHASE2_FILENAME

    with pd.ExcelWriter(p1_path, engine="openpyxl") as writer:
        pd.DataFrame(data_notes, columns=["Key", "Value"]).to_excel(writer, sheet_name="Data_Notes", index=False)
        pd.DataFrame(run_log, columns=["Metric", "Value"]).to_excel(writer, sheet_name="Run_Log", index=False)
        phase1_managers.to_excel(writer, sheet_name="Ranked_Managers", index=False)
        phase1_raw.to_excel(writer, sheet_name="Raw_Buildings", index=False)

    with pd.ExcelWriter(p2_path, engine="openpyxl") as writer:
        pd.DataFrame(data_notes, columns=["Key", "Value"]).to_excel(writer, sheet_name="Data_Notes", index=False)
        pd.DataFrame(run_log, columns=["Metric", "Value"]).to_excel(writer, sheet_name="Run_Log", index=False)
        phase2_managers.to_excel(writer, sheet_name="Ranked_Managers_Scored", index=False)
        phase2_raw.to_excel(writer, sheet_name="Raw_Buildings_Scored", index=False)

    return p1_path, p2_path


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, help="Directory containing CSV inputs")
    parser.add_argument("--output-dir", required=True, help="Directory to write Excel outputs")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)

    stats = RunStats()

    contacts, regs, pluto, violations = load_inputs(data_dir)
    stats.contacts_rows = len(contacts)
    stats.registrations_rows = len(regs)
    stats.pluto_rows = len(pluto)
    stats.violations_rows = len(violations) if violations is not None else 0

    # Normalize / prep
    pluto = prepare_pluto(pluto)
    contacts, regs = prepare_contacts_and_regs(contacts, regs)
    if violations is not None:
        violations = prepare_violations(violations)

    # Build raw building table
    raw = build_raw_buildings(contacts, regs, pluto, violations, stats)

    # Phase 1
    managers = aggregate_managers(raw, stats)

    # Phase 2
    managers_scored = score_phase2(managers)

    # Notes + run log
    notes = [
        ["Inputs", ", ".join(REQUIRED_FILES + OPTIONAL_FILES)],
        ["Neighborhood ZIPs", "; ".join([f"{n['name']}: {', '.join(sorted(n['zips']))}" for n in NEIGHBORHOODS])],
        ["Units Filter", f"{UNIT_MIN}-{UNIT_MAX} UnitsRes inclusive"],
        ["Join Method", "HPD Contacts -> HPD Registrations on registrationid; join to PLUTO on BBL"],
        ["Density Flag", "same-ZIP heuristic: >=3 unique buildings per manager per ZIP"],
        ["Mixed-use Flag", "PLUTO LandUse=='04' indicates mixed residential/commercial (heuristic)"],
        ["Compliance Risk", "violations/building top quartile -> 25 pts (if violations present)"],
    ]

    run_log = [
        ["contacts_rows", stats.contacts_rows],
        ["registrations_rows", stats.registrations_rows],
        ["pluto_rows", stats.pluto_rows],
        ["violations_rows", stats.violations_rows],
        ["merged_contacts_regs_rows", stats.merged_contacts_regs_rows],
        ["merged_all_rows", stats.merged_all_rows],
        ["filtered_rows", stats.filtered_rows],
        ["managers_rows", stats.managers_rows],
    ]

    # checksums
    for f in REQUIRED_FILES + OPTIONAL_FILES:
        p = data_dir / f
        if p.exists():
            notes.append([f"sha256:{f}", sha256_file(p)])
        else:
            notes.append([f"sha256:{f}", "MISSING (optional)"])

    p1, p2 = write_excels(output_dir, managers, raw, managers_scored, raw, notes, run_log)

    print("Pipeline complete.")
    print(f"Phase 1 -> {p1}")
    print(f"Phase 2 -> {p2}")
    print(f"Managers: {len(managers_scored)} | Raw rows: {len(raw)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
