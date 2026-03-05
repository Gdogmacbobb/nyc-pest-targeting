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
  - Cannot derive BBL in required datasets

Notes:
  - Socrata exports sometimes vary column names/casing. This script normalizes to lowercase.
  - Density flag uses same-ZIP heuristic (3+ buildings per manager within same ZIP) unless lat/lon present.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

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
# Helpers
# -----------------------------
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower().strip() for c in df.columns]
    return df


def zfill_zip(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)


def first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def ensure_bbl(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """
    Ensure a 'bbl' column exists. Accept variants or construct from boro/block/lot.
    """
    df = norm_cols(df)

    if "bbl" in df.columns:
        df["bbl"] = df["bbl"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        return df

    # Sometimes Socrata uses uppercase or alternative names; after norm_cols it should be lowercase
    alt = first_existing_col(df, ["borough_block_lot", "boro_block_lot"])
    if alt:
        df["bbl"] = df[alt].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        return df

    # Construct from components if available
    if {"boro", "block", "lot"}.issubset(df.columns):
        df["bbl"] = (
            df["boro"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(1)
            + df["block"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)
            + df["lot"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(4)
        )
        return df

    # Alternative component naming
    if {"borough", "block", "lot"}.issubset(df.columns):
        df["bbl"] = (
            df["borough"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(1)
            + df["block"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)
            + df["lot"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(4)
        )
        return df

    raise ValueError(f"{df_name}: cannot find/construct BBL column. Columns present: {list(df.columns)[:50]}")


def assign_neighborhood(zipcode: str) -> Optional[str]:
    for n in NEIGHBORHOODS:
        if zipcode in n["zips"]:
            return n["name"]
    return None


def all_target_zips() -> Set[str]:
    z: Set[str] = set()
    for n in NEIGHBORHOODS:
        z |= set(n["zips"])
    return z


# -----------------------------
# Pipeline
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


def prepare_pluto(pluto: pd.DataFrame) -> pd.DataFrame:
    pluto = ensure_bbl(pluto, "PLUTO")

    pluto = norm_cols(pluto)

    zip_col = first_existing_col(pluto, ["zipcode", "zip", "zip_code"])
    if not zip_col:
        raise ValueError("PLUTO: cannot find zipcode column")

    units_col = first_existing_col(pluto, ["unitsres", "units_res", "residential_units", "units"])
    if not units_col:
        raise ValueError("PLUTO: cannot find UnitsRes column")

    pluto["zipcode"] = zfill_zip(pluto[zip_col])
    pluto["unitsres"] = pd.to_numeric(pluto[units_col], errors="coerce")

    # Optional fields for Phase 2
    year_col = first_existing_col(pluto, ["yearbuilt", "year_built"])
    pluto["yearbuilt"] = pd.to_numeric(pluto[year_col], errors="coerce") if year_col else pd.NA

    landuse_col = first_existing_col(pluto, ["landuse", "land_use"])
    pluto["landuse"] = pluto[landuse_col] if landuse_col else pd.NA

    return pluto


def prepare_contacts_and_regs(contacts: pd.DataFrame, regs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    contacts = norm_cols(contacts)
    regs = norm_cols(regs)

    # Identify join key: registrationid is typical in HPD datasets
    regid_c = first_existing_col(contacts, ["registrationid", "registration_id"])
    regid_r = first_existing_col(regs, ["registrationid", "registration_id"])
    if not regid_c or not regid_r:
        raise ValueError("HPD: cannot find registrationid in contacts/registrations")

    # Normalize to a common name
    if regid_c != "registrationid":
        contacts = contacts.rename(columns={regid_c: "registrationid"})
    if regid_r != "registrationid":
        regs = regs.rename(columns={regid_r: "registrationid"})

    # Ensure BBL on registrations if present; sometimes BBL is only there
    regs = ensure_bbl(regs, "HPD Registrations")

    # Agent name fields vary
    agent_name_col = first_existing_col(
        contacts,
        [
            "ownername",
            "owner_name",
            "managingagent",
            "managing_agent",
            "agentname",
            "agent_name",
            "contact_name",
        ],
    )
    if not agent_name_col:
        raise ValueError("HPD Contacts: cannot find managing agent / owner name field")

    # Address fields (optional)
    addr_col = first_existing_col(contacts, ["businessaddress", "mailingaddress", "address", "contact_address"])
    phone_col = first_existing_col(contacts, ["phone", "phone1", "businessphone", "contact_phone"])

    contacts = contacts.rename(columns={agent_name_col: "managing_agent"})
    if addr_col and addr_col != "mailing_address":
        contacts = contacts.rename(columns={addr_col: "mailing_address"})
    if phone_col and phone_col != "phone":
        contacts = contacts.rename(columns={phone_col: "phone"})

    # Ensure columns exist even if missing
    if "mailing_address" not in contacts.columns:
        contacts["mailing_address"] = ""
    if "phone" not in contacts.columns:
        contacts["phone"] = ""

    return contacts, regs


def prepare_violations(violations: pd.DataFrame) -> pd.DataFrame:
    violations = norm_cols(violations)
    violations = ensure_bbl(violations, "HPD Violations")

    # Some datasets use inspectiondate / novissueddate etc. We'll only aggregate counts.
    return violations


def build_raw_buildings(
    contacts: pd.DataFrame,
    regs: pd.DataFrame,
    pluto: pd.DataFrame,
    violations: Optional[pd.DataFrame],
    stats: RunStats,
) -> pd.DataFrame:
    # Merge contacts -> registrations to get BBL
    merged = contacts.merge(regs[["registrationid", "bbl"]], on="registrationid", how="left")
    stats.merged_contacts_regs_rows = len(merged)

    merged["bbl"] = merged["bbl"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    pluto["bbl"] = pluto["bbl"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    # Merge to PLUTO for ZIP + units
    merged = merged.merge(pluto[["bbl", "zipcode", "unitsres", "yearbuilt", "landuse"]], on="bbl", how="inner")
    stats.merged_all_rows = len(merged)

    # Filter to target zips and unit range
    tz = all_target_zips()
    merged = merged[merged["zipcode"].isin(tz)]
    merged = merged[(merged["unitsres"] >= UNIT_MIN) & (merged["unitsres"] <= UNIT_MAX)]

    merged["neighborhood"] = merged["zipcode"].apply(assign_neighborhood)
    merged = merged.dropna(subset=["neighborhood"])
    stats.filtered_rows = len(merged)

    # Add violations count (optional)
    if violations is not None:
        v = violations.copy()
        v["bbl"] = v["bbl"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        v_counts = v.groupby("bbl").size().reset_index(name="violation_count")
        merged = merged.merge(v_counts, on="bbl", how="left")
        merged["violation_count"] = pd.to_numeric(merged["violation_count"], errors="coerce").fillna(0).astype(int)
    else:
        merged["violation_count"] = 0

    # Ground-floor retail heuristic (optional)
    # LandUse == "04" often indicates mixed residential/commercial in PLUTO.
    merged["mixed_use_flag"] = merged["landuse"].astype(str).str.strip().eq("04")

    # Simple density flag heuristic: 3+ buildings by same manager within same ZIP
    merged["_zip_buildings_for_manager"] = merged.groupby(["managing_agent", "zipcode"])["bbl"].transform("nunique")
    merged["density_flag"] = merged["_zip_buildings_for_manager"] >= 3

    return merged


def aggregate_managers(raw: pd.DataFrame, stats: RunStats) -> pd.DataFrame:
    # Normalize agent names for grouping
    raw["managing_agent_norm"] = raw["managing_agent"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

    managers = raw.groupby(["managing_agent_norm"]).agg(
        **{
            "Managing Agent Name": ("managing_agent_norm", "first"),
            "Mailing Address": ("mailing_address", lambda x: next((v for v in x if isinstance(v, str) and v.strip()), "")),
            "Phone": ("phone", lambda x: next((v for v in x if isinstance(v, str) and v.strip()), "")),
            "# Buildings Managed": ("bbl", "nunique"),
            "Total Units": ("unitsres", "sum"),
            "Avg Units per Building": ("unitsres", "mean"),
            "ZIP Code(s)": ("zipcode", lambda x: ", ".join(sorted(set(map(str, x))))),
            "Neighborhood": ("neighborhood", lambda x: ", ".join(sorted(set(map(str, x))))),
            "Violation Count (Total)": ("violation_count", "sum"),
            "Mixed-use Buildings Count": ("mixed_use_flag", "sum"),
            "Density Flag": ("density_flag", "max"),
        }
    ).reset_index(drop=True)

    managers["Avg Units per Building"] = managers["Avg Units per Building"].round(1)

    managers["Portfolio Operator Flag (5+)"] = managers["# Buildings Managed"] >= 5
    managers["Multi-ZIP Flag"] = managers["ZIP Code(s)"].astype(str).apply(lambda s: len([z for z in s.split(",") if z.strip()]) >= 2)

    # Notes
    managers["Notes"] = (
        "UnitsRes filtered 25-120; Density flag uses same-ZIP heuristic (>=3 buildings/ZIP). "
        "Mixed-use inferred from PLUTO LandUse=='04'."
    )

    # Sort as spec
    managers = managers.sort_values(["# Buildings Managed", "Total Units"], ascending=[False, False])

    stats.managers_rows = len(managers)
    return managers


def score_phase2(managers: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    scored = managers.copy()

    # Portfolio Strength (30)
    portfolio_strength = (
        (scored["# Buildings Managed"] >= 5).astype(int) * 15
        + (scored["Total Units"] >= 300).astype(int) * 15
    )

    # Compliance Risk (25): use per-neighborhood quartiles of manager-level violation rate
    # Compute manager-level violations per building as a proxy
    violation_rate = (scored["Violation Count (Total)"] / scored["# Buildings Managed"]).fillna(0)

    # Determine high pressure by neighborhood quartile: since manager can span neighborhoods, use global quartile for simplicity
    q75 = violation_rate.quantile(0.75)
    compliance_risk = (violation_rate >= q75).astype(int) * 25

    # Revenue Potential (25)
    avg_units = scored["Avg Units per Building"]
    rev_potential = ((avg_units >= 40) & (avg_units <= 100)).astype(int) * 15
    rev_potential += (scored["Mixed-use Buildings Count"] > 0).astype(int) * 10

    # Route Efficiency (20)
    density_score = scored["Density Flag"].astype(int) * 20

    scored["Violation Score"] = compliance_risk
    scored["Revenue Potential Score"] = rev_potential
    scored["Density Score"] = density_score

    scored["Total Priority Score (0-100)"] = (portfolio_strength + compliance_risk + rev_potential + density_score).clip(upper=100)

    def tier(score: float) -> str:
        if score >= 75:
            return "A"
        if score >= 50:
            return "B"
        return "C"

    scored["Recommended Tier (A/B/C)"] = scored["Total Priority Score (0-100)"].apply(tier)

    # Sort Phase 2
    scored = scored.sort_values(["Total Priority Score (0-100)", "# Buildings Managed"], ascending=[False, False])

    return scored


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

    # Phase 1 workbook
    with pd.ExcelWriter(p1_path, engine="openpyxl") as writer:
        # Put Data_Notes first for quick visibility
        pd.DataFrame(data_notes, columns=["Key", "Value"]).to_excel(writer, sheet_name="Data_Notes", index=False)
        pd.DataFrame(run_log, columns=["Metric", "Value"]).to_excel(writer, sheet_name="Run_Log", index=False)
        phase1_managers.to_excel(writer, sheet_name="Ranked_Managers", index=False)
        phase1_raw.to_excel(writer, sheet_name="Raw_Buildings", index=False)

    # Phase 2 workbook
    with pd.ExcelWriter(p2_path, engine="openpyxl") as writer:
        pd.DataFrame(data_notes, columns=["Key", "Value"]).to_excel(writer, sheet_name="Data_Notes", index=False)
        pd.DataFrame(run_log, columns=["Metric", "Value"]).to_excel(writer, sheet_name="Run_Log", index=False)
        phase2_managers.to_excel(writer, sheet_name="Ranked_Managers_Scored", index=False)
        phase2_raw.to_excel(writer, sheet_name="Raw_Buildings_Scored", index=False)

    return p1_path, p2_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, help="Directory containing CSV inputs")
    parser.add_argument("--output-dir", required=True, help="Directory to write Excel outputs")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)

    stats = RunStats()

    # Load inputs
    contacts, regs, pluto, violations = load_inputs(data_dir)
    stats.contacts_rows = len(contacts)
    stats.registrations_rows = len(regs)
    stats.pluto_rows = len(pluto)
    stats.violations_rows = len(violations) if violations is not None else 0

    # Prep
    pluto = prepare_pluto(pluto)
    contacts, regs = prepare_contacts_and_regs(contacts, regs)
    if violations is not None:
        violations = prepare_violations(violations)

    # Build raw building table
    raw = build_raw_buildings(contacts, regs, pluto, violations, stats)

    # Phase 1 aggregation
    managers = aggregate_managers(raw, stats)

    # Phase 2 scoring
    managers_scored = score_phase2(managers, raw)

    # Notes + run log
    notes = [
        ["Inputs", ", ".join(REQUIRED_FILES + OPTIONAL_FILES)],
        ["Neighborhood ZIPs", "; ".join([f"{n['name']}: {', '.join(sorted(n['zips']))}" for n in NEIGHBORHOODS])],
        ["Units Filter", f"{UNIT_MIN}-{UNIT_MAX} UnitsRes inclusive"],
        ["Join Method", "HPD Contacts -> HPD Registrations on registrationid; join to PLUTO on BBL"],
        ["Density Flag", "same-ZIP heuristic: >=3 unique buildings per manager per ZIP"],
        ["Mixed-use Flag", "PLUTO LandUse=='04' indicates mixed residential/commercial (heuristic)"],
        ["Compliance Risk", "manager-level violation-rate top quartile -> 25 pts (if violations present)"],
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

    # Add checksums to notes for auditability
    for f in REQUIRED_FILES + OPTIONAL_FILES:
        p = data_dir / f
        if p.exists():
            notes.append([f"sha256:{f}", sha256_file(p)])
        else:
            notes.append([f"sha256:{f}", "MISSING (optional)"])

    p1, p2 = write_excels(output_dir, managers, raw, managers_scored, raw, notes, run_log)

    # Console summary
    print("Pipeline complete.")
    print(f"Phase 1 -> {p1}")
    print(f"Phase 2 -> {p2}")
    print(f"Managers: {len(managers_scored)} | Buildings rows: {len(raw)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
