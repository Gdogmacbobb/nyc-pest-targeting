#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import re
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
    s = s.mask(s.str.lower().isin({"nan", "none"}), "")
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


def canon_bbl_series(s: pd.Series) -> pd.Series:
    """
    Canonicalize BBL to 10-digit numeric string:
      - cast to str
      - remove scientific notation artifacts by extracting digits
      - remove non-digits
      - zfill(10)
      - invalid -> <NA>
    """
    s = s.astype(str).str.strip()

    # Common Socrata float export: "1012345678.0"
    s = s.str.replace(r"\.0$", "", regex=True)

    # If scientific notation slipped in, extract digits only
    s = s.str.replace(r"[^\d]", "", regex=True)

    # pad
    s = s.str.zfill(10)

    # keep only real 10-digit numbers (not all zeros)
    s = s.mask(~s.str.fullmatch(r"\d{10}"), pd.NA)
    s = s.mask(s == "0000000000", pd.NA)
    return s


def build_bbl_from_components(df: pd.DataFrame) -> Optional[pd.Series]:
    """
    If boro/block/lot exist, build a canonical 10-digit BBL.
    Returns series or None.
    """
    if {"boro", "block", "lot"}.issubset(df.columns):
        boro = df["boro"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(1)
        block = df["block"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)
        lot = df["lot"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(4)
        return canon_bbl_series(boro + block + lot)

    if {"borough", "block", "lot"}.issubset(df.columns):
        boro = df["borough"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(1)
        block = df["block"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)
        lot = df["lot"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(4)
        return canon_bbl_series(boro + block + lot)

    return None


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

    # Prefer building BBL from components if available (most reliable).
    bbl_from_parts = build_bbl_from_components(pluto)
    if bbl_from_parts is not None:
        pluto["bbl"] = bbl_from_parts
    else:
        # Otherwise use existing bbl-ish column
        bbl_col = first_existing_col(pluto, ["bbl", "borough_block_lot", "boro_block_lot"])
        if not bbl_col:
            raise ValueError(f"PLUTO: cannot find or build BBL. Cols sample: {list(pluto.columns)[:80]}")
        pluto["bbl"] = canon_bbl_series(pluto[bbl_col])

    zip_col = first_existing_col(pluto, ["zipcode", "zip", "zip_code"])
    if not zip_col:
        raise ValueError(f"PLUTO: cannot find zipcode column. Cols sample: {list(pluto.columns)[:80]}")

    units_col = first_existing_col(pluto, ["unitsres", "units_res", "residential_units"])
    if not units_col:
        # last resort
        candidates = [c for c in pluto.columns if "units" in c and "res" in c]
        if candidates:
            units_col = candidates[0]
        else:
            raise ValueError(f"PLUTO: cannot find UnitsRes column. Cols sample: {list(pluto.columns)[:80]}")

    pluto["zipcode"] = zfill_zip(pluto[zip_col])
    pluto["unitsres"] = pd.to_numeric(pluto[units_col], errors="coerce")

    year_col = first_existing_col(pluto, ["yearbuilt", "year_built"])
    pluto["yearbuilt"] = pd.to_numeric(pluto[year_col], errors="coerce") if year_col else pd.NA

    landuse_col = first_existing_col(pluto, ["landuse", "land_use"])
    pluto["landuse"] = pluto[landuse_col] if landuse_col else pd.NA

    # Drop rows with invalid BBL early
    pluto = pluto.dropna(subset=["bbl"])

    return pluto


def prepare_contacts_and_regs(contacts: pd.DataFrame, regs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    contacts = norm_cols(contacts)
    regs = norm_cols(regs)

    regid_c = first_existing_col(contacts, ["registrationid", "registration_id"])
    regid_r = first_existing_col(regs, ["registrationid", "registration_id"])
    if not regid_c or not regid_r:
        raise ValueError("HPD: missing registrationid in contacts/registrations")

    if regid_c != "registrationid":
        contacts = contacts.rename(columns={regid_c: "registrationid"})
    if regid_r != "registrationid":
        regs = regs.rename(columns={regid_r: "registrationid"})

    # Ensure registrations has canonical BBL
    bbl_col = first_existing_col(regs, ["bbl", "borough_block_lot", "boro_block_lot"])
    bbl_from_parts = build_bbl_from_components(regs)
    if bbl_from_parts is not None:
        regs["bbl"] = bbl_from_parts
    elif bbl_col:
        regs["bbl"] = canon_bbl_series(regs[bbl_col])
    else:
        raise ValueError(f"HPD Registrations: cannot find/build BBL. Cols sample: {list(regs.columns)[:80]}")

    regs = regs.dropna(subset=["bbl"])

    # Managing agent name field (expand list)
    agent_name_col = first_existing_col(
        contacts,
        [
            "managing_agent", "managingagent",
            "agent_name", "agentname",
            "contact_name",
            "ownername", "owner_name",
            "ownercorpname", "corporateownername",
            "registrant_name",
            "business_name", "company_name",
            "name", "entity_name",
        ],
    )

    if not agent_name_col:
        # heuristic fallback: pick a column with "name/owner/agent" in it
        name_like = [c for c in contacts.columns if any(k in c for k in ["name", "owner", "agent", "entity", "company"])]
        name_like = [c for c in name_like if c not in {"registrationid"}]
        if name_like:
            agent_name_col = name_like[0]

    if not agent_name_col:
        raise ValueError(f"HPD Contacts: cannot determine name column. Cols sample: {list(contacts.columns)[:120]}")

    contacts = contacts.rename(columns={agent_name_col: "managing_agent"})

    addr_col = first_existing_col(
        contacts,
        ["mailing_address", "mailingaddress", "businessaddress", "business_address", "address", "contact_address", "streetaddress", "street_address"]
    )
    phone_col = first_existing_col(
        contacts,
        ["phone", "phone1", "businessphone", "business_phone", "contact_phone", "phonenumber", "phone_number"]
    )

    if addr_col:
        contacts = contacts.rename(columns={addr_col: "mailing_address"})
    else:
        contacts["mailing_address"] = ""

    if phone_col:
        contacts = contacts.rename(columns={phone_col: "phone"})
    else:
        contacts["phone"] = ""

    contacts["managing_agent"] = contacts["managing_agent"].astype(str).str.strip()
    contacts["mailing_address"] = contacts["mailing_address"].astype(str).str.strip()
    contacts["phone"] = contacts["phone"].astype(str).str.strip()

    return contacts, regs


def prepare_violations(violations: pd.DataFrame) -> pd.DataFrame:
    violations = norm_cols(violations)
    bbl_col = first_existing_col(violations, ["bbl", "borough_block_lot", "boro_block_lot"])
    bbl_from_parts = build_bbl_from_components(violations)
    if bbl_from_parts is not None:
        violations["bbl"] = bbl_from_parts
    elif bbl_col:
        violations["bbl"] = canon_bbl_series(violations[bbl_col])
    else:
        # optional dataset; if schema unknown just return empty
        violations["bbl"] = pd.NA

    violations = violations.dropna(subset=["bbl"])
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
    merged = contacts.merge(regs[["registrationid", "bbl"]], on="registrationid", how="left")
    stats.merged_contacts_regs_rows = len(merged)

    merged["bbl"] = canon_bbl_series(merged["bbl"])
    merged = merged.dropna(subset=["bbl"])

    # JOIN to PLUTO (this is where you were getting 0)
    merged2 = merged.merge(
        pluto[["bbl", "zipcode", "unitsres", "yearbuilt", "landuse"]],
        on="bbl",
        how="inner",
    )
    stats.merged_all_rows = len(merged2)

    if stats.merged_all_rows == 0:
        # Diagnostic: show examples of BBL formats
        sample_reg = regs["bbl"].dropna().astype(str).head(5).tolist()
        sample_plu = pluto["bbl"].dropna().astype(str).head(5).tolist()
        raise ValueError(
            "BBL join to PLUTO produced 0 rows. This indicates BBL format mismatch.\n"
            f"Sample regs BBL: {sample_reg}\n"
            f"Sample pluto BBL: {sample_plu}\n"
            "Ensure both are canonical 10-digit numeric strings."
        )

    tz = all_target_zips()
    merged2 = merged2[merged2["zipcode"].isin(tz)]
    merged2 = merged2[(merged2["unitsres"] >= UNIT_MIN) & (merged2["unitsres"] <= UNIT_MAX)]

    merged2["neighborhood"] = merged2["zipcode"].apply(assign_neighborhood)
    merged2 = merged2.dropna(subset=["neighborhood"])
    stats.filtered_rows = len(merged2)

    if violations is not None and len(violations) > 0:
        v_counts = violations.groupby("bbl").size().reset_index(name="violation_count")
        merged2 = merged2.merge(v_counts, on="bbl", how="left")
        merged2["violation_count"] = pd.to_numeric(merged2["violation_count"], errors="coerce").fillna(0).astype(int)
    else:
        merged2["violation_count"] = 0

    merged2["mixed_use_flag"] = merged2["landuse"].astype(str).str.strip().eq("04")
    merged2["_zip_buildings_for_manager"] = merged2.groupby(["managing_agent", "zipcode"])["bbl"].transform("nunique")
    merged2["density_flag"] = merged2["_zip_buildings_for_manager"] >= 3

    return merged2


# -----------------------------
# Aggregate managers (Phase 1)
# -----------------------------
def aggregate_managers(raw: pd.DataFrame, stats: RunStats) -> pd.DataFrame:
    raw["managing_agent_norm"] = raw["managing_agent"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

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

    portfolio_strength = ((scored["# Buildings Managed"] >= 5).astype(int) * 15) + (
        (scored["Total Units"] >= 300).astype(int) * 15
    )

    violation_rate = (scored["Violation Count (Total)"] / scored["# Buildings Managed"]).fillna(0)
    q75 = violation_rate.quantile(0.75) if len(violation_rate) else 0
    compliance_risk = (violation_rate >= q75).astype(int) * 25 if q75 > 0 else 0

    avg_units = scored["Avg Units per Building"]
    rev_potential = ((avg_units >= 40) & (avg_units <= 100)).astype(int) * 15
    rev_potential += (scored["Mixed-use Buildings Count"] > 0).astype(int) * 10

    density_score = scored["Density Flag"].astype(int) * 20

    scored["Violation Score"] = compliance_risk
    scored["Revenue Potential Score"] = rev_potential
    scored["Density Score"] = density_score

    scored["Total Priority Score (0-100)"] = (portfolio_strength + compliance_risk + rev_potential + density_score).clip(
        upper=100
    )

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
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)

    stats = RunStats()

    contacts, regs, pluto, violations = load_inputs(data_dir)
    stats.contacts_rows = len(contacts)
    stats.registrations_rows = len(regs)
    stats.pluto_rows = len(pluto)
    stats.violations_rows = len(violations) if violations is not None else 0

    pluto = prepare_pluto(pluto)
    contacts, regs = prepare_contacts_and_regs(contacts, regs)
    if violations is not None:
        violations = prepare_violations(violations)

    raw = build_raw_buildings(contacts, regs, pluto, violations, stats)
    managers = aggregate_managers(raw, stats)
    managers_scored = score_phase2(managers)

    notes = [
        ["Inputs", ", ".join(REQUIRED_FILES + OPTIONAL_FILES)],
        ["Neighborhood ZIPs", "; ".join([f"{n['name']}: {', '.join(sorted(n['zips']))}" for n in NEIGHBORHOODS])],
        ["Units Filter", f"{UNIT_MIN}-{UNIT_MAX} UnitsRes inclusive"],
        ["Join Method", "HPD Contacts -> HPD Registrations on registrationid; join to PLUTO on canonical 10-digit BBL"],
        ["BBL Canonicalization", "digits-only + zfill(10) applied to registrations/PLUTO and post-merge"],
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
