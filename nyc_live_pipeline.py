#!/usr/bin/env python3

import argparse
import pandas as pd
from pathlib import Path

TARGET_ZIPS = {
    "Upper East Side": {"10021", "10028", "10075"},
    "Bay Ridge": {"11209"},
    "Forest Hills / Rego Park": {"11375", "11374"}
}

def load_data(data_dir):
    data_dir = Path(data_dir)

    contacts = pd.read_csv(data_dir / "hpd_contacts.csv", dtype=str, low_memory=False)
    registrations = pd.read_csv(data_dir / "hpd_registrations.csv", dtype=str, low_memory=False)
    pluto = pd.read_csv(data_dir / "pluto.csv", dtype=str, low_memory=False)

    return contacts, registrations, pluto


def normalize_columns(df):
    df.columns = [c.lower() for c in df.columns]
    return df


def filter_buildings(pluto):
    pluto = normalize_columns(pluto)

    pluto["zipcode"] = pluto["zipcode"].astype(str).str.zfill(5)
    pluto["unitsres"] = pd.to_numeric(pluto["unitsres"], errors="coerce")

    zips = set().union(*TARGET_ZIPS.values())

    pluto = pluto[
        (pluto["zipcode"].isin(zips)) &
        (pluto["unitsres"] >= 25) &
        (pluto["unitsres"] <= 120)
    ]

    return pluto


def assign_neighborhood(zipcode):
    for name, zips in TARGET_ZIPS.items():
        if zipcode in zips:
            return name
    return None


def aggregate_managers(contacts, registrations, pluto):

    contacts = normalize_columns(contacts)
    registrations = normalize_columns(registrations)

    merged = contacts.merge(
        registrations,
        on="registrationid",
        how="left"
    )

    merged["bbl"] = merged["bbl"].astype(str)

    pluto["bbl"] = pluto["bbl"].astype(str)

    merged = merged.merge(
        pluto[["bbl", "zipcode", "unitsres"]],
        on="bbl",
        how="inner"
    )

    merged["neighborhood"] = merged["zipcode"].apply(assign_neighborhood)

    merged = merged.dropna(subset=["neighborhood"])

    grouped = merged.groupby("ownername").agg(
        buildings=("bbl", "nunique"),
        total_units=("unitsres", "sum"),
        avg_units=("unitsres", "mean"),
        zips=("zipcode", lambda x: ",".join(sorted(set(x))))
    ).reset_index()

    grouped["portfolio_flag"] = grouped["buildings"] >= 5
    grouped["avg_units"] = grouped["avg_units"].round(1)

    grouped = grouped.sort_values(
        ["buildings", "total_units"],
        ascending=False
    )

    return grouped, merged


def write_outputs(grouped, raw, output_dir):

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    phase1 = output_dir / "NYC_Multifamily_TargetManagers_Phase1.xlsx"
    phase2 = output_dir / "NYC_Multifamily_TargetManagers_Phase2_Scored.xlsx"

    with pd.ExcelWriter(phase1) as writer:
        grouped.to_excel(writer, sheet_name="Ranked_Managers", index=False)
        raw.to_excel(writer, sheet_name="Raw_Buildings", index=False)

    scored = grouped.copy()

    scored["priority_score"] = (
        scored["buildings"] * 5 +
        scored["total_units"] / 20
    )

    scored = scored.sort_values("priority_score", ascending=False)

    with pd.ExcelWriter(phase2) as writer:
        scored.to_excel(writer, sheet_name="Ranked_Managers_Scored", index=False)
        raw.to_excel(writer, sheet_name="Raw_Buildings_Scored", index=False)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", required=True)

    args = parser.parse_args()

    contacts, registrations, pluto = load_data(args.data_dir)

    pluto = filter_buildings(pluto)

    grouped, raw = aggregate_managers(contacts, registrations, pluto)

    write_outputs(grouped, raw, args.output_dir)

    print("Pipeline complete.")
    print(f"Managers identified: {len(grouped)}")
    print(f"Buildings analyzed: {len(raw)}")


if __name__ == "__main__":
    main()
