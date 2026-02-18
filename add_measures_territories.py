#!/usr/bin/env python
"""
Fetch measures and target_territories from the Directus API and add them
to the individual program YAML files.
"""

import yaml
import requests
from pathlib import Path

DIRECTUS_URL = (
    "https://opensanctions.directus.app/items/programs"
    "?limit=1000&sort=title"
    "&fields=id,measures.programs_measures_id.label,target_territories.territories_code"
)


def add_measures_territories():
    programs_dir = Path("programs")

    # Fetch resolved data from Directus
    response = requests.get(DIRECTUS_URL)
    response.raise_for_status()
    api_programs = response.json()["data"]

    # Build lookup by program id
    lookup: dict[int, dict] = {}
    for p in api_programs:
        measures = [
            m["programs_measures_id"]["label"]
            for m in p.get("measures", [])
            if m.get("programs_measures_id")
        ]
        territories = [
            t["territories_code"]
            for t in p.get("target_territories", [])
            if t.get("territories_code")
        ]
        lookup[p["id"]] = {
            "measures": sorted(measures),
            "target_territories": sorted(territories),
        }

    print(f"Fetched data for {len(lookup)} programs from Directus")

    # Update each program YAML file
    program_files = list(programs_dir.glob("*.yml"))
    print(f"Found {len(program_files)} program files")

    updated = 0
    for program_file in program_files:
        with open(program_file, "r") as f:
            data = yaml.safe_load(f)

        program_id = data.get("id")
        if program_id not in lookup:
            print(f"  Warning: {program_file.name} (id={program_id}) not found in API")
            continue

        resolved = lookup[program_id]

        # Only add non-empty lists
        if resolved["target_territories"]:
            data["target_territories"] = resolved["target_territories"]
        if resolved["measures"]:
            data["measures"] = resolved["measures"]

        with open(program_file, "w") as f:
            yaml.dump(
                data,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
                indent=2,
            )

        n_terr = len(resolved["target_territories"])
        n_meas = len(resolved["measures"])
        if n_terr or n_meas:
            print(f"  Updated {program_file.name}: {n_terr} territories, {n_meas} measures")
            updated += 1

    print(f"\nUpdated {updated} program files with measures/territories")


if __name__ == "__main__":
    add_measures_territories()
