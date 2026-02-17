#!/usr/bin/env python
"""
Update program files to reference issuers by basename instead of ID.
"""

import yaml
from pathlib import Path


def update_program_issuers():
    issuers_dir = Path("programs/issuers")
    programs_dir = Path("programs")

    # Build a dict of issuer id → basename
    issuer_id_to_basename = {}

    # Read all issuer files recursively
    issuer_files = list(issuers_dir.rglob("*.yml"))
    print(f"Found {len(issuer_files)} issuer files")

    for issuer_file in issuer_files:
        with open(issuer_file, "r") as f:
            issuer = yaml.safe_load(f)

        issuer_id = issuer.get("id")
        if issuer_id is None:
            print(f"Warning: Issuer file {issuer_file} has no id, skipping")
            continue

        # Get basename without extension
        basename = issuer_file.stem
        issuer_id_to_basename[issuer_id] = basename
        print(f"Mapped issuer id {issuer_id} → {basename}")

    print(f"\nBuilt mapping for {len(issuer_id_to_basename)} issuers")

    # Process all program files
    program_files = [f for f in programs_dir.glob("*.yml")]
    print(f"Found {len(program_files)} program files")

    updated_count = 0
    for program_file in program_files:
        # Read the program file
        with open(program_file, "r") as f:
            program = yaml.safe_load(f)

        issuer_id = program.get("issuer")
        if issuer_id is None:
            print(f"Warning: Program {program_file.name} has no issuer field, skipping")
            continue

        # Check if issuer_id is already a string (already converted)
        if isinstance(issuer_id, str):
            print(f"Program {program_file.name} already has string issuer: {issuer_id}")
            continue

        # Look up the issuer basename
        if issuer_id not in issuer_id_to_basename:
            print(
                f"Warning: Program {program_file.name} references unknown issuer id {issuer_id}, skipping"
            )
            continue

        basename = issuer_id_to_basename[issuer_id]

        # Update the issuer field
        program["issuer"] = basename

        # Write the updated program back to the file
        with open(program_file, "w") as f:
            yaml.dump(
                program,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
                indent=2,
            )

        print(f"Updated {program_file.name}: issuer {issuer_id} → {basename}")
        updated_count += 1

    print(f"\nSuccessfully updated {updated_count} program files")


if __name__ == "__main__":
    update_program_issuers()
