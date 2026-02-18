#!/usr/bin/env python
"""
Remove date_ and user_ metadata fields from program and issuer YAML files.
"""

import yaml
from pathlib import Path


def clean_programs():
    """Remove metadata fields from program YAML files."""
    programs_dir = Path("programs")
    fields_to_remove = [
        "user_created",
        "user_updated",
        "date_created",
        "date_updated",
        "sort",
        "status",
    ]

    program_files = list(programs_dir.glob("*.yml"))
    print(f"Cleaning {len(program_files)} program files...")

    for program_file in program_files:
        with open(program_file, "r") as f:
            data = yaml.safe_load(f)

        # Remove metadata fields
        modified = False
        for field in fields_to_remove:
            if field in data:
                del data[field]
                modified = True

        if modified:
            # Write back with proper formatting
            with open(program_file, "w") as f:
                yaml.dump(
                    data,
                    f,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                    indent=2,
                )
            print(f"  Cleaned {program_file.name}")

    print("✓ Programs cleaned")


def clean_issuers():
    """Remove metadata fields from issuer YAML files."""
    issuers_dir = Path("programs/issuers")
    fields_to_remove = ["date_created", "date_updated", "status"]

    issuer_files = list(issuers_dir.glob("*.yml"))
    print(f"\nCleaning {len(issuer_files)} issuer files...")

    for issuer_file in issuer_files:
        with open(issuer_file, "r") as f:
            data = yaml.safe_load(f)

        # Remove date fields
        modified = False
        for field in fields_to_remove:
            if field in data:
                del data[field]
                modified = True

        if modified:
            # Write back with proper formatting
            with open(issuer_file, "w") as f:
                yaml.dump(
                    data,
                    f,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                    indent=2,
                )
            print(f"  Cleaned {issuer_file.name}")

    print("✓ Issuers cleaned")


def main():
    clean_programs()
    clean_issuers()
    print("\n✓ All metadata fields removed successfully")


if __name__ == "__main__":
    main()
