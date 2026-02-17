#!/usr/bin/env python
"""
Split issuers YAML file into individual issuer files.
"""

import re
import yaml
from pathlib import Path

def slugify(text: str | int) -> str:
    """Convert text to a slug suitable for filenames (lowercase with underscores)."""
    text_str = str(text).lower()
    text_str = re.sub(r"[^a-z0-9]+", "_", text_str)
    return text_str.strip("_")


def split_issuers():
    # Read the issuers file
    issuers_file = Path("issuers 20260217-16204.yaml")
    base_dir = Path("programs/issuers")

    # Load all issuers
    with open(issuers_file, "r") as f:
        issuers = yaml.safe_load(f)

    print(f"Found {len(issuers)} issuers")

    # Write each issuer to a separate file
    for issuer in issuers:
        # Determine the slugified name
        acronym = issuer.get("acronym")
        organisation = issuer.get("organisation")

        if acronym:
            slugified_name = slugify(acronym)
        elif organisation:
            slugified_name = slugify(organisation)
        else:
            raise AssertionError(
                f"Issuer with id {issuer.get('id')} has neither acronym nor organisation"
            )

        # Get territory
        territory = issuer.get("territory")
        if not territory:
            print(
                f"Warning: Issuer with id {issuer.get('id')} has no territory, using 'unknown'"
            )
            territory = "unknown"

        # Create directory structure
        issuer_dir = base_dir / territory
        issuer_dir.mkdir(parents=True, exist_ok=True)

        # Create filename
        filename = f"{slugified_name}.yml"
        output_path = issuer_dir / filename

        # Write the issuer to its own file
        with open(output_path, "w") as f:
            yaml.dump(
                issuer, f, default_flow_style=False, allow_unicode=True, sort_keys=False, indent=2
            )

        print(f"Written: {output_path}")

    print(f"\nSuccessfully split {len(issuers)} issuers into {base_dir}")


if __name__ == "__main__":
    split_issuers()
