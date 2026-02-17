#!/usr/bin/env python
"""
Split programs YAML file into individual program files.
"""
import yaml
from pathlib import Path


def split_programs():
    # Read the programs file
    programs_file = Path("programs 20260217-161920.yaml")
    output_dir = Path("data/programs")

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load all programs from the file
    with open(programs_file, "r") as f:
        programs = yaml.safe_load(f)

    print(f"Found {len(programs)} programs")

    # Write each program to a separate file
    for program in programs:
        key = program.get("key")
        if not key:
            print(f"Warning: Program with id {program.get('id')} has no key, skipping")
            continue

        # Replace - with _ in the key for the filename
        filename = key.replace("-", "_") + ".yml"
        output_path = output_dir / filename

        # Write the program to its own file
        with open(output_path, "w") as f:
            yaml.dump(program, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        print(f"Written: {output_path}")

    print(f"\nSuccessfully split {len(programs)} programs into {output_dir}")


if __name__ == "__main__":
    split_programs()
