#!/usr/bin/env python
"""
Merge program aliases into individual program files.
"""

import yaml
from pathlib import Path
from collections import defaultdict


def merge_aliases():
    # Read the aliases file
    aliases_file = Path("programs_aliases 20260217-162026.yaml")
    programs_dir = Path("programs")

    # Load all aliases
    with open(aliases_file, "r") as f:
        aliases = yaml.safe_load(f)

    print(f"Found {len(aliases)} aliases")

    # Group aliases by program id
    aliases_by_program = defaultdict(list)
    for alias_entry in aliases:
        program_id = alias_entry.get("program")
        alias_text = alias_entry.get("alias")

        if program_id is not None and alias_text:
            aliases_by_program[program_id].append(alias_text)
        elif program_id is None:
            print(f"Warning: Alias '{alias_text}' has no program id, skipping")

    print(f"Found aliases for {len(aliases_by_program)} programs")

    # Process all program files
    program_files = list(programs_dir.glob("*.yml"))
    print(f"Found {len(program_files)} program files")

    updated_count = 0
    for program_file in program_files:
        # Read the program file
        with open(program_file, "r") as f:
            program = yaml.safe_load(f)

        program_id = program.get("id")
        if program_id is None:
            print(f"Warning: Program file {program_file.name} has no id, skipping")
            continue

        # Check if this program has aliases
        if program_id in aliases_by_program:
            # Add aliases to the program
            program["aliases"] = aliases_by_program[program_id]

            # Write the updated program back to the file
            with open(program_file, "w") as f:
                yaml.dump(
                    program,
                    f,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                )

            print(
                f"Updated {program_file.name} (id={program_id}) with {len(aliases_by_program[program_id])} alias(es)"
            )
            updated_count += 1

    print(f"\nSuccessfully updated {updated_count} program files with aliases")


if __name__ == "__main__":
    merge_aliases()
