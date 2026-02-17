import functools
import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Program:
    id: int
    key: str
    title: Optional[str]
    url: Optional[str]


# Since we'll only ever have a few programs, it's cheaper to just read them all once.
@functools.cache
def get_all_programs_by_key() -> dict[str, Program]:
    programs = [
        Program(
            id=data["id"],
            key=data["key"],
            title=data.get("title"),
            url=data.get("url"),
        )
        for path in Path("programs").glob("*.yml")
        if (data := yaml.safe_load(path.read_text()))
    ]
    return {p.key: p for p in programs}


def get_program_by_key(program_key: str) -> Optional[Program]:
    return get_all_programs_by_key().get(program_key, None)
