import functools
import yaml
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field


class Issuer(BaseModel):
    id: int
    name: str
    acronym: Optional[str] = None
    organisation: Optional[str] = None
    territory: Optional[str] = None
    status: str
    date_created: Optional[str] = None
    date_updated: Optional[str] = None


class Program(BaseModel):
    id: int
    key: str
    title: str
    url: Optional[str] = None
    summary: Optional[str] = None
    dataset: Optional[str] = None
    status: str
    issuer: Optional[Issuer] = None
    aliases: list[str] = Field(default_factory=list)
    sort: Optional[int] = None
    user_created: Optional[str] = None
    date_created: Optional[str] = None
    user_updated: Optional[str] = None
    date_updated: Optional[str] = None


@functools.cache
def _load_issuers() -> dict[str, Issuer]:
    """Load all issuers from YAML files, indexed by filename (without extension)."""
    root = Path(__file__).parent.parent.parent.parent
    return {
        path.stem: Issuer(**yaml.safe_load(path.read_text()))
        for path in (root / "programs" / "issuers").glob("*.yml")
    }


# Since we'll only ever have a few programs, it's cheaper to just read them all once.
@functools.cache
def get_all_programs_by_key() -> dict[str, Program]:
    root = Path(__file__).parent.parent.parent.parent
    issuers = _load_issuers()

    programs: list[Program] = []
    for path in (root / "programs").glob("*.yml"):
        data = yaml.safe_load(path.read_text())
        if not data:
            continue

        # Replace issuer reference with actual Issuer object
        issuer_key = data.get("issuer")
        if issuer_key and issuer_key in issuers:
            data["issuer"] = issuers[issuer_key].model_dump()
        else:
            data["issuer"] = None

        programs.append(Program(**data))

    return {p.key: p for p in programs}


def get_program_by_key(program_key: str) -> Optional[Program]:
    return get_all_programs_by_key().get(program_key, None)
