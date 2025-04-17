import functools
from dataclasses import dataclass
from typing import Optional

from zavod import Context
from zavod.stateful.model import program_table


@dataclass
class Program:
    id: int
    key: str
    title: Optional[str]
    url: Optional[str]


# Since we'll only ever have a few programs, it's cheaper to just read them all once.
@functools.cache
def get_all_programs_by_key(context: Context) -> dict[str, Program]:
    stmt = program_table.select()
    programs = [
        Program(
            id=row.id,
            key=row.key,
            title=row.title,
            url=row.url,
        )
        for row in context.conn.execute(stmt).fetchall()
    ]
    return {p.key: p for p in programs}


def get_program_by_key(context: Context, program_key: str) -> Optional[Program]:
    return get_all_programs_by_key(context).get(program_key, None)
