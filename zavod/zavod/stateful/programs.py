import functools
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from zavod import Context
from zavod.stateful.model import Program


# Since we'll only ever have a few positions, it's cheaper to just read them all once.
@functools.cache
def get_all_positions_by_key(context: Context) -> dict[str, Program]:
    session = Session(context.conn)
    programs = session.scalars(select(Program)).all()
    return {p.key: p for p in programs}


def get_program_by_key(context: Context, program_key: str) -> Optional[Program]:
    return get_all_positions_by_key(context).get(program_key, None)
