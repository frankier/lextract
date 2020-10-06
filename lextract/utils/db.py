import os
from typing import Dict, Any

from sqlalchemy import create_engine


_query_cache: Dict[Any, Any] = {}


def get_connection(db=None):
    # TODO: Increase cache size for sqlite to 32768 at this point?
    # TODO: Enable mmap for sqlite at this point?

    if db is None:
        db = os.getenv("DATABASE_URL")
        if db is None:
            raise RuntimeError("DATABASE_URL not set")
    engine = create_engine(
        db, execution_options={"autocommit": False, "compiled_cache": _query_cache}
    )

    return engine.connect()


def update(session, table, pk, **kwargs):
    return session.execute(table.update().where(id=pk).values(**kwargs))
