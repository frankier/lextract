import click
from wikiparse.cmd.mk_db import mk_cmds

from lextract.keyed_db.tables import metadata
from lextract.keyed_db.builddb import add_keyed_words


db_group = mk_cmds(metadata)

merged = click.CommandCollection(
    sources=[db_group, click.Group(commands={"add_keyed_words": add_keyed_words})],
    help="Commands for lextract ETL",
)
