import click
from wikiparse.cmd.mk_db import mk_cmds

from lextract.keyed_db.tables import metadata
from lextract.keyed_db.builddb import add_keyed_words
from lextract.keyed_db.repl import extract_toks_cmd


db_group = mk_cmds(metadata)

merged = click.CommandCollection(
    sources=[db_group, click.Group(
        commands={
            "add_keyed_words": add_keyed_words,
            "extract-toks": extract_toks_cmd
        }
    )],
    help="Commands for lextract ETL",
)
