import click
from wikiparse.cmd.mk_db import mk_cmds

from lextract.keyed_db.builddb import add_keyed_words_cmd
from lextract.keyed_db.repl import extract_toks_cmd


#db_group = mk_cmds(metadata)

merged = click.CommandCollection(
    sources=[click.Group(
        commands={
            "add-keyed-words": add_keyed_words_cmd,
            "extract-toks": extract_toks_cmd
        }
    )],
    help="Commands for lextract ETL",
)
