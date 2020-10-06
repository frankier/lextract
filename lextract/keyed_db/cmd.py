import click
from wikiparse.cmd.mk_db import mk_cmds

from lextract.keyed_db.builddb import add_keyed_words_cmd
from lextract.keyed_db.repl import extract_toks_cmd


def mk_metadata():
    from lextract.keyed_db.tables import extend_mweproc
    return extend_mweproc()


merged = click.CommandCollection(
    sources=[mk_cmds(mk_metadata), click.Group(
        commands={
            "add-keyed-words": add_keyed_words_cmd,
            "extract-toks": extract_toks_cmd
        }
    )],
    help="Commands for lextract ETL",
)
