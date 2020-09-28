import contextlib
import logging
import click_log
import click
import os

from .core import all_wordlists, WORDLIST_NAMES
from .db.confs import setup_dist, setup_embed
from .db.tables import metadata
from .db.muts import insert_mwe
from ..utils.db import get_connection
from wikiparse.cmd.parse import mod_data_opt, fsts_dir_opt, parse_filterfile

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

BATCH_SIZE = 50000


@click.command()
@click.option("--embed/--dist")
@click.option("--skip-freqs/--include-freqs", is_flag=True)
@click.option("--wl", type=click.Choice(WORDLIST_NAMES), multiple=True,
              default=WORDLIST_NAMES)
@click.option("--headwords", type=click.File("r"))
@mod_data_opt
@fsts_dir_opt
@click_log.simple_verbosity_option(logger)
def builddb(embed, skip_freqs, wl, headwords):
    """
    Insert MWEs into database
    """
    headwords_list = parse_filterfile(headwords)
    conn = get_connection()
    if embed:
        setup_embed()
    else:
        conn.execute("PRAGMA synchronous = OFF;")
        conn.execute("PRAGMA journal_mode = OFF;")
        setup_dist()
    metadata.create_all(conn)
    if not embed:
        wikiparse_db = os.getenv("WIKIPARSE_URL")
        if wikiparse_db is None:
            raise RuntimeError("WIKIPARSE_URL not set")
        wikiparse_conn = get_connection(wikiparse_db)
    else:
        wikiparse_conn = conn
    mwes = all_wordlists(wikiparse_conn, wl, headwords_list)
    if logger.isEnabledFor(logging.INFO):
        ctx = contextlib.nullcontext(mwes)
    else:
        ctx = click.progressbar(mwes, label="Inserting MWEs")
    with ctx as mwes_wrap:
        trans = conn.begin()
        hw_cnts_cache = {}
        try:
            for idx, (ud_mwe, mwe_wls) in enumerate(mwes_wrap):
                insert_mwe(
                    conn,
                    ud_mwe,
                    hw_cnts_cache,
                    freqs=not embed and not skip_freqs,
                    materialize=not embed
                )
                if (idx + 1) % BATCH_SIZE == 0:
                    trans.commit()
                    trans = conn.begin()
        except Exception:
            trans.rollback()
            raise
        else:
            trans.commit()
    if not embed:
        conn.execute("PRAGMA optimize;")
        conn.execute("VACUUM;")
