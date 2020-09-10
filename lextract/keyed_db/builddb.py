import contextlib
import logging
import click_log
import click

from lextract.keyed_db.tables import metadata
from lextract.mweproc.core import all_wordlists, WORDLIST_NAMES
from lextract.keyed_db.tables import metadata, key_lemma as key_lemma_t, word as word_t, subword as subword_t
from wikiparse.db.tables import headword as headword_t
from wikiparse.utils.db import get_session, insert, insert_get_id

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


def insert_indexed(session, indexed, dry=False):
    for form, sources, word_type, key_idx, subwords, payload in indexed:
        logger.info(f"Inserting %s %s from %s", word_type, form, " ".join(sources))
        if dry:
            continue
        if payload is None:
            payload = {}
        word_id = insert_get_id(session, word_t, key_idx=key_idx, form=form, type=word_type, sources=sources, payload=payload)
        key_lemmas = list(subwords[key_idx][1].keys())
        assert len(key_lemmas) >= 1
        for lemma in key_lemmas:
            insert(session, key_lemma_t, key_lemma=lemma, word_id=word_id)
        for subword_idx, (subword_form, subword_feats) in enumerate(subwords):
            lemma_feats = {k: list(v) for k, v in subword_feats.items()}
            insert(session, subword_t, word_id=word_id, subword_idx=subword_idx, form=subword_form, lemma_feats=lemma_feats)
    session.commit()


@click.command()
@click.option("--wl", type=click.Choice(WORDLIST_NAMES), multiple=True,
              default=WORDLIST_NAMES)
@click.option("--filter-by", type=click.File("r"))
@click.option("--dry-run", is_flag=True)
@click_log.simple_verbosity_option(logger)
def add_keyed_words(wl, filter_by, dry_run):
    """
    Index multiwords/inflections/frames into database
    """
    included = None
    if filter_by is not None:
        included = []
        for line in filter_by:
            line = line.strip()
            if not line:
                continue
            included.append(line)
    session = get_session()
    if not dry_run:
        metadata.create_all(session().get_bind().engine)
    """
    indexed = indexed_wordlists(session, wl)
    if logger.isEnabledFor(logging.INFO):
        ctx = contextlib.nullcontext(indexed)
    else:
        ctx = click.progressbar(indexed, label="Inserting word keys")
    with ctx as indexed:
        if included is not None:
            indexed = (tpl for tpl in indexed if tpl[0] in included)
        if not dry_run:
            insert_indexed(session, indexed, dry=dry_run)
    """
    if not dry_run:
        session.commit()
