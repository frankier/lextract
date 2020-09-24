import contextlib
import enum
import logging
from enum import Enum
from operator import itemgetter
from collections import Counter
from typing import List, Tuple, Optional, Dict

import click_log
import click
from itertools import groupby
from wordfreq import word_frequency

from lextract.mweproc.consts import SURF, WILDCARD
from lextract.mweproc.db.queries import mwe_for_indexing
from lextract.keyed_db.tables import tables, extend_mweproc
from lextract.utils.lemmatise import fi_lemmatise
from wikiparse.utils.db import get_session, insert, insert_get_id

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


def get_key_idx(subwords):
    key_idx = 0
    min_freq = 1
    for idx, (_payload, _payload_is_lemma, _poses, feats) in enumerate(subwords):
        skip = False
        total_freq = 0
        for subword_lemma in feats.keys():
            if subword_lemma == WILDCARD:
                skip = True
                break
            total_freq += word_frequency(subword_lemma, "fi")
        if skip:
            continue
        if total_freq < min_freq:
            min_freq = total_freq
            key_idx = idx
    return key_idx


class IndexingResult(Enum):
    HEAD_INDEXED = enum.auto()
    RAREST_INDEXED = enum.auto()
    FAIL = enum.auto()


def insert_indexed(
    session,
    subwords_list: List[Tuple[Optional[str], bool, List[str], Dict[str, str]]],
    ud_mwe_headword_idx,
    ud_mwe_id,
    *,
    ignore_bare_lemma=True,
    lemmatise=fi_lemmatise,
    dry_run=False,
    add_surf=True,
) -> IndexingResult:
    subword_keys = []
    for (payload, payload_is_lemma, poses, feats) in subwords_list:
        bare_lemma = payload_is_lemma and len(feats) == 0 and not ignore_bare_lemma
        if bare_lemma or len(feats) > 0:
            assert payload is None or payload_is_lemma
            lemma = payload if payload is not None else WILDCARD
            keyed_feats = {lemma: {tuple(feats.items())}}
        elif payload is not None:
            keyed_feats = lemmatise(payload)
            if add_surf:
                keyed_feats.setdefault(payload.lower(), set()).add(((SURF, SURF),))
        else:
            # TODO: Might like to blacklist anything too simple composed with this open wildcard
            # e.g. we should probably just forget about headword + ___
            keyed_feats = {WILDCARD: set()}
        subword_keys.append(keyed_feats)
    if ud_mwe_headword_idx is not None and subword_keys[ud_mwe_headword_idx]:
        key_idx = ud_mwe_headword_idx
        key_is_head = True
    else:
        key_idx = get_key_idx(subwords_list)
        key_is_head = False
    assert key_idx is not None
    key_lemmas = list(subword_keys[key_idx].keys())
    if not len(key_lemmas):
        return IndexingResult.FAIL
    if not dry_run:
        word_id = insert_get_id(
            session,
            tables["word"],
            key_idx=key_idx,
            key_is_head=key_is_head,
            ud_mwe_id=ud_mwe_id
        )
    for lemma in key_lemmas:
        if not dry_run:
            insert(
                session,
                tables["key_lemma"],
                key_lemma=lemma,
                word_id=word_id,
            )
    for subword_idx, constrained_lemmas in enumerate(subword_keys):
        lemma_feats = {k: list(v) for k, v in constrained_lemmas.items()}
        if not dry_run:
            insert(
                session,
                tables["subword"],
                word_id=word_id,
                subword_idx=subword_idx,
                lemma_feats=lemma_feats
            )
    if key_is_head:
        return IndexingResult.HEAD_INDEXED
    else:
        return IndexingResult.RAREST_INDEXED


def add_keyed_words(
    session,
    mwe_it,
    ignore_bare_lemma: bool = True,
    add_surf: bool = True,
    dry_run: bool = False
):
    for (ud_mwe_id, ud_mwe_headword_idx), subwords in groupby(mwe_it, itemgetter(0, 1)):
        subwords_list = list((tpl[2:] for tpl in subwords))
        yield insert_indexed(
            session,
            subwords_list,
            ud_mwe_headword_idx,
            ud_mwe_id,
            ignore_bare_lemma=ignore_bare_lemma,
            add_surf=add_surf,
            dry_run=dry_run
        )


@click.command("add-keyed-words")
@click.option("--ignore-bare-lemma/--use-bare-lemma", default=True)
@click.option("--add-surf/--no-add-surf", default=True)
@click.option("--dry-run", is_flag=True)
@click_log.simple_verbosity_option(logger)
def add_keyed_words_cmd(ignore_bare_lemma: bool, add_surf: bool, dry_run: bool):
    """
    Index multiwords/inflections/frames into database
    """
    session = get_session()
    metadata = extend_mweproc()
    if not dry_run:
        metadata.create_all(session().get_bind().engine)
    inner_it = session.execute(mwe_for_indexing())
    if logger.isEnabledFor(logging.INFO):
        ctx = contextlib.nullcontext(inner_it)
    else:
        ctx = click.progressbar(inner_it, label="Inserting word keys")
    cnt: Counter = Counter()
    with ctx as outer_it:
        for indexing_result in add_keyed_words(session, outer_it, ignore_bare_lemma, add_surf, dry_run):
            if indexing_result == IndexingResult.HEAD_INDEXED:
                cnt["headword_idxd"] += 1
            elif indexing_result == IndexingResult.RAREST_INDEXED:
                cnt["rarest_idxd"] += 1
            else:
                cnt["fail_idxd"] += 1

    if not dry_run:
        session.commit()
