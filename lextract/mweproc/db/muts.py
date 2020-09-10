import logging
from ..models import UdMwe
from ..enrichment.freq import (
    headword_freq,
    turkudepsearch_freq,
    turkudepsearch_headword_freq,
    turkudepsearch_propbank_freqs,
    turkudepsearch_propbank_headword_freqs
)
from ..formatters.human import gapped_mwe, pos_template
from ..formatters.turkudepsearch import tds, tds_tok
from ..sources.wiktionary_headword import WiktionaryHeadwordLink
from ...utils.db import update
from .tables import tables
from wikiparse.utils.db import insert_get_id, insert

logger = logging.getLogger(__name__)


def listify_poses(poses):
    return list(poses) if poses is not None else None


def insert_mwe(session, mwe: UdMwe, hw_cnts_cache, freqs=False, materialize=False):
    gap_mwe = gapped_mwe(mwe)
    logger.info(f"Inserting %s", gap_mwe)
    mwe_id = insert_get_id(
        session,
        tables["ud_mwe"],
        typ=mwe.typ,
        poses=listify_poses(mwe.poses),
        headword_idx=mwe.headword_idx
    )
    for subword_idx, token in enumerate(mwe.tokens):
        insert(
            session,
            tables["ud_mwe_token"],
            mwe_id=mwe_id,
            subword_idx=subword_idx,
            payload=token.payload,
            payload_is_lemma=token.payload_is_lemma,
            poses=listify_poses(token.poses),
            feats=token.feats
        )
    for link in mwe.links:
        insert(
            session,
            tables["link"],
            mwe_id=mwe_id,
            name=link.link_name,
            payload=link.get_cols(),
        )
        if isinstance(link, WiktionaryHeadwordLink):
            insert(
                session,
                tables["wiktionary_hw_link"],
                mwe_id=mwe_id,
                page_exists=link.page_exists,
                has_senses=link.has_senses,
            )
    if freqs:
        insert_freqs(session, mwe_id, mwe, hw_cnts_cache)
    if materialize:
        insert(
            session,
            tables["mwe_fmt"],
            mwe_id=mwe_id,
            gapped_mwe=gapped_mwe(mwe),
            pos_info=pos_template(mwe),
            turkudepsearch=tds(mwe),
        )
        # TODO: separate defn from links


def insert_headword_freqs(session, mwe, lemma_query, lemma):
    freqs_res = headword_freq(mwe)
    assert freqs_res is not None
    headword_freq_id = insert_get_id(
        session,
        tables["headword_freq"],
        lemma_query=lemma_query,
        lemma=lemma,
        wordfreq=freqs_res[0],
        wordfreq_zipf=freqs_res[1],
        internet_parsebank_cnt=turkudepsearch_headword_freq(lemma_query),
    )
    hw_cnts = turkudepsearch_propbank_headword_freqs(lemma_query)
    for prop, cnt in hw_cnts.items():
        insert(
            session,
            tables["headword_propbank_freqs"],
            headword_freq_id=headword_freq_id,
            prop=prop,
            cnt=cnt,
        )
    return headword_freq_id, hw_cnts


def insert_freqs(session, mwe_id: int, mwe: UdMwe, hw_cnts_cache):
    from .queries import headword_id_query
    headword = mwe.headword
    if headword is not None:
        lemma_query = tds_tok(headword)
        headword_id = session.execute(headword_id_query(lemma_query)).fetchall()
        if not headword_id:
            lemma = headword.payload
            headword_id, hw_cnts = insert_headword_freqs(session, mwe, lemma_query, lemma)
            hw_cnts_cache[lemma_query] = hw_cnts
        else:
            hw_cnts = hw_cnts_cache[lemma_query]
        update(session, tables["ud_mwe"], mwe_id, headword_id=headword_id)

    query = tds(mwe)

    insert(
        session,
        tables["ud_mwe_freq"],
        mwe_id=mwe_id,
        internet_parsebank_cnt=turkudepsearch_freq(query),
    )

    frame_cnts = turkudepsearch_propbank_freqs(query)
    for prop, cnt in frame_cnts.items():
        insert(
            session,
            tables["frame_propbank_freqs"],
            mwe_id=mwe_id,
            prop=prop,
            cnt=cnt,
        )

    if headword is not None:
        from ..enrichment.propbank import Evaluator
        propbank_eval = Evaluator(hw_cnts, frame_cnts)
        for prop, surv in propbank_eval.survival_at_thresh(0.8):
            insert(
                session,
                tables["frame_propbank_surv"],
                mwe_id=mwe_id,
                prop=prop,
                surv=surv,
            )
