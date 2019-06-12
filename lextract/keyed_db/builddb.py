import contextlib
import logging
import click_log
import click
import heapq
from more_itertools import groupby_transform
from sqlalchemy import select
from itertools import chain

from wordfreq import word_frequency
from lextract.keyed_db.tables import metadata, key_lemma as key_lemma_t, word as word_t, subword as subword_t
from wikiparse.tables import headword as headword_t
from wikiparse.utils.db import get_session, insert, insert_get_id
from wikiparse.parse_assoc import proc_assoc
from lextract.aho_corasick.fin import FIN_SPACE
from operator import itemgetter
from .consts import WILDCARD
from .utils import fi_lemmatise
from .queries import wiktionary_gram_query
from wikiparse.gram_words import CASES
from finntk.data.omorfi_normseg import CASE_NAME_MAP
from finntk.data.wiktionary_normseg import CASE_NORMSEG_MAP
from finntk.wordnet import has_abbrv

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


WIKTIONARY_TO_OMORFI_CASE_MAP = {v: k for k, v in CASE_NAME_MAP.items()}


def null_lemmatise(x):
    return {x: [{}]}


def index_word(word, lemmatise=null_lemmatise):
    key_idx = 0
    subwords = []
    if len(word) == 1:
        word_type = "inflection"
        lemma_feats = lemmatise(word[0])
        #if word[0] in lemma_feats.keys():
            # XXX: We don't want to add lemmas because they will get looked up
            # anyway during decompounding
            # -but-
            # It could be that this is actually a word form that happens
            # to be the same as some lemma. Happens a lot with infinitives
            # versus derived words. Haven't figured out whether this is a
            # problem in practice. POS information could help.
            #return
        subwords.append((word[0], lemma_feats))
    else:
        word_type = "multiword"
        min_freq = 1
        for idx, subword in enumerate(word):
            total_freq = 0
            lemma_feats = lemmatise(subword)
            for subword_lemma in lemma_feats.keys():
                total_freq += word_frequency(subword_lemma, "fi")
            if total_freq < min_freq:
                min_freq = total_freq
                key_idx = idx
            subwords.append((subword, lemma_feats))
    return word_type, key_idx, subwords


def index_wordlist(words, lemmatise=null_lemmatise):
    for word, sources in words:
        res = index_word(word, lemmatise)
        if res is None:
            continue
        word_type, key_idx, subwords = res
        yield " ".join(word), list(sources), word_type, key_idx, subwords, None


def insert_indexed(session, indexed, lemmatise=null_lemmatise):
    for form, sources, word_type, key_idx, subwords, payload in indexed:
        logger.info(f"Inserting %s %s from %s", word_type, form, " ".join(sources))
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


def wordnet_wordlist():
    from lextract.wordnet.fin import Wordnet as FinWordnet
    all_lemmas = []
    for lemma in FinWordnet.lemma_names().keys():
        all_lemmas.append(FIN_SPACE.split(lemma))
    all_lemmas.sort()
    return all_lemmas


def wiktionary_wordlist(session):
    headwords = session.execute(select([headword_t.c.name]).select_from(headword_t).order_by(headword_t.c.name))
    all_lemmas = []
    for (word,) in headwords:
        if has_abbrv(word):
            continue
        all_lemmas.append(word.split(" "))
    all_lemmas.sort()
    return all_lemmas


def wiktionary_frames(session, lemmatise=fi_lemmatise):

    def headword_subword():
        # At the moment the headword should always be in lemma form
        # return (word, lemmatise(word))
        return (word, {word: {()}})

    grams = session.execute(wiktionary_gram_query())
    for word, sense_id, pos, extra in grams:
        if pos != "Verb":
            # XXX: Possibly grammatical notes for other POSs could be taken
            # into account later. How many words actually have them?
            continue
        form_bits = []
        subwords = []
        headword_idx = None
        for cmd, payload in proc_assoc(extra["raw_defn"]):
            if cmd == "headword":
                assert headword_idx is None
                headword_idx = len(subwords)
                subwords.append(headword_subword())
                form_bits.append(word)
            elif cmd in ("subj", "obj"):
                if payload in CASES:
                    mapped_case = WIKTIONARY_TO_OMORFI_CASE_MAP.get(payload)
                    if mapped_case is not None:
                        subwords.append((payload, {WILDCARD: {(("case", mapped_case.upper()),)}}))
                        mapped_normseg = CASE_NORMSEG_MAP.get(payload)
                        if mapped_normseg is not None:
                            form_bits.append("___" + mapped_normseg)
                else:
                    # TODO: deal with ASSOC_POS and `direct object`
                    pass
            elif cmd == "verb":
                # TODO: Use transitive/intransitive on headword as an additional clue
                # TODO: Add requirement for verb that is not the headword
                pass
            elif cmd == "assoc":
                subwords.append((payload, lemmatise(payload)))
                form_bits.append(payload)
        if headword_idx is None:
            headword_idx = 0
            subwords.insert(0, headword_subword())
            form_bits.insert(0, word)
        if len(subwords) < 2:
            continue
        # XXX: Headword may not always be the best choice of key
        # -- but at least it always has a lemma!
        yield (
            " ".join(form_bits),
            ("wiktionary_frames",),
            "frame",
            headword_idx,
            subwords, {
                "type": "wiktionary_frame",
                "sense_id": sense_id,
            }
        )


def combine_wordlists(*wordlist_source_pairs):
    merged = heapq.merge(((word, source) for word_list, source in wordlist_source_pairs for word in word_list))
    return groupby_transform(merged, itemgetter(0), itemgetter(1))


def wordlists(session, wl):
    wordlist_its = []
    if "wordnet" in wl:
        wordlist_its.append((wordnet_wordlist(), "wordnet"))
    if "wiktionary" in wl:
        wordlist_its.append((wiktionary_wordlist(session), "wiktionary"))
    return combine_wordlists(*wordlist_its)


WORDLIST_NAMES = ["wordnet", "wiktionary", "wiktionary_frames"]


@click.command()
@click.option("--wl", type=click.Choice(WORDLIST_NAMES), multiple=True, default=WORDLIST_NAMES)
@click_log.simple_verbosity_option(logger)
def add_keyed_words(wl):
    """
    Index multiwords/inflections/frames into database
    """
    session = get_session()
    metadata.create_all(session().get_bind().engine)
    indexed = index_wordlist(wordlists(session, wl))
    if "wiktionary_frames" in wl:
        indexed = wiktionary_frames(session)
    if logger.isEnabledFor(logging.INFO):
        ctx = contextlib.nullcontext(indexed)
    else:
        ctx = click.progressbar(indexed, label="Inserting word keys")
    with ctx as indexed:
        insert_indexed(session, indexed, fi_lemmatise)
    session.commit()
