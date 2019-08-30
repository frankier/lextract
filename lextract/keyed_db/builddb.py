import contextlib
import logging
import click_log
import click
import heapq
from itertools import groupby
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
from finntk.data.wordnet import ALL_ABBRVS, PRON_CASE
from finntk.wordnet import has_abbrv

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


WIKTIONARY_TO_OMORFI_CASE_MAP = {v: k for k, v in CASE_NAME_MAP.items()}


def null_lemmatise(x):
    return {x: [{}]}


def get_key_idx(subwords):
    key_idx = 0
    min_freq = 1
    for idx, (_subword, lemma_feats) in enumerate(subwords):
        skip = False
        total_freq = 0
        for subword_lemma in lemma_feats.keys():
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


def index_word(word, lemmatise=null_lemmatise):
    subwords = []
    if len(word) == 1:
        key_idx = 0
        word_type = "inflection"
        lemma_feats = lemmatise(word[0])
        lemmas = list(lemma_feats.keys())
        if lemmas == [word[0]]:
            # We don't want to add lemmas because they will get looked up
            # anyway during decompounding
            return
        subwords.append((word[0], lemma_feats))
    else:
        word_type = "multiword"
        for subword in word:
            lemma_feats = lemmatise(subword)
            subwords.append((subword, lemma_feats))
        key_idx = get_key_idx(subwords)
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
        if has_abbrv(lemma):
            continue
        all_lemmas.append(FIN_SPACE.split(lemma))
    all_lemmas.sort()
    return all_lemmas


def wiktionary_wordlist(session):
    headwords = session.execute(select([headword_t.c.name]).select_from(headword_t).order_by(headword_t.c.name))
    all_lemmas = []
    for (word,) in headwords:
        all_lemmas.append(word.split(" "))
    all_lemmas.sort()
    return all_lemmas


def mk_frame(wordlist_name, subwords, headword_idx, **extra):
    return (
        " ".join((surf for surf, anal in subwords)),
        (wordlist_name,),
        "frame",
        headword_idx,
        subwords, {
            "type": wordlist_name,
            **extra
        }
    )


def case_surf(full_case_name):
    if full_case_name in CASE_NORMSEG_MAP:
        mapped_normseg = CASE_NORMSEG_MAP[full_case_name] or ""
        return "___" + mapped_normseg
    else:
        return full_case_name


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
        subwords = []
        headword_idx = None
        for cmd, payload in proc_assoc(extra["raw_defn"]):
            if cmd == "headword":
                assert headword_idx is None
                headword_idx = len(subwords)
                subwords.append(headword_subword())
            elif cmd in ("subj", "obj"):
                if payload in CASES:
                    mapped_case = WIKTIONARY_TO_OMORFI_CASE_MAP.get(payload)
                    if mapped_case is not None:
                        subwords.append((case_surf(payload), {WILDCARD: {(("case", mapped_case.upper()),)}}))
                else:
                    # TODO: deal with ASSOC_POS and `direct object`
                    pass
            elif cmd == "verb":
                # TODO: Use transitive/intransitive on headword as an additional clue
                # TODO: Add requirement for verb that is not the headword
                pass
            elif cmd == "assoc":
                subwords.append((payload, lemmatise(payload)))
        if headword_idx is None:
            headword_idx = 0
            subwords.insert(0, headword_subword())
        if len(subwords) < 2:
            continue
        # XXX: Headword may not always be the best choice of key
        # -- but at least it always has a lemma!
        yield mk_frame("wiktionary_frame", subwords, headword_idx, sense_id=sense_id)


def wordnet_frames(session, lemmatise=fi_lemmatise):
    from lextract.wordnet.fin import Wordnet as FinWordnet
    for lemma in FinWordnet.lemma_names().keys():
        if not has_abbrv(lemma):
            continue
        lemma_bits = [bit for bit in lemma.split("_") if bit]
        subwords = []
        for lemma_bit in lemma_bits:
            if lemma_bit in ALL_ABBRVS:
                mapped_case = PRON_CASE[ALL_ABBRVS[lemma_bit]]
                full_case_name = CASE_NAME_MAP[mapped_case]
                subwords.append((case_surf(full_case_name), {WILDCARD: {(("case", mapped_case.upper()),)}}))
            else:
                subwords.append((lemma_bit, lemmatise(lemma_bit)))
        key_idx = get_key_idx(subwords)
        yield mk_frame("wordnet_frame", subwords, key_idx, wordnet_lemma=lemma)


def combine_wordlists(*wordlist_source_pairs):
    to_merge = []
    for word_list, source in wordlist_source_pairs:
        word_sources = []
        for word in word_list:
            word_sources.append((word, source))
        to_merge.append(word_sources)
    merged = list(heapq.merge(*to_merge))
    return groupby_transform(merged, itemgetter(0), itemgetter(1))


def wordlists(session, wl):
    wordlist_its = []
    if "wordnet" in wl:
        wordlist_its.append((wordnet_wordlist(), "wordnet"))
    if "wiktionary" in wl:
        wordlist_its.append((wiktionary_wordlist(session), "wiktionary"))

    return combine_wordlists(*wordlist_its)


def framelists(session, wl):
    frames_in = []
    key_f = itemgetter(0)
    if "wiktionary_frame" in wl:
        frames_in.append(sorted(wiktionary_frames(session), key=key_f))
    if "wordnet_frame" in wl:
        frames_in.append(sorted(wordnet_frames(session), key=key_f))
    merged = heapq.merge(*frames_in, key=key_f)
    for key, group in groupby(merged, key=key_f):
        group_list = list(group)
        result = list(group_list[0])
        result[1] = list(result[1])
        result[5] = {
            "defns": [result[5]]
        }
        for other in group_list[1:]:
            result[1].extend(other[1])
            result[5]["defns"].append(other[5])
        yield result


def indexed_wordlists(session, wl):
    indexed = index_wordlist(wordlists(session, wl))
    indexed = chain(indexed, framelists(session, wl))
    return indexed


WORDLIST_NAMES = ["wordnet", "wiktionary", "wiktionary_frame", "wordnet_frame"]


@click.command()
@click.option("--wl", type=click.Choice(WORDLIST_NAMES), multiple=True, default=WORDLIST_NAMES)
@click_log.simple_verbosity_option(logger)
def add_keyed_words(wl):
    """
    Index multiwords/inflections/frames into database
    """
    session = get_session()
    metadata.create_all(session().get_bind().engine)
    indexed = indexed_wordlists(session, wl)
    if logger.isEnabledFor(logging.INFO):
        ctx = contextlib.nullcontext(indexed)
    else:
        ctx = click.progressbar(indexed, label="Inserting word keys")
    with ctx as indexed:
        insert_indexed(session, indexed, fi_lemmatise)
    session.commit()
