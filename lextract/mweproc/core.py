import logging
import click_log
import heapq
from itertools import groupby
from more_itertools import groupby_transform
from sqlalchemy import select
from itertools import chain
from typing import Iterator

from wordfreq import word_frequency
from wikiparse.db.tables import headword as headword_t
from operator import itemgetter
from .consts import WILDCARD
from .models import UdMwe
from ..utils.lemmatise import fi_lemmatise
from .db.queries import wiktionary_gram_query
from finntk.data.wordnet import ALL_ABBRVS, PRON_CASE


logger = logging.getLogger(__name__)
click_log.basic_config(logger)


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


def combine_wordlists(*wordlist_source_pairs):
    to_merge = []
    for word_list, source in wordlist_source_pairs:
        word_sources = []
        for word in word_list:
            word_sources.append((word, source))
        to_merge.append(word_sources)
    merged = list(heapq.merge(*to_merge))
    return groupby_transform(merged, itemgetter(0), itemgetter(1))


def tag(lbl, it):
    return ((x, lbl) for x in it)


def all_wordlists(session, wl) -> Iterator[UdMwe]:
    from .sources import wordnet_wordlist, wiktionary_defn_wordlist, wiktionary_hw_wordlist, wiktionary_deriv_wordlist
    if "wordnet" in wl:
        yield from tag("wordnet", wordnet_wordlist())
    if "wiktionary_defn" in wl:
        yield from tag("wiktionary_defn", wiktionary_defn_wordlist(session))
    if "wiktionary_hw" in wl:
        yield from tag("wiktionary_hw", wiktionary_hw_wordlist(session))
    if "wiktionary_deriv" in wl:
        yield from tag("wiktionary_deriv", wiktionary_deriv_wordlist(session))


WORDLIST_NAMES = ["wordnet", "wiktionary_defn", "wiktionary_hw", "wiktionary_deriv"]
