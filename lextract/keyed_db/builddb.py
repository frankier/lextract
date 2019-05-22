import click
import heapq
from more_itertools import groupby_transform
from sqlalchemy import select

from wikiparse.utils.db import get_session, insert, insert_get_id
from wordfreq import word_frequency
from lextract.keyed_db.tables import metadata, key_lemma as key_lemma_t, word as word_t, subword as subword_t
from wikiparse.tables import headword as headword_t
from lextract.aho_corasick.fin import FIN_SPACE
from operator import itemgetter
from .utils import fi_lemmatise


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


def insert_wordlist(session, words, lemmatise=null_lemmatise):
    for word, sources in words:
        res = index_word(word, lemmatise)
        if res is None:
            continue
        word_type, key_idx, subwords = res
        word_id = insert_get_id(session, word_t, key_idx=key_idx, form=" ".join(word), type=word_type, sources=sources, payload={})
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
    headwords = session.execute(select([headword_t.c.name]).select_from(headword_t.order_by(headword_t.c.name)))
    all_lemmas = []
    for word in headwords:
        all_lemmas.append(word.split(" "))
    all_lemmas.sort()
    return all_lemmas


def combine_wordlists(*wordlist_source_pairs):
    merged = heapq.merge((((word, source) for word in word_list) for word_list, source in wordlist_source_pairs))
    return groupby_transform(merged, itemgetter(0), itemgetter(1))


def wordlists(session):
    return combine_wordlists((wordnet_wordlist, "wordnet"), (lambda: wiktionary_wordlist(session), "wiktionary"))


@click.command()
def add_keyed_words():
    """
    Add table of frequencies to DB
    """
    session = get_session()
    metadata.create_all(session().get_bind().engine)
    with click.progressbar(wordlists(session), label="Inserting word keys") as words:
        insert_wordlist(session, words, fi_lemmatise)
    session.commit()