import logging
import click_log
from typing import Iterator

from .models import UdMwe


logger = logging.getLogger(__name__)
click_log.basic_config(logger)


def tag(lbl, it):
    return ((x, lbl) for x in it)


def all_wordlists(session, wl, headwords) -> Iterator[UdMwe]:
    from .sources import wordnet_wordlist, wiktionary_defn_wordlist, wiktionary_hw_wordlist, wiktionary_deriv_wordlist
    if "wordnet" in wl:
        yield from tag("wordnet", wordnet_wordlist(headwords))
    if "wiktionary_defn" in wl:
        yield from tag("wiktionary_defn", wiktionary_defn_wordlist(session, headwords))
    if "wiktionary_hw" in wl:
        yield from tag("wiktionary_hw", wiktionary_hw_wordlist(session, headwords))
    if "wiktionary_deriv" in wl:
        yield from tag("wiktionary_deriv", wiktionary_deriv_wordlist(session, headwords))


WORDLIST_NAMES = ["wordnet", "wiktionary_defn", "wiktionary_hw", "wiktionary_deriv"]
