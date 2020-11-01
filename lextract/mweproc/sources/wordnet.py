from finntk.wordnet import has_abbrv
from lextract.utils.space import FIN_SPACE
from lextract.wordnet.fin import (
    Wordnet as FinWordnet,
    get_lemma_objs,
    preferred_synset,
)
from typing import Iterator, ClassVar, Dict, List, Set
from .common import build_mwe_or_inflections, map_pos_to_ud, guess_headword
from dataclasses import dataclass
from ...utils.lemmatise import fi_lemmatise
from finntk.data.wordnet import ALL_ABBRVS, PRON_CASE
from ..models import UdMwe, UdMweToken, MweType


@dataclass
class WordNetHeadwordLink:
    link_name: ClassVar[str] = "wnhw"
    headword: str

    def get_cols(self) -> Dict[str, str]:
        lemma_objs = get_lemma_objs(self.headword)
        wns: Set[str] = set()
        descriptions = []
        for synset_key, wn_lemma_objs in lemma_objs.items():
            wns.update((wn for wn, _ in wn_lemma_objs))
            ss = preferred_synset(wn_lemma_objs)
            descriptions.append(ss.definition())
        return {
            "headword": self.headword,
            "wns": ", ".join(wns),
            "desc": "; ".join(descriptions),
        }


def classify_headword(word: List[str], lemmatise=fi_lemmatise) -> MweType:
    from .common import classify_nonframe_headword

    if has_abbrv("_".join(word)):
        return MweType.frame
    else:
        return classify_nonframe_headword(word, lemmatise)


def split_headword(headword: str) -> List[str]:
    return [bit for bit in FIN_SPACE.split(headword) if bit]


def get_poses(headword):
    lemma_objs = get_lemma_objs(headword)
    return {
        lemma_obj.synset().pos()
        for lemmas in lemma_objs.values()
        for _, lemma_obj in lemmas
    }


def wordnet_wordlist(included_headwords) -> Iterator[UdMwe]:
    return map(guess_headword, wordnet_wordlist_bare(included_headwords))


def pos_tag_token(token):
    if token.payload is None:
        return
    token_poses = get_poses(token.payload)
    if token_poses:
        token.poses = map_pos_to_ud("wn", *token_poses)


def wordnet_wordlist_bare(included_headwords) -> Iterator[UdMwe]:
    for headword in FinWordnet.lemma_names().keys():
        mwe_poses = get_poses(headword)
        ud_poses = map_pos_to_ud("wn", *mwe_poses)
        hw_bits = split_headword(headword)
        headword_space = " ".join(hw_bits)
        if included_headwords is not None and headword_space not in included_headwords:
            continue
        typ = classify_headword(hw_bits)
        links = [WordNetHeadwordLink(headword)]
        if typ in (MweType.inflection, MweType.multiword):
            # TODO: Might be able to improve inflection handling with
            # idiomatic-pos note from FiWN
            yield from build_mwe_or_inflections(
                hw_bits,
                typ=typ,
                links=links,
                poses=ud_poses,
                token_callback=pos_tag_token,
            )
        elif typ == MweType.frame:
            yield wordnet_frame(hw_bits, typ=typ, links=links, poses=ud_poses)
        else:
            assert typ == MweType.lemma


def wordnet_frame(hw_bits: List[str], **kwargs) -> UdMwe:
    tokens = []
    for hw_bit in hw_bits:
        if hw_bit in ALL_ABBRVS:
            mapped_case = PRON_CASE[ALL_ABBRVS[hw_bit]]
            token = UdMweToken(
                feats={"Case": mapped_case.title()}, poses=map_pos_to_ud("wn", "n", "a")
            )
        else:
            token = UdMweToken(hw_bit)
            pos_tag_token(token)
        tokens.append(token)
    return UdMwe(tokens, **kwargs)
