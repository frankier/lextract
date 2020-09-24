from finntk.wordnet import has_abbrv
from lextract.utils.space import FIN_SPACE
from lextract.wordnet.fin import Wordnet as FinWordnet, get_lemma_objs, preferred_synset, get_lemma_objs
from typing import Iterator, ClassVar, Dict, List, Set
from .common import build_simple_mwe, map_pos_to_ud
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
    return {lemma_obj.synset().pos() for lemmas in lemma_objs.values() for _, lemma_obj in lemmas}


def guess_headword(ud_mwe: UdMwe):
    candidate_idxs = []
    link = ud_mwe.links[0]
    assert isinstance(link, WordNetHeadwordLink)
    mwe = link.headword
    mwe_poses = get_poses(mwe)
    for token_idx, token in enumerate(ud_mwe.tokens):
        if token.payload is None:
            continue
        token_poses = get_poses(token.payload)
        # We just put poses on tokens here for now -- maybe it should go somewhere else really
        if token_poses:
            token.poses = map_pos_to_ud("wn", *token_poses)
        intersection = token_poses.intersection(mwe_poses)
        if len(intersection):
            candidate_idxs.append(token_idx)
    ud_mwe.poses = map_pos_to_ud("wn", *mwe_poses)
    if len(candidate_idxs) == 1:
        ud_mwe.headword_idx = candidate_idxs[0]
    else:
        # TODO: Note down why it failed
        pass
    return ud_mwe


def wordnet_wordlist() -> Iterator[UdMwe]:
    return map(guess_headword, wordnet_wordlist_bare())


def wordnet_wordlist_bare() -> Iterator[UdMwe]:
    for headword in FinWordnet.lemma_names().keys():
        hw_bits = split_headword(headword)
        typ = classify_headword(hw_bits)
        links = [WordNetHeadwordLink(headword)]
        poses = get_lemma_objs(headword)
        if typ in (MweType.inflection, MweType.multiword):
            # TODO: inflection should be turned to lemma + features
            # TODO: Can probably used idiomatic-pos note from FiWN
            yield build_simple_mwe(
                hw_bits,
                typ=typ, 
                links=links
            )
        elif typ == MweType.frame:
            yield wordnet_frame(hw_bits, typ=typ, links=links)


def wordnet_frame(hw_bits: List[str], **kwargs) -> UdMwe:
    tokens = []
    for hw_bit in hw_bits:
        if hw_bit in ALL_ABBRVS:
            mapped_case = PRON_CASE[ALL_ABBRVS[hw_bit]]
            token = UdMweToken(
                feats={
                    "Case": mapped_case.title()
                },
                poses=map_pos_to_ud("wn", "n", "a")
            )
        else:
            token = UdMweToken(hw_bit)
        tokens.append(token)
    return UdMwe(tokens, **kwargs)
