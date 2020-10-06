from typing import Iterable, List, Set
from finntk.data.wordnet_pos import (
    POS_MAP as WN_TO_WIKI_POS,
    UD_POS_MAP as WN_TO_UD_MAP,
)
from finntk.data.omorfi_normseg import CASE_NAME_MAP
from ..models import UdMwe, UdMweToken, MweType
from ...utils.lemmatise import fi_lemmatise


WIKI_TO_UD_POS = {
    wiki_pos: WN_TO_UD_MAP[wn_pos]
    for wn_pos, wiki_poses in WN_TO_WIKI_POS.items()
    for wiki_pos in wiki_poses
}

WIKI_TO_UD_CASE = {v: k for k, v in CASE_NAME_MAP.items() if v is not None}


def map_pos_to_ud(frm: str, *headword_poses: str) -> Set[str]:
    if frm == "wiki":
        to_ud_map = WIKI_TO_UD_POS
    elif frm == "wn":
        to_ud_map = WN_TO_UD_MAP
    else:
        assert False
    return {
        ud_pos
        for headword_pos in headword_poses
        for ud_pos in to_ud_map.get(headword_pos, [])
    }


def build_simple_mwe(words: Iterable[str], **kwargs) -> UdMwe:
    tokens = []
    for word in words:
        tokens.append(UdMweToken(word))
    return UdMwe(tokens, **kwargs)


def classify_nonframe_headword(word: List[str], lemmatise=fi_lemmatise) -> MweType:
    if len(word) > 1:
        return MweType.multiword
    elif list(lemmatise(word[0]).keys()) == [word[0]]:
        return MweType.lemma
    else:
        return MweType.inflection
