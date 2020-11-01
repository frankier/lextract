from typing import Iterable, List, Set, Iterator
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


def build_mwe_or_inflections(
    words: Iterable[str], typ=MweType.multiword, *, token_callback=None, **kwargs
) -> Iterator[UdMwe]:
    if typ == MweType.multiword:
        yield build_simple_mwe(words, typ=typ, token_callback=token_callback, **kwargs)
    else:
        assert typ == MweType.inflection
        yield from build_inflections(list(words)[0], typ=typ, **kwargs)


def build_simple_mwe(words: Iterable[str], *, token_callback=None, **kwargs) -> UdMwe:
    tokens = []
    for word in words:
        token = UdMweToken(word)
        if token_callback is not None:
            token_callback(token)
        tokens.append(token)
    return UdMwe(tokens, **kwargs)


def build_inflections(word: str, lemmatise=fi_lemmatise, **kwargs) -> Iterator[UdMwe]:
    lemmatised = lemmatise(word, return_pos=True)
    for lemma, all_pos_feats in lemmatised.items():
        for pos, feats in all_pos_feats:
            if not feats:
                continue
            poses = {pos}
            yield UdMwe(
                [
                    UdMweToken(
                        lemma, payload_is_lemma=True, poses=poses, feats=dict(feats)
                    )
                ],
                headword_idx=0,
                **kwargs
            )


def classify_nonframe_headword(word: List[str], lemmatise=fi_lemmatise) -> MweType:
    if len(word) > 1:
        return MweType.multiword
    elif list(lemmatise(word[0]).keys()) == [word[0]]:
        return MweType.lemma
    else:
        return MweType.inflection


def guess_headword(ud_mwe: UdMwe):
    if ud_mwe.headword_idx is not None:
        return ud_mwe
    candidate_idxs = []
    mwe_poses = ud_mwe.poses
    if mwe_poses is None:
        # TODO: Note down why it failed
        return ud_mwe
    for token_idx, token in enumerate(ud_mwe.tokens):
        if token.payload is None:
            continue
        token_poses = token.poses
        if token_poses is None:
            intersection = mwe_poses
        else:
            intersection = token_poses.intersection(mwe_poses)
        if len(intersection):
            candidate_idxs.append(token_idx)
    if len(candidate_idxs) == 1:
        ud_mwe.headword_idx = candidate_idxs[0]
    else:
        # TODO: Note down why it failed
        pass
    return ud_mwe
