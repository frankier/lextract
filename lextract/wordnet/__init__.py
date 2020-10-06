from typing import List, Dict, Optional
from nltk.corpus import wordnet
from nltk.corpus.reader.wordnet import Lemma
from finntk.wordnet.reader import fiwn
from .base import ExtractableWordnet
from .utils import wn_lemma_map


def lemmas(lemma_name: str, wn: str, pos: Optional[str] = None) -> List[Lemma]:
    if wn == "qf2":
        return fiwn.lemmas(lemma_name, pos=pos)
    else:
        return wordnet.lemmas(lemma_name, pos=pos, lang=wn)


def objify_lemmas(wn_to_lemma: Dict[str, List[str]]) -> Dict[str, List[Lemma]]:
    return {
        wn: [lemma_obj for lemma in lemma_list for lemma_obj in lemmas(lemma, wn)]
        for wn, lemma_list in wn_to_lemma.items()
    }


def get_lemma_names(ssof, wns):
    from finntk.wordnet.utils import en2fi_post

    wns = list(wns)
    lemmas = []
    if "qf2" in wns:
        fi_ssof = en2fi_post(ssof)
        ss = fiwn.of2ss(fi_ssof)
        lemmas.extend(ss.lemmas())
        wns.remove("qf2")
    for wnref in wns:
        ss = wordnet.of2ss(ssof)
        lemmas.extend(ss.lemmas(lang=wnref))
    return {l.name() for l in lemmas}


__all__ = ["ExtractableWordnet", "lemmas", "wn_lemma_map", "objify_lemmas"]
