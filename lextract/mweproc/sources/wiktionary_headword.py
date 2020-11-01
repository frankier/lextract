import orjson
from dataclasses import dataclass
from typing import ClassVar, Dict, Iterator

from .common import build_mwe_or_inflections, classify_nonframe_headword, map_pos_to_ud
from ..db.queries import wiktionary_defined_headword_query
from ..models import UdMwe, MweType


@dataclass
class WiktionaryHeadwordLink:
    link_name: ClassVar[str] = "wikihw"
    headword: str
    page_exists: bool
    has_senses: bool

    def get_cols(self) -> Dict[str, str]:
        return {
            "headword": self.headword,
        }


def wiktionary_hw_wordlist(session, included_headwords) -> Iterator[UdMwe]:
    headwords = session.execute(wiktionary_defined_headword_query(session.dialect))
    for word, redlink, poses_raw, has_senses in headwords:
        if isinstance(poses_raw, str):
            poses = orjson.loads(poses_raw)
        else:
            poses = poses_raw
        poses = map_pos_to_ud("wiki", *poses)
        if included_headwords is not None and word not in included_headwords:
            continue
        word_bits = word.split(" ")
        # TODO: Add in type information from Wiktionary
        # TODO: Follow derived term parent to get headword
        typ = classify_nonframe_headword(word_bits)
        if typ == MweType.lemma:
            continue
        yield from build_mwe_or_inflections(
            word_bits,
            typ=typ,
            poses=poses,
            links=[WiktionaryHeadwordLink(word, not redlink, has_senses)],
        )
