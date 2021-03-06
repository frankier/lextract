from dataclasses import dataclass
from typing import ClassVar, Dict, Iterator, cast
from wikiparse.assoc import proc_assoc
from wikiparse.assoc.models import AssocNode
from wikiparse.context import ParseContext

from ..db.queries import wiktionary_deriv_query
from ..models import UdMwe, MweType


@dataclass
class WiktionaryDerivLink:
    link_name: ClassVar[str] = "wikideriv"
    headword: str

    def get_cols(self) -> Dict[str, str]:
        return {
            "headword": self.headword,
        }


def wiktionary_deriv_wordlist(session, headwords) -> Iterator[UdMwe]:
    from .wiktionary_defn import flatten, defn_mwes

    derivs = session.execute(wiktionary_deriv_query)
    for (word, disp, gloss, extra) in derivs:
        if headwords is not None and word not in headwords:
            continue
        raw = extra["grams"][0]["span"]["payload"]
        print("deriv", word, raw)
        links = [WiktionaryDerivLink(word)]
        # TODO: Add in POS
        assocs = proc_assoc(ParseContext(word, None), raw)
        for assoc in assocs:
            if not assoc.tree_has_gram:
                continue
            for mwe in flatten(cast(AssocNode, assoc.tree)):
                yield from defn_mwes(
                    word,
                    mwe,
                    # TODO: headword_pos
                    headword_pos=None,
                    typ=MweType.frame,
                    links=links,
                )
