from wikiparse.assoc.models import PlusNode, AssocWord, WordType

from lextract.mweproc.models import MweType
from lextract.mweproc.sources.wiktionary_defn import defn_mwes

OLLA_MUST_ASSOC_NODE = PlusNode(
    children=[
        AssocWord(
            pos={'noun', 'adjective'},
            inflection_bits={'case': ['genitive']}
        ),
        AssocWord(
            word_type=WordType.headword,
            pos={'verb'},
            inflection_bits={'trans': ['intransitive'], 'pers': ['sg3']}
        ),
        AssocWord(
            pos={'verb'},
            inflection_bits={
                'part': ['participle'],
                'tense': ['present'],
                'pass': ['passive']
            }
        )
    ]
)


def must_olla_mwe():
    return list(defn_mwes(
        "olla",
        OLLA_MUST_ASSOC_NODE,
        headword_pos="Verb",
        typ=MweType.frame,
    ))
