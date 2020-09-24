import pytest
from lextract.mweproc.sources.wiktionary_defn import defn_mwes
from wikiparse.assoc.models import TreeFragToken, AssocWord, walk, AssocSpanType, PlusNode, WordType
from wikiparse.assoc import proc_assoc
from wikiparse.context import ParseContext
from lextract.mweproc.models import MweType
from lextract.mweproc.formatters.human import gapped_mwe


EXAMPLES = [
    (
        "pitää", "Verb",
        "{{lb|fi|transitive|_|+ partitive}} to [[hold]], [[grasp]], [[grip]]",
        "",
        "pitää ___-ta",
        0,
    ),
    (
        "pitää", "Verb",
        "{{lb|fi|transitive|_|+ accusative}} to [[keep]], [[take]]",
        "",
        "pitää ___-ut",
        0,
    ),
    (
        "pitää", "Verb",
        "{{lb|fi|transitive|_|+ elative}} to [[like]], [[be]] [[fond]] of",
        "",
        "pitää ___-sta",
        0,
    ),
    (
        "pitää", "Verb",
        "{{lb|fi|transitive|impersonal|genitive + 3rd-pers. singular + 1st infinitive}} to [[have]] (to do); (''in conditional mood'') [[should]] (do), [[ought]] (to do), [[be]] [[suppose]]d (to do), [[would]] [[have]] (to do)",
        "",
        "___-n pitää ___-da",
        1,
    ),
    (
        "pitää", "Verb",
        "{{lb|fi|transitive|_|+ partitive + essive}} to [[consider]] (to be), to [[assess]], to [[see]] as",
        "",
        "pitää ___-ta ___-na",
        0,
    ),
    (
        "pitää", "Verb",
        "{{lb|fi|transitive|_|+ elative + [[kiinni]]}} to [[hold]] [[onto]]",
        "",
        "pitää ___-sta kiinni",
        0,
    ),
    (
        "pitää", "Verb",
        "{{lb|fi|transitive|_|+ partitive}} to [[keep]] {{gloss|an animal}}",
        "",
        "pitää ___-ta",
        0,
    ),
    (
        "olla", "Verb",
        "{{lb|fi|intransitive|adessive + 3rd person singular + ~}} to [[have]]; to [[own]], to [[possess]]",
        "Minulla on kissa.",
        "___-lla on ___",
        1,
    ),
    (
        "olla", "Verb",
        "{{lb|fi|intransitive|inessive + 3rd person singular + ~}} to [[have]], to [[possess]] {{gloss|as a feature or capability, as opposed to simple possession; almost always for inanimate subjects}}",
        "Tässä autossa on kaikki lisävarusteet.",
        "___-ssa on ___",
        1,
    ),
    (
        "olla", "Verb",
        "{{lb|fi|intransitive|+ genitive + 3rd person singular + passive present participle}} to [[have to]] do something, [[must]] do something; [[be]] [[obliged]]/[[forced]] to do something",
        "Minun (gen.) on nyt mentävä.",
        "___-n on ___-tava",
        1,
    ),
    #(
        #"olla", "Verb",
        #"{{lb|fi|intransitive|~ ({{l|fi|olla olemassa|olemassa}})}} to [[exist]] {{gloss|1=the subject often indefinite = in partitive case -> verb in 3rd-pers. singular}}",
        #"Rakkautta ei ole (olemassa).", 
        #"olla olemassa",
        #0,
    #),
]


@pytest.mark.parametrize("lemma, pos, defn, ex, gap, headword_idx", EXAMPLES)
def test_wikitionary_frame_e2e(lemma, pos, defn, ex, gap, headword_idx):
    results = proc_assoc(ParseContext(lemma, pos), defn)
    templates = [res for res in results if res.span.typ == AssocSpanType.lb_template]
    assert len(templates) == 1
    assert templates[0].tree_has_gram
    mwes = list(defn_mwes(
        lemma,
        templates[0].tree,
        headword_pos=pos,
        typ=MweType.frame,
        links=[],
    ))
    assert len(mwes) == 1
    mwe = mwes[0]
    assert gapped_mwe(mwe) == gap
    assert mwe.headword_idx == headword_idx


#paeta
#"(transitive + partitive, or intransitive + elative or ablative) To escape, get away."

#yltää
# {{lb|fi|intransitive|_|+ allative '''or''' illative}} To ([[be able]] to) [[reach]] (''usually with one's limbs'').
#: '''''Yllät'''[[-kö|kö]] [[ylähylly]]lle?''
#:: '''Can''' you '''reach''' the top shelf?
# {{lb|fi|intransitive|(+ a person in partitive)|_|+ illative}} To [[be]]/[[come]] up to.
#: ''[[vesi|Vesi]] '''ylsi''' [[minä|minua]] [[vyötärö]]ön.''
#:: The water '''was up''' to my waist.

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


def test_olla_must_mwes():
    from common import must_olla_mwe
    mwes = must_olla_mwe()
    assert len(mwes[0].tokens) == 3
    assert mwes[0].tokens[-1].feats == {'Tense': 'Pres', 'Voice': 'Pass', 'VerbForm': 'Part'}
