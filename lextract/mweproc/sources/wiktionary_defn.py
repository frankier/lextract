import logging
from dataclasses import dataclass
from typing import ClassVar, Dict, List, Iterator, Any
from wikiparse.assoc import proc_assoc
from wikiparse.assoc.models import AssocWord, WordType, AssocNode, PlusNode
from wikiparse.assoc.interpret import flatten
from wikiparse.context import ParseContext
from .common import map_pos_to_ud, WIKI_TO_UD_CASE
from ..models import UdMwe, UdMweToken, MweType
from ..db.queries import wiktionary_gram_query
from ...utils.lemmatise import fi_lemmatise


@dataclass
class WiktionaryDefnLink:
    link_name: ClassVar[str] = "wikidefn"
    word: str
    sense_id: str
    pos: str

    def get_cols(self) -> Dict[str, str]:
        return {
            "word": self.word,
            "sense_id": self.sense_id,
            "pos": self.pos,
        }


def wiktionary_defn_wordlist(session, lemmatise=fi_lemmatise):
    grams = session.execute(wiktionary_gram_query)
    for word, sense_id, pos, extra in grams:
        links = [WiktionaryDefnLink(word, sense_id, pos)]
        assocs = proc_assoc(ParseContext(word, pos), extra["raw_defn"])
        for assoc in assocs:
            if not assoc.tree_has_gram:
                continue
            for mwe in flatten(assoc.tree):
                yield from defn_mwes(
                    word,
                    mwe,
                    headword_pos=pos,
                    typ=MweType.frame,
                    links=links
                )


def gen_conf_net(conf_net: Iterator[List[UdMweToken]]) -> Iterator[List[UdMweToken]]:
    options = next(conf_net, None)
    if options is None:
        yield []
        return
    for rest in gen_conf_net(conf_net):
        for tok in options:
            yield [tok] + rest


def gen_udmwe(feats_vals: Dict[str, List[str]]) -> Iterator[Dict[str, str]]:
    if not feats_vals:
        empty: Dict[str, str] = {}
        return iter((empty,))
    feat, vals = feats_vals.popitem()
    return (
        {feat: val, **feats_rest}
        for feats_rest in gen_udmwe(feats_vals)
        for val in vals
    )


def defn_mwes(headword: str, assoc_node: AssocNode, headword_is_lemma=True, headword_pos=None, **kwargs) -> Iterator[UdMwe]:
    assert isinstance(assoc_node, PlusNode)
    conf_net: List[List[UdMweToken]] = []
    headword_idx = 0
    for idx, assoc_word in enumerate(assoc_node.children):
        assert isinstance(assoc_word, AssocWord)
        if assoc_word.word_type == WordType.headword:
            headword_idx = idx
        feats_vals: Dict[str, List[str]] = {}
        cases = assoc_word.inflection_bits.get("case", [])
        for case in cases:
            # TODO: assert everything else is absent
            ud_case = WIKI_TO_UD_CASE.get(case)
            if ud_case is None:
                continue
            feats_vals.setdefault("Case", []).append(ud_case.title())
        pers = assoc_word.inflection_bits["pers"][0] if "pers" in assoc_word.inflection_bits else None
        personal = assoc_word.inflection_bits["personal"][0] if "personal" in assoc_word.inflection_bits else None
        if pers is not None or personal == "impersonal":
            if pers is not None:
                assert pers == "sg3"
                assert personal != "personal"
            feats_vals.setdefault("Number", []).append("Sing")
            feats_vals.setdefault("Person", []).append("3")
        infs = assoc_word.inflection_bits.get("inf", [])
        for inf in infs:
            feats_vals.setdefault("VerbForm", []).append("Inf")
            feats_vals.setdefault("InfForm", []).append(inf[0])
        parts = assoc_word.inflection_bits.get("part", [])
        for part in parts:
            if part == "participle":
                feats_vals.setdefault("VerbForm", []).append("Part")
        passes = assoc_word.inflection_bits.get("pass", [])
        for passi in passes:
            if passi == "passive":
                feats_vals.setdefault("Voice", []).append("Pass")
        tenses = assoc_word.inflection_bits.get("tense", [])
        for tense in tenses:
            if tense == "present":
                mapped_tense = "Pres"
            elif tense == "past":
                mapped_tense = "Past"
            else:
                break
            feats_vals.setdefault("Tense", []).append(mapped_tense)
        for key, val in assoc_word.inflection_bits.items():
            # Pass through for title-cased feats
            if key != key.title():
                continue
            feats_vals.setdefault(key, []).extend(val)
        options = []
        for feats in gen_udmwe(feats_vals):
            extra: Any = {}
            if assoc_word.form:
                extra["payload"] = str(assoc_word.form)
            if assoc_word.word_type == WordType.headword:
                if "payload" not in extra:
                    extra["payload"] = headword
                extra = {
                    **extra,
                    # TODO: Should this be guaranteed to be done beforehand in wikiparse instead?
                    "poses": (
                        map_pos_to_ud(
                            "wiki",
                            *(pos.title() for pos in (assoc_word.pos or []))
                        ) or
                        map_pos_to_ud("wiki", headword_pos)
                    ),
                    "payload_is_lemma": headword_is_lemma,
                }
            options.append(UdMweToken(feats=feats, **extra))
        if options:
            conf_net.append(options)
        else:
            # TODO: Log skipping token due to no options
            pass
        
        #assoc_word.inflection_bits
        #assoc_word.word_type
        #word_type: Optional[WordType] = None
        #form: Optional[str] = None
        #pos: Optional[str] = None
        #inflection_bits: Dict[str, List[str]] = field(default_factory=dict)
        #gram_role_bits: List[str] = field(default_factory=list)
        #lex_raw: Dict[str, str] = field(default_factory=dict)
    for bits in gen_conf_net(iter(conf_net)):
        yield UdMwe(
            bits,
            headword_idx=headword_idx,
            **kwargs
        )
