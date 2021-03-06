from finntk.data.wiktionary_normseg import CASE_NORMSEG_MAP as FULL_CASE_NORMSEG
from finntk.data.omorfi_normseg import CASE_NAME_MAP as ABBRV_FULL_CASE_MAP
from finntk.data.wordnet import STD_ABBRVS, PRON_CASE, PRON_LEMMAS
from finntk.omor.anlys import generate_dict, ud_to_omor
from ..models import UdMwe, UdMweToken
from typing import Dict, Optional, Set, List


CASE_NORMSEG = {
    abbrv.title(): FULL_CASE_NORMSEG[full]
    for abbrv, full in ABBRV_FULL_CASE_MAP.items()
    if full is not None
}

STD_ABBRVS_REV = {pro: abbr for abbr, pro in STD_ABBRVS.items()}
CASE_ABBRV = {
    ud.title(): STD_ABBRVS_REV[pro]
    for pro, ud in PRON_CASE.items()
    if PRON_LEMMAS.get(pro) == "jokin" and pro in STD_ABBRVS_REV
}


def gap_case(full_case_name):
    if full_case_name in CASE_NORMSEG:
        mapped_normseg = CASE_NORMSEG[full_case_name]
        if mapped_normseg:
            return ["___", mapped_normseg]
        else:
            return ["___"]
    else:
        return [f"(({full_case_name}))"]


def jnk_case(full_case_name):
    if full_case_name in CASE_ABBRV:
        return [CASE_ABBRV[full_case_name]]
    else:
        return [f"(({full_case_name}))"]


DEFAULT_VERB_FEATS = {
    "Voice": "Act",
    "Mood": "Ind",
    "Tense": "Pres",
    "Number": "Sing",
    "Person": "3",
    "VerbForm": "Fin",
}


def gen_verb_default_feats(token: UdMweToken) -> Optional[Set[str]]:
    def get_def(feat):
        return token.feats.get(feat, DEFAULT_VERB_FEATS[feat])

    if not token.poses or "VERB" not in token.poses:
        return None
    if not token.payload_is_lemma:
        # Unexpected condition: log
        return None
    if len(token.feats.keys() - DEFAULT_VERB_FEATS.keys()):
        # Unexpected condition: log
        return None
    if get_def("VerbForm") != "Fin":
        return None
    joined_feats = {}
    for feat in DEFAULT_VERB_FEATS:
        joined_feats[feat] = get_def(feat)
    omor = ud_to_omor(token.payload, "Verb", joined_feats)
    return generate_dict(omor)


def wildcard_feats_to_segs(feats: Dict[str, str], use_jnk: bool = False) -> List[str]:
    if feats.get("VerbForm") == "Inf":
        if feats.get("InfForm") == "1":
            if len(feats) > 2:
                # Log that we can't express all feats
                return ["UNK"]
            else:
                return ["___", "da"]
        elif feats.get("InfForm") == "3":
            if "Case" in feats:
                case = feats["Case"]
                mapped_normseg = CASE_NORMSEG[case]
                inf_segs = ["___", "ma"]
                if mapped_normseg:
                    inf_segs.extend(mapped_normseg[1:])
                return inf_segs
            else:
                return ["UNK"]
        else:
            # Log that we can't express all feats
            return ["UNK"]
    elif feats.get("VerbForm") == "Part":
        feats = feats.copy()
        feats.pop("VerbForm", None)
        tense = feats.pop("Tense", "Pres")
        voice = feats.pop("Voice", "Act")
        if len(feats):
            # Log that we can't express all feats
            return ["UNK"]
        elif tense == "Pres" and voice == "Act":
            return ["___", "va"]
        elif tense == "Pres" and voice == "Pass":
            return ["___", "tava"]
        elif tense == "Past" and voice == "Act":
            return ["___", "nut"]
        elif tense == "Past" and voice == "Pass":
            return ["___", "ttu"]
        else:
            assert False
    elif "Case" in feats:
        case = feats["Case"]
        if len(feats) > 1:
            # Log that we can't express all feats
            return ["UNK"]
        else:
            if use_jnk:
                return jnk_case(case)
            else:
                return gap_case(case)
    elif not feats:
        return ["___"]
    else:
        # Log that we can't express all feats here
        return ["UNK"]


def wildcard_feats_to_str(feats: Dict[str, str], use_jnk: bool = False) -> str:
    segs = wildcard_feats_to_segs(feats)
    return segs[0] + "".join("-" + seg for seg in segs[1:])


def gapped_mwe_tok(
    token: UdMweToken, use_jnk: bool = False,
):
    if token.payload is not None:
        if token.feats:
            # TODO: Generator for nominals
            generated = gen_verb_default_feats(token)
            if not generated:
                # Log that we can't express all feats here
                return "UNK"
            else:
                return "/".join(generated)
        else:
            return token.payload
    else:
        return wildcard_feats_to_str(token.feats, use_jnk)


def gapped_mwe(
    mwe: UdMwe,
    use_jnk: bool = False,
    strong_head: bool = False,
    strong_start: str = "<strong>",
    strong_end: str = "</strong>",
) -> str:
    bits = []
    for idx, token in enumerate(mwe.tokens):
        bit = gapped_mwe_tok(token, use_jnk)
        is_headword = idx == mwe.headword_idx
        if is_headword and strong_head:
            bit = strong_start + bit + strong_end
        bits.append(bit)
    return " ".join(bits)


def pos_template(mwe: UdMwe) -> str:
    bits = []
    for idx, token in enumerate(mwe.tokens):
        if token.payload is not None:
            if token.payload_is_lemma:
                gap_type = "l"
            else:
                gap_type = "s"
        else:
            gap_type = "w"
        bit = (
            (",".join(token.poses) if token.poses else "u")
            + ","
            + gap_type
            + (",h" if mwe.headword_idx == idx else "")
        )
        bits.append(bit)
    return " ".join(bits)
