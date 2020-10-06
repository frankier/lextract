from itertools import chain

from ..models import UdMwe, UdMweToken


def tds_tok(tok: UdMweToken) -> str:
    bits = []
    if tok.payload is not None:
        if tok.payload_is_lemma:
            bits.append(f"(L={tok.payload})")
        else:
            bits.append(f'"{tok.payload}"')
    if tok.poses is not None and len(tok.poses):
        bits.append("({})".format("|".join(tok.poses)))
    for feat, val in tok.feats.items():
        if feat == "Case" and val == "Acc":
            bits.append("(Case=Acc|Case=Gen)")
        else:
            bits.append(f"({feat}={val})")
    if not bits:
        return "UNK"
    return "&".join(bits)


def tds(mwe: UdMwe):
    toks = [f"({tds_tok(tok)})" for tok in mwe.tokens]
    # TODO: transitive/intransitive here
    # TODO: subj/obj here
    if mwe.headword_idx is None:
        return " + ".join(toks)
    else:
        return " > ".join(
            chain(
                (toks[mwe.headword_idx],),
                (tok for idx, tok in enumerate(toks) if idx != mwe.headword_idx),
            )
        )
