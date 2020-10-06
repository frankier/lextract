from sqlalchemy import select, func, case
from functools import reduce

from wikiparse.db.tables import headword, word_sense, derived_term
from .tables import tables


RELATED_TABLES = [
    "mwe_fmt",
    "defn",
    "ud_mwe_freq",
    # "link",
]


wiktionary_gram_query = (
    select([headword.c.name, word_sense.c.id, word_sense.c.pos, word_sense.c.extra])
    .select_from(headword.join(word_sense, word_sense.c.headword_id == headword.c.id))
    .order_by(headword.c.name,)
)


wiktionary_defined_headword_query = (
    select(
        [
            headword.c.name,
            headword.c.redlink,
            case(
                value=func.sum(case(value=word_sense.c.sense, whens={"": 0}, else_=1)),
                whens={0: False},
                else_=True,
            ).label("has_senses"),
        ]
    )
    .select_from(
        headword.outerjoin(word_sense, word_sense.c.headword_id == headword.c.id)
    )
    .group_by(headword.c.name)
)


wiktionary_deriv_query = (
    select(
        [
            headword.c.name,
            derived_term.c.disp,
            derived_term.c.gloss,
            derived_term.c.extra,
        ]
    )
    .select_from(
        headword.join(derived_term, derived_term.c.headword_id == headword.c.id)
    )
    .where(derived_term.c.derived_id.is_(None))
)


def headword_id_query(lemma_query):
    return (
        select([tables["headword_freq"].c.id])
        .select_from(tables["headword_freq"],)
        .where(tables["headword_freq"].c.lemma_query == lemma_query,)
    )


def content_cols(table):
    return (col for col in table.columns if col.key not in ("id", "mwe_id"))


def joined_mat_query():
    return (
        select(
            [
                col
                for tbl in ("ud_mwe", *RELATED_TABLES)
                for col in content_cols(tables[tbl])
            ]
            + [
                tables["link"].c.name,
                # func.group_concat(
                #   case([
                #       (tables["propbank_surv"].c.prop >= 0.5, tables["propbank_surv"].c.prop),
                #   ])
                # )
            ],
        )
        .select_from(
            reduce(
                lambda acc, elm: acc.outerjoin(
                    tables[elm], tables[elm].c.mwe_id == tables["ud_mwe"].c.id
                ),
                RELATED_TABLES,
                # , "propbank_surv"
                tables["ud_mwe"],
            ).outerjoin(tables["headword_freq"])
        )
        .group_by(tables["ud_mwe"].c.id)
        .order_by(tables["ud_mwe"].c.id,)
    )


def headword_grouped(headwords=None, typ=None):
    query = (
        select(
            [
                tables["ud_mwe_token"].c.payload,
                tables["ud_mwe"].c.id,
                tables["ud_mwe"].c.typ,
                tables["mwe_fmt"].c.turkudepsearch,
                tables["link"].c.name,
            ]
        )
        .select_from(
            tables["ud_mwe"]
            .join(
                tables["ud_mwe_token"],
                (tables["ud_mwe_token"].c.mwe_id == tables["ud_mwe"].c.id)
                & (
                    tables["ud_mwe_token"].c.subword_idx
                    == tables["ud_mwe"].c.headword_idx
                ),
            )
            .join(
                tables["mwe_fmt"], tables["mwe_fmt"].c.mwe_id == tables["ud_mwe"].c.id
            )
            .join(tables["link"], tables["link"].c.mwe_id == tables["ud_mwe"].c.id)
        )
        .order_by(tables["ud_mwe_token"].c.payload)
    )
    if headwords is not None:
        query = query.where(tables["ud_mwe_token"].c.payload.in_(headwords))
    if typ is not None:
        query = query.where(tables["ud_mwe"].c.typ.in_(typ))
    return query


def mwe_for_indexing():
    # TODO: Once frequency information is included we can select the index token at query time
    return (
        select(
            [
                tables["ud_mwe"].c.id,
                tables["ud_mwe"].c.headword_idx,
                tables["ud_mwe_token"].c.payload,
                tables["ud_mwe_token"].c.payload_is_lemma,
                tables["ud_mwe_token"].c.poses,
                tables["ud_mwe_token"].c.feats,
            ]
        )
        .select_from(
            tables["ud_mwe"].join(
                tables["ud_mwe_token"],
                tables["ud_mwe_token"].c.mwe_id == tables["ud_mwe"].c.id,
            )
        )
        .order_by(tables["ud_mwe"].c.id, tables["ud_mwe_token"].c.subword_idx)
    )
