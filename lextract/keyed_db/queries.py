from sqlalchemy import select, bindparam


def key_lemmas_query():
    from .tables import tables

    key_lemma = tables["key_lemma"]
    word = tables["word"]
    return (
        select([key_lemma.c.key_lemma, word.c.id])
        .select_from(key_lemma.join(word, key_lemma.c.word_id == word.c.id))
        .where(key_lemma.c.key_lemma.in_(bindparam("key_lemmas", expanding=True)))
    )


def word_subwords_query():
    from .tables import tables

    word = tables["word"]
    subword = tables["subword"]
    return (
        select(
            [
                word.c.id,
                word.c.key_idx,
                word.c.key_is_head,
                word.c.ud_mwe_id,
                subword.c.word_id,
                subword.c.subword_idx,
                subword.c.lemma_feats,
            ]
        )
        .select_from(word.join(subword, subword.c.word_id == word.c.id))
        .where(word.c.id.in_(bindparam("word_ids", expanding=True)))
        .order_by(subword.c.subword_idx,)
    )


def mwe_ids_as_gapped_mwes():
    from lextract.mweproc.db.tables import tables

    ud_mwe = tables["ud_mwe"]
    mwe_fmt = tables["mwe_fmt"]
    return (
        select([mwe_fmt.c.gapped_mwe])
        .select_from(ud_mwe.join(mwe_fmt, mwe_fmt.c.mwe_id == ud_mwe.c.id))
        .where(ud_mwe.c.id == bindparam("ud_mwe_id"))
    )
