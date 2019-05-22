from sqlalchemy import select

from .tables import key_lemma, word, subword


def key_lemmas_query(key_lemmas):
    return \
        select([
            key_lemma.c.key_lemma,
            word.c.id,
        ]).select_from(
            key_lemma.join(
                word,
                key_lemma.c.word_id == word.c.id
            )
        ).where(
            key_lemma.c.key_lemma.in_(key_lemmas)
        ).order_by(
            key_lemma.c.key_lemma,
            word.c.id,
        )


def word_subwords_query(word_ids):
    return \
        select([
            word.c.id,
            word.c.key_idx,
            word.c.form,
            word.c.type,
            word.c.sources,
            word.c.payload,
            subword.c.subword_idx,
            subword.c.form,
            subword.c.lemma_feats,
        ]).select_from(
            word.join(
                subword,
                subword.c.word_id == word.c.id
            )
        ).where(
            word.c.id.in_(word_ids)
        ).order_by(
            word.c.id,
            subword.c.subword_idx,
        )