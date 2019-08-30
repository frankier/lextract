from sqlalchemy import select, bindparam

from .tables import key_lemma, word, subword
from wikiparse.tables import headword, word_sense


key_lemmas_query = select([
    key_lemma.c.key_lemma,
    word.c.id,
]).select_from(
    key_lemma.join(
        word,
        key_lemma.c.word_id == word.c.id
    )
).where(
    key_lemma.c.key_lemma.in_(bindparam("key_lemmas", expanding=True))
)


word_subwords_query = select([
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
    word.c.id.in_(bindparam("word_ids", expanding=True))
).order_by(
    subword.c.subword_idx,
)


wiktionary_gram_query = select([
    headword.c.name,
    word_sense.c.id,
    word_sense.c.pos,
    word_sense.c.extra,
]).select_from(
    headword.join(
        word_sense,
        word_sense.c.headword_id == headword.c.id
    )
).order_by(
    headword.c.name,
)
