from sqlalchemy import Column, String, MetaData, Table, Float, ForeignKey, Integer, JSON
from wikiparse.utils.db import get_session, insert

metadata = MetaData()


key_lemma = Table(
    "key_lemma",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("key_lemma", String, index=True),
    Column("word_id", ForeignKey("word.id")),
)


word = Table(
    "word",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("key_idx", Integer),
    Column("form", String),
    Column("type", String),
    # e.g. inflection/multiword
    Column("sources", JSON),
    # e.g. ["wordnet", "wiktionary"]
    Column("payload", JSON),
    # Usually a reference to the actual thing in case its not obvious how to
    # get if from the form and sources alone
)


subword = Table(
    "subword",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("word_id", ForeignKey("word.id"), index=True),
    Column("subword_idx", Integer, index=True),
    Column("form", String),
    Column("lemma_feats", JSON),
)
