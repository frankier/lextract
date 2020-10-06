from sqlalchemy import (
    Column,
    String,
    MetaData,
    Table,
    ForeignKey,
    Integer,
    JSON,
    Boolean,
)


tables = {}


def create_tables(metadata=None):
    if metadata is None:
        metadata = MetaData()
    tables["key_lemma"] = Table(
        "key_lemma",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("key_lemma", String, index=True),
        Column("word_id", ForeignKey("word.id")),
    )

    tables["word"] = Table(
        "word",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("ud_mwe_id", ForeignKey("ud_mwe.id"), unique=True, index=True),
        Column("key_idx", Integer),
        Column("key_is_head", Boolean),
    )

    tables["subword"] = Table(
        "subword",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("word_id", ForeignKey("word.id"), index=True),
        Column("subword_idx", Integer, index=True),
        Column("lemma_feats", JSON),
    )
    return metadata


def extend_mweproc():
    from lextract.mweproc.db.confs import setup_embed
    from lextract.mweproc.db.tables import metadata as mweproc_metadata

    setup_embed()
    return create_tables(mweproc_metadata)
