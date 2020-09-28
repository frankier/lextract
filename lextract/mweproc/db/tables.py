from sqlalchemy import Column, String, MetaData, Table, ForeignKey, Integer, JSON, Boolean, Float
from sqlalchemy.types import Enum
from sqlalchemy.schema import UniqueConstraint
from lextract.utils.core import run_once
from ..models import MweType

metadata = MetaData()
tables = {}


def add_table(name, *cols):
    tables[name] = Table(
        name,
        metadata,
        *cols,
    )


def contribute_to_table(name, *cols):
    for col in cols:
        tables[name].append_column(col)


@run_once
def add_base():
    add_table(
        "ud_mwe",
        Column("id", Integer, primary_key=True),
        Column("typ", Enum(MweType)),
        Column("poses", JSON),
        Column("headword_idx", Integer),
    )

    add_table(
        "ud_mwe_token",
        Column("id", Integer, primary_key=True),
        Column("mwe_id", ForeignKey("ud_mwe.id"), index=True),
        Column("subword_idx", Integer, index=True),
        Column("payload", String),
        Column("payload_is_lemma", Boolean),
        Column("poses", JSON),
        Column("feats", JSON),
    )

    add_table(
        "link",
        Column("id", Integer, primary_key=True),
        Column("mwe_id", ForeignKey("ud_mwe.id"), index=True, unique=True),
        Column("name", String),
        Column("payload", JSON),
    )

    add_table(
        "wiktionary_hw_link",
        Column("id", Integer, primary_key=True),
        Column("mwe_id", ForeignKey("ud_mwe.id"), index=True, unique=True),
        Column("page_exists", Boolean),
        Column("has_senses", Boolean),
    )


@run_once
def add_freq():
    add_table(
        "headword_freq",
        Column("id", Integer, primary_key=True),
        Column("lemma_query", String, unique=True),
        Column("lemma", String),
        Column("wordfreq", Float),
        Column("wordfreq_zipf", Float),
        Column("internet_parsebank_cnt", Integer),
    )

    add_table(
        "headword_propbank_freqs",
        Column("id", Integer, primary_key=True),
        Column("headword_freq_id", ForeignKey("headword_freq.id"), index=True),
        Column("prop", String, index=True),
        Column("cnt", Integer),
        UniqueConstraint('headword_freq_id', 'prop', name='headword_prop'),
    )

    contribute_to_table(
        "ud_mwe",
        Column("headword_freq_id", ForeignKey("headword_freq.id"), index=True),
    )

    add_table(
        "ud_mwe_freq",
        Column("mwe_id", ForeignKey("ud_mwe.id"), unique=True, index=True),
        Column("internet_parsebank_cnt", Integer),
    )

    add_table(
        "propbank_freqs",
        Column("id", Integer, primary_key=True),
        Column("mwe_id", ForeignKey("ud_mwe.id"), index=True),
        Column("prop", String, index=True),
        Column("cnt", Integer),
        UniqueConstraint('mwe_id', 'prop', name='mwe_prop_freq_uniq'),
    )

    add_table(
        "propbank_surv",
        Column("id", Integer, primary_key=True),
        Column("mwe_id", ForeignKey("ud_mwe.id"), index=True),
        Column("prop", String, index=True),
        Column("surv", Float),
        UniqueConstraint('mwe_id', 'prop', name='prop_prop_surv_uniq'),
    )


@run_once
def add_mat():
    add_table(
        "mwe_fmt",
        Column("id", Integer, primary_key=True),
        Column("mwe_id", ForeignKey("ud_mwe.id"), index=True, unique=True),
        Column("gapped_mwe", String),
        Column("pos_info", String),
        Column("turkudepsearch", String),
    )

    add_table(
        "defn",
        Column("id", Integer, primary_key=True),
        Column("mwe_id", ForeignKey("ud_mwe.id"), index=True, unique=True),
        Column("defn", String),
    )
