from boltons.dictutils import FrozenDict
from wikiparse.utils.db import get_session
from sqlalchemy import select, func
import conllu

from lextract.keyed_db.utils import fi_lemmatise
from lextract.keyed_db.builddb import insert_wordlist
from lextract.keyed_db.extract import extract_deps, extract_toks
from lextract.keyed_db.tables import metadata, word as word_t

import pytest


TEST_WORDS = [
    ("tulla", "humalaan"),
    ("humalaan",),
    ("humalassa",),
    ("tuleva",),
    ("tuleminen",),
]


def create_test_db(db_path):
    session = get_session(db_path)
    metadata.create_all(session().get_bind().engine)
    insert_wordlist(session, [(w, ["test"]) for w in TEST_WORDS], fi_lemmatise)
    return session


@pytest.fixture(scope="module")
def testdb():
    return create_test_db("sqlite://")


def test_insert_worked(testdb):
    word_count = testdb.execute(
        select([func.count(word_t.c.id)])
    ).scalar()
    assert word_count == 5


@pytest.mark.parametrize(
    "toks,expected_matches",
    [
        (["humaloissa"], [(0, "humalassa")]),
        (["älä", "tule", "humalaan"], [(1, "tulla humalaan"), (2, "humalaan")]),
        (["tulevasta"], [(0, "tuleva")]),
    ],
)
def test_token_matches(testdb, toks, expected_matches):
    matches = list(extract_toks(testdb, toks))
    assert len(matches) == len(expected_matches)
    for (match_start, word), (expected_match_start, expected_match_form) in zip(matches, expected_matches):
        assert match_start == expected_match_start
        assert word["form"] == expected_match_form


CONLLS = """
1	Minä	minä	PRON	_	Case=Nom|Number=Sing|Person=1|PronType=Prs	2	nsubj	_	_
2	pidän	pitää	VERB	_	Mood=Ind|Number=Sing|Person=1|Tense=Pres|VerbForm=Fin|Voice=Act	0	root	_	_
3	voileipäkakusta	voi#leipä#kakku	NOUN	_	Case=Ela|Number=Sing	2	nmod	_	_
4	.	.	PUNCT	_	_	2	punct	_	_

1	Hän	hän	PRON	_	Case=Nom|Number=Sing|Person=3|PronType=Prs	2	nsubj	_	_
2	tuli	tulla	VERB	_	Mood=Ind|Number=Sing|Person=3|Tense=Past|VerbForm=Fin|Voice=Act	0	root	_	_
3	humalaan	humala	NOUN	_	Case=Ill|Number=Sing	2	nmod	_	_
4	.	.	PUNCT	_	_	2	punct	_	_
""".strip().split("\n\n")


@pytest.mark.parametrize(
    "conll,expected_matches",
    [
        # XXX: TODO: Add pitää ___-sta
        (CONLLS[0], []),
        (CONLLS[1], {"humalaan": FrozenDict({0: 3}), "tulla humalaan": FrozenDict({0: 2, 1: 3})}),

    ],
)
def test_dep_matches(testdb, conll, expected_matches):
    sent = conllu.parse(conll)[0]
    matches = list(extract_deps(testdb, sent))
    assert len(matches) == len(expected_matches)
    for matchings, word in matches:
        assert word["form"] in expected_matches
        assert {expected_matches[word["form"]]} == matchings
