from boltons.dictutils import FrozenDict
from wikiparse.utils.db import get_session
from sqlalchemy import select, func
import conllu

from lextract.keyed_db.utils import fi_lemmatise
from lextract.keyed_db.builddb import index_wordlist, insert_indexed
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


def create_phrase_test_db(db_path):
    session = get_session(db_path)
    metadata.create_all(session().get_bind().engine)
    insert_indexed(
        session,
        index_wordlist(
            [(w, ["test"]) for w in TEST_WORDS],
            fi_lemmatise
        )
    )
    return session


TEST_FRAMES = [
    (
        'pitää ___-ta ___-na',
        ('wiktionary_frames',),
        'frame',
        0,
        [
            ('pitää', {'pitää': {()}}),
            ('partitive', {'__WILDCARD__': {(('case', 'PAR'),)}}),
            ('essive', {'__WILDCARD__': {(('case', 'ESS'),)}})
        ],
        {'type': 'wiktionary_frame', 'sense_id': 70087}
    ),
    (
        'pitää ___-sta kiinni',
        ('wiktionary_frames',),
        'frame',
        0,
        [
            ('pitää', {'pitää': {()}}),
            ('elative', {'__WILDCARD__': {(('case', 'ELA'),)}}),
            ('kiinni', {'kiinni': {()}})
        ],
        {'type': 'wiktionary_frame', 'sense_id': 70090}
    ),
    (
        'pitää ___-sta',
        ('wiktionary_frames',),
        'frame',
        0,
        [
            ('pitää', {'pitää': {()}}),
            ('elative', {'__WILDCARD__': {(('case', 'ELA'),)}})
        ],
        {'type': 'wiktionary_frame', 'sense_id': 70086}
    ),
    (
        'kummuta ___-sta',
        ('wiktionary_frames',),
        'frame',
        0,
        [
            ('kummuta', {'kummuta': {()}}),
            ('elative', {'__WILDCARD__': {(('case', 'ELA'),)}})
        ],
        {'type': 'wiktionary_frame', 'sense_id': 169567}
    ),
]


def create_frame_test_db(db_path):
    session = get_session(db_path)
    metadata.create_all(session().get_bind().engine)
    insert_indexed(session, TEST_FRAMES)
    return session


@pytest.fixture(scope="module")
def phrase_testdb():
    return create_phrase_test_db("sqlite://")


@pytest.fixture(scope="module")
def frame_testdb():
    return create_frame_test_db("sqlite://")


def test_insert_worked(phrase_testdb):
    word_count = phrase_testdb.execute(
        select([func.count(word_t.c.id)])
    ).scalar()
    assert word_count == 5


def assert_token_matches(matches, expected_matches):
    assert len(matches) == len(expected_matches)
    for (matchings, word), (expected_matchings, expected_match_form) in zip(matches, expected_matches):
        assert matchings == expected_matchings
        assert word["form"] == expected_match_form


fd = FrozenDict


@pytest.mark.parametrize(
    "toks,expected_matches",
    [
        (["humaloissa"], [({fd({0: (0,)})}, "humalassa")]),
        (["älä", "tule", "humalaan"], [({fd({0: (1,), 1: (2,)})}, "tulla humalaan"), ({fd({0: (2,)})}, "humalaan")]),
        (["tulevasta"], [({fd({0: (0,)})}, "tuleva")]),
    ],
)
def test_token_matches(phrase_testdb, toks, expected_matches):
    matches = list(extract_toks(phrase_testdb, toks))
    assert_token_matches(matches, expected_matches)


CONLLS = """
1	Hän	hän	PRON	_	Case=Nom|Number=Sing|Person=3|PronType=Prs	2	nsubj	_	_
2	tuli	tulla	VERB	_	Mood=Ind|Number=Sing|Person=3|Tense=Past|VerbForm=Fin|Voice=Act	0	root	_	_
3	humalaan	humala	NOUN	_	Case=Ill|Number=Sing	2	nmod	_	_
4	.	.	PUNCT	_	_	2	punct	_	_

1	Minä	minä	PRON	_	Case=Nom|Number=Sing|Person=1|PronType=Prs	2	nsubj	_	_
2	pidän	pitää	VERB	_	Mood=Ind|Number=Sing|Person=1|Tense=Pres|VerbForm=Fin|Voice=Act	0	root	_	_
3	voileipäkakusta	voi#leipä#kakku	NOUN	_	Case=Ela|Number=Sing	2	nmod	_	_
4	.	.	PUNCT	_	_	2	punct	_	_
""".strip().split("\n\n")


@pytest.mark.parametrize(
    "conll,expected_matches",
    [
        (CONLLS[0], {"humalaan": fd({0: (3,)}), "tulla humalaan": fd({0: (2,), 1: (3,)})}),
    ],
)
def test_dep_matches(phrase_testdb, conll, expected_matches):
    sent = conllu.parse(conll)[0]
    matches = list(extract_deps(phrase_testdb, sent))
    assert len(matches) == len(expected_matches)
    for matchings, word in matches:
        assert word["form"] in expected_matches
        assert {expected_matches[word["form"]]} == matchings


@pytest.mark.parametrize(
    "toks,expected_matches",
    [
        (
            ["Minä", "pidän", "voileipäkakusta"],
            [({fd({0: (1,), 1: (2,)})}, 'pitää ___-sta')]
        ),
        (
            ["Minä", "pidän", "herkullisesta", "voileipäkakusta"],
            [({fd({0: (1,), 1: (2, 3)}), fd({0: (1,), 1: (2,)})}, 'pitää ___-sta')]
        ),
        (
            ["Minä", "voin", "pitää", "laukustasi", "kiinni", "."],
            [
                ({fd({0: (2,), 1: (3,), 2: (4,)})}, 'pitää ___-sta kiinni'),
                ({fd({0: (2,), 1: (3,)})}, 'pitää ___-sta')
            ]
        ),
        (
            ["Minä", "pidän", "ihmisiä", "vihamielisinä"],
            [({fd({0: (1,), 1: (2,), 2: (3,)})}, 'pitää ___-ta ___-na')]
        ),
        (
            ["Minä", "pidän", "siistiä", "ihmisiä", "vihamielisinä"],
            [({fd({0: (1,), 1: (2, 3), 2: (4,)})}, 'pitää ___-ta ___-na')]
        ),
    ],
)
def test_token_frame_matches(frame_testdb, toks, expected_matches):
    matches = list(extract_toks(frame_testdb, toks))
    assert_token_matches(matches, expected_matches)
