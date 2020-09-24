from typing import List, Optional, Set

from boltons.dictutils import FrozenDict
from wikiparse.utils.db import get_session
from sqlalchemy import select, func
import conllu
import pytest

from lextract.keyed_db.queries import mwe_ids_as_gapped_mwes
from lextract.mweproc.db.muts import insert_mwe
from lextract.mweproc.db.queries import mwe_for_indexing
from lextract.mweproc.models import MweType, UdMwe, UdMweToken
from lextract.mweproc.sources.common import build_simple_mwe
from lextract.keyed_db.builddb import add_keyed_words, IndexingResult
from lextract.keyed_db.extract import extract_deps, extract_toks

fd = FrozenDict


def fs(*it):
    return frozenset(it)


TEST_WORDS = [
    ("tulla", "humalaan"),
    ("humalaan",),
    ("humalassa",),
    ("tuleva",),
    ("tuleminen",),
]


def mk_test_mwe(hw_bits, headword_idx):
    if len(hw_bits) > 1:
        typ = MweType.multiword
    else:
        typ = MweType.inflection
    mwe = build_simple_mwe(
        hw_bits,
        typ=typ,
        links=[],
        headword_idx=headword_idx,
    )
    return mwe


def create_db(db_path):
    from lextract.mweproc.db.tables import metadata as mweproc_metadata
    session = get_session(db_path)
    engine = session().get_bind().engine
    mweproc_metadata.create_all(engine)
    return session


def create_phrase_test_db(db_path):
    session = create_db(db_path)
    hw_cnts_cache = {}
    for word in TEST_WORDS:
        mwe = mk_test_mwe(word, 0)
        insert_mwe(
            session,
            mwe,
            hw_cnts_cache,
            freqs=False,
            materialize=True
        )
    session.commit()
    mwe_it = session.execute(mwe_for_indexing())
    for indexing_result in add_keyed_words(session, mwe_it, True, True, False):
        assert indexing_result != IndexingResult.FAIL
    session.commit()
    return session


TEST_FRAMES = [
    (
        'pitää ___-ta ___-na',
        0,
        [
            ('pitää', {}),
            (None, {'Case': 'Par'}),
            (None, {'Case': 'Ess'})
        ],
    ),
    (
        'pitää ___-sta kiinni',
        0,
        [
            ('pitää', {}),
            (None, {'Case': 'Ela'}),
            ('kiinni', {})
        ],
    ),
    (
        'pitää ___-sta',
        0,
        [
            ('pitää', {}),
            (None, {'Case': 'Ela'})
        ],
    ),
    (
        'kummuta ___-sta',
        0,
        [
            ('kummuta', {}),
            (None, {'Case': 'Ela'})
        ],
    ),
]


def create_frame_test_db(db_path):
    session = create_db(db_path)
    hw_cnts_cache = {}
    for gapped, headword_idx, subwords in TEST_FRAMES:
        mwe = UdMwe(
            tokens=[
                UdMweToken(
                    payload=payload,
                    feats=feats,
                )
                for payload, feats in subwords
            ],
            typ=MweType.frame,
            headword_idx=headword_idx
        )
        insert_mwe(
            session,
            mwe,
            hw_cnts_cache,
            freqs=False,
            materialize=True
        )
    session.commit()
    mwe_it = session.execute(mwe_for_indexing())
    for indexing_result in add_keyed_words(session, mwe_it, True, True, False):
        assert indexing_result != IndexingResult.FAIL
    session.commit()
    return session


@pytest.fixture(scope="module")
def phrase_testdb():
    return create_phrase_test_db("sqlite://")


@pytest.fixture(scope="module")
def frame_testdb():
    return create_frame_test_db("sqlite://")


def test_insert_worked(phrase_testdb):
    from lextract.keyed_db.tables import tables
    word_count = phrase_testdb.execute(
        select([func.count(tables["word"].c.id)])
    ).scalar()
    assert word_count == 5


def assert_token_matches(session, matches, expected_matches):
    assert len(matches) == len(expected_matches)
    gapped_query = mwe_ids_as_gapped_mwes()
    actual_matches = set()
    for matchings, word in matches:
        gapped_mwe = next(session.execute(gapped_query, params={"ud_mwe_id": word["ud_mwe_id"]}))[0]
        actual_matches.add((frozenset(matchings), gapped_mwe))
    assert actual_matches == set(expected_matches)


def assert_dep_matches(session, matches, expected_matches):
    assert len(matches) == len(expected_matches)
    gapped_query = mwe_ids_as_gapped_mwes()
    for matchings, word in matches:
        gapped_mwe = next(session.execute(gapped_query, params={"ud_mwe_id": word["ud_mwe_id"]}))[0]
        assert gapped_mwe in expected_matches
        assert {expected_matches[gapped_mwe]} == matchings


@pytest.mark.parametrize(
    "toks,expected_matches",
    [
        (["humaloissa"], [(fs(fd({0: fs(0)})), "humalassa")]),
        (["älä", "tule", "humalaan"], [(fs(fd({0: fs(1), 1: fs(2)})), "tulla humalaan"), (fs(fd({0: fs(2)})), "humalaan")]),
        (["tulevasta"], [(fs(fd({0: fs(0)})), "tuleva")]),
    ],
)
def test_token_matches(phrase_testdb, toks, expected_matches):
    matches = list(extract_toks(phrase_testdb, toks))
    assert_token_matches(phrase_testdb, matches, expected_matches)


CONLLS = """
1	Hän	hän	PRON	_	Case=Nom|Number=Sing|Person=3|PronType=Prs	2	nsubj	_	_
2	tuli	tulla	VERB	_	Mood=Ind|Number=Sing|Person=3|Tense=Past|VerbForm=Fin|Voice=Act	0	root	_	_
3	humalaan	humala	NOUN	_	Case=Ill|Number=Sing	2	nmod	_	_
4	.	.	PUNCT	_	_	2	punct	_	_

1	Minä	minä	PRON	_	Case=Nom|Number=Sing|Person=1|PronType=Prs	2	nsubj	_	_
2	pidän	pitää	VERB	_	Mood=Ind|Number=Sing|Person=1|Tense=Pres|VerbForm=Fin|Voice=Act	0	root	_	_
3	voileipäkakusta	voi#leipä#kakku	NOUN	_	Case=Ela|Number=Sing	2	nmod	_	_
4	.	.	PUNCT	_	_	2	punct	_	_

1	Minä	minä	PRON	_	Case=Nom|Number=Sing|Person=1|PronType=Prs	2	nsubj	_	_
2	pidän	pitää	VERB	_	Mood=Ind|Number=Sing|Person=1|Tense=Pres|VerbForm=Fin|Voice=Act	0	root	_	_
3	herkullisesta	herkullinen	ADJ	_	Case=Ela|Degree=Pos|Number=Sing	4	amod	_	_
4	voileipäkakusta	voi#leipä#kakku	NOUN	_	Case=Ela|Number=Sing	2	nmod	_	_
5	.	.	PUNCT	_	_	2	punct	_	_

1	Minä	minä	PRON	_	Case=Nom|Number=Sing|Person=1|PronType=Prs	2	nsubj	_	_
2	pidän	pitää	VERB	_	Mood=Ind|Number=Sing|Person=1|Tense=Pres|VerbForm=Fin|Voice=Act	0	root	_	_
3	suomen	suomi	NOUN	_	Case=Gen|Number=Sing	4	nmod:poss	_	_
4	voileipäkakusta	voi#leipä#kakku	NOUN	_	Case=Ela|Number=Sing	2	nmod	_	_
5	.	.	PUNCT	_	_	2	punct	_	_

1	Mistä	mikä	PRON	_	Case=Ela|Number=Sing|PronType=Int	4	nmod	_	_
2	heidän	hän	PRON	_	Case=Gen|Number=Plur|Person=3|PronType=Prs	3	nmod:poss	_	_
3	vihansa	viha	NOUN	_	Case=Gen|Number=Sing|Person[psor]=3	4	dobj	_	_
4	kumpuaa	kummuta	VERB	_	Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin|Voice=Act	0	root	_	_
5	?	?	PUNCT	_	_	4	punct	_	_
""".strip().split("\n\n")


@pytest.mark.parametrize(
    "use_conllu_feats", [False, True]
)
@pytest.mark.parametrize(
    "conll,expected_matches",
    [
        (CONLLS[0], {"humalaan": fd({0: fs(2)}), "tulla humalaan": fd({0: fs(1), 1: fs(2)})}),
    ],
)
def test_dep_matches(phrase_testdb, use_conllu_feats, conll, expected_matches):
    sent = conllu.parse(conll)[0]
    matches = list(extract_deps(phrase_testdb, sent, use_conllu_feats=use_conllu_feats))
    assert_dep_matches(phrase_testdb, matches, expected_matches)


@pytest.mark.parametrize(
    "toks,expected_matches",
    [
        (
            ["Minä", "pidän", "voileipäkakusta"],
            [(fs(fd({0: fs(1), 1: fs(2)})), 'pitää ___-sta')]
        ),
        (
            ["Minä", "pidän", "herkullisesta", "voileipäkakusta"],
            [(fs(fd({0: fs(1), 1: fs(2, 3)}), fd({0: fs(1), 1: fs(2)})), 'pitää ___-sta')]
        ),
        (
            ["Minä", "voin", "pitää", "laukustasi", "kiinni", "."],
            [
                (fs(fd({0: fs(2), 1: fs(3), 2: fs(4)})), 'pitää ___-sta kiinni'),
                (fs(fd({0: fs(2), 1: fs(3)})), 'pitää ___-sta')
            ]
        ),
        (
            ["Minä", "pidän", "ihmisiä", "vihamielisinä"],
            [(fs(fd({0: fs(1), 1: fs(2), 2: fs(3)})), 'pitää ___-ta ___-na')]
        ),
        (
            ["Minä", "pidän", "siistiä", "ihmisiä", "vihamielisinä"],
            [(fs(fd({0: fs(1), 1: fs(2, 3), 2: fs(4)})), 'pitää ___-ta ___-na')]
        ),
    ],
)
def test_token_frame_matches(frame_testdb, toks, expected_matches):
    matches = list(extract_toks(frame_testdb, toks))
    assert_token_matches(frame_testdb, matches, expected_matches)


@pytest.mark.parametrize(
    "use_conllu_feats", [False, True]
)
@pytest.mark.parametrize(
    "conll,expected_matches",
    [
        (
            CONLLS[1],
            {
                'pitää ___-sta': fd({0: fs(1), 1: fs(2)})
            }
        ),
        (
            CONLLS[2],
            {
                'pitää ___-sta': fd({0: fs(1), 1: fs(2, 3)})
            }
        ),
        (
            CONLLS[3],
            {
                'pitää ___-sta': fd({0: fs(1), 1: fs(3)})
            }
        ),
        (
            CONLLS[4],
            {
                'kummuta ___-sta': fd({0: fs(3), 1: fs(0)})
            }
        ),
    ],
)
def test_dep_frame_matches(frame_testdb, use_conllu_feats, conll, expected_matches):
    sent = conllu.parse(conll)[0]
    matches = list(extract_deps(frame_testdb, sent, use_conllu_feats=use_conllu_feats))
    assert_dep_matches(frame_testdb, matches, expected_matches)


def test_longest_matches():
    from lextract.keyed_db.extract import longest_matches
    assert longest_matches({fd({0: fs(1), 1: fs(2, 3)}), fd({0: fs(1), 1: fs(2)})}) == {fd({0: fs(1), 1: fs(2, 3)})}


def test_olla_must():
    from common import must_olla_mwe
    session = create_db("sqlite://")
    mwes = must_olla_mwe()
    for mwe in mwes:
        insert_mwe(
            session,
            mwe,
            {},
            freqs=False,
            materialize=True
        )
    session.commit()
    last_row = list(session.execute(mwe_for_indexing()))[-1]
    assert last_row[-1] == {'Tense': 'Pres', 'Voice': 'Pass', 'VerbForm': 'Part'}
