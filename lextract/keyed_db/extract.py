from typing import Any, Dict, List
from boltons.dictutils import FrozenDict
from more_itertools import chunked

from ..mweproc.consts import WILDCARD
from ..utils.lemmatise import fi_lemmatise
from .utils import frozendict_append, frozendict_order_insert
from lextract.keyed_db.tables import key_lemma as key_lemma_t, word as word_t, subword as subword_t
from .queries import key_lemmas_query, word_subwords_query


LEMMAS_CHUNK_SIZE = 256


def get_matchers(conn, all_lemmas):
    query = key_lemmas_query
    lemma_key_rows = conn.execute(query, params={"key_lemmas": list(all_lemmas)})
    key_lemmas = {}
    word_ids = []
    for row in lemma_key_rows:
        word_id = row[word_t.c.id]
        if word_id not in word_ids:
            word_ids.append(word_id)
        key_lemmas.setdefault(row[key_lemma_t.c.key_lemma], []).append(word_id)
    words = {}
    word_subword_rows = conn.execute(word_subwords_query, params={"word_ids": word_ids})
    for row in word_subword_rows:
        if row[word_t.c.id] not in words:
            words[row[word_t.c.id]] = {
                "key_idx": row[word_t.c.key_idx],
                "form": row[word_t.c.form],
                "type": row[word_t.c.type],
                "sources": row[word_t.c.sources],
                "payload": row[word_t.c.payload],
                "subwords": []
            }
        words[row[word_t.c.id]]["subwords"].append(
            (row[subword_t.c.subword_idx], row[subword_t.c.form], row[subword_t.c.lemma_feats])
        )
    return key_lemmas, words


def feats_to_set(feats):
    if isinstance(feats, (list, tuple)):
        return {tuple(elem) for elem in feats}
    else:
        return set(feats.items())


def any_subset(matcher_feats_list, cand_feats_list):
    for matcher_feat in matcher_feats_list:
        for cand_feat in cand_feats_list:
            if feats_to_set(matcher_feat).issubset(feats_to_set(cand_feat)):
                return True
    return False


def index_sentence(surfs):
    lemma_map = {}
    all_lemma_feats = []
    for idx, surf in enumerate(surfs):
        lemma_feats = fi_lemmatise(surf)
        lemmas = lemma_feats.keys()
        for lemma in lemmas:
            lemma_map.setdefault(lemma, []).append(idx)
        all_lemma_feats.append(lemma_feats)
    return lemma_map, all_lemma_feats


def conllu_to_indexed(sent):
    lemma_map = {}
    all_lemma_feats = []
    for idx, tok in enumerate(sent):
        lemma_map.setdefault(tok["lemma"], []).append(idx)
        if tok["feats"] is not None:
            all_feats = [[
                (feat.lower(), val.upper())
                for feat, val in tok["feats"].items()
            ]]
        else:
            all_feats = []
        all_lemma_feats.append({tok["lemma"]: all_feats})
    return lemma_map, all_lemma_feats


def iter_match_cands(conn, lemma_map, all_lemma_feats):
    for lemma_chunk in chunked(lemma_map.keys(), LEMMAS_CHUNK_SIZE):
        key_lemmas, words = get_matchers(conn, lemma_chunk)
        # Matched lemma
        for key_lemma, word_ids in key_lemmas.items():
            # Potential matched word
            for word_id in word_ids:
                word = words[word_id]
                # Anchor point for match
                for lemma_idx in lemma_map[key_lemma]:
                    # Check feats on key lemma
                    matcher_feats = key_matcher_feats(word, key_lemma)
                    cand_feats = all_lemma_feats[lemma_idx][key_lemma]
                    if not any_subset(matcher_feats, cand_feats):
                        continue
                    yield lemma_idx, key_lemma, word


def key_matcher_feats(word, key_lemma):
    subword_idx, _, matcher_lemma_feats = word["subwords"][word["key_idx"]]
    matcher_feats = matcher_lemma_feats[key_lemma]
    assert word["key_idx"] == subword_idx
    return matcher_feats


def match_any(matcher_lemma_feats, cand_lemma_feats):
    for match_lemma, match_feats in matcher_lemma_feats.items():
        if match_lemma not in cand_lemma_feats:
            continue
        cand_feats = cand_lemma_feats[match_lemma]
        if any_subset(match_feats, cand_feats):
            return True, False
    if WILDCARD in matcher_lemma_feats:
        if any_subset(
            matcher_lemma_feats[WILDCARD],
            (
                cand_feats
                for cand_feats_list in cand_lemma_feats.values()
                for cand_feats in cand_feats_list
            )
        ):
            return True, True

    return False, None


def extract_toks(conn, surfs: List[str], extend_wildcards=True):
    lemma_map, all_lemma_feats = index_sentence(surfs)
    return extract_toks_indexed(conn, lemma_map, all_lemma_feats, extend_wildcards=extend_wildcards)


def extract_toks_indexed(conn, lemma_map: Dict[str, int], all_lemma_feats: List[Dict[str, str]], extend_wildcards=True):
    key_lemmas, words = get_matchers(conn, lemma_map)
    for lemma_idx, key_lemma, word in iter_match_cands(conn, lemma_map, all_lemma_feats):
        # Check lemma and feats for other lemmas
        subwords = list(enumerate(word["subwords"]))

        def step(dir):
            return select_tok_step(
                lambda n: n + dir,
                extend_wildcards,
                all_lemma_feats,
                subwords,
                lemma_idx + dir,
                word["key_idx"] + dir,
                FrozenDict(),
            )
        left_matches = step(-1)
        if not left_matches:
            continue
        right_matches = step(1)
        if not right_matches:
            continue
        key_matching = FrozenDict(((word["key_idx"], frozenset((lemma_idx,))),))
        yield (
            {
                FrozenDict({**left_matching, **key_matching, **right_matching})
                for left_matching in left_matches
                for right_matching in right_matches
            },
            word
        )


def select_tok_step(next_idx, extend_wildcards, all_lemma_feats, subwords, word_idx, matcher_idx, matchings):
    if matcher_idx < 0 or matcher_idx >= len(subwords):
        return {matchings}
    if word_idx < 0 or word_idx >= len(all_lemma_feats):
        return set()
    idx, (subword_idx, _, matcher_lemma_feats) = subwords[matcher_idx]
    assert idx == subword_idx
    matches, is_wildcard_match = match_any(matcher_lemma_feats, all_lemma_feats[word_idx])
    all_matches = set()
    if not matches:
        return all_matches
    new_matchings = frozendict_append(matchings, matcher_idx, word_idx)
    if is_wildcard_match and extend_wildcards:
        all_matches.update(
            select_tok_step(
                next_idx,
                extend_wildcards,
                all_lemma_feats,
                subwords,
                next_idx(word_idx),
                matcher_idx,
                new_matchings,
            )
        )
    all_matches.update(
        select_tok_step(
            next_idx,
            extend_wildcards,
            all_lemma_feats,
            subwords,
            next_idx(word_idx),
            next_idx(matcher_idx),
            new_matchings,
        )
    )
    return all_matches


def make_tree_index(tree, index):
    index[tree.token["id"]] = tree
    for child in tree.children:
        make_tree_index(child, index)


def expand_node(tree_index, lemma_idx, cand_set=frozenset()):
    node = tree_index[lemma_idx]
    return cand_set | frozenset(
        ([tree_index[node.token["head"]].token["id"]] if node.token["head"] != 0 else []) + 
        [child.token["id"] for child in node.children]
    )


def extract_deps(conn, sent, use_conllu_feats=False):
    for idx, tok in enumerate(sent):
        assert tok["id"] == idx + 1
    tree = sent.to_tree()
    if use_conllu_feats:
        # XXX: Might be good add an option to combine both?
        lemma_map, all_lemma_feats = conllu_to_indexed(sent)
    else:
        lemma_map, all_lemma_feats = index_sentence((token["form"] for token in sent))
    tree_index = {}
    make_tree_index(tree, tree_index)
    for lemma_idx, key_lemma, word in iter_match_cands(conn, lemma_map, all_lemma_feats):
        lemma_id = lemma_idx + 1
        matches = select_dep_step(
            all_lemma_feats,
            tree_index,
            word["subwords"],
            expand_node(tree_index, lemma_id),
            frozenset((lemma_id,)),
            frozenset((word["key_idx"],)),
            FrozenDict(((word["key_idx"], frozenset((lemma_idx,))),))
        )
        if matches:
            yield matches, word


def select_dep_step(all_lemma_feats, tree_index, subwords, cand_set, used_cands, used_subwords, matchings, extend_wildcards=True):
    if len(used_subwords) == len(subwords):
        return {matchings}
    all_matches = set()
    for cand_id in cand_set - used_cands:
        for idx, (subword_idx, _, matcher_lemma_feats) in enumerate(subwords):
            assert idx == subword_idx
            if subword_idx in used_subwords:
                continue
            matches, is_wildcard_match = match_any(matcher_lemma_feats, all_lemma_feats[cand_id - 1])
            if not matches:
                continue
            new_cand_set = expand_node(tree_index, cand_id, cand_set)
            new_used_cands = used_cands | {cand_id}
            new_matchings = frozendict_order_insert(matchings, subword_idx, cand_id - 1)
            if is_wildcard_match and extend_wildcards:
                extra_matched, extra_cand_set, new_used_cands = expand_wildcard_dep(
                    all_lemma_feats,
                    tree_index,
                    matcher_lemma_feats,
                    expand_node(tree_index, cand_id),
                    new_used_cands,
                )
                new_cand_set = new_cand_set | extra_cand_set
                for extra_match in extra_matched:
                    new_matchings = frozendict_order_insert(new_matchings, subword_idx, extra_match)
            all_matches.update(select_dep_step(
                all_lemma_feats,
                tree_index,
                subwords,
                new_cand_set,
                new_used_cands,
                used_subwords | {subword_idx},
                new_matchings
            ))
    return all_matches


def expand_wildcard_dep(all_lemma_feats, tree_index, wildcard_lemma_feats, cand_set, used_cands):
    matched = []
    while 1:
        changed = False
        for cand_id in cand_set - used_cands:
            matches, is_wildcard_match = match_any(wildcard_lemma_feats, all_lemma_feats[cand_id - 1])
            if not matches:
                continue
            assert is_wildcard_match
            changed = True
            cand_set = expand_node(tree_index, cand_id, cand_set)
            used_cands = used_cands | {cand_id}
            matched.append(cand_id - 1)
        if not changed:
            break
    return matched, cand_set, used_cands


def match_length(matching):
    return sum((len(tok_idxs) for tok_idxs in matching.values()))


def longest_matches(matchings):
    longest_match_len = 0
    longest_matches = set()
    for matching in matchings:
        cur_match_len = match_length(matching)
        if cur_match_len > longest_match_len:
            longest_match_len = cur_match_len
            longest_matches = {matching}
        elif cur_match_len == longest_match_len:
            longest_matches.add(matching)
    return longest_matches
