from typing import List
from boltons.dictutils import FrozenDict

from .consts import WILDCARD
from .utils import fi_lemmatise, frozendict_append
from lextract.keyed_db.tables import key_lemma as key_lemma_t, word as word_t, subword as subword_t
from .queries import key_lemmas_query, word_subwords_query


def get_matchers(session, all_lemmas):
    query = key_lemmas_query(all_lemmas)
    lemma_key_rows = session.execute(query)
    key_lemmas = {}
    word_ids = set()
    for row in lemma_key_rows:
        word_ids.add(row[word_t.c.id])
        key_lemmas.setdefault(row[key_lemma_t.c.key_lemma], []).append(row[word_t.c.id])
    words = {}
    word_subword_rows = session.execute(word_subwords_query(word_ids))
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


def iter_match_cands(session, lemma_map, all_lemma_feats):
    key_lemmas, words = get_matchers(session, lemma_map.keys())
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


def extract_toks(session, surfs: List[str], extend_wildcards=True):
    lemma_map, all_lemma_feats = index_sentence(surfs)
    key_lemmas, words = get_matchers(session, lemma_map)
    for lemma_idx, key_lemma, word in iter_match_cands(session, lemma_map, all_lemma_feats):
        # Check lemma and feats for other lemmas
        subwords = list(enumerate(word["subwords"]))
        left_matches = select_tok_step(
            lambda n: n - 1,
            extend_wildcards,
            all_lemma_feats,
            subwords,
            lemma_idx - 1,
            word["key_idx"] - 1,
            FrozenDict(),
        )
        if not left_matches:
            continue
        right_matches = select_tok_step(
            lambda n: n + 1,
            extend_wildcards,
            all_lemma_feats,
            subwords,
            lemma_idx + 1,
            word["key_idx"] + 1,
            FrozenDict(),
        )
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


def extract_deps(session, sent):
    for idx, tok in enumerate(sent):
        assert tok["id"] == idx + 1
    tree = sent.to_tree()
    # XXX: Might be good to (optionally?) use feats from conll as-well or instead
    lemma_map, all_lemma_feats = index_sentence((token["form"] for token in sent))
    tree_index = {}
    make_tree_index(tree, tree_index)
    for lemma_idx, key_lemma, word in iter_match_cands(session, lemma_map, all_lemma_feats):
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
            new_matchings = frozendict_append(matchings, subword_idx, cand_id - 1)
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
                    new_matchings = frozendict_append(new_matchings, subword_idx, extra_match)
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
