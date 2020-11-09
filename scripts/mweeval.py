import re
import csv
import click
from operator import itemgetter
from itertools import groupby
from heapq import merge
from bitarray import bitarray, frozenbitarray
from numpy import array
from sklearn.metrics import mutual_info_score
from scipy.stats import entropy
import seaborn as sns
import pandas as pd
from matplotlib import pyplot as plt

from lextract.mweproc.db.confs import setup_dist
from lextract.mweproc.db.queries import headword_grouped
from lextract.utils.db import get_connection
from lextract.mweproc.models import MweType
from lextract.vendor.dep_search import dep_searcher


BAD_CHAR_RE = re.compile(r"\.|\-")


def parse_hittoken(hittoken):
    hit_bits = hittoken.split()
    tok_idx = hit_bits[0]
    pb_part = hit_bits[-1]
    pb_frame = "_"
    if pb_part != "_":
        for bit in pb_part.split("|"):
            k, v = bit.split("=", 1)
            if k == "PBSENSE":
                pb_frame = v
    return tok_idx, pb_frame, hit_bits[3] == "VERB"


class objectview(object):
    def __init__(self, **d):
        self.__dict__ = d


def cont_mat(searcher, queries):
    """
    Ugly slow hack alert. A better way would be to give dep-search an OR like
    query and get it to give as back answers as well as a boolean vector of
    whether each term was a hit.
    """

    results = []
    frame_dict = {}
    frame_idx = 0
    print("queries", queries)
    for sid, group in groupby(searcher.multi_query(queries), itemgetter(0)):
        col = []
        for _, idx, hittoken in group:
            tok_idx, pb_frame, is_verb = parse_hittoken(hittoken)
            if not is_verb:
                continue
            col.append(((idx, tok_idx), sid, pb_frame))
            if pb_frame not in frame_dict:
                frame_dict[pb_frame] = frame_idx
                frame_idx += 1
        col.sort()
        results.append(col)

    # Regroup by sent_tok_id and get bit vector of queries
    bitvec_dict = {}
    bitvec_idx = 0
    counters = []
    frame_marginal = [0] * frame_idx
    query_marginal = []
    hits = 0
    for sent_tok_id, group in groupby(
        merge(*results, key=itemgetter(0)), key=itemgetter(0)
    ):
        bitvec = bitarray(len(queries))
        bitvec.setall(0)
        pb_frame = None
        for sent_tok_id, sid, g_pb_frame in group:
            bitvec[sid] = 1
            if pb_frame is None:
                pb_frame = g_pb_frame
            else:
                assert pb_frame == g_pb_frame
        bitvec = frozenbitarray(bitvec)
        if bitvec not in bitvec_dict:
            cur_bitvec_idx = bitvec_idx
            bitvec_dict[bitvec] = bitvec_idx
            bitvec_idx += 1
            counters.append([0] * frame_idx)
            query_marginal.append(0)
        else:
            cur_bitvec_idx = bitvec_dict[bitvec]
        counters[cur_bitvec_idx][frame_dict[pb_frame]] += 1
        frame_marginal[frame_dict[pb_frame]] += 1
        query_marginal[cur_bitvec_idx] += 1
        hits += 1

    # Join results into contingency matrix
    return counters, frame_dict, bitvec_dict, frame_marginal, query_marginal, hits


def extract_queries(group):
    queries = []
    for (payload, id, typ, query, gapped, name,) in group:
        if BAD_CHAR_RE.search(query) or typ not in (MweType.multiword, MweType.frame):
            continue
        queries.append(query)
    return queries


def dump_dist_info(
    key,
    hits,
    frame_dict,
    frame_marginal,
    queries,
    bitvec_dict,
    query_marginal,
    contingency,
    mi,
    nmi,
):
    print("# " + key)
    print("Hits: " + str(hits))
    print("\n".join(frame_dict.keys()))
    print("Frame marginal:", frame_marginal)
    print("  Entropy:", entropy(frame_marginal))
    print("\n".join(queries))
    print("\n".join((repr(bv) for bv in bitvec_dict.keys())))
    print("Query marginal:", query_marginal)
    print("  Entropy:", entropy(query_marginal))
    print("Continguency:")
    print(contingency)
    print("Mutual information:", mi)
    print("Normalised mutual information:", nmi)


def proc_results(propbank_db, results, outf=None, frame_counts=None):
    if outf:
        outf.write(
            "headword,hits,wiki_defns,wiki_queries,wiki_combs,wiki_entropy,frames,frame_entropy,mi,nmi\n"
        )
        outf.flush()
    args = objectview(query_dir="/tmp/", case=False, database=propbank_db,)
    with dep_searcher(args) as searcher:
        for key, group in groupby(results, key=itemgetter(0)):
            if frame_counts and frame_counts[key] < 2:
                print("Skipping", key, "due to lack of frames")
                continue
            group = list(group)
            queries = extract_queries(group)
            if len(queries) < 2:
                print("Skipping", key, "due to lack of queries")
                continue
            (
                counters,
                frame_dict,
                bitvec_dict,
                frame_marginal,
                query_marginal,
                hits,
            ) = cont_mat(searcher, queries)
            if len(counters) < 2:
                print("Skipping", key, "due to lack of results")
                continue
            contingency = array(counters)
            query_entropy = entropy(query_marginal)
            frame_entropy = entropy(frame_marginal)
            mi = mutual_info_score(None, None, contingency=contingency)
            nmi = mi / ((query_entropy + frame_entropy) / 2)
            if outf:
                outf.write(
                    ",".join(
                        (
                            str(x)
                            for x in [
                                key,
                                hits,
                                len(group),
                                len(queries),
                                len(bitvec_dict),
                                query_entropy,
                                frame_counts[key] if frame_counts else 0,
                                frame_entropy,
                                mi,
                                nmi,
                            ]
                        )
                    )
                    + "\n"
                )
                outf.flush()
            else:
                dump_dist_info(
                    key,
                    hits,
                    frame_dict,
                    frame_marginal,
                    queries,
                    bitvec_dict,
                    query_marginal,
                    contingency,
                    mi,
                    nmi,
                )


@click.group()
@click.pass_context
@click.option("--out", type=click.File("w"))
@click.option(
    "--typ", type=click.Choice((mwet.name for mwet in MweType)), multiple=True
)
def mweeval(ctx, out, typ):
    setup_dist()
    ctx.ensure_object(dict)
    ctx.obj["out"] = out
    if typ:
        ctx.obj["typ"] = [MweType[t] for t in typ]
    else:
        ctx.obj["typ"] = None


@mweeval.command()
@click.pass_context
@click.argument("propbank_db")
@click.argument("headword")
def single(ctx, propbank_db, headword):
    conn = get_connection()
    proc_results(
        propbank_db,
        conn.execute(headword_grouped((headword,), typ=ctx.obj["typ"])),
        outf=ctx.obj["out"],
    )


def bitvecs_to_labels(bitvecs, gapped_mwes, headword=None, tidle="~"):
    print("gapped_mwes", gapped_mwes)
    for bitvec in bitvecs:
        bitvec = bitarray(bitvec)
        print("bitvec", bitvec)
        bitvec_mwes = []
        while 1:
            try:
                idx = bitvec.index(True)
            except ValueError:
                break
            bitvec[idx] = False
            print(idx)
            mwe = gapped_mwes[idx]
            if headword:
                mwe = mwe.replace(headword, tidle)
            mwe = mwe.replace("___-", "__-")
            if mwe in bitvec_mwes:
                continue
            bitvec_mwes.append(mwe)
        if len(bitvec_mwes) > 1 and headword is not None and tidle in bitvec_mwes:
            bitvec_mwes.remove(tidle)
        # XXX TODO: Add some general notion of syntactic inclusion
        if (
            headword == "pitää"
            and len([x for x in bitvec_mwes if "__-sta" in x]) > 1
            and tidle + " __-sta" in bitvec_mwes
        ):
            bitvec_mwes.remove(tidle + " __-sta")
        if headword == "pitää" and tidle + " __-ta __-na" in bitvec_mwes:
            if tidle + " __-ta" in bitvec_mwes:
                bitvec_mwes.remove(tidle + " __-ta")
            if tidle + " __-na" in bitvec_mwes:
                bitvec_mwes.remove(tidle + " __-na")
        # XXX: TODO: Should have option to hide (redundant) accusative)
        if headword == "pitää" and tidle + " __-ut" in bitvec_mwes:
            bitvec_mwes.remove(tidle + " __-ut")
        # yield ",\n".join([", ".join(tpl) for tpl in chunked(bitvec_mwes, 2)])
        yield ", ".join(bitvec_mwes)


@mweeval.command()
@click.pass_context
@click.argument("propbank_db")
@click.argument("headword")
@click.argument("figout", type=click.Path(), required=False)
def confmat(ctx, propbank_db, headword, figout):
    conn = get_connection()
    group = conn.execute(headword_grouped((headword,), typ=ctx.obj["typ"]))
    group = list(group)
    gapped_mwes = [tpl[4] for tpl in group]
    print("gapped_mwes", gapped_mwes)
    args = objectview(query_dir="/tmp/", case=False, database=propbank_db,)
    with dep_searcher(args) as searcher:
        queries = extract_queries(group)
        (
            counters,
            frame_dict,
            bitvec_dict,
            frame_marginal,
            query_marginal,
            hits,
        ) = cont_mat(searcher, queries)
        contingency = array(counters)
        print("contingency", contingency)
        # XXX: Here
        df = pd.DataFrame(contingency)
        df.columns = list(frame_dict.keys())
        print("gapped_mwes", gapped_mwes)
        bitvec_labels = list(
            bitvecs_to_labels(
                bitvec_dict.keys(),
                gapped_mwes,
                headword,
                "\\~{}" if figout and figout.endswith(".pgf") else "~",
            )
        )
        print("bitvec_labels", bitvec_labels)
        df.index = bitvec_labels
        print(df)
        if figout:
            plt.figure(figsize=(5, 3.4))
        sns.set(font_scale=0.5)
        sns.heatmap(df, annot=True, fmt="d", cmap="YlGnBu")
        plt.xticks(rotation=90)
        plt.yticks(rotation=0, ha="right")
        if figout:
            plt.tight_layout()
            plt.savefig(figout)
        else:
            plt.show()


@mweeval.command()
@click.pass_context
@click.argument("propbank_db")
@click.argument("propbank_tsv", type=click.File("r"))
def propbank(ctx, propbank_db, propbank_tsv):
    headwords = {
        key: len(list(group))
        for key, group in groupby(
            (row[0] for row in csv.reader(propbank_tsv, delimiter="\t"))
        )
    }
    conn = get_connection()
    proc_results(
        propbank_db,
        conn.execute(headword_grouped(headwords.keys(), typ=ctx.obj["typ"])),
        outf=ctx.obj["out"],
        frame_counts=headwords,
    )


@mweeval.command()
@click.pass_context
@click.argument("propbank_db")
def db(ctx, propbank_db):
    conn = get_connection()
    proc_results(
        propbank_db,
        conn.execute(headword_grouped(typ=ctx.obj["typ"])),
        outf=ctx.obj["out"],
    )


if __name__ == "__main__":
    mweeval()
