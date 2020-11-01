import click
from sqlalchemy import select, func, and_

from lextract.mweproc.models import MweType
from lextract.utils.db import get_connection
from lextract.mweproc.db.confs import setup_dist
from lextract.mweproc.db.tables import tables
from wikiparse.db.insert import insert_metadata


BEGIN = """
\\begin{tabular}{lrr}
\\toprule
 & Number & Proportion \\\\
"""

END = """
\\bottomrule
\\end{tabular}
"""


LINK_NAME_MAP = {
    "wikidefn": "syntactic frames within Wiktionary definitions",
    "wikihw": "extracted from Wiktionary titles",
    "wnhw": "extracted from FinnWordNet titles",
}

TYP_MAP = {
    MweType.inflection: "inflections",
    MweType.multiword: "multiwords",
    MweType.frame: "syntactic frames",
}


def mk_joined():
    return (
        tables["ud_mwe"]
        .outerjoin(tables["link"], tables["link"].c.mwe_id == tables["ud_mwe"].c.id,)
        .outerjoin(
            tables["wiktionary_hw_link"],
            tables["wiktionary_hw_link"].c.mwe_id == tables["ud_mwe"].c.id,
        )
    )


def mwe_typ_group(conn, joined, link_name):
    return conn.execute(
        select([tables["ud_mwe"].c.typ, func.count(tables["ud_mwe"].c.id)])
        .where(tables["link"].c.name == link_name)
        .select_from(joined)
        .group_by(tables["ud_mwe"].c.typ)
    )


def have_head_cnt(conn, joined, link_name):
    return conn.execute(
        select([func.count(tables["ud_mwe"].c.headword_idx)])
        .where(tables["link"].c.name == link_name)
        .select_from(joined)
    ).scalar()


def wiki_hw_group(conn, joined, mwe_typ, grouper):
    return conn.execute(
        select([grouper, func.count(tables["ud_mwe"].c.id)])
        .where(
            and_(tables["link"].c.name == "wikihw", tables["ud_mwe"].c.typ == mwe_typ)
        )
        .select_from(joined)
        .group_by(grouper)
    )


def postpositional_case(conn, joined, *ands):
    return conn.execute(
        select([func.count(tables["ud_mwe"].c.id)])
        .where(
            and_(
                tables["ud_mwe"].c.typ == MweType.inflection,
                func.json_extract(tables["ud_mwe_token"].c.feats, "$.Case").isnot(None),
                func.json_extract(tables["ud_mwe_token"].c.feats, "$.Case") != "Par",
                func.json_extract(tables["ud_mwe_token"].c.feats, "$.Case") != "Gen",
                func.json_extract(tables["ud_mwe_token"].c.feats, "$.Case") != "Nom",
                *ands
            )
        )
        .select_from(
            joined.outerjoin(
                tables["ud_mwe_token"],
                tables["ud_mwe_token"].c.mwe_id == tables["ud_mwe"].c.id,
            )
        )
    ).scalar()


def fmt_row(level, title, cnt, parent_cnt=None, noare=False):
    if level == 0:
        first_col = title
    else:
        first_col = "\\hspace{{{}mm}}\\ldots{{}}of which {}{}".format(
            5 * level - 2, "" if noare else "are ", title
        )
    print(
        "{} & \\num{{{}}} & {} \\\\".format(
            first_col,
            cnt,
            "\\SI{{{:.1f}}}{{\\percent}}".format(cnt / parent_cnt * 100)
            if parent_cnt
            else "",
        )
    )


@click.command()
@click.option("--insert/--no-insert")
def mwesize(insert):
    conn = get_connection()
    setup_dist()
    joined = mk_joined()
    print(BEGIN)
    cnts = {}
    total_cnt = conn.execute(
        select([func.count(tables["ud_mwe"].c.id)]).select_from(tables["ud_mwe"])
    ).scalar()
    fmt_row(0, "Total multiwords", total_cnt)
    cnts["total"] = total_cnt
    link_cnts = conn.execute(
        select([tables["link"].c.name, func.count(tables["ud_mwe"].c.id)])
        .select_from(joined)
        .group_by(tables["link"].c.name)
    )
    for idx, (link_name, src_cnt) in enumerate(link_cnts):
        cnts[link_name] = src_cnt
        print("\\midrule\n")
        fmt_row(1, LINK_NAME_MAP[link_name], src_cnt, total_cnt)
        fmt_row(2, "have head", have_head_cnt(conn, joined, link_name), src_cnt)
        if link_name != "wikidefn":
            for mwe_typ, typ_cnt in mwe_typ_group(conn, joined, link_name):
                fmt_row(2, TYP_MAP[mwe_typ], typ_cnt, src_cnt)
                if link_name == "wikihw":
                    if mwe_typ == MweType.inflection:
                        for has_sense, cnt in wiki_hw_group(
                            conn,
                            joined,
                            mwe_typ,
                            tables["wiktionary_hw_link"].c.has_senses,
                        ):
                            fmt_row(
                                3,
                                "from a page with definitions"
                                if has_sense
                                else "from a page without definitions",
                                cnt,
                                typ_cnt,
                            )
                            if has_sense:
                                pospos_case = postpositional_case(
                                    conn,
                                    joined,
                                    tables["ud_mwe"].c.typ == MweType.inflection,
                                    tables["wiktionary_hw_link"].c.has_senses,
                                )
                                fmt_row(
                                    4, "postpositional case", pospos_case, cnt,
                                )
                    elif mwe_typ == MweType.multiword:
                        for page_exists, cnt in wiki_hw_group(
                            conn,
                            joined,
                            mwe_typ,
                            tables["wiktionary_hw_link"].c.page_exists,
                        ):
                            fmt_row(
                                3,
                                "have a Wiktionary page"
                                if page_exists
                                else "are a redlink",
                                cnt,
                                typ_cnt,
                            )
                elif link_name == "wnhw" and mwe_typ == MweType.inflection:
                    pospos_case = postpositional_case(
                        conn,
                        joined,
                        tables["link"].c.name == "wnhw",
                        tables["ud_mwe"].c.typ == MweType.inflection,
                    )
                    fmt_row(
                        3, "postpositional case", pospos_case, typ_cnt,
                    )
    if insert:
        insert_metadata(
            conn, {"mweproc_" + k: v for k, v in cnts.items()}, table=tables["meta"]
        )
    print(END)


if __name__ == "__main__":
    mwesize()
