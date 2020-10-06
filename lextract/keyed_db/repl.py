import sys
import click
from pprint import pprint
from finntk import get_omorfi
from wikiparse.utils.db import get_session

from .extract import extract_toks


@click.command("extract-toks")
def extract_toks_cmd():
    paragraph = sys.stdin.read()
    omorfi = get_omorfi()
    tokenised = omorfi.tokenise(paragraph)
    starts = []
    start = 0
    for token in tokenised:
        start = paragraph.index(token["surf"], start)
        starts.append(start)

    surfs = [tok["surf"] for tok in tokenised]
    session = get_session().get_bind()
    pprint(list(extract_toks(session, surfs)))
