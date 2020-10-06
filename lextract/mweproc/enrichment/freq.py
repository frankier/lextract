from wordfreq import word_frequency, zipf_frequency
from ..models import UdMwe
from typing import Optional, Tuple


def headword_freq(mwe: UdMwe) -> Optional[Tuple[float, float]]:
    return (
        (
            word_frequency(mwe.headword.payload, "fi"),
            zipf_frequency(mwe.headword.payload, "fi"),
        )
        if mwe.headword
        else None
    )


def turkudepsearch_propbank_freqs(query: str):
    return 0


def turkudepsearch_propbank_headword_freqs(query: str):
    return 0


def turkudepsearch_freq(query: str):
    return 0


def turkudepsearch_headword_freq(query: str):
    return 0
