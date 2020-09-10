from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Set, Dict, Iterator
from typing_extensions import Protocol


class Link(Protocol):
    link_name: str

    def get_cols(self) -> Dict[str, str]:
        ...


class MweType(Enum):
    lemma = 1
    inflection = 2
    multiword = 3
    frame = 4


@dataclass(eq=True, order=True)
class UdMweToken:
    payload: Optional[str] = None
    payload_is_lemma: bool = True
    # Poses are implicitly OR'd
    poses: Optional[Set[str]] = None
    # Feats can only have a single value
    feats: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if self.payload is not None:
            assert isinstance(self.payload, str)
        if not self.payload_is_lemma:
            assert len(list(self.feats.keys())) == 0


@dataclass(eq=True, order=True)
class UdMwe:
    tokens: List[UdMweToken]
    typ: MweType
    poses: Optional[Set[str]] = None
    headword_idx: Optional[int] = None
    links: List[Link] = field(compare=False, default_factory=list)

    @property
    def has_headword(self) -> bool:
        return self.headword_idx is not None

    @property
    def headword(self) -> Optional[UdMweToken]:
        if self.headword_idx is None:
            return None
        return self.tokens[self.headword_idx]

    def non_headwords(self) -> Iterator[UdMweToken]:
        assert self.headword_idx is not None
        for idx, token in enumerate(self.tokens):
            if idx == self.headword_idx:
                continue
            yield token
