from boltons.dictutils import FrozenDict


def frozendict_append(fd, k, v):
    return fd.updated({k: fd.get(k, frozenset()) | frozenset((v,))})


def frozendict_order_insert(fd, new_key, new_value):
    new = []
    inserted = False
    for existing_key, existing_value in fd.items():
        if not inserted and existing_key == new_key:
            inserted = True
            new.append((existing_key, existing_value | frozenset((new_value,))))
            continue
        if not inserted and existing_key > new_key:
            inserted = True
            new.append((new_key, frozenset((new_value,))))
        new.append((existing_key, existing_value))
    if not inserted:
        new.append((new_key, frozenset((new_value,))))
    return FrozenDict(new)
