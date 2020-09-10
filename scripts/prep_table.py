import sys
import pandas as pd


data = pd.read_csv(sys.argv[1])
data = data[data["hits"] >= 50]
data.sort_values("nmi", ascending=False, inplace=True)
print(data)
del data["wiki_defns"]
data.columns = pd.MultiIndex.from_tuples([
    ("X", "Headword"),
    ("X", "Hits"),
    ("Wiktionary", "Frames"),
    ("Wiktionary", "Combs"),
    ("Wiktionary", "Entropy"),
    ("PropBank", "Frames"),
    ("PropBank", "Entropy"),
    ("Agreement", "MI",),
    ("Agreement", "NMI",),
])
print(data.to_latex(
    column_format="lr|rrr|rr|rr",
    multicolumn_format="c",
    index=False,
    float_format="%.2f"
))