## Environment variables
from os.path import join as pjoin

def cnf(name, val):
    globals()[name] = config.setdefault(name, val)

# Intermediate dirs
cnf("WORK", "work")
cnf("EVALWORK", WORK + "/eval")
cnf("EVALDB", EVALWORK + "/mwedbwikiframe.db")
cnf("EVALF", EVALWORK + "/eval.txt")


# Input
PROPBANKDB = config["PROPBANKDB"]
WIKIDB = config["WIKIDB"]
FSTS_DIR = config["FSTS_DIR"]

rule all:
    input:
        EVALF

rule mkmwedb:
    input:
        indb = WIKIDB,
        fsts_dir = FSTS_DIR
    output:
        outdb = EVALDB
    shell:
        "mkdir -p " + EVALWORK +
        " && WIKIPARSE_URL=sqlite:///{input.indb}" +
        " DATABASE_URL=sqlite:///{output.outdb}" +
        " python scripts/mkmwedb.py" +
        " --skip-freqs" +
        " --fsts-dir {input.fsts_dir}" +
        " > " + EVALWORK + "/mwedbwikiframe.log"


rule eval:
    input:
        db = EVALDB
    output:
        eval = EVALF
    shell:
        "DATABASE_URL=sqlite:///{input.db}" +
        " python scripts/mweeval.py " + PROPBANKDB + " > {output.eval}"
