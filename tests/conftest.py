import os
from wikiparse.utils.fst import LazyFst


def pytest_configure(config):
    if "FST_DIR" in os.environ:
        LazyFst.set_fst_dir(os.environ["FST_DIR"])