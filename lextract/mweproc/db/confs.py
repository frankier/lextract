from .tables import add_base, add_mat, add_freq, add_meta
from .views import add_view


def setup_dist():
    """
    This sets up the database as is recommended for distribution.
    """
    add_base()
    add_freq()
    add_mat()
    add_view()
    add_meta()


def setup_embed():
    """
    This set up the database as is recommended for integration into an
    application.
    """
    add_base()
    add_mat()
