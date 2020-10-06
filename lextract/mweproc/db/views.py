from sqlalchemy_utils.view import create_view


def add_view():
    from .queries import joined_mat_query
    from .tables import metadata, tables

    tables["joined"] = create_view("joined", joined_mat_query(), metadata)
    # create_view('joined_filtered', joined_frames(), metadata)
