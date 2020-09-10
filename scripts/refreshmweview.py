from lextract.mweproc.db.confs import setup_dist
from lextract.mweproc.db.tables import metadata
from lextract.utils.db import get_connection
from sqlalchemy_utils.view import DropView


def main():
    conn = get_connection()
    setup_dist()
    conn.execute(DropView("joined", cascade=False))
    metadata.create_all(conn)


if __name__ == "__main__":
    main()
