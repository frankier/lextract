import pickle
import json
import importlib
from contextlib import contextmanager

from dep_search.query import get_query_mod


def query_from_db(q_obj, args, db, fdb, set_id_db, comp_dict):
    # init the dbs
    q_obj.set_db(set_id_db)

    q = fdb.tree_id_queue
    while True:
        idx = q.get()

        if idx == -1:
            break

        res_set = q_obj.check_tree_id(idx, db)

        if len(res_set) > 0:
            hit = q_obj.get_tree_text(comp_dict)
            tree_lines = hit.split("\n")

            for r in res_set:
                # for r in res_set:
                # print ("# db_tree_id:",idx)
                # print ("# visual-style\t" + str(r + 1) + "\tbgColor:lightgreen")
                # print ("# hittoken:\t"+tree_lines[r])
                # tree_comms = q_obj.get_tree_comms()
                # hit_url=get_url(tree_comms)
                yield idx, tree_lines[r]

    fdb.kill_threads()


class DepSearch:
    def __init__(self, args):
        self.args = args

        # The blob and id database
        with open(args.database + '/db_config.json', 'rt') as inf:
            db_args = json.load(inf)

        with open(args.database + '/comp_dict.pickle','rb') as inf:
            self.comp_dict = pickle.load(inf)

        print("db_args", db_args)

        db_class = importlib.import_module('dep_search.' + db_args['blobdb'])
        self.db = db_class.DB(db_args['dir'])
        self.db.open()

        self.set_id_db = db_class.DB(db_args['dir'])
        self.set_id_db.open(foldername='/set_id_db/')

        # ... and lets load the filter db for fetching the filter list
        self.fdb_class = importlib.import_module('dep_search.' + db_args['filterdb'])
        self.db_args = db_args

    def query(self, search):
        mod, cleanup = get_query_mod(
            self.args.query_dir,
            search,
            self.args.case,
            self.set_id_db,
            self.args.database
        )
        query_obj = mod.GeneratedSearch()
        rarest, c_args_s, s_args_s, c_args_m, s_args_m, just_all_set_ids, types, optional, solr_args, solr_or_groups = query_obj.map_set_id(self.set_id_db)
        fdb = self.fdb_class.Query([], [item[1:] for item in solr_args if item.startswith('!')], solr_or_groups, self.db_args['dir'], self.args.case, query_obj, extra_params={}, langs=[])

        yield from query_from_db(query_obj, self.args, self.db, fdb, self.set_id_db, self.comp_dict)

        cleanup()
        del fdb

    def multi_query(self, queries):
        # Make queries
        for sid, search in enumerate(queries):
            for idx, hittoken in self.query(search):
                yield sid, idx, hittoken

    def close(self):
        del self.db 


@contextmanager
def dep_searcher(*args, **kwargs):
    dep_search = DepSearch(*args, **kwargs)
    yield dep_search
    dep_search.close()
    del dep_search
