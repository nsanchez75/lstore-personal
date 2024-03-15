from lstore.lock_info import Lock_Manager
from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    def __init__(self):
        """
        Creates a transaction object.
        """
        self.queries:list[tuple] = list() # [(query method, (args))]

    def add_query(self, query, table:Table, *args):
        """
        Adds the given query to this transaction
        
        Example:
        - q = Query(grades_table)
        - t = Transaction()
        - t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
        """
        self.queries.append((query, args))
        # use grades_table for aborting


    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()


    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False


    def commit(self):
        # TODO: commit to database
        return True

