from lstore.lock_info import Lock_Manager
from lstore.table import Table, Record
from lstore.index import Index

num_transactions = 0

class Transaction:

    def __init__(self):
        """
        Creates a transaction object.
        """
        global num_transactions
        self.id:int = num_transactions
        num_transactions += 1
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
        print(f"RUNNING TRANSACTION {self.id}")
        with open(f"test_transaction_actual_{self.id}.log", 'w') as f:
            for query, args in self.queries:
                if query.__name__ == "insert":
                    f.write(f"{args}\n")

        for query, args in self.queries:
            result = query(*args)
            if query.__name__ == "insert":
                print(f"RUNNING INSERT ON {args[0] - 92106429}")
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

