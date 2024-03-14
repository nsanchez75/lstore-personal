from threading import Thread

from lstore.transaction import Transaction
from lstore.table import Table, Record
from lstore.index import Index


threads:list[Thread] = list()


class TransactionWorker:

    def __init__(self, transactions:list = [])->None:
        """
        Creates a transaction worker object.
        """
        self.transactions:list[Transaction] = transactions
        self.stats:list[bool]               = list()
        self.result:int                     = 0
        self.thread:Thread                  = None


    def add_transaction(self, t:Transaction):
        """
        Appends t to transactions
        """
        self.transactions.append(t)


    def run(self):
        """
        Runs all transaction as a thread
        """
        thread = Thread(target=self.__run, args=())
        self.thread = thread
        threads.append(thread)
        thread.start()


    def join(self):
        """
        Waits for the worker to finish.
        """
        self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
