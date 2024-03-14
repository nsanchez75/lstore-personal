from threading import Lock, Condition


class Lock_Manager:

    def __init__(self)->None:
        self.lock_table:dict = dict()


LM = Lock_Manager()