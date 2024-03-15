from threading import Lock
from collections import defaultdict

from lstore.record_info import RID


class Lock_Manager:

    def __init__(self)->None:
        self.locks:defaultdict[int,RWL] = defaultdict(RWL)

    def get_locks(self)->defaultdict:
        return self.locks

    def acquire_read(self, page_range_index:int)->bool:
        return self.locks[page_range_index].acquire_read()

    def acquire_write(self, page_range_index:int)->bool:
        return self.locks[page_range_index].acquire_write()
    
    def release_read(self, page_range_index:int)->None:
        self.locks[page_range_index].release_read()

    def release_write(self, page_range_index:int)->None:
        self.locks[page_range_index].release_write()


class RWL:

    def __init__(self)->None:
        self.num_readers:int = 0
        self.is_writer:bool  = False
        self.lock:Lock       = Lock()

    def acquire_read(self)->bool:
        self.lock.acquire()
        if self.is_writer:
            self.lock.release()
            return False
        self.num_readers += 1
        self.lock.release()
        return True

    def acquire_write(self)->bool:
        self.lock.acquire()
        if self.is_writer or self.num_readers:
            self.lock.release()
            return False
        self.is_writer = True
        self.lock.release()
        return True

    def release_read(self)->None:
        self.lock.acquire()
        self.num_readers -= 1
        self.lock.release()

    def release_write(self)->None:
        self.lock.acquire()
        self.is_writer = False
        self.lock.release()

LM = Lock_Manager()